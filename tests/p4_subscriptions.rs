mod common;

use br_service_core::passport_header::PassportHeader;
use serde_json::json;
use uuid::Uuid;

const CONSUME_WAIT: std::time::Duration = std::time::Duration::from_secs(3);
const SSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Read SSE events from a reqwest streaming response body.
/// Returns the first `event: next` data payload parsed as JSON, or None on timeout.
async fn read_next_sse_event(
    mut stream: impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
) -> Option<serde_json::Value> {
    use futures::StreamExt;

    let mut buffer = String::new();

    let deadline = tokio::time::Instant::now() + SSE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }

        let chunk = match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(bytes))) => String::from_utf8_lossy(&bytes).to_string(),
            _ => return None,
        };

        buffer.push_str(&chunk);

        // Parse SSE protocol: look for "event: next\ndata: {...}\n\n"
        while let Some(block_end) = buffer.find("\n\n") {
            let block = buffer[..block_end].to_string();
            buffer = buffer[block_end + 2..].to_string();

            let mut event_type = None;
            let mut data = None;

            for line in block.lines() {
                if let Some(val) = line.strip_prefix("event:") {
                    event_type = Some(val.trim().to_string());
                } else if let Some(val) = line.strip_prefix("data:") {
                    data = Some(val.trim().to_string());
                }
            }

            if event_type.as_deref() == Some("next") {
                if let Some(data_str) = data {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data_str) {
                        return Some(parsed);
                    }
                }
            }
        }
    }
}

#[tokio::test]
#[serial_test::serial]
async fn sse_subscription_receives_notification() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    // Open SSE stream
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/graphql", ctx.base_url))
        .header("X-Passport", passport.to_header())
        .header("Accept", "text/event-stream")
        .json(&json!({
            "query": "subscription { notifierNotificationAdded { id template payload } }"
        }))
        .send()
        .await
        .expect("SSE request failed");

    assert!(resp.status().is_success(), "SSE request returned {}", resp.status());

    let stream = resp.bytes_stream();

    // Publish a notification via NATS
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [user_id],
            "template": "sse_test",
            "payload": {"key": "value"}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    // Read the SSE event
    let event: Option<serde_json::Value> = read_next_sse_event(stream).await;
    assert!(event.is_some(), "expected an SSE event but got none");

    let data = event.unwrap();
    let notif = &data["data"]["notifierNotificationAdded"];
    assert_eq!(notif["template"], "sse_test");
    assert_eq!(notif["payload"]["key"], "value");
}

#[tokio::test]
#[serial_test::serial]
async fn sse_subscription_user_isolation() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();
    let alice_passport = common::TestContext::make_passport(alice, false);

    // Alice opens SSE stream
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/graphql", ctx.base_url))
        .header("X-Passport", alice_passport.to_header())
        .header("Accept", "text/event-stream")
        .json(&json!({
            "query": "subscription { notifierNotificationAdded { id template } }"
        }))
        .send()
        .await
        .expect("SSE request failed");

    assert!(
        resp.status().is_success(),
        "SSE subscription request failed with status {}",
        resp.status()
    );

    let stream = resp.bytes_stream();

    // Publish a notification for Bob (not Alice)
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [bob],
            "template": "for_bob_only",
            "payload": {}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    // Alice should NOT receive anything (timeout)
    let event: Option<serde_json::Value> = read_next_sse_event(stream).await;
    assert!(event.is_none(), "Alice received an event meant for Bob: {event:?}");
}

#[tokio::test]
#[serial_test::serial]
async fn sse_without_passport_returns_401() {
    let ctx = common::TestContext::setup().await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/graphql", ctx.base_url))
        .header("Accept", "text/event-stream")
        .json(&json!({
            "query": "subscription { notifierNotificationAdded { id } }"
        }))
        .send()
        .await
        .expect("request failed");

    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
}
