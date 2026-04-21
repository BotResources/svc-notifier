mod common;

use serde_json::json;
use uuid::Uuid;

/// Time to wait for the NATS consumer to process a message.
const CONSUME_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

#[tokio::test]
#[serial_test::serial]
async fn single_recipient_creates_notification() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let source = Uuid::now_v7();
    let recipient = Uuid::now_v7();

    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": source,
            "recipient_ids": [recipient],
            "template": "test_template",
            "payload": {"key": "value"}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    assert_eq!(ctx.count_notifications().await, 1);
    assert_eq!(ctx.count_notifications_for(recipient).await, 1);
}

#[tokio::test]
#[serial_test::serial]
async fn multi_recipient_creates_separate_notifications() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let source = Uuid::now_v7();
    let r1 = Uuid::now_v7();
    let r2 = Uuid::now_v7();
    let r3 = Uuid::now_v7();

    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": source,
            "recipient_ids": [r1, r2, r3],
            "template": "multi",
            "payload": {}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    assert_eq!(ctx.count_notifications().await, 3);
    assert_eq!(ctx.count_notifications_for(r1).await, 1);
    assert_eq!(ctx.count_notifications_for(r2).await, 1);
    assert_eq!(ctx.count_notifications_for(r3).await, 1);
}

#[tokio::test]
#[serial_test::serial]
async fn idempotent_same_source_and_recipient() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let source = Uuid::now_v7();
    let recipient = Uuid::now_v7();
    let msg = json!({
        "source_event_id": source,
        "recipient_ids": [recipient],
        "template": "dup_test",
        "payload": {}
    });

    // Publish twice with same source_event_id + recipient_id
    ctx.nats_publish("notify.deliver", &msg).await;
    tokio::time::sleep(CONSUME_WAIT).await;
    ctx.nats_publish("notify.deliver", &msg).await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // Should still be only 1 notification
    assert_eq!(ctx.count_notifications().await, 1);
}

#[tokio::test]
#[serial_test::serial]
async fn same_source_different_recipients_creates_separate() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let source = Uuid::now_v7();
    let r1 = Uuid::now_v7();
    let r2 = Uuid::now_v7();

    // Two separate messages, same source, different recipients
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": source,
            "recipient_ids": [r1],
            "template": "t",
            "payload": {}
        }),
    )
    .await;
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": source,
            "recipient_ids": [r2],
            "template": "t",
            "payload": {}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    assert_eq!(ctx.count_notifications().await, 2);
}

#[tokio::test]
#[serial_test::serial]
async fn empty_recipient_ids_is_acked() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [],
            "template": "empty",
            "payload": {}
        }),
    )
    .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    // No notification created, but message was acked (not stuck in retry)
    assert_eq!(ctx.count_notifications().await, 0);
}

#[tokio::test]
#[serial_test::serial]
async fn malformed_message_is_acked() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    // Missing required fields — malformed
    ctx.nats_publish("notify.deliver", &json!({"garbage": true}))
        .await;

    tokio::time::sleep(CONSUME_WAIT).await;

    // No notification created, message was acked (not retried forever)
    assert_eq!(ctx.count_notifications().await, 0);
}
