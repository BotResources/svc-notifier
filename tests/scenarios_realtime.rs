// Realtime behavior scenarios: the reconnect contract, and the proof that
// pushes derive from committed PG state — not from the memory of the process
// that handled the write.
mod common;

use common::*;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn s13_subscribe_then_snapshot_loses_nothing_across_the_gap() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    // given: the documented reconnect order — subscription first
    let mut subscription = ctx.instance.subscribe(&passport).await;

    // when: a notification lands between subscription open and snapshot query
    let command = deliver(&[recipient], "in_the_gap", json!({}));
    ctx.stack.publish_deliver(&command).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await
    );
    let row_id = ctx.stack.notification_rows().await[0].id.to_string();

    let snapshot = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    let snapshot_ids: Vec<String> = snapshot["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {snapshot}"))
        .iter()
        .map(|node| node["id"].as_str().unwrap().to_string())
        .collect();

    let mut event_ids = Vec::new();
    while let Some(event) = subscription.next_event(SSE_TIMEOUT).await {
        if event["__typename"] == "NotificationAdded" {
            event_ids.push(event["notification"]["id"].as_str().unwrap().to_string());
        }
    }

    // then: the notification is observed at least once (event, snapshot or
    // both), and folding by id yields exactly one notification
    let mut folded: Vec<&String> = snapshot_ids.iter().chain(event_ids.iter()).collect();
    folded.sort();
    folded.dedup();
    assert_eq!(
        folded,
        vec![&row_id],
        "snapshot ∪ events must contain the gap notification exactly once after id-dedup \
         (snapshot: {snapshot_ids:?}, events: {event_ids:?})"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s15_two_replicas_pushes_derive_from_committed_pg_state() {
    let stack = TestStack::up().await;

    // given: instance A serves GraphQL only (no intake), instance B owns the
    // NATS consumer — same Postgres, same NATS
    let instance_a = stack.spawn_instance(false).await;
    let instance_b = stack.spawn_instance(true).await;

    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let mut subscriber_on_a = instance_a.subscribe(&passport).await;

    // when: the deliver command is consumed by instance B
    let command = deliver(&[recipient], "cross_instance", json!({}));
    stack.publish_deliver(&command).await;
    assert!(
        stack
            .wait_until(RECOVERY_TIMEOUT, || async { stack.count_rows().await == 1 })
            .await,
        "instance B must consume the command"
    );

    // then: the subscriber connected to instance A receives the push
    let event = subscriber_on_a
        .expect_event("NotificationAdded across instances")
        .await;
    assert_eq!(event["__typename"], "NotificationAdded");
    assert_eq!(event["notification"]["template"], "cross_instance");
    let notification_id = event["notification"]["id"].as_str().unwrap().to_string();

    // when: the recipient marks it read through instance B's GraphQL
    let ack = instance_b
        .graphql(
            &passport,
            "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) }",
            json!({"id": notification_id}),
        )
        .await;
    assert_eq!(ack["data"]["notifierMarkAsRead"], true, "{ack}");

    // then: instance A's subscriber observes the read fact
    let event = subscriber_on_a
        .expect_event("NotificationsRead across instances")
        .await;
    assert_eq!(event["__typename"], "NotificationsRead");
    assert_eq!(event["ids"], json!([notification_id]));
}
