// Intake behavior scenarios — each pins the three external envelopes:
// NATS (ack/NAK/redelivery, consumer state), PG (exact rows via the
// assertion connection), GraphQL (what a forged Passport observes).
mod common;

use br_notifier_contract::{DELIVER_SUBJECT, DeliverNotification, RelativeLink};
use common::*;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn s01_deliver_command_reaches_the_recipient_on_all_three_envelopes() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let mut subscription = ctx.instance.subscribe(&passport).await;

    // when: a fully-populated deliver command is published on the contract subject
    let mut command = deliver(
        &[recipient],
        "meeting_scheduled",
        json!({"meeting_id": "m-1"}),
    );
    command.link = Some(RelativeLink::parse("/meetings/m-1").unwrap());
    ctx.stack.publish_deliver(&command).await;

    // then: PG — exactly one row, exact content, unread
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "expected exactly 1 notification row"
    );
    let rows = ctx.stack.notification_rows().await;
    assert_eq!(rows[0].source_event_id, command.source_event_id);
    assert_eq!(rows[0].recipient_id, recipient);
    assert_eq!(rows[0].template, "meeting_scheduled");
    assert_eq!(rows[0].payload, json!({"meeting_id": "m-1"}));
    assert_eq!(rows[0].link.as_deref(), Some("/meetings/m-1"));
    assert_eq!(rows[0].read_at, None);

    // then: NATS — message acked, consumer drained, no redelivery
    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(info.num_pending, 0, "message must be consumed");
    assert_eq!(info.num_ack_pending, 0, "message must be acked");
    assert_eq!(info.num_redelivered, 0, "no redelivery on the happy path");

    // then: GraphQL — subscription push, query and unread count agree
    let raw = subscription
        .expect_event("NotificationAdded", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationAdded");
    let pushed = &event["notification"];
    assert_eq!(pushed["id"], json!(rows[0].id));
    assert_eq!(pushed["template"], "meeting_scheduled");
    assert_eq!(pushed["payload"], json!({"meeting_id": "m-1"}));
    assert_eq!(pushed["link"], "/meetings/m-1");

    let listed = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    let nodes = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["link"], "/meetings/m-1");

    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 1);
}

#[tokio::test]
#[serial_test::serial]
async fn s02_multi_recipient_fans_out_one_row_each_and_isolates_subscribers() {
    let ctx = TestContext::setup().await;
    let (alice, bob, carol) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut alice_sub = ctx.instance.subscribe(&make_passport(alice)).await;
    let mut bob_sub = ctx.instance.subscribe(&make_passport(bob)).await;

    ctx.stack
        .publish_deliver(&deliver(&[alice, bob, carol], "fanout", json!({})))
        .await;

    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 3
            })
            .await,
        "expected 3 rows, one per recipient"
    );
    for recipient in [alice, bob, carol] {
        assert_eq!(ctx.stack.rows_for(recipient).await.len(), 1);
    }

    let alice_raw = alice_sub
        .expect_event("alice's NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&alice_raw)["notification"]["template"],
        "fanout"
    );
    let bob_raw = bob_sub
        .expect_event("bob's NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&bob_raw)["notification"]["template"],
        "fanout"
    );
    alice_sub
        .expect_silence("only one event per recipient", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s03_duplicate_source_event_first_wins_even_with_a_different_payload() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let mut subscription = ctx.instance.subscribe(&passport).await;

    let first = deliver(&[recipient], "first", json!({"version": 1}));
    ctx.stack.publish_deliver(&first).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await
    );

    // when: the same source event arrives again with different content
    let duplicate = DeliverNotification {
        source_event_id: first.source_event_id,
        recipient_ids: vec![recipient],
        template: "second".to_string(),
        payload: json!({"version": 2}),
        link: None,
    };
    ctx.stack.publish_deliver(&duplicate).await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: PG — still one row, first content untouched
    let rows = ctx.stack.notification_rows().await;
    assert_eq!(rows.len(), 1, "dedup must keep exactly one row");
    assert_eq!(rows[0].template, "first");
    assert_eq!(rows[0].payload, json!({"version": 1}));

    // then: NATS — both messages acked, nothing in flight
    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(info.num_pending, 0);
    assert_eq!(info.num_ack_pending, 0);

    // then: GraphQL — one notification, one push (no duplicate event)
    let raw = subscription
        .expect_event("the first NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(notifier_event(&raw)["notification"]["template"], "first");
    subscription
        .expect_silence("no push for the deduplicated message", CONSUME_WAIT)
        .await;
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 1);
}

#[tokio::test]
#[serial_test::serial]
async fn s04_malformed_payload_is_acked_without_persistence_or_push() {
    let ctx = TestContext::setup().await;
    let observer = Uuid::now_v7();
    let mut subscription = ctx.instance.subscribe(&make_passport(observer)).await;

    ctx.stack
        .publish_raw(DELIVER_SUBJECT, b"this is not a deliver command".to_vec())
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(info.num_pending, 0, "malformed message must be consumed");
    assert_eq!(
        info.num_ack_pending, 0,
        "malformed message must be acked, not NAKed"
    );
    assert_eq!(
        info.num_redelivered, 0,
        "no redelivery storm on poison messages"
    );
    assert_eq!(ctx.stack.count_rows().await, 0);
    subscription
        .expect_silence("nothing reaches GraphQL", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s05_invalid_link_rejects_the_whole_message_fail_closed() {
    let ctx = TestContext::setup().await;
    let (alice, bob) = (Uuid::now_v7(), Uuid::now_v7());
    let mut alice_sub = ctx.instance.subscribe(&make_passport(alice)).await;

    for unsafe_link in ["https://evil.com", "//evil.com", "javascript:alert(1)"] {
        let raw = json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [alice, bob],
            "template": "tempting",
            "payload": {},
            "link": unsafe_link,
        });
        ctx.stack
            .publish_raw(DELIVER_SUBJECT, serde_json::to_vec(&raw).unwrap())
            .await;
    }
    tokio::time::sleep(CONSUME_WAIT).await;

    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(
        info.num_pending, 0,
        "rejected messages must still be consumed"
    );
    assert_eq!(info.num_ack_pending, 0, "rejected messages must be acked");
    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "no partial fan-out: zero rows for any recipient"
    );
    alice_sub
        .expect_silence("nothing reaches any recipient", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s08_nothing_consumes_the_legacy_subject() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    ctx.stack
        .publish_raw(
            LEGACY_SUBJECT,
            serde_json::to_vec(&json!({
                "source_event_id": Uuid::now_v7(),
                "recipient_ids": [recipient],
                "template": "legacy",
                "payload": {},
            }))
            .unwrap(),
        )
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "a legacy notify.deliver message must not be consumed"
    );
    assert_eq!(
        ctx.stack.stream_message_count().await,
        1,
        "the message sits unconsumed in the stream"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s14_redelivery_of_a_partially_applied_batch_completes_without_duplicates() {
    let ctx = TestContext::setup().await;
    let recipients: Vec<Uuid> = (0..5).map(|_| Uuid::now_v7()).collect();
    let passport = make_passport(recipients[0]);
    let mut first_recipient_sub = ctx.instance.subscribe(&passport).await;

    // given: the batch was partially applied (two recipients already inserted)
    let source_event_id = Uuid::now_v7();
    let partial = DeliverNotification {
        source_event_id,
        recipient_ids: recipients[..2].to_vec(),
        template: "batch".to_string(),
        payload: json!({}),
        link: None,
    };
    ctx.stack.publish_deliver(&partial).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 2
            })
            .await
    );
    let first_created_at = ctx.stack.rows_for(recipients[0]).await[0].created_at;

    // when: the full batch is delivered again (redelivery semantics)
    let full = DeliverNotification {
        source_event_id,
        recipient_ids: recipients.clone(),
        template: "batch".to_string(),
        payload: json!({}),
        link: None,
    };
    ctx.stack.publish_deliver(&full).await;

    // then: PG — exactly five rows, the pre-existing ones untouched
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 5
            })
            .await,
        "redelivery must complete the remaining recipients"
    );
    for recipient in &recipients {
        assert_eq!(
            ctx.stack.rows_for(*recipient).await.len(),
            1,
            "zero duplicates"
        );
    }
    assert_eq!(
        ctx.stack.rows_for(recipients[0]).await[0].created_at,
        first_created_at,
        "already-inserted rows are not rewritten"
    );

    // then: NATS — everything acked
    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(info.num_pending, 0);
    assert_eq!(info.num_ack_pending, 0);

    // then: GraphQL — the already-served recipient gets exactly one push
    first_recipient_sub
        .expect_event("the initial NotificationAdded", SSE_TIMEOUT)
        .await;
    first_recipient_sub
        .expect_silence("no duplicate push on redelivery", CONSUME_WAIT)
        .await;
}
