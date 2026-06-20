mod common;

use br_notifier_contract::{DeliverNotification, RelativeLink};
use br_test_harness::BareFabricNats;
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

    // when: a fully-populated deliver command is published on the deliver coords
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
async fn s08_nothing_consumes_the_legacy_subject() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    ctx.stack
        .publish_dead_subject(
            LEGACY_SUBJECT,
            &serde_json::to_vec(&json!({
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

    // then: GraphQL — the already-served recipient gets exactly one push
    first_recipient_sub
        .expect_event("the initial NotificationAdded", SSE_TIMEOUT)
        .await;
    first_recipient_sub
        .expect_silence("no duplicate push on redelivery", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s15_service_fails_loud_when_the_command_stream_is_absent() {
    // given: a broker with INTEGRATION_EVT but NO INTEGRATION_CMD — the lib
    // never auto-provisions a stream
    let bare = BareFabricNats::with_only_event_stream().await;

    // when: a real svc-notifier is spawned against it (intake enabled)
    let boot = spawn_against_bare_broker(&bare.url()).await;

    // then: it fails loud — the process exits non-zero and never serves /readyz
    // (binding the deliver consumer against the missing fixed stream errors out,
    // so readiness is never set and the service does not come up)
    assert!(
        !boot.outcome.is_ready(),
        "service must NOT become ready without INTEGRATION_CMD; logs:\n{}",
        boot.logs
    );
    if let Some(status) = boot.outcome.exit_status() {
        assert!(
            !status.success(),
            "service must exit non-zero when the command stream is absent; logs:\n{}",
            boot.logs
        );
    }

    bare.shutdown().await;
}
