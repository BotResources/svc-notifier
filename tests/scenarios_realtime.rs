// Live-stream scenarios: what a client folding the subscription is guaranteed.
// A push derives from committed state and crosses replicas (s16); the documented
// reconnect order loses nothing across the gap between subscribing and
// snapshotting (s13); and when the service cannot keep a session whole it says
// so and closes it, rather than skipping facts in silence (s25).
mod common;

use std::time::Duration;

use br_test_harness::verdict;
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
    while let Some(raw) = subscription.next_event(SSE_TIMEOUT).await {
        let event = notifier_event(&raw);
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
async fn s16_two_replicas_pushes_derive_from_committed_pg_state() {
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
    let raw = subscriber_on_a
        .expect_event("NotificationAdded across instances", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
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
    verdict::expect_ack(&ack, "notifierMarkAsRead across instances");

    // then: instance A's subscriber observes the read fact
    let raw = subscriber_on_a
        .expect_event("NotificationsRead across instances", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsRead");
    assert_eq!(event["ids"], json!([notification_id]));
}

// The per-recipient broadcast ring the service fans out through
// (`src/realtime.rs`, `broadcast::channel(256)`). A session that falls further
// behind than this has provably lost facts it will never be handed.
const BROADCAST_RING: usize = 256;

// Enough deliveries to overrun that ring *behind* a session that is not reading.
// The margin above the ring covers what the transport absorbs before the back
// pressure reaches the service: bytes already written into the socket buffers
// were served, not lost, so only the surplus piles up in the ring. Each delivery
// carries a heavy payload precisely so that absorption is counted in tens of
// frames instead of thousands, and the flood stays a few hundred commands long.
const OVERFLOWING_DELIVERIES: usize = 3 * BROADCAST_RING;
const HEAVY_PAYLOAD_BYTES: usize = 16 * 1024;

// The flood rides the real intake — one command, one transaction, one announced
// row — so it is the longest `given` in the suite and gets a window to match.
const FLOOD_TIMEOUT: Duration = Duration::from_secs(120);

// The listener announces each committed row on its own, a step behind the
// intake's commit. The scenario must have every announcement *behind* the
// starved session before it starts reading, or it would read a stream that is
// merely still filling as a stream that never lagged.
const FAN_OUT_SETTLE: Duration = Duration::from_secs(5);

fn heavy(index: usize) -> serde_json::Value {
    json!({"index": index, "filler": "x".repeat(HEAVY_PAYLOAD_BYTES)})
}

#[tokio::test]
#[serial_test::serial]
async fn s25_a_subscriber_that_falls_behind_is_cut_off_by_a_verdict_it_can_rebuild_from() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    // given: a live session, proven live — it is served one delivery before it
    // stops reading. Without that proof, a flood piling up behind a session the
    // service never registered would look exactly like a lag.
    let mut starved = ctx.instance.open_unread_events(&passport).await;
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "before_the_flood", json!({})))
        .await;
    let served_before = starved
        .expect_event("the session is live before it is starved", RECOVERY_TIMEOUT)
        .await;
    assert_eq!(
        served_before["data"]["notifierNotificationEvents"]["notification"]["template"],
        "before_the_flood",
        "the starved session must be a real one: {served_before}"
    );

    // when: far more facts than the ring holds are broadcast to that recipient
    // while nobody drains their stream — every one of them created by a real
    // deliver command, never by a seeded row
    for index in 0..OVERFLOWING_DELIVERIES {
        ctx.stack
            .publish_deliver(&deliver(&[recipient], "ring_overflow", heavy(index)))
            .await;
    }
    let expected_rows = OVERFLOWING_DELIVERIES + 1;
    assert!(
        ctx.stack
            .wait_until(FLOOD_TIMEOUT, || async {
                ctx.stack.count_rows().await == expected_rows
            })
            .await,
        "the intake must write every flooded delivery, got {} of {expected_rows}",
        ctx.stack.count_rows().await
    );
    tokio::time::sleep(FAN_OUT_SETTLE).await;

    // then: the stream does not quietly skip what it dropped — it says so, by
    // code, and says how much
    let (served, verdict) = starved
        .expect_verdict("the starved session's terminal frame", SSE_TIMEOUT)
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&verdict, "a lagged subscription"),
        "INVALID_STATE",
        "a truncated session is unusable state, and the client must read it as a code: {verdict}"
    );
    assert_eq!(
        verdict["errors"][0]["extensions"]["reason"], "subscription_lagged",
        "the client keys on the stable reason code, never on the message: {verdict}"
    );
    let lost: usize = verdict["errors"][0]["extensions"]["params"]["lost_events"]
        .as_str()
        .unwrap_or_else(|| panic!("the verdict must carry how much was lost: {verdict}"))
        .parse()
        .unwrap_or_else(|_| panic!("lost_events must be a count: {verdict}"));
    assert!(
        lost > 0,
        "a lag verdict claiming nothing was lost is not a lag: {verdict}"
    );
    assert!(
        verdict["data"].is_null(),
        "a verdict frame carries no event to fold: {verdict}"
    );
    // the arithmetic the client is handed has to add up: everything this session
    // was ever served, plus everything the verdict admits it lost, cannot exceed
    // what actually happened
    let served_in_total = served + 1;
    assert!(
        served_in_total + lost <= expected_rows,
        "the client is told what happened, not more: {served_in_total} served + {lost} lost, \
         but only {expected_rows} facts ever existed"
    );

    // then: and the session is closed, not left dangling — a client must not go
    // on folding a stream that admits it is incomplete
    starved
        .expect_end("the lagged session after its verdict", SSE_TIMEOUT)
        .await;

    // then: the documented recovery works — resubscribe first, then resnapshot.
    // s13 pins that order across a gap; here it is across a disconnection, and
    // the rebuilt state must be complete, not merely non-empty.
    let mut rebuilt_session = ctx.instance.subscribe(&passport).await;
    let listed = listed_ids(&ctx.instance, &passport).await;
    let mut rebuilt = listed.clone();
    rebuilt.sort();
    rebuilt.dedup();
    assert_eq!(
        rebuilt.len(),
        listed.len(),
        "the cursor walk serves no notification twice"
    );
    let mut expected: Vec<String> = ctx
        .stack
        .rows_for(recipient)
        .await
        .iter()
        .map(|row| row.id.to_string())
        .collect();
    expected.sort();
    assert_eq!(
        rebuilt, expected,
        "a client that resnapshots after a lag misses nothing — that is what makes the \
         disconnection safe"
    );
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(
        ServiceInstance::unread_count(&count),
        expected_rows as i64,
        "the badge is exact after the disconnection, not approximate"
    );

    // then: the recipient is still reachable live. Their channel was dropped
    // with the lagged session — an idle recipient's channel is evicted, not
    // kept for the life of the process — and subscribing again must rebuild it,
    // not leave the recipient permanently silent.
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "after_the_rebuild", json!({})))
        .await;
    let raw = rebuilt_session
        .expect_event("NotificationAdded on the rebuilt session", RECOVERY_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationAdded");
    assert_eq!(
        event["notification"]["template"], "after_the_rebuild",
        "a recipient whose session was cut off is not a recipient the service stops serving"
    );
}
