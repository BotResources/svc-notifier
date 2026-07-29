mod common;

use common::*;
use reqwest::StatusCode;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

const RETIRED_REDELIVERY_BUDGET: usize = 5;
const OUTAGE_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::test]
#[serial_test::serial]
async fn s07a_short_db_outage_naks_then_recovers() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    // given: the database is down when the command is delivered — the fan-out
    // write fails and the frame is NAKed with a short redelivery delay
    let paused = PausedPostgres::pause();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "survives_outage", json!({})))
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // when: the outage ends
    drop(paused);

    // then: PG — redelivery completes the write; nothing is lost
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "a transient outage must not lose the notification"
    );
    assert_eq!(
        ctx.stack.rows_for(recipient).await.len(),
        1,
        "exactly one row after recovery"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s07b_a_reemit_across_an_outage_delivers_exactly_once() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let command = deliver(&[recipient], "reemit", json!({}));

    // given: the command is delivered during an outage (NAK), then recovers
    let paused = PausedPostgres::pause();
    ctx.stack.publish_deliver(&command).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    drop(paused);

    // when: the producer re-emits the same source event (the documented
    // recovery path — dedup makes it safe)
    ctx.stack.publish_deliver(&command).await;

    // then: PG — exactly one row, no duplicate from the redelivery + re-emit
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "the command must be delivered after recovery"
    );
    tokio::time::sleep(CONSUME_WAIT).await;
    assert_eq!(
        ctx.stack.rows_for(recipient).await.len(),
        1,
        "dedup on (source_event_id, recipient_id) keeps exactly one row"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s07c_an_outage_past_the_retired_budget_still_delivers_exactly_once() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    // given: a live subscriber is already listening when the outage starts — the
    // never-lose promise is "the recipient is told", not "the row exists"
    let mut session = ctx.instance.subscribe(&make_passport(recipient)).await;

    // given: the database is down when the command is delivered, and stays down
    let paused = PausedPostgres::pause();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "outlives_the_budget", json!({})))
        .await;

    // when: JetStream's own redelivery counter passes the retired five-delivery
    // budget — the old intake terminated the frame here and lost the request.
    // The counter comes from the broker, not from our loop, so it cannot be
    // satisfied by the service merely logging more often.
    let redelivered_past_the_budget = br_test_harness::wait_until(OUTAGE_TIMEOUT, || async {
        ctx.instance.max_delivered_count() > RETIRED_REDELIVERY_BUDGET as i64
    })
    .await;
    assert!(
        redelivered_past_the_budget,
        "JetStream must redeliver past the retired budget (highest delivered_count seen: {}), logs:\n{}",
        ctx.instance.max_delivered_count(),
        ctx.instance.logs()
    );
    assert!(
        ctx.instance.log_hits(STORAGE_HELD_LOG_MARKER) >= STORAGE_OUTAGE_ALERT_AFTER,
        "every redelivery must be held, not dropped"
    );

    // then: the outage is visible to operators — readiness is DOWN, the pod
    // leaves rotation instead of silently dropping work
    let (status, body) = ctx.instance.get("/readyz").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a sustained storage outage must surface on /readyz: {body}"
    );

    // when: the outage ends
    drop(paused);

    // then: PG — the notification the old budget would have dropped is written
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "an outage longer than the redelivery budget must not lose the notification"
    );
    tokio::time::sleep(CONSUME_WAIT).await;
    assert_eq!(
        ctx.stack.rows_for(recipient).await.len(),
        1,
        "exactly one row — the redeliveries stay idempotent"
    );

    // then: the live path survived the outage too — the subscriber opened before
    // the pause is told, not just the table written. A pg_notify lost while the
    // listener was reconnecting would leave the row unannounced forever.
    let raw = session
        .expect_event("NotificationAdded after the outage", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationAdded");
    assert_eq!(event["notification"]["template"], "outlives_the_budget");

    // then: a transient outage leaves no dead letter — nothing was abandoned
    assert_eq!(
        ctx.stack.dead_letters().await.len(),
        0,
        "a storage outage is held, never dead-lettered"
    );

    // then: readiness recovers on its own once the write lands
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.instance.get("/readyz").await.0 == StatusCode::OK
            })
            .await,
        "readiness must come back up once storage is reachable again"
    );
}
