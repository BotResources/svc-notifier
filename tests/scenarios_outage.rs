mod common;

use common::*;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

// How many broker-counted redeliveries a scenario waits for before it calls the
// hold "unbounded". The number itself is arbitrary — the property is structural:
// the Fabric consumer is configured `max_deliver: -1`, so JetStream never gives
// up on a frame the service keeps NAKing, and that configuration is pinned by
// the Fabric's own conformance battery, not by this suite. Any value above the
// five-delivery budget the intake used to enforce demonstrates the budget is
// gone; the assertion reads JetStream's own counter, so the service cannot
// satisfy it by logging more often.
const REDELIVERIES_PROVING_AN_UNBOUNDED_HOLD: usize = 5;
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

    // given: nothing has failed yet, and the streak gauge already says so — it
    // reads zero rather than being absent. An alert built on a series that only
    // appears with the first failure cannot tell "healthy" from "not reporting",
    // so the series has to exist before the incident does.
    assert_eq!(
        ctx.instance
            .metric(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await,
        Some(0.0),
        "the held-command gauge must be published from startup, not minted by the first \
         failure"
    );

    // given: a live subscriber is already listening when the outage starts — the
    // never-lose promise is "the recipient is told", not "the row exists"
    let mut session = ctx.instance.subscribe(&make_passport(recipient)).await;

    // given: the database is frozen when the command is delivered, and stays
    // frozen. The container is paused, so the connections stay open and the
    // writes block and time out — the shape a real storage stall takes, not a
    // torn-down socket.
    let paused = PausedPostgres::pause();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "outlives_the_budget", json!({})))
        .await;

    // when: JetStream's own redelivery counter passes the retired five-delivery
    // budget — the old intake terminated the frame here and lost the request.
    // The counter comes from the broker, not from our loop, so it cannot be
    // satisfied by the service merely logging more often.
    let redelivered_past_the_budget = br_test_harness::wait_until(OUTAGE_TIMEOUT, || async {
        ctx.instance.max_delivered_count() > REDELIVERIES_PROVING_AN_UNBOUNDED_HOLD as i64
    })
    .await;
    assert!(
        redelivered_past_the_budget,
        "JetStream must redeliver past the retired budget (highest delivered_count seen: {}), logs:\n{}",
        ctx.instance.max_delivered_count(),
        ctx.instance.logs()
    );
    assert!(
        ctx.instance.log_hits(STORAGE_HELD_LOG_MARKER) >= OUTAGE_ALERT_THRESHOLD,
        "every redelivery must be held, not dropped"
    );

    // then: the outage is visible to operators on /metrics — and NOT on
    // readiness. Readiness DOWN would pull the pod out of the Service endpoints
    // and cut the queries and subscriptions that are still perfectly healthy,
    // turning a write-side outage into a total one across every replica sharing
    // this database. The condition is alertable instead: the streak gauge is
    // rising and every held redelivery is counted.
    let streak_is_visible = ctx
        .stack
        .wait_until(RECOVERY_TIMEOUT, || async {
            ctx.instance
                .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
                .await
                >= OUTAGE_ALERT_THRESHOLD as f64
        })
        .await;
    assert!(
        streak_is_visible,
        "a sustained storage outage must be readable on /metrics as a rising streak, got {}",
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await
    );
    assert!(
        ctx.instance
            .metric_or_zero(TRANSIENT_FAILURES_TOTAL_METRIC, &[])
            .await
            >= OUTAGE_ALERT_THRESHOLD as f64,
        "every held redelivery is counted"
    );
    let (status, body) = ctx.instance.get("/readyz").await;
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "a storage outage must NOT take the pod out of rotation — the read and \
         subscription surface is still healthy: {body}"
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

    // then: the alert clears itself — the streak gauge returns to zero once the
    // write lands, so an operator sees the incident close without acting
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.instance
                    .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
                    .await
                    == 0.0
            })
            .await,
        "the streak gauge must fall back to zero once storage answers again, still at {}",
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await
    );
    assert!(
        ctx.instance
            .metric_or_zero(TRANSIENT_FAILURES_TOTAL_METRIC, &[])
            .await
            > 0.0,
        "the cumulative counter keeps the incident's history — only the gauge resets"
    );
}
