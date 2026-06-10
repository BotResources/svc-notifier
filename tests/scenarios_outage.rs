// Database-outage behavior scenarios. These pause the Postgres container —
// they require the docker CLI and unpause even when the scenario panics.
mod common;

use common::*;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn s07a_short_db_outage_naks_then_recovers_within_the_redelivery_budget() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    let paused = PausedPostgres::pause();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "survives_outage", json!({})))
        .await;

    // then: NATS — the failed attempt is NAKed, the message stays in flight
    assert!(
        ctx.stack
            .wait_until(Duration::from_secs(10), || async {
                ctx.stack
                    .consumer_info()
                    .await
                    .is_some_and(|info| info.delivered.consumer_sequence >= 1)
            })
            .await,
        "the consumer must attempt delivery during the outage"
    );

    // when: the outage ends
    drop(paused);

    // then: PG — redelivery completes the write; NATS — drained, within budget
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "a transient outage must not lose the notification"
    );
    let info = ctx
        .stack
        .consumer_info()
        .await
        .expect("durable consumer must exist");
    assert_eq!(
        info.num_ack_pending, 0,
        "the message is acked after the successful retry"
    );
    assert!(
        info.delivered.consumer_sequence <= 5,
        "recovery must fit within max_deliver, got {} deliveries",
        info.delivered.consumer_sequence
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s07b_exhausted_redeliveries_drop_the_command_and_a_reemit_recovers() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let command = deliver(&[recipient], "exhausted", json!({}));

    let paused = PausedPostgres::pause();
    ctx.stack.publish_deliver(&command).await;

    // then: NATS — delivery attempts stop at max_deliver
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack
                    .consumer_info()
                    .await
                    .is_some_and(|info| info.delivered.consumer_sequence >= 5)
            })
            .await,
        "the consumer must exhaust its redelivery budget during a long outage"
    );

    drop(paused);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // then: PG — the command is dropped, not retried forever
    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "an exhausted command is dropped — no late write after recovery"
    );

    // when: the producer re-emits the same source event (the documented
    // recovery path — dedup makes it safe)
    ctx.stack.publish_deliver(&command).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "a re-emitted command must be delivered normally"
    );
    assert_eq!(
        ctx.stack.rows_for(recipient).await.len(),
        1,
        "still exactly one row"
    );
}
