mod common;

use common::*;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

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
