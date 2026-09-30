// Outage scenarios: delivery is reliable. A short storage failure delays a
// delivery, it never loses it, and it never doubles it.
mod common;

use common::*;
use serde_json::json;
use uuid::Uuid;

// registry test 01a0f2a9-06b4-76e7-abd2-c363e4cf3ced
// A short storage outage delays a delivery, never loses it
//
// Given the storage is down when a delivery request arrives.
// When the storage comes back, the person gets the notification. The person gets
// it once. Nobody sends the request again.
#[tokio::test]
#[serial_test::serial]
async fn a_short_storage_outage_delays_a_delivery_never_loses_it() {
    let ctx = TestContext::setup().await;
    let person = Uuid::now_v7();

    // Given the storage is down when the delivery request arrives
    let paused = PausedPostgres::pause();
    ctx.stack
        .publish_deliver(&deliver(&[person], "survives_outage", json!({})))
        .await;
    // and the service has received it while the storage is still frozen — the
    // request really met the outage, it did not simply arrive after it
    ctx.stack.wait_commands_received(1).await;

    // When the storage comes back
    drop(paused);

    // Then the person gets the notification, without any new request
    ctx.stack
        .eventually(
            "the notification is stored once the storage is back",
            || async { ctx.stack.rows_for(person).await.len() == 1 },
        )
        .await;

    // And she gets it once: the count is still one after a quiet window in which
    // the test sent nothing
    tokio::time::sleep(QUIET_WINDOW).await;
    assert_eq!(ctx.stack.rows_for(person).await.len(), 1);

    // And a stream opened now starts with exactly that notification
    let session = Session::open(&ctx.instance, &make_passport(person)).await;
    assert_eq!(session.first_payload().len(), 1);
    assert_eq!(session.first_payload()[0]["template"], "survives_outage");
}
