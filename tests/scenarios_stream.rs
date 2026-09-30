// Stream scenarios: one subscription is all a client needs. Its first payload is
// the whole list, newest first, and that payload and the deltas after it leave
// no gap — nothing is lost and nothing appears twice.
mod common;

use std::collections::BTreeSet;

use common::*;
use serde_json::{Value, json};
use uuid::Uuid;

// registry test 01a0f2a9-0e11-76b6-87cf-40f21a063bec
// A new stream starts with the whole list
//
// Given a person who has 25 notifications, some of them read.
// When the person subscribes, the first payload holds all 25, newest first.
// While the person is away, one notification is read and one is deleted.
// When the person subscribes again, the first payload holds the updated full list.
#[tokio::test]
#[serial_test::serial]
async fn a_new_stream_starts_with_the_whole_list() {
    let ctx = TestContext::setup().await;
    let person = Uuid::now_v7();
    let passport = make_passport(person);

    // Given 25 notifications, delivered one after the other so that every
    // creation time is different
    let mut ids = Vec::new();
    for n in 1..=25 {
        let stored = deliver_to(&ctx, person, &format!("n{n:02}"), json!({ "n": n })).await;
        ids.push(stored.id);
    }

    // And some of them read, by the person herself
    let read_first = [2, 7, 12, 17, 22];
    for index in read_first {
        let response = ctx
            .instance
            .graphql(
                &passport,
                MARK_AS_READ,
                json!({ "id": ids[index].to_string() }),
            )
            .await;
        expect_ack_only(&response, "notifierMarkAsRead");
    }
    ctx.stack
        .eventually("the five read times are stored", || async {
            let rows = ctx.stack.rows_for(person).await;
            rows.iter().filter(|row| row.read_at.is_some()).count() == 5
        })
        .await;

    // When she subscribes, the first payload holds all 25, newest first, and
    // exactly the five she read carry a read time
    let session = Session::open(&ctx.instance, &passport).await;
    let first = session.first_payload();
    assert_eq!(first.len(), 25, "the first payload holds the whole list");
    let expected_order: Vec<String> = ids.iter().rev().map(ToString::to_string).collect();
    assert_eq!(id_list(first), expected_order, "newest first");
    for pair in first.windows(2) {
        assert!(instant(&pair[0]["createdAt"]) > instant(&pair[1]["createdAt"]));
    }
    let read_ids: BTreeSet<String> = read_first.iter().map(|i| ids[*i].to_string()).collect();
    let carrying_a_read_time: BTreeSet<String> = first
        .iter()
        .filter(|view| !view["readAt"].is_null())
        .map(|view| view["id"].as_str().expect("id").to_owned())
        .collect();
    assert_eq!(carrying_a_read_time, read_ids, "exactly the five read ones");
    drop(session);

    // While she is away, one unread notification is read and one is deleted
    let (marked, deleted) = (ids[9], ids[19]);
    let response = ctx
        .instance
        .graphql(&passport, MARK_AS_READ, json!({ "id": marked.to_string() }))
        .await;
    expect_ack_only(&response, "notifierMarkAsRead");
    let response = ctx
        .instance
        .graphql(&passport, DELETE_ONE, json!({ "id": deleted.to_string() }))
        .await;
    expect_ack_only(&response, "notifierDeleteNotification");
    ctx.stack
        .eventually("the read and the deletion are stored", || async {
            ctx.stack.row(deleted).await.is_none()
                && ctx
                    .stack
                    .row(marked)
                    .await
                    .is_some_and(|row| row.read_at.is_some())
        })
        .await;

    // When she subscribes again, the first payload holds the updated full list
    let session = Session::open(&ctx.instance, &passport).await;
    let first = session.first_payload();
    assert_eq!(first.len(), 24, "the deleted one is gone");
    let expected_order: Vec<String> = ids
        .iter()
        .rev()
        .filter(|id| **id != deleted)
        .map(ToString::to_string)
        .collect();
    assert_eq!(id_list(first), expected_order, "still newest first");
    let marked_view = first
        .iter()
        .find(|view| view["id"] == json!(marked.to_string()))
        .expect("the notification she read is still in the list");
    assert!(
        !marked_view["readAt"].is_null(),
        "and it now carries its read time"
    );
    let read_now = first
        .iter()
        .filter(|view| !view["readAt"].is_null())
        .count();
    assert_eq!(
        read_now, 6,
        "the five from before and the one read while away"
    );
}

fn id_list(views: &[Value]) -> Vec<String> {
    views
        .iter()
        .map(|view| view["id"].as_str().expect("a string id").to_owned())
        .collect()
}

// registry test 01a0f2a9-1584-7bf2-8dd0-212c8ce4347e
// Nothing is lost or doubled while the stream opens
//
// Given a person who opens the stream while 20 notifications arrive.
// When all the notifications have arrived, the person sees each one once.
// No notification is lost. No notification shows twice.
//
// GAP: the harness gives no control over the interleaving of the intake and the
// subscription, so gap-freedom is only SAMPLED here, never proven. Five rounds
// open the stream at different points of a 20-request burst — when storage holds
// 0, 1, 5, 10 and 19 of the person's notifications — so that the window is hit
// from several sides; the scenario cannot force a given notification to land
// exactly in the gap between the first payload and the deltas.
#[tokio::test]
#[serial_test::serial]
async fn nothing_is_lost_or_doubled_while_the_stream_opens() {
    let ctx = TestContext::setup().await;

    for stored_at_open in [0usize, 1, 5, 10, 19] {
        let person = Uuid::now_v7();
        let passport = make_passport(person);
        let requests: Vec<_> = (0..20)
            .map(|n| deliver(&[person], &format!("burst{n:02}"), json!({ "n": n })))
            .collect();

        // Given the person opens the stream while 20 notifications arrive: the
        // stream opens as soon as storage holds `stored_at_open` of them
        let publisher = async {
            for request in &requests {
                ctx.stack.publish_deliver(request).await;
            }
        };
        let opener = async {
            ctx.stack
                .eventually(
                    &format!("storage holds {stored_at_open} notifications of the burst"),
                    || async { ctx.stack.rows_for(person).await.len() >= stored_at_open },
                )
                .await;
            Session::open(&ctx.instance, &passport).await
        };
        let (_, mut session) = tokio::join!(publisher, opener);

        // When all the notifications have arrived
        ctx.stack
            .eventually("the 20 notifications are stored", || async {
                ctx.stack.rows_for(person).await.len() == 20
            })
            .await;
        session
            .settle("the person holds the 20 notifications", |view| {
                view.len() == 20
            })
            .await;

        // Then she sees each one: none lost, none foreign
        let stored: BTreeSet<String> = ctx
            .stack
            .rows_for(person)
            .await
            .iter()
            .map(|row| row.id.to_string())
            .collect();
        let held: BTreeSet<String> = session.view().keys().cloned().collect();
        assert_eq!(
            held, stored,
            "opened at {stored_at_open} stored: none lost, none foreign"
        );

        // And none twice: the wire never showed an id again inside one window (a
        // whole-window Reset may legally start the stream over, and is not a double)
        assert_eq!(
            session.doubled_ids(),
            Vec::<String>::new(),
            "opened at {stored_at_open} stored: a notification showed twice: {:#?}",
            session.deltas()
        );
    }
}
