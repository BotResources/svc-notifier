// Delete scenarios: what a recipient does to the existence of her own
// notifications. Deletion is final and only confirms success; the removal
// arrives on the stream of every open session, identified by the notification's
// id, and the storage no longer holds the notification.
mod common;

use std::collections::BTreeSet;

use common::*;
use serde_json::json;
use uuid::Uuid;

// registry test 01a0f2a9-2b0f-7084-b620-b91d6597ac18
// Deleting one reaches every session
//
// Given a person with two open sessions and one notification.
// When the person deletes the notification in the first session, the call only
// confirms success. Both sessions then remove the notification.
// The storage no longer holds it.
#[tokio::test]
#[serial_test::serial]
async fn deleting_one_reaches_every_session() {
    let ctx = TestContext::setup().await;
    let person = Uuid::now_v7();
    let passport = make_passport(person);

    // Given two open sessions, the notification to delete and one to keep
    let doomed = deliver_to(&ctx, person, "to_delete", json!({})).await;
    let kept = deliver_to(&ctx, person, "to_keep", json!({})).await;
    let (n, m) = (doomed.id.to_string(), kept.id.to_string());
    let mut first = Session::open(&ctx.instance, &passport).await;
    let mut second = Session::open(&ctx.instance, &passport).await;
    for session in [&mut first, &mut second] {
        session
            .settle("both are held", |view| view.len() == 2)
            .await;
    }
    let kept_before = first.get(&m).cloned().expect("held");

    // When she deletes the notification
    let response = ctx
        .instance
        .graphql(&passport, DELETE_ONE, json!({ "id": n }))
        .await;

    // Then the call only confirms success
    expect_ack_only(&response, "notifierDeleteNotification");

    // And both sessions remove it and keep the other one, unchanged. The removal
    // is identified by the id of the notification
    for session in [&mut first, &mut second] {
        session
            .settle("the notification is removed", |view| {
                view.len() == 1 && view.contains_key(&m)
            })
            .await;
        assert_eq!(
            session.get(&m),
            Some(&kept_before),
            "the other one is unchanged"
        );
        assert_eq!(
            session.removed_ids(),
            vec![n.clone()],
            "the only removal is the deleted one"
        );
    }

    // And storage no longer holds it, and still holds the other one
    assert!(
        ctx.stack.row(doomed.id).await.is_none(),
        "deletion is final"
    );
    assert!(ctx.stack.row(kept.id).await.is_some());
}

// registry test 01a0f2a9-326d-7a98-975c-9d344cde2db0
// Deleting several ignores what is not yours
//
// Given a person with three notifications, and another person with one
// notification.
// When the first person deletes all four ids in one request, the three own
// notifications are removed. The notification of the other person stays.
// The other person sees no change.
#[tokio::test]
#[serial_test::serial]
async fn deleting_several_ignores_what_is_not_yours() {
    let ctx = TestContext::setup().await;
    let (person, other) = (Uuid::now_v7(), Uuid::now_v7());
    let passport = make_passport(person);

    // Given three notifications for her and one for the other person
    let mut own = Vec::new();
    for template in ["own_1", "own_2", "own_3"] {
        own.push(deliver_to(&ctx, person, template, json!({})).await);
    }
    let foreign = deliver_to(&ctx, other, "not_yours", json!({})).await;
    let mut hers = Session::open(&ctx.instance, &passport).await;
    let mut theirs = Session::open(&ctx.instance, &make_passport(other)).await;
    hers.settle("she holds her three", |view| view.len() == 3)
        .await;
    theirs
        .settle("the other holds hers", |view| view.len() == 1)
        .await;
    let foreign_before = theirs.get(foreign.id).cloned().expect("held");

    // When she deletes all four ids in one request
    let mut ids: Vec<String> = own.iter().map(|n| n.id.to_string()).collect();
    ids.push(foreign.id.to_string());
    let response = ctx
        .instance
        .graphql(&passport, DELETE_MANY, json!({ "ids": ids }))
        .await;

    // Then the request is not refused: it confirms success
    expect_ack_only(&response, "notifierDeleteNotifications");

    // And her three are removed — from her stream, identified by their ids, and
    // from storage
    hers.settle("her three are removed", |view| view.is_empty())
        .await;
    let removed: BTreeSet<String> = hers.removed_ids().into_iter().collect();
    let expected: BTreeSet<String> = own.iter().map(|n| n.id.to_string()).collect();
    assert_eq!(
        removed, expected,
        "she is told of her own removals, never of the foreign one"
    );
    assert!(ctx.stack.rows_for(person).await.is_empty());

    // And the notification of the other person stays, unchanged, and she notices
    // nothing
    assert_eq!(ctx.stack.rows_for(other).await, vec![foreign.clone()]);
    theirs
        .expect_no_delta("the other person is not told of anything")
        .await;
    assert_eq!(theirs.get(foreign.id), Some(&foreign_before));
}
