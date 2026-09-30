// Impersonation scenario: a notification belongs to the person who is acting.
// While an administrator acts as another person, every surface — list, stream,
// mark, delete — stays on the administrator's own notifications and never
// reaches those of the person acted as.
mod common;

use common::*;
use serde_json::{Value, json};
use uuid::Uuid;

// registry test 01a0f2a9-48cb-73c3-8a24-cf39ccf42a6c
// Acting as someone gives no access to their notifications
//
// Given an administrator with one notification, and a person named Sam with one
// unread notification.
// When the administrator acts as Sam, the list and the stream show only the
// notification of the administrator.
// The administrator cannot mark the notification of Sam as read. The administrator
// cannot delete it.
// When Sam looks later, the notification is there and it is still unread.
#[tokio::test]
#[serial_test::serial]
async fn acting_as_someone_gives_no_access_to_their_notifications() {
    let ctx = TestContext::setup().await;
    let (admin, sam) = (Uuid::now_v7(), Uuid::now_v7());

    // Given the administrator with one notification, and Sam with one unread one
    let admins = deliver_to(&ctx, admin, "admin_own", json!({})).await;
    let sams = deliver_to(&ctx, sam, "sam_own", json!({})).await;
    let (admin_id, sam_id) = (admins.id.to_string(), sams.id.to_string());

    // When the administrator acts as Sam: the Passport names Sam as the person
    // acted as and the administrator as the real actor
    let acting = make_impersonating_passport(admin, sam);

    // Then the list shows only the notification of the administrator
    let response = ctx.instance.graphql(&acting, LIST_QUERY, json!({})).await;
    let ids: Vec<_> = listed(&response)
        .iter()
        .map(|node| node["id"].clone())
        .collect();
    assert_eq!(
        ids,
        vec![json!(admin_id)],
        "the list is the administrator's own: {response}"
    );

    // And so does the stream: from its first payload on, and while Sam receives
    // something new
    let mut stream = Session::open(&ctx.instance, &acting).await;
    assert_eq!(
        stream.view().keys().collect::<Vec<_>>(),
        vec![&admin_id],
        "the first payload is the administrator's own list"
    );
    let mut sams_own_stream = Session::open(&ctx.instance, &make_passport(sam)).await;
    let news = deliver_to(&ctx, sam, "sam_news", json!({})).await;
    sams_own_stream
        .settle("Sam herself receives it", |view| {
            view.contains_key(&news.id.to_string())
        })
        .await;
    stream
        .expect_no_delta("what is delivered to Sam never reaches the one acting as Sam")
        .await;

    // When the administrator tries to mark, or delete, the notification of Sam,
    // or to mark everything as read — the spec does not fix the verdict of an
    // attempt on a foreign notification, so none is asserted: only what stays
    ctx.instance
        .graphql(&acting, MARK_AS_READ, json!({ "id": sam_id }))
        .await;
    ctx.instance
        .graphql(&acting, DELETE_ONE, json!({ "id": sam_id }))
        .await;
    ctx.instance
        .graphql(&acting, DELETE_MANY, json!({ "ids": [sam_id] }))
        .await;
    let response = ctx
        .instance
        .graphql(&acting, MARK_ALL_AS_READ, json!({}))
        .await;
    expect_ack_only(&response, "notifierMarkAllAsRead");

    // Then only the administrator's own notification changed: it is read now
    stream
        .settle("the administrator's own notification is read", |view| {
            view.len() == 1 && view.get(&admin_id).is_some_and(|v| !v["readAt"].is_null())
        })
        .await;

    // And when Sam looks later, her notification is there and still unread: in
    // storage, on demand and on a new stream
    let row = ctx
        .stack
        .row(sams.id)
        .await
        .expect("Sam's notification is still stored");
    assert_eq!(row.read_at, None);
    let response = ctx
        .instance
        .graphql(&make_passport(sam), LIST_QUERY, json!({}))
        .await;
    let mine = listed(&response)
        .iter()
        .find(|node| node["id"] == json!(sam_id))
        .unwrap_or_else(|| panic!("Sam's notification is still listed: {response}"));
    assert_eq!(mine["readAt"], Value::Null);
    let looking = Session::open(&ctx.instance, &make_passport(sam)).await;
    let there = looking.get(&sam_id).expect("on Sam's new stream too");
    assert_eq!(there["readAt"], Value::Null);
}
