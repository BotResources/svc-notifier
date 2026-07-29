// Impersonation scenarios: a *true* notification belongs to the human who is
// acting. While an administrator impersonates a user, every surface — list,
// unread count, live stream, mark-as-read, delete — must stay on the
// administrator's own notifications and never reach the impersonated user's.
mod common;

use br_test_harness::verdict;
use common::*;
use serde_json::json;
use uuid::Uuid;

const MARK_AS_READ: &str = "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) }";
const DELETE_ONE: &str = "mutation($id: ID!) { notifierDeleteNotification(notificationId: $id) }";

#[tokio::test]
#[serial_test::serial]
async fn s17_impersonation_reads_the_acting_admins_own_notifications_only() {
    let ctx = TestContext::setup().await;
    let (admin, impersonated) = (Uuid::now_v7(), Uuid::now_v7());
    let acting = make_impersonating_passport(admin, impersonated);

    // given: both the administrator and the user they impersonate have
    // notifications of their own
    let mine = seed_one(&ctx, admin, "admin_own").await;
    seed_one(&ctx, impersonated, "impersonated_own").await;

    // when: the administrator lists while impersonating
    let listed = ctx.instance.graphql(&acting, LIST_QUERY, json!({})).await;

    // then: only the administrator's own notification is visible
    let nodes = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"));
    assert_eq!(
        nodes.len(),
        1,
        "the impersonated user's notifications must never surface: {listed}"
    );
    assert_eq!(nodes[0]["id"], json!(mine.to_string()));
    assert_eq!(nodes[0]["template"], "admin_own");

    // then: the unread count counts the administrator's own only
    let count = ctx.instance.graphql(&acting, UNREAD_QUERY, json!({})).await;
    assert_eq!(
        ServiceInstance::unread_count(&count),
        1,
        "the unread count must be the acting human's own"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s18_the_impersonated_users_stream_never_leaks_to_the_acting_admin() {
    let ctx = TestContext::setup().await;
    let (admin, impersonated) = (Uuid::now_v7(), Uuid::now_v7());
    let acting = make_impersonating_passport(admin, impersonated);

    let mut session = ctx.instance.subscribe(&acting).await;

    // when: a notification lands for the impersonated user, then one for the
    // administrator
    seed_one(&ctx, impersonated, "shadow").await;
    let landed = seed_one(&ctx, admin, "mine_live").await;

    // then: the stream carries the administrator's notification, and only it
    let raw = session
        .expect_event("NotificationAdded for the acting admin", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationAdded");
    assert_eq!(
        event["notification"]["id"],
        json!(landed.to_string()),
        "the first pushed event must be the acting human's own: {event}"
    );
    assert_eq!(event["notification"]["template"], "mine_live");
    session
        .expect_silence(
            "the impersonated user's push must never reach the impersonator",
            CONSUME_WAIT,
        )
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s19_impersonated_mutations_act_on_the_acting_admins_own_notifications() {
    let ctx = TestContext::setup().await;
    let (admin, impersonated) = (Uuid::now_v7(), Uuid::now_v7());
    let acting = make_impersonating_passport(admin, impersonated);

    let mine = seed_one(&ctx, admin, "admin_own").await;
    let to_delete = seed_one(&ctx, admin, "admin_disposable").await;
    let theirs = seed_one(&ctx, impersonated, "impersonated_own").await;

    // when: the administrator marks their own notification as read while
    // impersonating
    let ack = ctx
        .instance
        .graphql(&acting, MARK_AS_READ, json!({"id": mine.to_string()}))
        .await;
    verdict::expect_ack(&ack, "notifierMarkAsRead on the acting admin's own");

    // then: reaching for the impersonated user's notification is a not-found —
    // the application layer and the RLS context resolve the same identity
    let denied = ctx
        .instance
        .graphql(&acting, MARK_AS_READ, json!({"id": theirs.to_string()}))
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&denied, "marking the impersonated user's notification"),
        "NOT_FOUND"
    );

    // when: the administrator deletes their own, then reaches for the
    // impersonated user's
    let ack = ctx
        .instance
        .graphql(&acting, DELETE_ONE, json!({"id": to_delete.to_string()}))
        .await;
    verdict::expect_ack(&ack, "notifierDeleteNotification on the acting admin's own");
    let denied = ctx
        .instance
        .graphql(&acting, DELETE_ONE, json!({"id": theirs.to_string()}))
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&denied, "deleting the impersonated user's notification"),
        "NOT_FOUND"
    );

    // then: PG — the administrator's own rows moved, the impersonated user's
    // row is untouched
    let admin_rows = ctx.stack.rows_for(admin).await;
    assert_eq!(admin_rows.len(), 1, "the disposable row is gone");
    assert!(
        admin_rows[0].read_at.is_some(),
        "the administrator's own notification was marked read"
    );
    let impersonated_rows = ctx.stack.rows_for(impersonated).await;
    assert_eq!(impersonated_rows.len(), 1, "nothing of theirs was deleted");
    assert!(
        impersonated_rows[0].read_at.is_none(),
        "nothing of theirs was marked read"
    );
}
