// Impersonation scenarios: a *true* notification belongs to the human who is
// acting. While an administrator impersonates a user, every surface — list,
// unread count, live stream, mark-as-read, delete — must stay on the
// administrator's own notifications and never reach the impersonated user's.
mod common;

use br_test_harness::verdict;
use common::*;
use serde_json::json;
use uuid::Uuid;

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

    // given: both live streams are open before anything is mutated — the
    // acting administrator's own, and the impersonated user's. A unitary
    // mutation must be announced on the first and be inaudible on the second.
    let mut acting_session = ctx.instance.subscribe(&acting).await;
    let mut impersonated_session = ctx.instance.subscribe(&make_passport(impersonated)).await;

    // given: the impersonated user's own notification lands after their session
    // opens, and is served on it. That push is the proof the session is really in
    // the fan-out — its silence below is only worth what that proof is worth.
    let theirs = seed_one(&ctx, impersonated, "impersonated_own").await;
    let served = impersonated_session
        .expect_event("the impersonated user's session is live", RECOVERY_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&served)["notification"]["id"],
        json!(theirs.to_string()),
        "the impersonated user's own session carries their own notification: {served}"
    );

    // when: the administrator marks their own notification as read while
    // impersonating
    let ack = ctx
        .instance
        .graphql(&acting, MARK_AS_READ, json!({"id": mine.to_string()}))
        .await;
    verdict::expect_ack(&ack, "notifierMarkAsRead on the acting admin's own");

    // then: the acting session is told, with the acting human's own id
    let raw = acting_session
        .expect_event("NotificationsRead for the acting admin", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsRead");
    assert_eq!(
        event["ids"],
        json!([mine.to_string()]),
        "the announcement carries the acting human's id, never the impersonated user's: {event}"
    );

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

    // then: the delete is announced on the acting session, again with the
    // acting human's own id
    let raw = acting_session
        .expect_event("NotificationsDeleted for the acting admin", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsDeleted");
    assert_eq!(event["ids"], json!([to_delete.to_string()]));

    // then: the impersonated user's own session heard neither mutation — not
    // the one that succeeded, not the one that was refused on their behalf
    impersonated_session
        .expect_silence(
            "an impersonated user is never told about their impersonator's reading habits",
            CONSUME_WAIT,
        )
        .await;

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

    // then: re-reading under the acting identity shows the new state — one
    // notification left, read, and nothing owed to the badge
    let listed = ctx.instance.graphql(&acting, LIST_QUERY, json!({})).await;
    let nodes = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"));
    assert_eq!(
        nodes.len(),
        1,
        "only the read notification remains: {listed}"
    );
    assert_eq!(nodes[0]["id"], json!(mine.to_string()));
    assert!(
        nodes[0]["readAt"].is_string(),
        "the query reflects the mark-as-read: {listed}"
    );
    let count = ctx.instance.graphql(&acting, UNREAD_QUERY, json!({})).await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
}

#[tokio::test]
#[serial_test::serial]
async fn s19b_impersonated_bulk_mutations_never_reach_the_impersonated_users_inbox() {
    let ctx = TestContext::setup().await;
    let (admin, impersonated) = (Uuid::now_v7(), Uuid::now_v7());
    let acting = make_impersonating_passport(admin, impersonated);

    // given: both inboxes are stocked. The bulk mutations are the only ones with
    // no id to target, so their whole blast radius is the resolved identity.
    let mine = [
        seed_one(&ctx, admin, "admin_bulk_one").await,
        seed_one(&ctx, admin, "admin_bulk_two").await,
    ];

    let mut acting_session = ctx.instance.subscribe(&acting).await;
    let mut impersonated_session = ctx.instance.subscribe(&make_passport(impersonated)).await;

    // given: the impersonated user's half of the stock lands after their session
    // opens, so the two pushes it is served prove the session is in the fan-out.
    // Without that proof its silence below would hold even if the service had
    // never registered it at all.
    let theirs = [
        seed_one(&ctx, impersonated, "impersonated_bulk_one").await,
        seed_one(&ctx, impersonated, "impersonated_bulk_two").await,
    ];
    for expected in theirs {
        let served = impersonated_session
            .expect_event("the impersonated user's session is live", RECOVERY_TIMEOUT)
            .await;
        assert_eq!(
            notifier_event(&served)["notification"]["id"],
            json!(expected.to_string()),
            "the impersonated user's own session carries their own notifications: {served}"
        );
    }

    // when: the administrator marks everything read while impersonating
    let ack = ctx
        .instance
        .graphql(&acting, "mutation { notifierMarkAllAsRead }", json!({}))
        .await;
    verdict::expect_ack(&ack, "notifierMarkAllAsRead while impersonating");

    // then: PG — only the administrator's inbox moved
    assert!(
        ctx.stack
            .rows_for(admin)
            .await
            .iter()
            .all(|row| row.read_at.is_some()),
        "the acting human's own notifications are the ones marked read"
    );
    assert!(
        ctx.stack
            .rows_for(impersonated)
            .await
            .iter()
            .all(|row| row.read_at.is_none()),
        "mark-all-as-read must not touch the impersonated user's inbox"
    );

    // then: the bulk announcement carries the administrator's ids, and the
    // impersonated user's own session observes nothing
    let raw = acting_session
        .expect_event("bulk NotificationsRead", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsRead");
    let mut read_ids: Vec<String> = event["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("ids must be a list: {event}"))
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    read_ids.sort();
    let mut expected: Vec<String> = mine.iter().map(Uuid::to_string).collect();
    expected.sort();
    assert_eq!(
        read_ids, expected,
        "only the acting human's ids are announced"
    );

    // when: the administrator bulk-deletes, sneaking in the impersonated user's ids
    let all_ids: Vec<String> = mine
        .iter()
        .chain(theirs.iter())
        .map(Uuid::to_string)
        .collect();
    let ack = ctx
        .instance
        .graphql(
            &acting,
            "mutation($ids: [ID!]!) { notifierDeleteNotifications(ids: $ids) }",
            json!({"ids": all_ids}),
        )
        .await;
    verdict::expect_ack(&ack, "notifierDeleteNotifications while impersonating");

    // then: PG — the administrator's inbox is emptied, the impersonated user's is intact
    assert_eq!(ctx.stack.rows_for(admin).await.len(), 0);
    assert_eq!(
        ctx.stack.rows_for(impersonated).await.len(),
        2,
        "an impersonated id passed to a bulk delete must be invisible, not deleted"
    );

    let raw = acting_session
        .expect_event("bulk NotificationsDeleted", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsDeleted");
    let mut deleted_ids: Vec<String> = event["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("ids must be a list: {event}"))
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    deleted_ids.sort();
    assert_eq!(
        deleted_ids, expected,
        "the impersonated user's ids are absent from the announcement"
    );

    impersonated_session
        .expect_silence(
            "the impersonated user's own session observes neither bulk",
            CONSUME_WAIT,
        )
        .await;
}
