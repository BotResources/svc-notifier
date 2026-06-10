// GraphQL-surface behavior scenarios: RLS isolation, ack-only mutations,
// and the contract that every state change reaches every session's stream.
mod common;

use common::*;
use serde_json::json;
use uuid::Uuid;

async fn seed_one(ctx: &TestContext, recipient: Uuid, template: &str) -> Uuid {
    let before = ctx.stack.rows_for(recipient).await.len();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], template, json!({})))
        .await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.rows_for(recipient).await.len() == before + 1
            })
            .await,
        "seeding through the intake failed for template {template}"
    );
    ctx.stack
        .rows_for(recipient)
        .await
        .into_iter()
        .find(|row| row.template == template)
        .expect("seeded row must exist")
        .id
}

#[tokio::test]
#[serial_test::serial]
async fn s06_rls_isolates_recipients_on_query_count_and_stream() {
    let ctx = TestContext::setup().await;
    let (owner, intruder) = (Uuid::now_v7(), Uuid::now_v7());
    let intruder_passport = make_passport(intruder);
    let mut intruder_sub = ctx.instance.subscribe(&intruder_passport).await;

    seed_one(&ctx, owner, "private").await;

    let listed = ctx
        .instance
        .graphql(&intruder_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        listed["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "another user's notifications must be invisible"
    );
    let count = ctx
        .instance
        .graphql(&intruder_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
    intruder_sub.expect_silence("no cross-recipient push").await;

    assert_eq!(
        ctx.stack.rows_for(owner).await.len(),
        1,
        "the row itself exists"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s09_mark_as_read_propagates_to_every_session_of_the_recipient() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    let notification_id = seed_one(&ctx, recipient, "to_read").await;

    let mut session_a = ctx.instance.subscribe(&passport).await;
    let mut session_b = ctx.instance.subscribe(&passport).await;

    // when: session A marks it as read (ack-only mutation)
    let ack = ctx
        .instance
        .graphql(
            &passport,
            "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) }",
            json!({"id": notification_id}),
        )
        .await;
    assert_eq!(
        ack["data"]["notifierMarkAsRead"], true,
        "mutation returns an ack: {ack}"
    );

    // then: both sessions receive the same bulk-shaped fact
    for (name, session) in [("A", &mut session_a), ("B", &mut session_b)] {
        let event = session.expect_event("NotificationsRead").await;
        assert_eq!(event["__typename"], "NotificationsRead", "session {name}");
        assert_eq!(event["ids"], json!([notification_id]), "session {name}");
        assert!(
            event["readAt"].is_string(),
            "readAt carried in the event (session {name})"
        );
    }

    // then: PG + query agree
    let rows = ctx.stack.rows_for(recipient).await;
    assert!(rows[0].read_at.is_some(), "read_at must be set");
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
}

#[tokio::test]
#[serial_test::serial]
async fn s10_mark_all_as_read_emits_exactly_one_bulk_event() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    let mut expected_ids = [
        seed_one(&ctx, recipient, "one").await,
        seed_one(&ctx, recipient, "two").await,
        seed_one(&ctx, recipient, "three").await,
    ];
    expected_ids.sort();

    let mut session = ctx.instance.subscribe(&passport).await;

    let ack = ctx
        .instance
        .graphql(&passport, "mutation { notifierMarkAllAsRead }", json!({}))
        .await;
    assert_eq!(ack["data"]["notifierMarkAllAsRead"], true);

    let event = session.expect_event("one bulk NotificationsRead").await;
    assert_eq!(event["__typename"], "NotificationsRead");
    let mut ids: Vec<String> = event["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("ids must be a list: {event}"))
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        expected_ids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "the single event carries every affected id"
    );
    session
        .expect_silence("exactly one event, not one per row")
        .await;

    assert!(
        ctx.stack
            .rows_for(recipient)
            .await
            .iter()
            .all(|row| row.read_at.is_some())
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s11_bulk_delete_skips_foreign_ids_and_emits_only_owned_ones() {
    let ctx = TestContext::setup().await;
    let (caller, other) = (Uuid::now_v7(), Uuid::now_v7());
    let caller_passport = make_passport(caller);

    let owned_one = seed_one(&ctx, caller, "mine_1").await;
    let owned_two = seed_one(&ctx, caller, "mine_2").await;
    let foreign = seed_one(&ctx, other, "not_mine").await;

    let mut caller_session = ctx.instance.subscribe(&caller_passport).await;
    let mut other_session = ctx.instance.subscribe(&make_passport(other)).await;

    // when: the caller bulk-deletes, sneaking in a foreign id
    let ack = ctx
        .instance
        .graphql(
            &caller_passport,
            "mutation($ids: [ID!]!) { notifierDeleteNotifications(ids: $ids) }",
            json!({"ids": [owned_one, owned_two, foreign]}),
        )
        .await;
    assert_eq!(
        ack["data"]["notifierDeleteNotifications"], true,
        "foreign ids are invisible, not an error: {ack}"
    );

    // then: PG — own rows gone, the foreign row untouched
    assert_eq!(ctx.stack.rows_for(caller).await.len(), 0);
    assert_eq!(ctx.stack.rows_for(other).await.len(), 1);

    // then: GraphQL — one event, owned ids only, foreign id absent; the
    // other user sees nothing at all
    let event = caller_session.expect_event("NotificationsDeleted").await;
    assert_eq!(event["__typename"], "NotificationsDeleted");
    let mut ids: Vec<String> = event["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("ids must be a list: {event}"))
        .iter()
        .map(|id| id.as_str().unwrap().to_string())
        .collect();
    ids.sort();
    let mut expected = vec![owned_one.to_string(), owned_two.to_string()];
    expected.sort();
    assert_eq!(
        ids, expected,
        "the foreign id must be absent from the event"
    );
    other_session
        .expect_silence("the other user observes nothing")
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s12_single_delete_is_observed_by_other_sessions() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    let notification_id = seed_one(&ctx, recipient, "ephemeral").await;
    let mut other_session = ctx.instance.subscribe(&passport).await;

    let ack = ctx
        .instance
        .graphql(
            &passport,
            "mutation($id: ID!) { notifierDeleteNotification(notificationId: $id) }",
            json!({"id": notification_id}),
        )
        .await;
    assert_eq!(ack["data"]["notifierDeleteNotification"], true);

    let event = other_session.expect_event("NotificationsDeleted").await;
    assert_eq!(event["__typename"], "NotificationsDeleted");
    assert_eq!(event["ids"], json!([notification_id]));
    assert_eq!(ctx.stack.rows_for(recipient).await.len(), 0);
}
