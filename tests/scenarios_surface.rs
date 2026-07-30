// GraphQL-surface behavior scenarios: RLS isolation, ack-only mutations,
// and the contract that every state change reaches every session's stream.
mod common;

use br_test_harness::verdict;
use common::*;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn s05_sdl_route_serves_the_schema_for_the_gateway_composer() {
    // The gateway composer polls GET {base_url}/sdl on every subgraph; serving
    // the SDL anywhere else gets the subgraph rejected at composition. This
    // pins the route name so a rename goes red here, not in deployment.
    let ctx = TestContext::setup().await;

    let (status, body) = ctx.instance.get("/sdl").await;
    assert!(
        status.is_success(),
        "GET /sdl must return 2xx, got {status}"
    );
    assert!(!body.trim().is_empty(), "the SDL body must not be empty");
    assert!(
        body.contains("type Query") && body.contains("type Subscription"),
        "the SDL must expose the Query and Subscription roots: {body}"
    );
    assert!(
        body.contains("notifierNotificationEvents"),
        "the SDL must carry this service's root fields: {body}"
    );

    let (legacy_status, _) = ctx.instance.get("/schema").await;
    assert_eq!(
        legacy_status,
        reqwest::StatusCode::NOT_FOUND,
        "the old /schema route is gone — one route, one truth"
    );
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
    intruder_sub
        .expect_silence("no cross-recipient push", CONSUME_WAIT)
        .await;

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
    verdict::expect_ack(&ack, "notifierMarkAsRead");

    // then: both sessions receive the same bulk-shaped fact
    for (name, session) in [("A", &mut session_a), ("B", &mut session_b)] {
        let raw = session.expect_event("NotificationsRead", SSE_TIMEOUT).await;
        let event = notifier_event(&raw);
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
    verdict::expect_ack(&ack, "notifierMarkAllAsRead");

    let raw = session
        .expect_event("one bulk NotificationsRead", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
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
        .expect_silence("exactly one event, not one per row", CONSUME_WAIT)
        .await;

    assert!(
        ctx.stack
            .rows_for(recipient)
            .await
            .iter()
            .all(|row| row.read_at.is_some())
    );
}

// One more notification than fits in a single announcement. The service caps a
// bulk fact at SIGNAL_ID_CHUNK (150) ids per PostgreSQL NOTIFY payload — above
// 8000 bytes PostgreSQL raises inside the write transaction and would abort the
// whole mutation — so a bulk read of 151 notifications must arrive as several
// announcements. The contract a client folds against is therefore the UNION of
// the ids announced, not "exactly one event": complete, without duplicates, and
// still atomic with the write.
const BEYOND_ONE_ANNOUNCEMENT: usize = 151;

#[tokio::test]
#[serial_test::serial]
async fn s10b_mark_all_as_read_past_the_chunk_bound_announces_every_id_exactly_once() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    // given: more unread notifications than one announcement can carry, every
    // one of them delivered through the real intake — one deliver command per
    // business fact, exactly as a producer would emit them
    for index in 0..BEYOND_ONE_ANNOUNCEMENT {
        ctx.stack
            .publish_deliver(&deliver(
                &[recipient],
                "chunk_bound",
                json!({"index": index}),
            ))
            .await;
    }
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.rows_for(recipient).await.len() == BEYOND_ONE_ANNOUNCEMENT
            })
            .await,
        "the intake must have written all {BEYOND_ONE_ANNOUNCEMENT} notifications, got {}",
        ctx.stack.rows_for(recipient).await.len()
    );
    let mut expected: Vec<String> = ctx
        .stack
        .rows_for(recipient)
        .await
        .iter()
        .map(|row| row.id.to_string())
        .collect();
    expected.sort();

    let mut session = ctx.instance.subscribe(&passport).await;

    // when: the recipient marks everything read in one gesture
    let ack = ctx
        .instance
        .graphql(&passport, "mutation { notifierMarkAllAsRead }", json!({}))
        .await;
    verdict::expect_ack(&ack, "notifierMarkAllAsRead past the chunk bound");

    // then: collecting every announcement until the stream falls silent yields
    // each affected id exactly once — a client folding by id lands on the same
    // state whatever the chunking does
    let mut announced: Vec<String> = Vec::new();
    let mut announcements = 0;
    while let Some(raw) = session.next_event(SSE_TIMEOUT).await {
        announcements += 1;
        let event = notifier_event(&raw);
        assert_eq!(
            event["__typename"], "NotificationsRead",
            "only read facts follow a mark-all-as-read: {event}"
        );
        assert!(
            event["readAt"].is_string(),
            "every chunk carries the read timestamp, so no client must re-read to learn it: {event}"
        );
        announced.extend(
            event["ids"]
                .as_array()
                .unwrap_or_else(|| panic!("ids must be a list: {event}"))
                .iter()
                .map(|id| id.as_str().unwrap().to_string()),
        );
    }
    assert!(
        announcements > 1,
        "past the chunk bound the fact travels as several announcements — one 151-id \
         payload would exceed the NOTIFY limit and abort the mutation; got {announcements} \
         announcement(s) for {} ids",
        announced.len()
    );
    let mut folded = announced.clone();
    folded.sort();
    folded.dedup();
    assert_eq!(
        folded.len(),
        announced.len(),
        "no id is announced twice across the chunks"
    );
    assert_eq!(
        folded, expected,
        "the union of the announcements is exactly the set of affected notifications — \
         nothing missing, nothing invented"
    );

    // then: storage and the badge agree with what was announced
    assert!(
        ctx.stack
            .rows_for(recipient)
            .await
            .iter()
            .all(|row| row.read_at.is_some()),
        "every row is read — the chunking is inside one transaction, not several"
    );
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
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
    verdict::expect_ack(
        &ack,
        "notifierDeleteNotifications (foreign ids are invisible)",
    );

    // then: PG — own rows gone, the foreign row untouched
    assert_eq!(ctx.stack.rows_for(caller).await.len(), 0);
    assert_eq!(ctx.stack.rows_for(other).await.len(), 1);

    // then: GraphQL — one event, owned ids only, foreign id absent; the
    // other user sees nothing at all
    let raw = caller_session
        .expect_event("NotificationsDeleted", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
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
        .expect_silence("the other user observes nothing", CONSUME_WAIT)
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
    verdict::expect_ack(&ack, "notifierDeleteNotification");

    let raw = other_session
        .expect_event("NotificationsDeleted", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationsDeleted");
    assert_eq!(event["ids"], json!([notification_id]));
    assert_eq!(ctx.stack.rows_for(recipient).await.len(), 0);
}
