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
    // the intruder's session is proven live before its silence is read as
    // isolation: nothing the owner does may reach it, but a stream the service
    // never registered would be just as quiet with no isolation at all
    let mut intruder_sub = subscribe_live(&ctx, intruder).await;

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

    let mut caller_session = ctx.instance.subscribe(&caller_passport).await;
    let mut other_session = ctx.instance.subscribe(&make_passport(other)).await;

    // given: the other user's own notification lands after their session opens,
    // and is served on it — the proof that this session is in the fan-out, which
    // is what makes its silence further down mean anything
    let foreign = seed_one(&ctx, other, "not_mine").await;
    let served = other_session
        .expect_event("the other user's session is live", RECOVERY_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&served)["notification"]["id"],
        json!(foreign.to_string()),
        "the other user's session carries their own notification: {served}"
    );

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

#[tokio::test]
#[serial_test::serial]
async fn s26_the_list_serves_the_newest_notification_first() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);

    // given: three notifications seeded in a known order, one at a time so their
    // created_at ordering is the arrival order and not a coin toss
    let oldest = seed_one(&ctx, recipient, "oldest").await;
    let middle = seed_one(&ctx, recipient, "middle").await;
    let newest = seed_one(&ctx, recipient, "newest").await;

    // then: the list is newest-first, asserted as a sequence. Every other
    // scenario sorts before comparing, so inverting the ORDER BY would stay green
    // everywhere else — this is the only place the spec's ordering clause is
    // actually pinned.
    let listed = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    let templates: Vec<&str> = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"))
        .iter()
        .map(|node| node["template"].as_str().expect("a template"))
        .collect();
    assert_eq!(
        templates,
        vec!["newest", "middle", "oldest"],
        "the inbox reads newest-first: {listed}"
    );

    // then: walking the cursor preserves that order across page boundaries
    let walked = listed_ids(&ctx.instance, &passport).await;
    assert_eq!(
        walked,
        vec![newest.to_string(), middle.to_string(), oldest.to_string()],
        "the cursor walk keeps the newest-first order it started with"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s27_marking_read_twice_changes_nothing_and_announces_once() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let notification_id = seed_one(&ctx, recipient, "read_once").await;
    // no warm-up is owed here: the first mark-as-read is announced on this very
    // session below, which is what proves it live before its silence is asserted
    let mut session = ctx.instance.subscribe(&passport).await;

    // when: the recipient marks it read, then marks it read again
    let first = ctx
        .instance
        .graphql(&passport, MARK_AS_READ, json!({"id": notification_id}))
        .await;
    verdict::expect_ack(&first, "notifierMarkAsRead");
    let raw = session.expect_event("NotificationsRead", SSE_TIMEOUT).await;
    let announced_read_at = notifier_event(&raw)["readAt"]
        .as_str()
        .expect("the fact carries readAt")
        .to_owned();
    let stored_read_at = ctx.stack.rows_for(recipient).await[0]
        .read_at
        .expect("read_at is set");

    let second = ctx
        .instance
        .graphql(&passport, MARK_AS_READ, json!({"id": notification_id}))
        .await;

    // then: the replay is accepted — an idempotent command is not an error — but
    // it is not a second fact. A client that decremented its unread badge per
    // announcement would drift negative.
    verdict::expect_ack(&second, "notifierMarkAsRead replayed");
    session
        .expect_silence(
            "a replayed mark-as-read announces nothing: the transition already happened",
            CONSUME_WAIT,
        )
        .await;

    // then: read_at never moves, and the notification never goes back to unread
    assert_eq!(
        ctx.stack.rows_for(recipient).await[0]
            .read_at
            .expect("still read"),
        stored_read_at,
        "the read timestamp is the first one, not the latest attempt"
    );
    assert!(announced_read_at.starts_with(&stored_read_at.format("%Y-%m-%d").to_string()));
    let listed = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    assert!(
        listed["data"]["notifierNotifications"]["nodes"][0]["readAt"].is_string(),
        "read is terminal — a replay never returns it to unread: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
}

#[tokio::test]
#[serial_test::serial]
async fn s28_a_cursor_whose_notification_is_gone_is_refused_not_answered_empty() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let anchor = seed_one(&ctx, recipient, "anchor").await;
    seed_one(&ctx, recipient, "still_there").await;

    // given: the client holds a cursor, then that notification is deleted —
    // by another session, or by this one on a page it has already left
    let ack = ctx
        .instance
        .graphql(&passport, DELETE_ONE, json!({"id": anchor.to_string()}))
        .await;
    verdict::expect_ack(&ack, "notifierDeleteNotification");

    // then: paging from the stale cursor is refused by code. Answering an empty
    // page would read to the client as "you have reached the end", silently
    // hiding everything after the deleted anchor.
    let stale = ctx
        .instance
        .graphql(
            &passport,
            PAGE_QUERY,
            json!({"first": 20, "after": anchor.to_string()}),
        )
        .await;
    assert_eq!(
        verdict::expect_code_shaped(&stale, "paging from a deleted cursor"),
        "NOT_FOUND",
        "a cursor that no longer resolves must say so: {stale}"
    );
    assert!(
        stale["data"]["notifierNotifications"].is_null(),
        "a refused page carries no nodes: {stale}"
    );

    // then: restarting from the top — the documented recovery — still works and
    // shows what the stale cursor would have hidden
    let listed = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    let templates: Vec<&str> = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"))
        .iter()
        .map(|node| node["template"].as_str().expect("a template"))
        .collect();
    assert_eq!(templates, vec!["still_there"]);
}

#[tokio::test]
#[serial_test::serial]
async fn s29_row_level_security_isolates_recipients_with_no_application_predicate_at_all() {
    let ctx = TestContext::setup().await;
    let (owner, intruder) = (Uuid::now_v7(), Uuid::now_v7());
    let owned = seed_one(&ctx, owner, "owner_only").await;

    // given: a connection as the service's own RLS-subject runtime role, and a
    // query with NO recipient predicate — the deliberate opposite of every
    // resolver, which now filters in the application layer too. If the policy is
    // the only thing standing between these rows and the caller, this is where it
    // shows.
    let app_pool = ctx.stack.app_role_pool().await;
    let bare_read = "SELECT id FROM notifications";

    // then: with no RLS context set at all, the policy denies everything. The GUC
    // is transaction-local, so a query issued outside a scoped transaction has no
    // identity — and must therefore see nothing rather than everything.
    let mut tx = app_pool.begin().await.expect("begin");
    let contextless: Vec<Uuid> = sqlx::query_scalar(bare_read)
        .fetch_all(&mut *tx)
        .await
        .unwrap_or_default();
    tx.rollback().await.expect("rollback");
    assert!(
        contextless.is_empty(),
        "with no app.current_user_id, the policy must yield no rows: {contextless:?}"
    );

    // then: with the intruder's identity in the RLS context, the owner's row is
    // still invisible — the policy compares recipient_id to the GUC, nothing else
    let mut tx = app_pool.begin().await.expect("begin");
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(intruder.to_string())
        .execute(&mut *tx)
        .await
        .expect("set the RLS context");
    let as_intruder: Vec<Uuid> = sqlx::query_scalar(bare_read)
        .fetch_all(&mut *tx)
        .await
        .expect("the read itself is permitted, the rows are what is filtered");
    tx.rollback().await.expect("rollback");
    assert!(
        as_intruder.is_empty(),
        "the wrong identity must see nothing, even with no WHERE clause: {as_intruder:?}"
    );

    // then: with the owner's identity, exactly their row appears — proving the
    // emptiness above is the policy filtering, not a broken query or a missing
    // grant, which would have shown up as zero rows here too
    let mut tx = app_pool.begin().await.expect("begin");
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(owner.to_string())
        .execute(&mut *tx)
        .await
        .expect("set the RLS context");
    let as_owner: Vec<Uuid> = sqlx::query_scalar(bare_read)
        .fetch_all(&mut *tx)
        .await
        .expect("the owner's own read");
    tx.rollback().await.expect("rollback");
    assert_eq!(
        as_owner,
        vec![owned],
        "the policy admits exactly the caller's own rows"
    );

    // then: the write side is guarded by the same policy, not merely the read —
    // an UPDATE with no predicate under the intruder's context touches nothing
    let mut tx = app_pool.begin().await.expect("begin");
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(intruder.to_string())
        .execute(&mut *tx)
        .await
        .expect("set the RLS context");
    let touched = sqlx::query("UPDATE notifications SET read_at = now()")
        .execute(&mut *tx)
        .await
        .expect("the statement runs")
        .rows_affected();
    tx.commit().await.expect("commit");
    assert_eq!(touched, 0, "a blanket UPDATE reaches no foreign row");
    assert!(
        ctx.stack.rows_for(owner).await[0].read_at.is_none(),
        "and the owner's row is untouched, read through the owner connection"
    );

    app_pool.close().await;
}

// The service's entire published edge, as an exact set. A subset check ("the SDL
// contains notifierMarkAsRead") cannot see what was *added*, and an addition is
// the dangerous direction: the one thing a client must never be able to do is
// mint a notification, and the only durable proof of that is that no such
// mutation exists in the schema at all. Add a root field on purpose and this goes
// red — that is the point; extend these lists deliberately, with the spec.
const PUBLISHED_QUERIES: [&str; 2] = ["notifierNotifications", "notifierUnreadCount"];
const PUBLISHED_MUTATIONS: [&str; 4] = [
    "notifierMarkAsRead",
    "notifierMarkAllAsRead",
    "notifierDeleteNotification",
    "notifierDeleteNotifications",
];
const PUBLISHED_SUBSCRIPTIONS: [&str; 1] = ["notifierNotificationEvents"];

// Verbs a create-shaped mutation would plausibly carry. None may ever appear as a
// root field: notifications enter this service only through the NATS intake, from
// a producer holding a service identity, never through the client edge.
const CREATE_SHAPED_VERBS: [&str; 6] = ["Create", "Deliver", "Send", "Notify", "Add", "Publish"];

fn root_fields(sdl: &str, type_name: &str) -> Vec<String> {
    let opening = format!("type {type_name} {{");
    let body = sdl
        .split_once(&opening)
        .unwrap_or_else(|| panic!("the SDL declares no {type_name}:\n{sdl}"))
        .1
        .split_once('}')
        .expect("a root type closes")
        .0;
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with('"'))
        .map(|line| {
            line.split(['(', ':'])
                .next()
                .expect("a field has a name")
                .trim()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
#[serial_test::serial]
async fn s30_the_published_edge_is_a_closed_world_with_no_way_to_create_a_notification() {
    let ctx = TestContext::setup().await;
    let (_, sdl) = ctx.instance.get("/sdl").await;

    for (root, expected) in [
        ("QueryRoot", PUBLISHED_QUERIES.as_slice()),
        ("MutationRoot", PUBLISHED_MUTATIONS.as_slice()),
        ("SubscriptionRoot", PUBLISHED_SUBSCRIPTIONS.as_slice()),
    ] {
        let mut served = root_fields(&sdl, root);
        served.sort();
        let mut expected: Vec<String> = expected.iter().map(|name| (*name).to_owned()).collect();
        expected.sort();
        assert_eq!(
            served, expected,
            "{root} exposes exactly the published edge and nothing more"
        );
    }

    // then: no root field anywhere is create-shaped. This is the assertion that
    // survives a refactor: whatever the schema grows, a client still cannot ask
    // this service to make a notification exist.
    let every_root_field: Vec<String> = ["QueryRoot", "MutationRoot", "SubscriptionRoot"]
        .iter()
        .flat_map(|root| root_fields(&sdl, root))
        .collect();
    for field in &every_root_field {
        for verb in CREATE_SHAPED_VERBS {
            assert!(
                !field.contains(verb),
                "root field {field} looks like a way to create a notification from the edge — \
                 notifications may only enter through the NATS intake"
            );
        }
    }

    // then: every root field is BC-prefixed, so the supergraph cannot collide
    for field in &every_root_field {
        assert!(
            field.starts_with("notifier"),
            "root field {field} must carry the BC prefix"
        );
    }
}
