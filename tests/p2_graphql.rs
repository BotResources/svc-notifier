mod common;

use serde_json::json;
use uuid::Uuid;

const CONSUME_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

// ── Queries ───────────────────────────────────────────────────────────

#[tokio::test]
#[serial_test::serial]
async fn notifier_notifications_returns_own_notifications() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    // Seed via NATS
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [user_id],
            "template": "welcome",
            "payload": {"msg": "hello"}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    let resp = ctx
        .graphql_query(
            &passport,
            "{ notifierNotifications { nodes { id template payload } hasNextPage } }",
            json!({}),
        )
        .await;

    let nodes = &resp["data"]["notifierNotifications"]["nodes"];
    assert_eq!(nodes.as_array().unwrap().len(), 1);
    assert_eq!(nodes[0]["template"], "welcome");
    assert_eq!(nodes[0]["payload"]["msg"], "hello");
}

#[tokio::test]
#[serial_test::serial]
async fn notifier_unread_count() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    // Seed 3 notifications
    for _ in 0..3 {
        ctx.nats_publish(
            "notify.deliver",
            &json!({
                "source_event_id": Uuid::now_v7(),
                "recipient_ids": [user_id],
                "template": "t",
                "payload": {}
            }),
        )
        .await;
    }
    tokio::time::sleep(CONSUME_WAIT).await;

    let resp = ctx
        .graphql_query(&passport, "{ notifierUnreadCount }", json!({}))
        .await;

    assert_eq!(resp["data"]["notifierUnreadCount"], 3);
}

#[tokio::test]
#[serial_test::serial]
async fn pagination_cursor() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    // Seed 5 notifications
    for _ in 0..5 {
        ctx.nats_publish(
            "notify.deliver",
            &json!({
                "source_event_id": Uuid::now_v7(),
                "recipient_ids": [user_id],
                "template": "t",
                "payload": {}
            }),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(CONSUME_WAIT).await;

    // First page of 3
    let resp = ctx
        .graphql_query(
            &passport,
            "query($first: Int!) { notifierNotifications(first: $first) { nodes { id } hasNextPage } }",
            json!({"first": 3}),
        )
        .await;

    let nodes = resp["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();
    assert_eq!(nodes.len(), 3);
    assert_eq!(resp["data"]["notifierNotifications"]["hasNextPage"], true);

    // Second page using cursor
    let cursor = nodes.last().unwrap()["id"].as_str().unwrap();
    let resp2 = ctx
        .graphql_query(
            &passport,
            "query($first: Int!, $after: UUID!) { notifierNotifications(first: $first, after: $after) { nodes { id } hasNextPage } }",
            json!({"first": 3, "after": cursor}),
        )
        .await;

    let nodes2 = resp2["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();
    assert_eq!(nodes2.len(), 2);
    assert_eq!(resp2["data"]["notifierNotifications"]["hasNextPage"], false);
}

// ── Mutations ─────────────────────────────────────────────────────────

#[tokio::test]
#[serial_test::serial]
async fn mark_as_read() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [user_id],
            "template": "t",
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // Get the notification ID
    let resp = ctx
        .graphql_query(
            &passport,
            "{ notifierNotifications { nodes { id readAt } } }",
            json!({}),
        )
        .await;
    let id = resp["data"]["notifierNotifications"]["nodes"][0]["id"]
        .as_str()
        .unwrap();
    assert!(resp["data"]["notifierNotifications"]["nodes"][0]["readAt"].is_null());

    // Mark as read
    let resp = ctx
        .graphql_query(
            &passport,
            "mutation($id: UUID!) { notifierMarkAsRead(notificationId: $id) }",
            json!({"id": id}),
        )
        .await;
    assert_eq!(resp["data"]["notifierMarkAsRead"], true);

    // Verify unread count is 0
    let resp = ctx
        .graphql_query(&passport, "{ notifierUnreadCount }", json!({}))
        .await;
    assert_eq!(resp["data"]["notifierUnreadCount"], 0);
}

#[tokio::test]
#[serial_test::serial]
async fn mark_all_as_read() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    for _ in 0..3 {
        ctx.nats_publish(
            "notify.deliver",
            &json!({
                "source_event_id": Uuid::now_v7(),
                "recipient_ids": [user_id],
                "template": "t",
                "payload": {}
            }),
        )
        .await;
    }
    tokio::time::sleep(CONSUME_WAIT).await;

    let resp = ctx
        .graphql_query(&passport, "mutation { notifierMarkAllAsRead }", json!({}))
        .await;
    assert_eq!(resp["data"]["notifierMarkAllAsRead"], true);

    let resp = ctx
        .graphql_query(&passport, "{ notifierUnreadCount }", json!({}))
        .await;
    assert_eq!(resp["data"]["notifierUnreadCount"], 0);
}

#[tokio::test]
#[serial_test::serial]
async fn delete_notification() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [user_id],
            "template": "t",
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    let resp = ctx
        .graphql_query(
            &passport,
            "{ notifierNotifications { nodes { id } } }",
            json!({}),
        )
        .await;
    let id = resp["data"]["notifierNotifications"]["nodes"][0]["id"]
        .as_str()
        .unwrap();

    let resp = ctx
        .graphql_query(
            &passport,
            "mutation($id: UUID!) { notifierDeleteNotification(notificationId: $id) }",
            json!({"id": id}),
        )
        .await;
    assert_eq!(resp["data"]["notifierDeleteNotification"], true);

    assert_eq!(ctx.count_notifications_for(user_id).await, 0);
}

#[tokio::test]
#[serial_test::serial]
async fn delete_nonexistent_returns_not_found() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let user_id = Uuid::now_v7();
    let passport = common::TestContext::make_passport(user_id, false);

    let resp = ctx
        .graphql_query(
            &passport,
            "mutation($id: UUID!) { notifierDeleteNotification(notificationId: $id) }",
            json!({"id": Uuid::now_v7()}),
        )
        .await;

    let errors = resp["errors"].as_array().unwrap();
    assert!(!errors.is_empty());
    assert_eq!(errors[0]["extensions"]["code"], "NOT_FOUND");
}

// ── RLS isolation ─────────────────────────────────────────────────────

#[tokio::test]
#[serial_test::serial]
async fn cross_user_isolation_on_queries() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();
    let alice_passport = common::TestContext::make_passport(alice, false);
    let bob_passport = common::TestContext::make_passport(bob, false);

    // Notification for Alice
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [alice],
            "template": "for_alice",
            "payload": {}
        }),
    )
    .await;
    // Notification for Bob
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [bob],
            "template": "for_bob",
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // Alice sees only her notification
    let resp = ctx
        .graphql_query(
            &alice_passport,
            "{ notifierNotifications { nodes { template } } }",
            json!({}),
        )
        .await;
    let nodes = resp["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["template"], "for_alice");

    // Bob sees only his notification
    let resp = ctx
        .graphql_query(
            &bob_passport,
            "{ notifierNotifications { nodes { template } } }",
            json!({}),
        )
        .await;
    let nodes = resp["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["template"], "for_bob");
}

#[tokio::test]
#[serial_test::serial]
async fn cross_user_cannot_delete_others_notification() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();
    let bob_passport = common::TestContext::make_passport(bob, false);

    // Notification for Alice
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [alice],
            "template": "alice_only",
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // Get Alice's notification ID via owner pool
    let row: (Uuid,) = sqlx::query_as("SELECT id FROM notifications WHERE recipient_id = $1")
        .bind(alice)
        .fetch_one(&ctx.owner_pool)
        .await
        .unwrap();
    let alice_notif_id = row.0;

    // Bob tries to delete Alice's notification — NOT_FOUND (RLS hides it)
    let resp = ctx
        .graphql_query(
            &bob_passport,
            "mutation($id: UUID!) { notifierDeleteNotification(notificationId: $id) }",
            json!({"id": alice_notif_id}),
        )
        .await;

    let errors = resp["errors"].as_array().unwrap();
    assert_eq!(errors[0]["extensions"]["code"], "NOT_FOUND");

    // Alice's notification still exists
    assert_eq!(ctx.count_notifications_for(alice).await, 1);
}

#[tokio::test]
#[serial_test::serial]
async fn no_rls_leak_between_consecutive_requests() {
    let ctx = common::TestContext::setup().await;
    ctx.reset_notifications().await;

    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();

    // Notifications for both
    ctx.nats_publish(
        "notify.deliver",
        &json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [alice, bob],
            "template": "shared_event",
            "payload": {}
        }),
    )
    .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // Alice query, then Bob query on the same service instance
    let alice_passport = common::TestContext::make_passport(alice, false);
    let bob_passport = common::TestContext::make_passport(bob, false);

    let resp_alice = ctx
        .graphql_query(
            &alice_passport,
            "{ notifierNotifications { nodes { id } } }",
            json!({}),
        )
        .await;
    let resp_bob = ctx
        .graphql_query(
            &bob_passport,
            "{ notifierNotifications { nodes { id } } }",
            json!({}),
        )
        .await;

    let alice_nodes = resp_alice["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();
    let bob_nodes = resp_bob["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap();

    // Each sees exactly 1 notification
    assert_eq!(alice_nodes.len(), 1);
    assert_eq!(bob_nodes.len(), 1);

    // And they're different notifications (different IDs)
    assert_ne!(
        alice_nodes[0]["id"].as_str().unwrap(),
        bob_nodes[0]["id"].as_str().unwrap()
    );
}
