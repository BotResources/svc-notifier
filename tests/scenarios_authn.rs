// Door scenarios: who gets in, and what a caller who is in may aim at. No
// Passport at all and a forged one are refused at the transport; a machine
// identity is refused by code on every field; and an ordinary human reaching for
// somebody else's notification — or for an id that is not one — is refused by
// code too, with the target's state untouched on every channel.
mod common;

use br_test_harness::verdict;
use common::*;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

const MARK_AS_READ: &str = "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) }";
const DELETE_ONE: &str = "mutation($id: ID!) { notifierDeleteNotification(notificationId: $id) }";

#[tokio::test]
#[serial_test::serial]
async fn graphql_without_passport_returns_401() {
    let ctx = TestContext::setup().await;
    let (status, _body) = ctx.instance.graphql_unauthenticated(UNREAD_QUERY).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial_test::serial]
async fn graphql_with_malformed_passport_returns_401() {
    let ctx = TestContext::setup().await;
    for bad_header in ["not-valid-base64!!!", ""] {
        let status = ctx
            .instance
            .graphql_bad_passport(UNREAD_QUERY, bad_header)
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "header: {bad_header:?}");
    }
}

#[tokio::test]
#[serial_test::serial]
async fn liveness_is_accessible_without_passport() {
    let ctx = TestContext::setup().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/livez", ctx.instance.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
#[serial_test::serial]
async fn service_passport_queries_and_mutations_are_forbidden() {
    let ctx = TestContext::setup().await;
    let service_passport = make_service_passport(Uuid::now_v7());

    // Every door, not a sample of them: a refusal table that omits a mutation is
    // an invitation to add the next one without a guard.
    let cases = [
        ("notifierUnreadCount", UNREAD_QUERY, json!({})),
        ("notifierNotifications", LIST_QUERY, json!({})),
        (
            "notifierMarkAllAsRead",
            "mutation { notifierMarkAllAsRead }",
            json!({}),
        ),
        (
            "notifierMarkAsRead",
            MARK_AS_READ,
            json!({"id": Uuid::now_v7().to_string()}),
        ),
        (
            "notifierDeleteNotification",
            DELETE_ONE,
            json!({"id": Uuid::now_v7().to_string()}),
        ),
        (
            "notifierDeleteNotifications",
            "mutation($ids: [ID!]!) { notifierDeleteNotifications(ids: $ids) }",
            json!({"ids": [Uuid::now_v7().to_string()]}),
        ),
    ];

    for (field, query, vars) in cases {
        let response = ctx.instance.graphql(&service_passport, query, vars).await;
        assert!(
            response["data"][field].is_null(),
            "{field}: a service passport must not get a result: {response}"
        );
        let code = verdict::expect_code_shaped(
            &response,
            &format!("{field}: a service passport must be rejected before any work"),
        );
        assert_eq!(
            code, "FORBIDDEN",
            "{field}: a service passport must be rejected with FORBIDDEN: {response}"
        );
    }
}

#[tokio::test]
#[serial_test::serial]
async fn service_passports_get_no_subscription_events() {
    let ctx = TestContext::setup().await;
    let service_passport = make_service_passport(Uuid::now_v7());
    let recipient = Uuid::now_v7();

    // given: a machine identity opens the live stream. The subscription is a
    // door like any other and answers a verdict — it used to hand back a
    // silently empty stream, which reads to a client exactly like "you have no
    // notifications" rather than "you may not have any".
    let refused = ctx.instance.subscribe_refused(&service_passport).await;
    assert_eq!(
        verdict::expect_code_shaped(
            &refused,
            "notifierNotificationEvents with a service passport"
        ),
        "FORBIDDEN",
        "a machine identity must be refused by code, not by an empty stream: {refused}"
    );
    assert!(
        refused["data"]["notifierNotificationEvents"].is_null(),
        "a refused subscription carries no data: {refused}"
    );

    // when: deliveries actually flow to a human while the machine keeps knocking
    ctx.stack
        .publish_deliver(&deliver(&[recipient], "humans_only", json!({})))
        .await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await
    );

    // then: it hears nothing — the refusal is the same verdict, still no event,
    // with live notification traffic in flight
    let refused_again = ctx.instance.subscribe_refused(&service_passport).await;
    assert_eq!(
        verdict::expect_code_shaped(
            &refused_again,
            "notifierNotificationEvents while traffic flows"
        ),
        "FORBIDDEN"
    );
    assert!(
        refused_again["data"]["notifierNotificationEvents"].is_null(),
        "no notification ever reaches a machine identity's stream: {refused_again}"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn an_ordinary_human_reaching_for_another_humans_notification_is_refused_by_code() {
    let ctx = TestContext::setup().await;
    let (intruder, target) = (Uuid::now_v7(), Uuid::now_v7());
    let intruder_passport = make_passport(intruder);
    let target_passport = make_passport(target);

    // given: the target is listening on their own session, and the notification
    // the intruder will reach for lands *after* it opens — created through the
    // real intake, never seeded. Serving it is what proves the session is in the
    // fan-out at all: a stream the service never registered would be silent below
    // for the wrong reason, and would stay silent with the isolation this
    // scenario defends removed.
    let mut target_session = ctx.instance.subscribe(&target_passport).await;
    let targeted = seed_one(&ctx, target, "not_yours").await;
    let landed = target_session
        .expect_event("the target's session is live", RECOVERY_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&landed)["notification"]["id"],
        json!(targeted.to_string()),
        "the session must carry the target's own notification: {landed}"
    );

    // when: an ordinary human aims the unitary mutations at someone else's id,
    // and at an id that is not an id at all. Every unitary door, both shapes:
    // a table that samples one mutation invites the next one in unguarded.
    let cases = [
        (
            "notifierMarkAsRead",
            MARK_AS_READ,
            targeted.to_string(),
            "NOT_FOUND",
        ),
        (
            "notifierDeleteNotification",
            DELETE_ONE,
            targeted.to_string(),
            "NOT_FOUND",
        ),
        (
            "notifierMarkAsRead",
            MARK_AS_READ,
            "not-a-uuid".to_string(),
            "BAD_USER_INPUT",
        ),
        (
            "notifierDeleteNotification",
            DELETE_ONE,
            "not-a-uuid".to_string(),
            "BAD_USER_INPUT",
        ),
    ];

    for (field, mutation, id, expected) in cases {
        let refused = ctx
            .instance
            .graphql(&intruder_passport, mutation, json!({"id": id}))
            .await;
        assert_eq!(
            verdict::expect_code_shaped(&refused, &format!("{field} aimed at {id}")),
            expected,
            "{field} aimed at {id}: a foreign notification must be invisible, never \
             forbidden — telling an intruder the row exists is itself a leak: {refused}"
        );
        assert!(
            refused["data"][field].is_null(),
            "{field} aimed at {id}: a refused mutation carries no result: {refused}"
        );
    }

    // then: the target's row never moved — a refused reach is not a partial one
    let rows = ctx.stack.rows_for(target).await;
    assert_eq!(rows.len(), 1, "the target's notification is still there");
    assert_eq!(rows[0].id, targeted);
    assert!(rows[0].read_at.is_none(), "and still unread");

    // then: nor did any of the target's own channels notice — no push, list and
    // badge exactly as before. A failed intrusion is invisible to its victim.
    target_session
        .expect_silence(
            "a refused reach announces nothing to the owner",
            CONSUME_WAIT,
        )
        .await;
    let listed = ctx
        .instance
        .graphql(&target_passport, LIST_QUERY, json!({}))
        .await;
    let nodes = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"));
    assert_eq!(nodes.len(), 1, "the target still has their notification");
    assert_eq!(nodes[0]["id"], json!(targeted.to_string()));
    assert!(
        nodes[0]["readAt"].is_null(),
        "and it is still unread: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&target_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 1);

    // then: and the intruder gained nothing to look at either
    let intruder_list = ctx
        .instance
        .graphql(&intruder_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        intruder_list["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "the notification the intruder named must not surface in their own list: {intruder_list}"
    );
    let intruder_count = ctx
        .instance
        .graphql(&intruder_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&intruder_count), 0);
}
