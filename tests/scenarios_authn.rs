mod common;

use br_test_harness::verdict;
use common::*;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

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
            "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) }",
            json!({"id": Uuid::now_v7().to_string()}),
        ),
        (
            "notifierDeleteNotification",
            "mutation($id: ID!) { notifierDeleteNotification(notificationId: $id) }",
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
