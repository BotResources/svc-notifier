mod common;

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
    ];

    for (field, query, vars) in cases {
        let response = ctx.instance.graphql(&service_passport, query, vars).await;
        assert!(
            response["data"][field].is_null(),
            "{field}: a service passport must not get a result: {response}"
        );
        assert_eq!(
            response["errors"][0]["extensions"]["code"], "FORBIDDEN",
            "{field}: a service passport must be rejected with FORBIDDEN before any work: {response}"
        );
    }
}

#[tokio::test]
#[serial_test::serial]
async fn service_passports_get_no_subscription_events() {
    let ctx = TestContext::setup().await;
    let service_passport = make_service_passport(Uuid::now_v7());
    let recipient = Uuid::now_v7();

    let mut subscription = ctx.instance.subscribe(&service_passport).await;
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

    subscription
        .expect_silence("a service is never a recipient")
        .await;
}
