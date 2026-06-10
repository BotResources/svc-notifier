// Trust-boundary scenarios: the service authorizes, never authenticates —
// no Passport means 401 at the middleware, before any resolver runs.
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
async fn health_is_accessible_without_passport() {
    let ctx = TestContext::setup().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/health", ctx.instance.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
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
