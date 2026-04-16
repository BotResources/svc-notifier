mod common;

use reqwest::StatusCode;

#[tokio::test]
#[serial_test::serial]
async fn graphql_without_passport_returns_401() {
    let ctx = common::TestContext::setup().await;

    let (status, _body) = ctx
        .graphql_query_unauthenticated("{ notifierUnreadCount }")
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial_test::serial]
async fn graphql_with_malformed_passport_returns_401() {
    let ctx = common::TestContext::setup().await;

    let status = ctx
        .graphql_query_bad_passport("{ notifierUnreadCount }", "not-valid-base64!!!")
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial_test::serial]
async fn graphql_with_empty_passport_returns_401() {
    let ctx = common::TestContext::setup().await;

    let status = ctx
        .graphql_query_bad_passport("{ notifierUnreadCount }", "")
        .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial_test::serial]
async fn health_accessible_without_passport() {
    let ctx = common::TestContext::setup().await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/health", ctx.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}
