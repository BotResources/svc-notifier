// Door scenarios: who gets in. A caller with no readable identity is refused at
// the transport; a machine identity is refused by code on every door — the list,
// the stream and the four mark / delete actions — and its refusal leaves the
// notification of the person exactly as it was.
mod common;

use common::*;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

// registry test 01a0f2a8-e19c-792c-89fa-f45b0af4ec24
// Only a valid identity gets in
//
// Given a caller with no identity, a caller with a garbled identity, and a caller
// with an empty identity.
// When each caller asks for the notification list, each request is refused and no
// data comes back.
// The liveness check of the service still answers a caller with no identity.
#[tokio::test]
#[serial_test::serial]
async fn only_a_valid_identity_gets_in() {
    let ctx = TestContext::setup().await;

    // Given a person who owns a notification — so that a refusal is a refusal to
    // hand something over, not an empty answer to an empty inbox
    let owner = Uuid::now_v7();
    let owned = deliver_to(
        &ctx,
        owner,
        "owned",
        json!({"secret": "for-the-owner-only"}),
    )
    .await;

    let list = "{ notifierNotifications { id template payload link readAt createdAt } }";

    // When a caller with no identity asks for the list
    let (status, body) = ctx.instance.graphql_unauthenticated(list).await;
    // Then it is refused and no data comes back
    assert_refused_without_data("no identity", status, &body, owned.id);

    // When a caller with a garbled identity asks for the list
    let (status, body) = ctx
        .instance
        .graphql_with_header("not-valid-base64!!!", list)
        .await;
    assert_refused_without_data("a garbled identity", status, &body, owned.id);

    // When a caller with an empty identity asks for the list
    let (status, body) = ctx.instance.graphql_with_header("", list).await;
    assert_refused_without_data("an empty identity", status, &body, owned.id);

    // And the liveness check answers a caller with no identity
    let (status, _) = ctx.instance.get("/livez").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the liveness check needs no identity"
    );
}

fn assert_refused_without_data(who: &str, status: StatusCode, body: &Value, owned_id: Uuid) {
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "{who}: the request must be refused: {body}"
    );
    assert!(
        body.get("data").is_none_or(Value::is_null),
        "{who}: no data may come back: {body}"
    );
    assert!(
        !body.to_string().contains(&owned_id.to_string()),
        "{who}: a refusal must not leak the notification: {body}"
    );
}

// registry test 01a0f2a8-e994-70fd-9c2c-b4b033f0febd
// A machine identity gets nothing
//
// Given a platform service that uses its own machine identity, and a person who
// has one notification.
// When the service asks for the list, subscribes to the stream, or tries to mark
// or delete the notification, each attempt is refused with a forbidden error and
// no data.
// The notification of the person does not change. A notification delivered to
// that person never reaches the service.
#[tokio::test]
#[serial_test::serial]
async fn a_machine_identity_gets_nothing() {
    let ctx = TestContext::setup().await;
    let service = make_service_passport(Uuid::now_v7());

    // Given a person with one unread notification, and her own stream as control
    let person = Uuid::now_v7();
    let person_passport = make_passport(person);
    let owned = deliver_to(&ctx, person, "humans_only", json!({})).await;
    let n = owned.id.to_string();
    let mut control = Session::open(&ctx.instance, &person_passport).await;
    control
        .settle("the person holds her notification", |view| view.len() == 1)
        .await;
    assert_eq!(control.get(&n).expect("held")["readAt"], Value::Null);

    // When the service asks for the list
    let response = ctx.instance.graphql(&service, LIST_QUERY, json!({})).await;
    // Then it is refused with FORBIDDEN and gets no data
    expect_forbidden(&response, "notifierNotifications");

    // When the service tries to mark or delete the notification
    let attempts = [
        ("notifierMarkAsRead", MARK_AS_READ, json!({ "id": n })),
        ("notifierMarkAllAsRead", MARK_ALL_AS_READ, json!({})),
        ("notifierDeleteNotification", DELETE_ONE, json!({ "id": n })),
        (
            "notifierDeleteNotifications",
            DELETE_MANY,
            json!({ "ids": [n] }),
        ),
    ];
    for (field, query, vars) in attempts {
        let response = ctx.instance.graphql(&service, query, vars).await;
        // Then each attempt is refused with FORBIDDEN and gets no data
        expect_forbidden(&response, field);
    }

    // When the service subscribes to the stream
    let refused = ctx.instance.subscribe_refused(&service).await;
    // Then the subscription is refused with FORBIDDEN and no data — not opened
    // with an empty first payload, which would read as "you have no notifications"
    // rather than "you may not have any"
    expect_forbidden(&refused, "notifierNotificationDeltas");

    // Then the notification of the person did not change: in storage, on her
    // stream, and on demand
    let row = ctx
        .stack
        .row(owned.id)
        .await
        .expect("the row is still stored");
    assert_eq!(
        row.read_at, None,
        "the refused attempts must not mark it read"
    );
    control
        .expect_no_delta("the refused attempts change nothing for the person")
        .await;
    assert_eq!(control.get(&n).expect("still held")["readAt"], Value::Null);
    let listed = ctx
        .instance
        .graphql(&person_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(listed_ids(&listed), vec![n.clone()]);

    // When a notification is delivered to the person, the delivery is live: her
    // stream gets it
    let second = deliver_to(&ctx, person, "humans_only_2", json!({})).await;
    control
        .settle("the second notification reaches the person", |view| {
            view.contains_key(&second.id.to_string())
        })
        .await;

    // Then a machine identity that subscribes now is refused the same way: one
    // FORBIDDEN frame and the stream ends, so nothing is left open to receive what
    // is delivered (`subscribe_refused` reads the answer under a bound and demands
    // exactly one frame)
    let refused_again = ctx.instance.subscribe_refused(&service).await;
    expect_forbidden(&refused_again, "notifierNotificationDeltas");
}

fn listed_ids(response: &Value) -> Vec<String> {
    listed(response)
        .iter()
        .map(|node| node["id"].as_str().expect("a string id").to_owned())
        .collect()
}
