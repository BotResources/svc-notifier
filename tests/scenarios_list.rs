// On-demand list: for the clients that cannot subscribe (an AI assistant, say).
// The same private, newest-first list the stream opens with, fetched in one call
// with no argument.
mod common;

use common::*;
use serde_json::{Value, json};
use uuid::Uuid;

// registry test 01a0f2a9-4187-71fb-8864-d098a0914196
// The list can be fetched on demand
//
// Given a person with a few notifications, and another person with one
// notification.
// When the person fetches the list, the answer holds only the notifications of
// this person, newest first.
#[tokio::test]
#[serial_test::serial]
async fn the_list_can_be_fetched_on_demand() {
    let ctx = TestContext::setup().await;
    let (person, other) = (Uuid::now_v7(), Uuid::now_v7());

    // Given five notifications for her, delivered one after the other so that the
    // creation times differ, and one for the other person
    let mut own = Vec::new();
    for n in 1..=5 {
        let request = br_notifier_contract::DeliverNotification {
            link: relative_link(&format!("/items/{n}")),
            ..deliver(
                &[person],
                &format!("item_{n}"),
                json!({ "n": n, "note": "é" }),
            )
        };
        let source = request.source_event_id;
        ctx.stack.publish_deliver(&request).await;
        ctx.stack
            .eventually("the delivery is stored", || async {
                !ctx.stack.rows_for_source(source).await.is_empty()
            })
            .await;
        own.push(ctx.stack.rows_for_source(source).await.remove(0));
    }
    let foreign = deliver_to(&ctx, other, "someone_elses", json!({})).await;

    // When she fetches the list
    let response = ctx
        .instance
        .graphql(&make_passport(person), LIST_QUERY, json!({}))
        .await;

    // Then it holds exactly her five notifications, newest first, with the values
    // that are stored
    let nodes = listed(&response);
    assert_eq!(nodes.len(), 5, "only her notifications: {response}");
    let expected: Vec<_> = own.iter().rev().collect();
    for (node, row) in nodes.iter().zip(expected) {
        assert_eq!(node["id"], json!(row.id.to_string()), "newest first");
        assert_eq!(node["template"], json!(row.template));
        assert_eq!(node["payload"], row.payload);
        assert_eq!(node["link"], json!(row.link));
        assert_eq!(node["readAt"], Value::Null);
        assert_eq!(instant(&node["createdAt"]), row.created_at);
    }
    assert!(
        nodes
            .iter()
            .all(|node| node["id"] != json!(foreign.id.to_string())),
        "the notification of another person never shows"
    );

    // And the other person gets her own single notification, and only that one
    let response = ctx
        .instance
        .graphql(&make_passport(other), LIST_QUERY, json!({}))
        .await;
    let nodes = listed(&response);
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["id"], json!(foreign.id.to_string()));
}
