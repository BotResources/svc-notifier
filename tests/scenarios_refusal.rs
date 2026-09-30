// Refusal scenarios: a deliver request the service will not honour creates
// nothing for anyone — the request is refused as a whole, never trimmed to the
// recipients or the fields that happened to be fine.
//
// The frames here are what a non-compliant or version-skewed producer puts on
// the wire: `br-notifier-publisher` cannot build them (the contract refuses the
// link at construction), so they are published as raw envelopes on the very same
// typed coordinates.
mod common;

use common::*;
use std::collections::BTreeSet;

use serde_json::{Value, json};
use uuid::Uuid;

fn raw_request(source_event_id: Uuid, recipients: &[Uuid], link: &str) -> Value {
    json!({
        "source_event_id": source_event_id,
        "recipient_ids": recipients,
        "template": "meeting_scheduled",
        "payload": { "meeting_id": "m-1" },
        "link": link,
    })
}

// registry test 01a0f2a9-3a54-7d67-a550-26ecadf07b21
// A link that is not a relative path refuses the whole request
//
// Given a producer asks to notify two people with a link that is not a relative
// path, for example a full web address.
// When the request arrives, the service refuses the whole request. Nobody gets a
// notification.
// A request with a relative link that starts with a single slash works.
#[tokio::test]
#[serial_test::serial]
async fn a_link_that_is_not_a_relative_path_refuses_the_whole_request() {
    let ctx = TestContext::setup().await;
    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
    let mut sessions = [
        Session::open(&ctx.instance, &make_passport(first)).await,
        Session::open(&ctx.instance, &make_passport(second)).await,
    ];

    // When the producer asks to notify both with a link that breaks the rule of
    // the offer — one frame per refusal class, everything else identical to the
    // valid frame below: not rooted (a full web address, a script scheme, a path
    // with no leading slash, the empty string), scheme-relative, a backslash, and
    // whitespace or control characters (space, tab, line feed)
    let refused_links = [
        "https://evil.example/phish",
        "//evil.example/x",
        "javascript:alert(1)",
        "meetings/m-1",
        "/\\evil.example",
        "/a b",
        "/a\tb",
        "/a\nb",
        "",
    ];
    for link in refused_links {
        let request = raw_request(Uuid::now_v7(), &[first, second], link);
        ctx.stack.publish_raw_deliver(&request).await;
    }

    // And then asks with a relative link that starts with a single slash, in a
    // frame that differs from the refused ones by the link alone
    let valid = br_notifier_contract::DeliverNotification {
        link: relative_link("/meetings/m-1"),
        ..deliver(
            &[first, second],
            "meeting_scheduled",
            json!({ "meeting_id": "m-1" }),
        )
    };
    ctx.stack.publish_deliver(&valid).await;

    // And the service has taken every frame off the stream: the absences below
    // are only worth asserting once the refused frames have reached it
    ctx.stack
        .wait_commands_received(refused_links.len() as u64 + 1)
        .await;

    // Then the valid request works: one notification each, with its link
    ctx.stack
        .eventually("the valid request is stored for both people", || async {
            ctx.stack.rows_for_source(valid.source_event_id).await.len() == 2
        })
        .await;
    for session in &mut sessions {
        session
            .settle("the valid notification arrives", |view| view.len() == 1)
            .await;
        let (_, arrived) = session.view().iter().next().expect("one notification");
        assert_eq!(arrived["link"], "/meetings/m-1");
    }

    // And nobody got anything from the refused requests: in storage, on the
    // stream, and on demand — the valid notification is the only one there is
    for person in [first, second] {
        let rows = ctx.stack.rows_for(person).await;
        assert_eq!(
            rows.len(),
            1,
            "a refused request creates nothing for anyone"
        );
        assert_eq!(rows[0].source_event_id, valid.source_event_id);
        let response = ctx
            .instance
            .graphql(&make_passport(person), LIST_QUERY, json!({}))
            .await;
        assert_eq!(listed(&response).len(), 1);
    }
    // And on the stream the valid notification is the only notification that was
    // ever shown, in a first payload, in a Reset or in an Upsert
    for person in [first, second] {
        let valid_id = ctx.stack.rows_for(person).await[0].id.to_string();
        let session = &sessions[usize::from(person == second)];
        assert_eq!(
            session.ids_seen(),
            BTreeSet::from([valid_id]),
            "the wire showed the valid notification and nothing else: {:#?}",
            session.deltas()
        );
    }
}
