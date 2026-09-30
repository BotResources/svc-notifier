// Intake scenarios: what a producer's deliver request turns into. Every Given is
// built through that request — the only way a notification is ever created — and
// every outcome is read on the stream of the recipients and in storage.
mod common;

use common::*;
use serde_json::{Value, json};
use uuid::Uuid;

// registry test 01a0f2a8-f11d-7e16-9619-1ff2161b2e6a
// A notification arrives whole
//
// Given a person who has an open stream.
// When a producer asks to notify this person with a template, a payload with
// nested data and a link, the notification arrives on the stream with exactly
// these values. It has a creation time and no read time.
// The storage holds one notification with the same values.
#[tokio::test]
#[serial_test::serial]
async fn a_notification_arrives_whole() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    // Given a person who has an open stream: the first payload is an empty list
    let mut session = Session::open(&ctx.instance, &make_passport(recipient)).await;
    assert!(
        session.first_payload().is_empty(),
        "nothing was delivered yet"
    );

    // When a producer asks to notify her with nested data and a link
    let payload = json!({
        "meeting": {
            "id": "m-1",
            "title": "Réunion — été",
            "attendees": ["a", "b"],
            "extra": { "n": 1 }
        }
    });
    let request = br_notifier_contract::DeliverNotification {
        link: relative_link("/meetings/m-1"),
        ..deliver(&[recipient], "meeting_scheduled", payload.clone())
    };
    ctx.stack.publish_deliver(&request).await;

    // Then storage holds exactly one notification with these very values
    ctx.stack
        .eventually("the notification is stored", || async {
            !ctx.stack
                .rows_for_source(request.source_event_id)
                .await
                .is_empty()
        })
        .await;
    let rows = ctx.stack.rows_for_source(request.source_event_id).await;
    assert_eq!(
        rows.len(),
        1,
        "one request, one recipient, one notification"
    );
    let row = &rows[0];
    assert_eq!(row.recipient_id, recipient);
    assert_eq!(row.template, "meeting_scheduled");
    assert_eq!(
        row.payload, payload,
        "nested data and non-ASCII text intact"
    );
    assert_eq!(row.link.as_deref(), Some("/meetings/m-1"));
    assert_eq!(row.read_at, None, "a new notification is unread");

    // And the stream shows it whole
    session
        .settle("the notification arrives on the stream", |view| {
            view.len() == 1
        })
        .await;
    let arrived = session
        .get(row.id)
        .expect("the stream shows the stored notification");
    assert_eq!(arrived["template"], "meeting_scheduled");
    assert_eq!(arrived["payload"], payload);
    assert_eq!(arrived["link"], "/meetings/m-1");
    assert_eq!(arrived["readAt"], Value::Null, "no read time");
    assert_eq!(
        instant(&arrived["createdAt"]),
        row.created_at,
        "the stream carries the creation time of the stored notification"
    );
}

// registry test 01a0f2a8-f82b-7b27-96b4-cc0497bd9262
// One private notification per recipient
//
// Given a producer names three people, and a fourth person is not named.
// When the producer asks to notify them, each of the three people gets one
// private notification. Each person sees only their own notification on their
// stream.
// The fourth person sees nothing, not on the stream and not in the list.
#[tokio::test]
#[serial_test::serial]
async fn one_private_notification_per_recipient() {
    let ctx = TestContext::setup().await;
    let named: Vec<Uuid> = (0..3).map(|_| Uuid::now_v7()).collect();
    let bystander = Uuid::now_v7();

    // Given streams open for the three named people and for the fourth
    let mut sessions = Vec::new();
    for person in &named {
        sessions.push(Session::open(&ctx.instance, &make_passport(*person)).await);
    }
    let mut bystander_session = Session::open(&ctx.instance, &make_passport(bystander)).await;

    // When the producer asks to notify the three named people
    let request = deliver(&named, "fanout", json!({ "kind": "fan-out" }));
    ctx.stack.publish_deliver(&request).await;

    // Then storage holds exactly three notifications, one per named person, and
    // none for the fourth
    ctx.stack
        .eventually("the three notifications are stored", || async {
            ctx.stack
                .rows_for_source(request.source_event_id)
                .await
                .len()
                == 3
        })
        .await;
    let rows = ctx.stack.rows_for_source(request.source_event_id).await;
    let mut ids = Vec::new();
    for person in &named {
        let own: Vec<_> = rows
            .iter()
            .filter(|row| row.recipient_id == *person)
            .collect();
        assert_eq!(
            own.len(),
            1,
            "exactly one notification for each named person"
        );
        ids.push(own[0].id);
    }
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        3,
        "the three notifications are three distinct ones"
    );
    assert!(ctx.stack.rows_for(bystander).await.is_empty());

    // And each named person sees only her own notification on her stream
    for (person, session) in named.iter().zip(sessions.iter_mut()) {
        let own_id = rows
            .iter()
            .find(|row| row.recipient_id == *person)
            .map(|row| row.id.to_string())
            .expect("her stored notification");
        session
            .settle("her own notification arrives", |view| {
                view.contains_key(&own_id)
            })
            .await;
        assert_eq!(
            session.view().keys().collect::<Vec<_>>(),
            vec![&own_id],
            "she sees her own notification and no other"
        );
    }

    // And the fourth person sees nothing: silence on the stream, an empty first
    // payload on a new stream, and an empty list
    bystander_session
        .expect_no_delta("a person who is not named hears nothing")
        .await;
    assert!(bystander_session.view().is_empty());
    let reopened = Session::open(&ctx.instance, &make_passport(bystander)).await;
    assert!(
        reopened.first_payload().is_empty(),
        "a new stream starts empty too"
    );
    let listed_response = ctx
        .instance
        .graphql(&make_passport(bystander), LIST_QUERY, json!({}))
        .await;
    assert!(listed(&listed_response).is_empty(), "the list is empty too");
}
