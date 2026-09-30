// Dedup scenario: for one source event and one recipient there is exactly one
// notification — first wins. A later request never changes it and never creates
// a second one, but it still serves the recipients who have none yet.
mod common;

use common::*;
use serde_json::json;
use uuid::Uuid;

// registry test 01a0f2a8-ff57-75cd-939c-a6cd31d5de27
// The first request wins
//
// Given a notification was delivered for a source event.
// When the producer sends the same source event again with different content, the
// person still has one notification, with the first content.
// When the producer sends the same source event again with more recipients, only
// the new recipients get a notification. The other recipients see no change.
#[tokio::test]
#[serial_test::serial]
async fn the_first_request_wins() {
    let ctx = TestContext::setup().await;

    // Given a notification delivered for a source event, and the person's stream
    let person = Uuid::now_v7();
    let mut session = Session::open(&ctx.instance, &make_passport(person)).await;
    let first = deliver(&[person], "first", json!({ "version": 1 }));
    ctx.stack.publish_deliver(&first).await;
    ctx.stack
        .eventually("the first request is stored", || async {
            !ctx.stack.rows_for(person).await.is_empty()
        })
        .await;
    let stored = ctx.stack.rows_for(person).await.remove(0);
    session
        .settle("the person holds the first notification", |view| {
            view.len() == 1
        })
        .await;

    // When the producer sends the same source event again with different content
    let reworded = br_notifier_contract::DeliverNotification {
        template: "second".into(),
        payload: json!({ "version": 2 }),
        ..first.clone()
    };
    ctx.stack.publish_deliver(&reworded).await;
    tokio::time::sleep(QUIET_WINDOW).await;

    // Then the person still has one notification, with the first content
    let rows = ctx.stack.rows_for(person).await;
    assert_eq!(
        rows,
        vec![stored.clone()],
        "storage: nothing new, nothing rewritten"
    );
    session
        .settle("the person still holds the first notification", |view| {
            view.len() == 1
        })
        .await;
    let held = session.get(stored.id).expect("the first notification");
    assert_eq!(held["template"], "first");
    assert_eq!(held["payload"], json!({ "version": 1 }));
    assert_eq!(instant(&held["createdAt"]), stored.created_at);

    // Given another source event delivered to two people, all five people having
    // an open stream
    let people: Vec<Uuid> = (0..5).map(|_| Uuid::now_v7()).collect();
    let mut sessions = Vec::new();
    for who in &people {
        sessions.push(Session::open(&ctx.instance, &make_passport(*who)).await);
    }
    let original = deliver(&people[..2], "original", json!({ "version": 1 }));
    ctx.stack.publish_deliver(&original).await;
    ctx.stack
        .eventually("the first two notifications are stored", || async {
            ctx.stack
                .rows_for_source(original.source_event_id)
                .await
                .len()
                == 2
        })
        .await;
    let before = ctx.stack.rows_for_source(original.source_event_id).await;
    for session in sessions.iter_mut().take(2) {
        session
            .settle("the first two people hold their notification", |view| {
                view.len() == 1
            })
            .await;
    }
    let held_before: Vec<_> = sessions.iter().take(2).map(|s| s.view().clone()).collect();

    // When the producer sends the same source event again with all five people
    // and other content
    let widened = br_notifier_contract::DeliverNotification {
        recipient_ids: people.clone(),
        template: "widened".into(),
        payload: json!({ "version": 2 }),
        ..original.clone()
    };
    ctx.stack.publish_deliver(&widened).await;

    // Then, for this source event, storage holds exactly one notification per
    // person: the two who had one are unchanged
    ctx.stack
        .eventually("the three new people are served", || async {
            ctx.stack
                .rows_for_source(original.source_event_id)
                .await
                .len()
                == 5
        })
        .await;
    let after = ctx.stack.rows_for_source(original.source_event_id).await;
    for who in &people {
        assert_eq!(
            after.iter().filter(|row| row.recipient_id == *who).count(),
            1,
            "exactly one notification per person for the source event"
        );
    }
    for old in &before {
        assert!(
            after.contains(old),
            "a person who had it keeps it unchanged: {old:?}"
        );
    }

    // And the three new people hold one notification with the new content, the
    // two others still hold their unchanged one
    for (index, session) in sessions.iter_mut().enumerate() {
        session
            .settle("each person holds exactly one notification", |view| {
                view.len() == 1
            })
            .await;
        let (_, held) = session.view().iter().next().expect("one notification");
        if index < 2 {
            assert_eq!(
                session.view(),
                &held_before[index],
                "no change for the others"
            );
        } else {
            assert_eq!(held["template"], "widened");
            assert_eq!(held["payload"], json!({ "version": 2 }));
        }
    }
}
