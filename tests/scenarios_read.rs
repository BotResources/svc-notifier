// Read scenarios: what a recipient does to the read state of her own
// notifications. A mutation only confirms success; the new state arrives on the
// stream, for every open session of the recipient alike. Every outcome is judged
// on the folded view of the sessions and in storage — never on how many events
// carried it.
mod common;

use common::*;
use std::collections::BTreeMap;

use serde_json::{Value, json};
use uuid::Uuid;

fn read_at(view: &Value) -> &Value {
    &view["readAt"]
}

type Folded = BTreeMap<String, Value>;

// A view with its read time taken out: what a read must leave untouched.
fn without_read_at(view: &Value) -> Value {
    let mut copy = view.clone();
    copy.as_object_mut()
        .expect("a view is an object")
        .remove("readAt");
    copy
}

// After a read the folded view is the view from before, and ONLY the read time
// of the `marked` notifications moved (null before, set after). Everything else
// — the same ids, and for every id the template, the payload, the link and the
// creation time — is untouched; a notification that was not marked keeps its
// read time as it was.
fn assert_only_read_at_changed(before: &Folded, after: &Folded, marked: &[&str], what: &str) {
    assert_eq!(
        after.keys().collect::<Vec<_>>(),
        before.keys().collect::<Vec<_>>(),
        "{what}: a read neither adds nor removes a notification"
    );
    for (id, view) in before {
        let now = &after[id];
        assert_eq!(
            without_read_at(now),
            without_read_at(view),
            "{what}: {id} changed by more than its read time"
        );
        if marked.contains(&id.as_str()) {
            assert!(
                read_at(view).is_null() && !read_at(now).is_null(),
                "{what}: {id} goes from unread to read: {view} -> {now}"
            );
        } else {
            assert_eq!(
                read_at(now),
                read_at(view),
                "{what}: {id} was not marked, its read time stays"
            );
        }
    }
}

// registry test 01a0f2a9-1cb4-71a1-8767-a6f7af8e6d64
// Marking one as read reaches every session
//
// Given a person with two open sessions and one unread notification.
// When the person marks the notification as read in the first session, the call
// only confirms success. Both sessions then show the notification with a read
// time.
// When the person marks it as read again, the read time does not change.
#[tokio::test]
#[serial_test::serial]
async fn marking_one_as_read_reaches_every_session() {
    let ctx = TestContext::setup().await;
    let person = Uuid::now_v7();
    let passport = make_passport(person);

    // Given two open sessions and unread notifications (one is marked, the other
    // shows that only the named one changes)
    // (each one with a link and nested data, so "only the read time changed" has
    // other fields to lose)
    let marked = deliver_rich_to(&ctx, person, "to_mark").await;
    let other = deliver_rich_to(&ctx, person, "left_alone").await;
    let (n, m) = (marked.id.to_string(), other.id.to_string());
    let mut first = Session::open(&ctx.instance, &passport).await;
    let mut second = Session::open(&ctx.instance, &passport).await;
    for session in [&mut first, &mut second] {
        session
            .settle("both are held", |view| view.len() == 2)
            .await;
        assert_eq!(read_at(session.get(&n).expect("held")), &Value::Null);
    }

    // And a bystander: another person, with her own open stream and her own
    // unread notification
    let bystander = Uuid::now_v7();
    let bystander_own = deliver_rich_to(&ctx, bystander, "not_yours").await;
    let mut watcher = Session::open(&ctx.instance, &make_passport(bystander)).await;
    watcher
        .settle("the bystander holds her notification", |view| {
            view.len() == 1
        })
        .await;

    // And the folded views before the read, links and nested data included
    let before: Vec<Folded> = [&first, &second]
        .iter()
        .map(|session| session.view().clone())
        .collect();
    assert!(
        before[0][&n]["link"] == json!("/meetings/to_mark")
            && before[0][&n]["payload"]["meeting"]["attendees"] == json!(["a", "b"]),
        "the views carry their link and nested data before the read: {:#?}",
        before[0]
    );

    // When she marks the notification as read
    let response = ctx
        .instance
        .graphql(&passport, MARK_AS_READ, json!({ "id": n }))
        .await;

    // Then the call only confirms success
    expect_ack_only(&response, "notifierMarkAsRead");

    // And both sessions show it with a read time, and the other one unread
    for session in [&mut first, &mut second] {
        session
            .settle("the notification shows read", |view| {
                view.get(&n).is_some_and(|v| !read_at(v).is_null())
                    && view.get(&m).is_some_and(|v| read_at(v).is_null())
            })
            .await;
    }
    let read_time = instant(read_at(first.get(&n).expect("held")));
    assert_eq!(instant(read_at(second.get(&n).expect("held"))), read_time);

    // And nothing else moved: each session shows the view it showed before, with
    // only the read time of the marked notification changed
    for (session, before) in [&first, &second].into_iter().zip(&before) {
        assert_only_read_at_changed(before, session.view(), &[&n], "marking one as read");
    }

    // And the bystander was told nothing: no delta, her notification still unread
    watcher
        .expect_no_delta("another person's read is none of hers")
        .await;
    assert_eq!(
        read_at(watcher.get(bystander_own.id).expect("held")),
        &Value::Null
    );

    // And storage holds the same read time, the other notification is unread
    let row = ctx.stack.row(marked.id).await.expect("stored");
    assert_eq!(row.read_at, Some(read_time));
    assert_eq!(ctx.stack.row(other.id).await.expect("stored").read_at, None);

    // When she marks it as read again
    let response = ctx
        .instance
        .graphql(&passport, MARK_AS_READ, json!({ "id": n }))
        .await;
    expect_ack_only(&response, "notifierMarkAsRead");

    // Then the read time does not change — on either session, in storage — and
    // the second marking sends no event
    for session in [&mut first, &mut second] {
        session
            .expect_no_delta("marking a read notification again changes nothing")
            .await;
        assert_eq!(instant(read_at(session.get(&n).expect("held"))), read_time);
    }
    assert_eq!(
        ctx.stack.row(marked.id).await.expect("stored").read_at,
        Some(read_time)
    );
    watcher
        .expect_no_delta("the bystander hears nothing of the second marking either")
        .await;
}

// registry test 01a0f2a9-2398-7551-8100-33f86136b768
// Marking all as read
//
// Given a person with three unread notifications and one notification that is
// already read.
// When the person marks all as read, all four notifications show as read on every
// open session. The storage holds all four as read.
// The notification that was already read keeps its first read time.
#[tokio::test]
#[serial_test::serial]
async fn marking_all_as_read() {
    let ctx = TestContext::setup().await;
    let person = Uuid::now_v7();
    let passport = make_passport(person);

    // Given one notification already read, and three unread ones
    // (each one with a link and nested data)
    let already = deliver_rich_to(&ctx, person, "already_read").await;
    let response = ctx
        .instance
        .graphql(
            &passport,
            MARK_AS_READ,
            json!({ "id": already.id.to_string() }),
        )
        .await;
    expect_ack_only(&response, "notifierMarkAsRead");
    let first_read_time = {
        ctx.stack
            .eventually("the first read time is stored", || async {
                ctx.stack
                    .row(already.id)
                    .await
                    .is_some_and(|row| row.read_at.is_some())
            })
            .await;
        ctx.stack
            .row(already.id)
            .await
            .expect("stored")
            .read_at
            .expect("read")
    };
    let mut ids = vec![already.id.to_string()];
    for template in ["unread_1", "unread_2", "unread_3"] {
        ids.push(deliver_rich_to(&ctx, person, template).await.id.to_string());
    }

    // And two open sessions
    let mut first = Session::open(&ctx.instance, &passport).await;
    let mut second = Session::open(&ctx.instance, &passport).await;
    for session in [&mut first, &mut second] {
        session
            .settle("the four are held", |view| view.len() == 4)
            .await;
    }

    // And a bystander: another person, with her own open stream and her own
    // unread notification
    let bystander = Uuid::now_v7();
    let bystander_own = deliver_rich_to(&ctx, bystander, "not_yours").await;
    let mut watcher = Session::open(&ctx.instance, &make_passport(bystander)).await;
    watcher
        .settle("the bystander holds her notification", |view| {
            view.len() == 1
        })
        .await;

    // And the folded views before the read
    let before: Vec<Folded> = [&first, &second]
        .iter()
        .map(|session| session.view().clone())
        .collect();
    for view in &before {
        assert!(
            view.values()
                .all(|v| v["link"].is_string() && v["payload"]["meeting"].is_object()),
            "the views carry their link and nested data before the read: {view:#?}"
        );
        assert_eq!(
            view.values().filter(|v| !read_at(v).is_null()).count(),
            1,
            "exactly one of the four is read before"
        );
    }

    // When she marks all as read
    let response = ctx
        .instance
        .graphql(&passport, MARK_ALL_AS_READ, json!({}))
        .await;
    expect_ack_only(&response, "notifierMarkAllAsRead");

    // Then all four show as read on every session — however many events it took
    for session in [&mut first, &mut second] {
        session
            .settle("all four show as read", |view| {
                view.len() == 4 && view.values().all(|v| !read_at(v).is_null())
            })
            .await;

        // And the one that was already read keeps its first read time
        assert_eq!(
            instant(read_at(session.get(&ids[0]).expect("held"))),
            first_read_time
        );
    }

    // And nothing else moved: each session shows the view it showed before, with
    // only the read time of the three unread ones changed
    let newly_read: Vec<&str> = ids[1..].iter().map(String::as_str).collect();
    for (session, before) in [&first, &second].into_iter().zip(&before) {
        assert_only_read_at_changed(before, session.view(), &newly_read, "marking all as read");
    }

    // And the bystander was told nothing: no delta, her notification still unread
    watcher
        .expect_no_delta("another person's read-all is none of hers")
        .await;
    assert_eq!(
        read_at(watcher.get(bystander_own.id).expect("held")),
        &Value::Null
    );
    assert_eq!(
        ctx.stack
            .row(bystander_own.id)
            .await
            .expect("stored")
            .read_at,
        None
    );

    // And storage holds all four as read, the first read time untouched
    let rows = ctx.stack.rows_for(person).await;
    assert_eq!(rows.len(), 4);
    assert!(
        rows.iter().all(|row| row.read_at.is_some()),
        "all four are read in storage"
    );
    assert_eq!(
        ctx.stack.row(already.id).await.expect("stored").read_at,
        Some(first_read_time)
    );
}
