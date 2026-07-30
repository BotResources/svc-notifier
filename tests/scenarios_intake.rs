mod common;

use br_notifier_contract::{DeliverNotification, RelativeLink};
use br_test_harness::BareFabricNats;
use chrono::Utc;
use common::*;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[serial_test::serial]
async fn s01_deliver_command_reaches_the_recipient_on_all_three_envelopes() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let mut subscription = ctx.instance.subscribe(&passport).await;

    // when: a fully-populated deliver command is published on the deliver coords
    let mut command = deliver(
        &[recipient],
        "meeting_scheduled",
        json!({"meeting_id": "m-1"}),
    );
    command.link = Some(RelativeLink::parse("/meetings/m-1").unwrap());
    ctx.stack.publish_deliver(&command).await;

    // then: PG — exactly one row, exact content, unread
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await,
        "expected exactly 1 notification row"
    );
    let rows = ctx.stack.notification_rows().await;
    assert_eq!(rows[0].source_event_id, command.source_event_id);
    assert_eq!(rows[0].recipient_id, recipient);
    assert_eq!(rows[0].template, "meeting_scheduled");
    assert_eq!(rows[0].payload, json!({"meeting_id": "m-1"}));
    assert_eq!(rows[0].link.as_deref(), Some("/meetings/m-1"));
    assert_eq!(rows[0].read_at, None);

    // then: GraphQL — subscription push, query and unread count agree
    let raw = subscription
        .expect_event("NotificationAdded", SSE_TIMEOUT)
        .await;
    let event = notifier_event(&raw);
    assert_eq!(event["__typename"], "NotificationAdded");
    let pushed = &event["notification"];
    assert_eq!(pushed["id"], json!(rows[0].id));
    assert_eq!(pushed["template"], "meeting_scheduled");
    assert_eq!(pushed["payload"], json!({"meeting_id": "m-1"}));
    assert_eq!(pushed["link"], "/meetings/m-1");

    let listed = ctx.instance.graphql(&passport, LIST_QUERY, json!({})).await;
    let nodes = listed["data"]["notifierNotifications"]["nodes"]
        .as_array()
        .unwrap_or_else(|| panic!("no nodes in {listed}"));
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["link"], "/meetings/m-1");

    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 1);
}

#[tokio::test]
#[serial_test::serial]
async fn s02_multi_recipient_fans_out_one_row_each_and_isolates_subscribers() {
    let ctx = TestContext::setup().await;
    let (alice, bob, carol) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut alice_sub = ctx.instance.subscribe(&make_passport(alice)).await;
    let mut bob_sub = ctx.instance.subscribe(&make_passport(bob)).await;

    ctx.stack
        .publish_deliver(&deliver(&[alice, bob, carol], "fanout", json!({})))
        .await;

    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 3
            })
            .await,
        "expected 3 rows, one per recipient"
    );
    for recipient in [alice, bob, carol] {
        assert_eq!(ctx.stack.rows_for(recipient).await.len(), 1);
    }

    let alice_raw = alice_sub
        .expect_event("alice's NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&alice_raw)["notification"]["template"],
        "fanout"
    );
    let bob_raw = bob_sub
        .expect_event("bob's NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(
        notifier_event(&bob_raw)["notification"]["template"],
        "fanout"
    );
    alice_sub
        .expect_silence("only one event per recipient", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s03_duplicate_source_event_first_wins_even_with_a_different_payload() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let mut subscription = ctx.instance.subscribe(&passport).await;

    let first = deliver(&[recipient], "first", json!({"version": 1}));
    ctx.stack.publish_deliver(&first).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 1
            })
            .await
    );

    // when: the same source event arrives again with different content
    let duplicate = DeliverNotification {
        source_event_id: first.source_event_id,
        recipient_ids: vec![recipient],
        template: "second".to_string(),
        payload: json!({"version": 2}),
        link: None,
    };
    ctx.stack.publish_deliver(&duplicate).await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: PG — still one row, first content untouched
    let rows = ctx.stack.notification_rows().await;
    assert_eq!(rows.len(), 1, "dedup must keep exactly one row");
    assert_eq!(rows[0].template, "first");
    assert_eq!(rows[0].payload, json!({"version": 1}));

    // then: GraphQL — one notification, one push (no duplicate event)
    let raw = subscription
        .expect_event("the first NotificationAdded", SSE_TIMEOUT)
        .await;
    assert_eq!(notifier_event(&raw)["notification"]["template"], "first");
    subscription
        .expect_silence("no push for the deduplicated message", CONSUME_WAIT)
        .await;
    let count = ctx
        .instance
        .graphql(&passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 1);
}

#[tokio::test]
#[serial_test::serial]
async fn s08_nothing_consumes_the_legacy_subject() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    ctx.stack
        .publish_dead_subject(
            LEGACY_SUBJECT,
            &serde_json::to_vec(&json!({
                "source_event_id": Uuid::now_v7(),
                "recipient_ids": [recipient],
                "template": "legacy",
                "payload": {},
            }))
            .unwrap(),
        )
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "a legacy notify.deliver message must not be consumed"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s14_redelivery_of_a_partially_applied_batch_completes_without_duplicates() {
    let ctx = TestContext::setup().await;
    let recipients: Vec<Uuid> = (0..5).map(|_| Uuid::now_v7()).collect();
    let passport = make_passport(recipients[0]);
    let mut first_recipient_sub = ctx.instance.subscribe(&passport).await;

    // given: the batch was partially applied (two recipients already inserted)
    let source_event_id = Uuid::now_v7();
    let partial = DeliverNotification {
        source_event_id,
        recipient_ids: recipients[..2].to_vec(),
        template: "batch".to_string(),
        payload: json!({}),
        link: None,
    };
    ctx.stack.publish_deliver(&partial).await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 2
            })
            .await
    );
    let first_created_at = ctx.stack.rows_for(recipients[0]).await[0].created_at;

    // when: the full batch is delivered again (redelivery semantics)
    let full = DeliverNotification {
        source_event_id,
        recipient_ids: recipients.clone(),
        template: "batch".to_string(),
        payload: json!({}),
        link: None,
    };
    ctx.stack.publish_deliver(&full).await;

    // then: PG — exactly five rows, the pre-existing ones untouched
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.count_rows().await == 5
            })
            .await,
        "redelivery must complete the remaining recipients"
    );
    for recipient in &recipients {
        assert_eq!(
            ctx.stack.rows_for(*recipient).await.len(),
            1,
            "zero duplicates"
        );
    }
    assert_eq!(
        ctx.stack.rows_for(recipients[0]).await[0].created_at,
        first_created_at,
        "already-inserted rows are not rewritten"
    );

    // then: GraphQL — the already-served recipient gets exactly one push
    first_recipient_sub
        .expect_event("the initial NotificationAdded", SSE_TIMEOUT)
        .await;
    first_recipient_sub
        .expect_silence("no duplicate push on redelivery", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s15_service_fails_loud_when_the_command_stream_is_absent() {
    // given: a broker with INTEGRATION_EVT but NO INTEGRATION_CMD — the lib
    // never auto-provisions a stream
    let bare = BareFabricNats::with_only_event_stream().await;

    // when: a real svc-notifier is spawned against it (intake enabled)
    let boot = spawn_against_bare_broker(&bare.url()).await;

    // then: it fails loud — the process exits non-zero and never serves /readyz
    // (binding the deliver consumer against the missing fixed stream errors out,
    // so readiness is never set and the service does not come up)
    assert!(
        !boot.outcome.is_ready(),
        "service must NOT become ready without INTEGRATION_CMD; logs:\n{}",
        boot.logs
    );
    if let Some(status) = boot.outcome.exit_status() {
        assert!(
            !status.success(),
            "service must exit non-zero when the command stream is absent; logs:\n{}",
            boot.logs
        );
    }

    bare.shutdown().await;
}

// A command a compliant producer can emit and the contract accepts, but that
// PostgreSQL can never store: a NUL character inside the `payload` is not
// representable in `jsonb` (SQLSTATE 22P05), and no redelivery will ever change
// that. The NUL rides the payload and not the `template`, which is where it used
// to sit: an unusable template is now refused up front as `template_rejected`
// (s31), before any write — a different path from this one, which is about a
// write PostgreSQL itself refuses.
fn poison(recipients: &[Uuid], marker: &str) -> DeliverNotification {
    deliver(
        recipients,
        "meeting_scheduled",
        json!({"why": "nul byte", "marker": format!("poison\u{0}{marker}")}),
    )
}

#[tokio::test]
#[serial_test::serial]
async fn s20_the_ledger_keeps_one_line_per_abandoned_command_not_per_source_event() {
    let ctx = TestContext::setup().await;
    let early = [Uuid::now_v7(), Uuid::now_v7()];
    let late = [Uuid::now_v7()];

    // given: a recipient named by the first command has a session open before
    // anything is published — an abandonment must be silent on every channel a
    // recipient can observe, not merely absent from the table
    let abandoned_recipient = make_passport(early[0]);
    let mut abandoned_session = subscribe_live(&ctx, early[0]).await;

    // given: one source event fanned out as two distinct deliver commands — the
    // chunked / late-added-recipient shape s14 already exercises. The business
    // dedup key is (source_event, recipient), so these are two legitimate
    // commands, and both are poison.
    let source_event_id = Uuid::now_v7();
    let mut first = poison(&early, "chunk_one");
    first.source_event_id = source_event_id;
    let mut second = poison(&late, "chunk_two");
    second.source_event_id = source_event_id;

    let first_command_id = Uuid::now_v7();
    let causing_event_id = Uuid::now_v7();
    let trace = Trace::caused_by(causing_event_id);
    ctx.stack
        .publish_deliver_envelope(first_command_id, &first, trace)
        .await;
    ctx.stack
        .publish_deliver_envelope(Uuid::now_v7(), &second, Trace::fresh())
        .await;

    // then: TWO audit lines — deduplicating on source_event_id alone would have
    // terminated the second command with no trace at all
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 2
            })
            .await,
        "every abandoned command must leave its own trace; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    let recorded = ledger
        .iter()
        .find(|row| row.command_id == first_command_id)
        .expect("the first command has its own line");
    let other = ledger
        .iter()
        .find(|row| row.command_id != first_command_id)
        .expect("the second command has its own line");
    assert_eq!(recorded.source_event_id, Some(source_event_id));
    assert_eq!(other.source_event_id, Some(source_event_id));
    assert_eq!(
        recorded.recipient_ids, early,
        "each line names exactly the recipients its own command abandoned"
    );
    assert_eq!(other.recipient_ids, late);
    assert_eq!(recorded.reason, REASON_STORAGE_REJECTED);

    // then: the trace is faithful — the whole command round-trips, not just the id
    assert_eq!(
        recorded.command(),
        serde_json::to_value(&first).unwrap(),
        "an operator must be able to re-emit the command verbatim"
    );
    // then: the causality the producer published is kept whole — re-emitting the
    // command verbatim is only half the promise if the chain it belonged to is
    // lost, and the row must say when the abandonment happened
    assert_eq!(recorded.correlation_id, trace.correlation_id);
    assert_eq!(
        recorded.causation_id,
        Some(causing_event_id),
        "the event that caused the command is part of the trace"
    );
    assert_eq!(
        other.causation_id, None,
        "a command with no declared cause records none, rather than inventing one"
    );
    let age = Utc::now() - recorded.recorded_at;
    assert!(
        age >= chrono::Duration::zero() && age < chrono::Duration::minutes(5),
        "recorded_at must be the moment of the abandonment, got {} (age {age})",
        recorded.recorded_at
    );
    assert_eq!(
        recorded.sqlstate.as_deref().map(|code| &code[..2]),
        Some("22"),
        "the SQLSTATE that made the write permanently invalid is kept: {:?}",
        recorded.sqlstate
    );

    // then: nothing was persisted, and each frame was settled exactly once
    assert_eq!(ctx.stack.count_rows().await, 0, "no partial fan-out");
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        2,
        "two commands, two terminations — no frame is being retried"
    );

    // then: the termination is real, not merely "not redelivered yet". A frame
    // the broker still holds comes back every NAK_DELAY; after several such
    // cycles neither has. The zero below is discriminant twice over: the same
    // marker is asserted non-zero further down, once the replay arrives, and
    // JetStream's own delivery counter — a positive number, not an absence —
    // says each frame was settled on its first delivery.
    tokio::time::sleep(TERM_OBSERVATION_WINDOW).await;
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER),
        0,
        "a terminated frame never re-enters triage; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.max_delivered_count(),
        1,
        "both frames were settled on their first delivery; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        2,
        "still exactly two abandonments after several redelivery cycles"
    );
    assert_eq!(ctx.stack.dead_letters().await.len(), 2);

    // then: the recipients the abandoned command named observe nothing at all —
    // no push, no row in their list, no unread badge. An abandonment is silent
    // on the recipient's side and loud on the operator's.
    abandoned_session
        .expect_silence("an abandoned command reaches no recipient", CONSUME_WAIT)
        .await;
    let listed = ctx
        .instance
        .graphql(&abandoned_recipient, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        listed["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "an abandoned command leaves no notification behind: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&abandoned_recipient, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);

    // when: the very same frame comes back (a term() the broker never registered)
    ctx.stack
        .publish_deliver_envelope(first_command_id, &first, trace)
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: the ledger is unchanged — one line per command, first trace wins
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger.len(), 2, "a replayed frame adds no audit line");
    assert_eq!(
        ledger
            .iter()
            .find(|row| row.command_id == first_command_id)
            .expect("still there")
            .id,
        recorded.id,
        "the first trace is kept, never overwritten"
    );
    assert!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER) >= 1,
        "the replay is logged as already-ledgered, not as a fresh abandonment"
    );
}

#[tokio::test]
#[serial_test::serial]
async fn s21_an_unwritable_ledger_holds_the_command_instead_of_terminating_it() {
    let ctx = TestContext::setup().await;
    let recipient = [Uuid::now_v7()];

    // given: the dead-letter ledger cannot be written (the ingest role lost its
    // INSERT grant) — the one guard-rail that must never silently drop a command
    ctx.stack.revoke_ledger_writes().await;
    let command = poison(&recipient, "ledger_down");
    ctx.stack.publish_deliver(&command).await;

    // then: the frame is NAKed and redelivered, never terminated — an untraceable
    // abandonment is worse than an endless hold, and the hold is loud
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.instance.log_hits(LEDGER_UNAVAILABLE_LOG_MARKER) >= 2
            })
            .await,
        "the command must be retried while the ledger is down; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(ctx.stack.dead_letters().await.len(), 0);
    assert_eq!(ctx.stack.count_rows().await, 0);
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        0,
        "nothing may be terminated while it cannot be traced"
    );

    // then: the hold is on /metrics too — an unwritable ledger degrades the
    // class to transient, which is exactly the condition operators alert on
    assert!(
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await
            >= 1.0,
        "an untraceable abandonment must be visible as a held command"
    );

    // when: the ledger comes back
    ctx.stack.restore_ledger_writes().await;

    // then: the held command is finally traced and terminated
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 1
            })
            .await,
        "the held command must be recorded once the ledger is writable; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger[0].source_event_id, Some(command.source_event_id));
    assert_eq!(ledger[0].recipient_ids, recipient);
    assert_eq!(ledger[0].reason, REASON_STORAGE_REJECTED);

    tokio::time::sleep(CONSUME_WAIT).await;
    assert_eq!(
        ctx.stack.dead_letters().await.len(),
        1,
        "once traced, the frame is terminated, not redelivered forever"
    );
    // then: the marker that had to be silent while the ledger was down now
    // fires exactly once — the earlier `== 0` is a real absence, not a typo in
    // a log message nothing would notice
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        1,
        "the abandonment is recorded once, after the ledger came back"
    );
    assert_eq!(
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await,
        0.0,
        "storage answered — it refused the row, which ends the hold"
    );
}

// The retention window `src/intake.rs` enforces (`DEAD_LETTER_RETENTION_DAYS`).
// The ages below straddle it by a day on either side, so a window widened or
// narrowed by even one day fails the scenario — an approximate bar would let the
// constant drift silently, and this one is a data-protection commitment, not a
// disk-space heuristic.
const RETENTION_DAYS: i32 = 90;

#[tokio::test]
#[serial_test::serial]
async fn s32_the_ledger_purges_itself_at_the_retention_edge_without_touching_readiness() {
    let stack = TestStack::up().await;

    // given: three audit lines planted around the retention edge. A dead letter
    // carries the producer's command in full, so it may hold personal data — the
    // ledger is a diagnostic tool with a lifetime, not an archive.
    let long_expired = stack.record_dead_letter_aged(RETENTION_DAYS * 4).await;
    let just_expired = stack.record_dead_letter_aged(RETENTION_DAYS + 1).await;
    let just_inside = stack.record_dead_letter_aged(RETENTION_DAYS - 1).await;

    // when: a service instance starts — the retention pass runs on its first
    // tick, through the ingest role, never the owner. That role holds the DELETE
    // grant only because migration 0002 gives it one; without it the pass would
    // fail on every tick and this scenario is what would say so.
    let instance = stack.spawn_instance(true).await;

    // then: everything past the window is gone and everything inside it is kept
    assert!(
        stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                stack.dead_letter_ids().await == vec![just_inside]
            })
            .await,
        "the retention pass must remove exactly the expired rows, ledger holds {:?} \
         (long expired {long_expired}, just expired {just_expired}, just inside {just_inside})",
        stack.dead_letter_ids().await
    );
    assert_eq!(
        instance.log_hits(RETENTION_FAILED_LOG_MARKER),
        0,
        "the pass succeeded, so it never logged a failure; logs:\n{}",
        instance.logs()
    );

    // when: the ingest role loses the DELETE grant — the shape of a broken
    // deployment, and the only realistic way this pass fails
    stack.revoke_ledger_purges().await;
    let crippled = stack.spawn_instance(true).await;

    // then: the failure is loud in the logs and invisible to readiness. Taking
    // the pod out of the Service endpoints over a housekeeping failure would cut
    // the queries and subscriptions that are perfectly healthy — the same reason
    // a storage outage does not escalate to readiness either.
    assert!(
        stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                crippled.log_hits(RETENTION_FAILED_LOG_MARKER) >= 1
            })
            .await,
        "a purge it cannot perform must be said out loud; logs:\n{}",
        crippled.logs()
    );
    let (readyz, _) = crippled.get("/readyz").await;
    assert!(
        readyz.is_success(),
        "a failed retention pass must not take readiness DOWN, got {readyz}"
    );

    // then: and the instance is still serving the surface that has nothing to do
    // with the ledger — the failure is contained to the pass
    let recipient = Uuid::now_v7();
    let passport = make_passport(recipient);
    let count = crippled.graphql(&passport, UNREAD_QUERY, json!({})).await;
    assert_eq!(
        ServiceInstance::unread_count(&count),
        0,
        "the read surface is untouched by a failing retention pass"
    );
    assert_eq!(
        stack.dead_letter_ids().await,
        vec![just_inside],
        "and nothing was purged behind the missing grant"
    );

    stack.restore_ledger_purges().await;
}
