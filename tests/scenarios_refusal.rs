// Refused-payload scenarios: a delivery command the service will not honour is
// abandoned *traceably* — never dropped, never half-applied, never silent.
//
// The request arrives as a well-formed integration envelope whose payload
// breaks the contract: an out-of-domain `link` (the contract's first-class
// business refusal), a shape the contract does not describe, or a command
// naming nobody. Producers using `br-notifier-publisher` cannot build these —
// the contract refuses them at construction — so the frames here are what a
// non-compliant or version-skewed producer actually puts on the wire. They ride
// the very same typed coordinates: `Fabric::publish_command` is generic over the
// payload, so no raw subject and no new harness affordance is involved.
//
// Every scenario walks the channels a refusal can be observed on:
//   settlement  — the frame is terminated, and only after the ledger row
//                 commits. Termination is not directly observable from outside
//                 the process, so it is proven by a pair of facts read after
//                 several redelivery cycles: JetStream's own delivery counter
//                 stayed at one (a positive number — a held frame comes back
//                 with a higher one), AND the redelivery branch's log marker
//                 fired zero times. The zero alone would be worthless, so it is
//                 never left alone: s22/s23/s24 drive that very marker above
//                 zero later in the same run with a deliberate replay, and s21b
//                 drives its sibling on the same code path (the abandonment
//                 marker, zero while the ledger is down and one after repair).
//   ledger      — one dead_letters line, stable `reason`, readable ids, the
//                 producer's command verbatim, its causality and its actor
//   operator    — the `/metrics` counters the alerts are built on
//   recipient   — absolute silence: no push, no list entry, no unread badge,
//                 no row in Postgres
//
// The ledger scenarios that predate the traced-refusal intake (s20, s21) stay in
// `scenarios_intake.rs`: their names are quoted in the README and in the
// service's runbook, and a rename buys nothing.
//
// Known scenario gap: an envelope the intake cannot decode *at all* is held
// forever and counted on `notifier_intake_undecodable_deliveries_total`, and
// nothing here pins it — putting raw bytes on the typed deliver coordinates
// needs a harness primitive that does not exist yet, and half an oracle would be
// worse than none.
mod common;

use common::*;
use serde_json::{Value, json};
use uuid::Uuid;

// A well-formed envelope whose payload the contract refuses as a whole: the
// link points out of the product. Nothing else about the command is wrong —
// which is the point, the named rule is "the request is refused as a whole",
// not "the link is dropped".
fn out_of_domain_link_payload(source_event_id: Uuid, recipients: &[Uuid]) -> Value {
    json!({
        "source_event_id": source_event_id,
        "recipient_ids": recipients,
        "template": "meeting_scheduled",
        "payload": {"meeting_id": "m-1"},
        "link": "https://evil.example/x",
    })
}

#[tokio::test]
#[serial_test::serial]
async fn s22_an_out_of_domain_link_refuses_the_whole_request_and_leaves_a_trace() {
    let ctx = TestContext::setup().await;
    let recipients = [Uuid::now_v7(), Uuid::now_v7()];

    // given: one of the named recipients has a session open before anything is
    // published — a refusal must be invisible to them on every channel, and an
    // "absent" push is only proven by a session that was there to hear it
    let recipient_passport = make_passport(recipients[0]);
    let mut session = subscribe_live(&ctx, recipients[0]).await;

    // when: a producer publishes a well-formed deliver envelope whose link
    // leaves the product
    let source_event_id = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let causing_event_id = Uuid::now_v7();
    let trace = Trace::caused_by(causing_event_id);
    let payload = out_of_domain_link_payload(source_event_id, &recipients);
    ctx.stack
        .publish_payload_envelope(command_id, &payload, trace)
        .await;

    // then: the abandonment is on the ledger — the refusal is traceable, which
    // is the whole reason the intake decodes permissively before it judges
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 1
            })
            .await,
        "a refused payload must be recorded, never dropped untraced; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    let recorded = &ledger[0];
    assert_eq!(recorded.reason, REASON_RELATIVE_LINK_REJECTED);
    assert_eq!(recorded.command_id, command_id);
    assert_eq!(recorded.source_event_id, Some(source_event_id));
    assert_eq!(
        recorded.recipient_ids, recipients,
        "every recipient the refused command would have reached is named — an \
         abandonment drops them all at once"
    );
    assert_eq!(
        recorded.command(),
        payload,
        "an operator must be able to re-emit the command verbatim, unsafe link included"
    );
    assert_eq!(recorded.correlation_id, trace.correlation_id);
    assert_eq!(recorded.causation_id, Some(causing_event_id));
    assert_eq!(
        recorded.actor_kind.as_deref(),
        Some("human"),
        "the ledger says who asked — re-emitting a command verbatim is only half an \
         audit trail without the actor that produced it"
    );
    assert_eq!(
        recorded.actor_id,
        Some(trace.actor_id),
        "and names them, exactly as the envelope's metadata did"
    );
    assert_eq!(
        recorded.sqlstate, None,
        "no database refused anything here — the service did"
    );

    // then: the operator's alert channel counts it under its stable reason
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_RELATIVE_LINK_REJECTED)]
            )
            .await,
        1.0,
        "the abandonment is countable by reason, not merely loggable"
    );

    // then: the frame is terminated — after several redelivery cycles it has not
    // come back. Two facts prove it together: JetStream's own delivery counter
    // stayed at one (a positive number, not an absence — a held frame returns
    // with a higher one), and the branch a returning frame takes never ran. That
    // second assertion is a zero, so it is only worth what its discriminant is
    // worth: the very same marker is driven above zero at the end of this
    // scenario, by a deliberate replay.
    tokio::time::sleep(TERM_OBSERVATION_WINDOW).await;
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        1,
        "exactly one abandonment; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.max_delivered_count(),
        1,
        "the frame was settled on its first delivery and never came back; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER),
        0,
        "a terminated frame never re-enters triage; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.stack.dead_letters().await.len(),
        1,
        "and the ledger stays at one line"
    );

    // then: the request was refused *as a whole* — not stored with the link
    // stripped, not stored for one recipient and not the other
    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "no notification is created"
    );

    // then: no recipient learns anything — stream, list and badge all silent
    session
        .expect_silence("a refused request reaches no recipient", CONSUME_WAIT)
        .await;
    let listed = ctx
        .instance
        .graphql(&recipient_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        listed["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "the refused notification must not surface: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&recipient_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
    for recipient in recipients {
        assert_eq!(ctx.stack.rows_for(recipient).await.len(), 0);
    }

    // when: the very same frame comes back — a term() the broker never
    // registered, or a producer replaying its outbox
    ctx.stack
        .publish_payload_envelope(command_id, &payload, trace)
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: the refusal is idempotent — one audit line per abandoned command,
    // the first trace wins, and the replay is logged as already-ledgered rather
    // than as a fresh abandonment
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger.len(), 1, "a replayed refusal adds no audit line");
    assert_eq!(ledger[0].id, recorded.id, "the first trace is kept");
    assert!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER) >= 1,
        "the replay is recognised as already on the ledger; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence("a replayed refusal is still a refusal", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s23_a_payload_the_contract_does_not_describe_is_recorded_with_whatever_ids_are_readable() {
    let ctx = TestContext::setup().await;
    let (unreadable_recipient, readable_recipient) = (Uuid::now_v7(), Uuid::now_v7());

    // given: a session for a recipient named by a malformed command — the ids
    // are readable even though the command as a whole is not
    let recipient_passport = make_passport(unreadable_recipient);
    let mut session = subscribe_live(&ctx, unreadable_recipient).await;

    // when: two payloads the contract does not describe are published — one
    // missing a mandatory field entirely, one carrying a wrong-typed field.
    // Together they pin both halves of the trace promise: what the service can
    // read from a broken payload it records, what it cannot it leaves NULL.
    let missing_field = json!({
        "recipient_ids": [unreadable_recipient],
        "template": "shape_violation",
        "payload": {"why": "no source_event_id at all"},
    });
    let source_event_id = Uuid::now_v7();
    let wrong_type = json!({
        "source_event_id": source_event_id,
        "recipient_ids": [readable_recipient],
        "template": 42,
        "payload": {"why": "template is not a string"},
    });
    let (missing_id, wrong_id) = (Uuid::now_v7(), Uuid::now_v7());
    ctx.stack
        .publish_payload_envelope(missing_id, &missing_field, Trace::fresh())
        .await;
    ctx.stack
        .publish_payload_envelope(wrong_id, &wrong_type, Trace::fresh())
        .await;

    // then: both are on the ledger under the same stable reason, one line each
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 2
            })
            .await,
        "each undescribed payload leaves its own trace; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    let missing = ledger
        .iter()
        .find(|row| row.command_id == missing_id)
        .expect("the field-less command has its own line");
    let wrong = ledger
        .iter()
        .find(|row| row.command_id == wrong_id)
        .expect("the wrong-typed command has its own line");

    assert_eq!(missing.reason, REASON_PAYLOAD_SHAPE_REJECTED);
    assert_eq!(wrong.reason, REASON_PAYLOAD_SHAPE_REJECTED);
    assert_eq!(
        missing.source_event_id, None,
        "an unreadable source event is recorded as NULL — it never blocks the row"
    );
    assert_eq!(
        wrong.source_event_id,
        Some(source_event_id),
        "a readable source event is kept, even when the rest of the payload is refused"
    );
    assert_eq!(
        missing.recipient_ids,
        [unreadable_recipient],
        "the recipients are readable and are recorded, broken shape or not"
    );
    assert_eq!(wrong.recipient_ids, [readable_recipient]);
    assert_eq!(missing.command(), missing_field);
    assert_eq!(
        wrong.command(),
        wrong_type,
        "the bytes are kept verbatim — that is what makes a producer bug diagnosable"
    );

    // then: the operator's counter carries both, under the shape reason
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_PAYLOAD_SHAPE_REJECTED)]
            )
            .await,
        2.0
    );

    // then: both frames are terminated, not held — each was settled on its
    // first delivery, as JetStream's own counter says, and neither ever took
    // the branch a returning frame takes. The replay at the end of this scenario
    // is what makes that zero discriminant.
    tokio::time::sleep(TERM_OBSERVATION_WINDOW).await;
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        2,
        "two abandonments; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.max_delivered_count(),
        1,
        "no refused frame came back for a second delivery; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER),
        0,
        "a terminated frame never re-enters triage; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(ctx.stack.dead_letters().await.len(), 2);

    // then: nothing reached anybody
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence("an undescribed payload reaches no recipient", CONSUME_WAIT)
        .await;
    let listed = ctx
        .instance
        .graphql(&recipient_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(listed["data"]["notifierNotifications"]["nodes"], json!([]));
    let count = ctx
        .instance
        .graphql(&recipient_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);

    // when: one of the two frames comes back — a term() the broker never
    // registered, or a producer replaying its outbox. The replayed one is the
    // command naming the listening recipient, so the silence below is theirs.
    ctx.stack
        .publish_payload_envelope(missing_id, &missing_field, Trace::fresh())
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: the ledger is unchanged, and the replay is recognised as
    // already-ledgered — which is what makes the zero asserted above an
    // absence rather than a stale log string
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger.len(), 2, "a replayed refusal adds no audit line");
    assert_eq!(
        ledger
            .iter()
            .filter(|row| row.command_id == missing_id)
            .count(),
        1,
        "still exactly one line for the replayed command"
    );
    assert!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER) >= 1,
        "the replay is logged as already-ledgered, not as a fresh abandonment; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence("a replayed refusal is still a refusal", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s24_a_command_naming_nobody_is_a_producer_bug_on_the_ledger_not_a_silent_no_op() {
    let ctx = TestContext::setup().await;
    let bystander = Uuid::now_v7();

    // given: a session belonging to nobody in particular — a recipient-less
    // command must not become a broadcast either
    let bystander_passport = make_passport(bystander);
    let mut session = subscribe_live(&ctx, bystander).await;

    // when: a producer publishes a well-shaped command that names no recipient
    let source_event_id = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let trace = Trace::fresh();
    let payload = json!({
        "source_event_id": source_event_id,
        "recipient_ids": [],
        "template": "meeting_scheduled",
        "payload": {"meeting_id": "m-1"},
    });
    ctx.stack
        .publish_payload_envelope(command_id, &payload, trace)
        .await;

    // then: it is recorded as the producer bug it is — acking it would leave the
    // producer believing someone was notified
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 1
            })
            .await,
        "a recipient-less command must be recorded, not silently acked; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    let recorded = &ledger[0];
    assert_eq!(recorded.reason, REASON_NO_RECIPIENTS);
    assert_eq!(recorded.command_id, command_id);
    assert_eq!(recorded.source_event_id, Some(source_event_id));
    assert!(
        recorded.recipient_ids.is_empty(),
        "the row names the recipients the command named: none"
    );
    assert_eq!(recorded.command(), payload);
    assert_eq!(recorded.correlation_id, trace.correlation_id);
    let recorded_id = recorded.id;

    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_NO_RECIPIENTS)]
            )
            .await,
        1.0
    );

    // then: terminated, and nothing was created or announced anywhere. The
    // termination rests on the same pair as its siblings — a delivery counter
    // still at one, and the returning-frame branch never taken — and the zero is
    // made discriminant by the replay at the end of this scenario.
    tokio::time::sleep(TERM_OBSERVATION_WINDOW).await;
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        1,
        "exactly one abandonment; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.max_delivered_count(),
        1,
        "the frame was settled on its first delivery and never came back; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER),
        0,
        "a terminated frame never re-enters triage; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence(
            "a command naming nobody is not a broadcast to everybody",
            CONSUME_WAIT,
        )
        .await;
    let listed = ctx
        .instance
        .graphql(&bystander_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        listed["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "a command naming nobody leaves nobody's list changed: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&bystander_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);

    // when: the same frame comes back
    ctx.stack
        .publish_payload_envelope(command_id, &payload, trace)
        .await;
    tokio::time::sleep(CONSUME_WAIT).await;

    // then: still one audit line, and the replay is recognised as
    // already-ledgered — the discriminant for the zero asserted above
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger.len(), 1, "a replayed refusal adds no audit line");
    assert_eq!(ledger[0].id, recorded_id, "the first trace is kept");
    assert!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER) >= 1,
        "the replay is logged as already-ledgered, not as a fresh abandonment; logs:\n{}",
        ctx.instance.logs()
    );
    // then: and the operator's counter did not move either. It counts *committed*
    // ledger rows, never settlements: a frame replayed a hundred times is one
    // abandonment, and an alert built on this counter must read a rate of new
    // dead letters, not of redeliveries.
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_NO_RECIPIENTS)]
            )
            .await,
        1.0,
        "the no-op on the unique index counts nothing"
    );
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence("a replayed refusal is still a refusal", CONSUME_WAIT)
        .await;
}

#[tokio::test]
#[serial_test::serial]
async fn s21b_a_refused_payload_is_held_while_the_ledger_is_unwritable() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    // given: the dead-letter ledger cannot be written (the ingest role lost its
    // INSERT grant). s21 proves the invariant for a write PostgreSQL refuses;
    // this is its counterpart for a payload the *service* refuses — the class
    // the traced-refusal intake added, and the one that used to be terminated
    // on a log line alone.
    ctx.stack.revoke_ledger_writes().await;
    let recipient_passport = make_passport(recipient);
    let mut session = subscribe_live(&ctx, recipient).await;

    let source_event_id = Uuid::now_v7();
    let payload = out_of_domain_link_payload(source_event_id, &[recipient]);
    let command_id = Uuid::now_v7();
    let trace = Trace::fresh();
    ctx.stack
        .publish_payload_envelope(command_id, &payload, trace)
        .await;

    // then: the frame is held and redelivered, never terminated — a refusal the
    // service cannot record is not a licence to destroy the request
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.instance.log_hits(LEDGER_UNAVAILABLE_LOG_MARKER) >= 2
            })
            .await,
        "the refused command must be retried while the ledger is down; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.stack.dead_letters().await.len(),
        0,
        "nothing could be recorded"
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        0,
        "and therefore nothing may be terminated"
    );
    assert_eq!(ctx.stack.count_rows().await, 0);

    // then: the operator sees the hold on /metrics, and sees it for what it is.
    // The streak gauge says a command is being held; the ledger's own counter
    // says why — a lost grant, not a PostgreSQL outage. Reading the two apart is
    // the difference between "restore the GRANT" and "page the DBA".
    assert!(
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await
            >= 1.0,
        "a refusal that cannot be traced must be visible as a held command"
    );
    assert!(
        ctx.instance
            .metric_or_zero(LEDGER_FAILURES_TOTAL_METRIC, &[])
            .await
            > 0.0,
        "the failing write is counted on the ledger's own series, not only on the \
         transient one — an unwritable ledger must be distinguishable from a storage outage"
    );

    // when: the ledger comes back
    ctx.stack.restore_ledger_writes().await;

    // then: the held refusal is finally traced — exactly once, with its reason
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 1
            })
            .await,
        "the held refusal must be recorded once the ledger is writable; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    assert_eq!(ledger[0].reason, REASON_RELATIVE_LINK_REJECTED);
    assert_eq!(ledger[0].command_id, command_id);
    assert_eq!(ledger[0].source_event_id, Some(source_event_id));
    assert_eq!(ledger[0].command(), payload);

    // then: and only then is it terminated — it does not come back, and the
    // marker that had to read zero above now reads one, so that zero was an
    // absence and not a stale string
    tokio::time::sleep(TERM_OBSERVATION_WINDOW).await;
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_RECORDED_LOG_MARKER),
        1,
        "recorded once, after the ledger came back; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.instance.log_hits(DEAD_LETTER_REPEATED_LOG_MARKER),
        0,
        "and once traced it never re-enters triage — the frame was terminated, not \
         redelivered into a second look; logs:\n{}",
        ctx.instance.logs()
    );
    assert_eq!(
        ctx.stack.dead_letters().await.len(),
        1,
        "once traced, the frame is terminated, not redelivered forever"
    );

    // then: the operator's channels close the incident by themselves — the
    // abandonment is now countable under its own reason, and the held-command
    // gauge is back to rest
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_RELATIVE_LINK_REJECTED)]
            )
            .await,
        1.0,
        "the repaired abandonment is counted exactly once, under its stable reason"
    );
    assert_eq!(
        ctx.instance
            .metric_or_zero(CONSECUTIVE_TRANSIENT_FAILURES_METRIC, &[])
            .await,
        0.0,
        "the ledger answered — the hold is over and the gauge says so"
    );

    // then: through the whole hold-and-abandon cycle the recipient learned
    // nothing — a request under repair is not a request delivered
    assert_eq!(ctx.stack.count_rows().await, 0);
    session
        .expect_silence(
            "a held-then-abandoned request reaches no recipient",
            CONSUME_WAIT,
        )
        .await;
    let count = ctx
        .instance
        .graphql(&recipient_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
}

#[tokio::test]
#[serial_test::serial]
async fn s31_a_template_no_renderer_could_key_on_is_refused_by_name_not_by_sqlstate() {
    let ctx = TestContext::setup().await;
    let recipient = Uuid::now_v7();

    // given: the named recipient is listening, on a session proven live
    let recipient_passport = make_passport(recipient);
    let mut session = subscribe_live(&ctx, recipient).await;

    // when: a producer publishes commands whose `template` is unusable — one
    // empty, one carrying the NUL PostgreSQL itself refuses. Before the intake
    // validated it, the second reached the INSERT and was abandoned as a storage
    // accident (`storage_rejected`, SQLSTATE 22021) and the first was stored as a
    // notification no renderer could key on.
    let unusable = [
        (Uuid::now_v7(), "", "the empty template"),
        (Uuid::now_v7(), "poison\u{0}template", "a NUL template"),
    ];
    for (command_id, template, what) in unusable {
        let payload = json!({
            "source_event_id": Uuid::now_v7(),
            "recipient_ids": [recipient],
            "template": template,
            "payload": {"why": what},
        });
        ctx.stack
            .publish_payload_envelope(command_id, &payload, Trace::fresh())
            .await;
    }

    // then: both are on the ledger under the fifth stable reason — a named
    // refusal decided before any write, not a SQLSTATE the operator has to read
    // backwards
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.dead_letters().await.len() == 2
            })
            .await,
        "an unusable template must be recorded, never stored and never dropped; logs:\n{}",
        ctx.instance.logs()
    );
    let ledger = ctx.stack.dead_letters().await;
    for (command_id, _, what) in unusable {
        let recorded = ledger
            .iter()
            .find(|row| row.command_id == command_id)
            .unwrap_or_else(|| panic!("{what} has its own audit line"));
        assert_eq!(
            recorded.reason, REASON_TEMPLATE_REJECTED,
            "{what} is refused by name, not as a storage accident"
        );
        assert_eq!(
            recorded.sqlstate, None,
            "{what}: no database refused anything — the intake did, before the write"
        );
        assert_eq!(
            recorded.recipient_ids,
            vec![recipient],
            "{what}: the row names the recipient the command would have reached"
        );
    }

    // then: the operator counts them under that reason, and nothing was counted
    // as a storage refusal
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_TEMPLATE_REJECTED)]
            )
            .await,
        2.0
    );
    assert_eq!(
        ctx.instance
            .metric_or_zero(
                DEAD_LETTERS_TOTAL_METRIC,
                &[("reason", REASON_STORAGE_REJECTED)]
            )
            .await,
        0.0,
        "the refusal is decided in the service, so the storage-refusal series never moves"
    );

    // then: nothing was stored, and the recipient learned nothing on any channel
    assert_eq!(
        ctx.stack.count_rows().await,
        0,
        "no notification is created"
    );
    session
        .expect_silence("an unusable template reaches no recipient", CONSUME_WAIT)
        .await;
    let listed = ctx
        .instance
        .graphql(&recipient_passport, LIST_QUERY, json!({}))
        .await;
    assert_eq!(
        listed["data"]["notifierNotifications"]["nodes"],
        json!([]),
        "a refused template must not surface as a blank notification: {listed}"
    );
    let count = ctx
        .instance
        .graphql(&recipient_passport, UNREAD_QUERY, json!({}))
        .await;
    assert_eq!(ServiceInstance::unread_count(&count), 0);
}
