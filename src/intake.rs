use std::time::Duration;

use br_core_events::Actor;
use br_notifier_contract::{DeliverNotification, RelativeLink, deliver_coords};
use br_util_axum_readiness::ReadinessHandle;
use br_util_nats_fabric::{
    CommandConsumer, Delivered, Fabric, FabricError, IntegrationCommand, MessageOutcome,
};
use metrics::{counter, describe_counter, describe_gauge, gauge};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tokio::sync::watch;
use uuid::Uuid;

use crate::notification::insert_notifications;

const DURABLE_NAME: &str = "svc-notifier";
const NAK_DELAY: Duration = Duration::from_secs(1);
const MAX_CONSECUTIVE_RECV_ERRORS: u32 = 10;
const PERMANENTLY_INVALID_SQLSTATE_CLASSES: [&str; 2] = ["22", "23"];

const DEAD_LETTERS_TOTAL: &str = "notifier_intake_dead_letters_total";
const TRANSIENT_FAILURES_TOTAL: &str = "notifier_intake_transient_failures_total";
const CONSECUTIVE_TRANSIENT_FAILURES: &str = "notifier_intake_consecutive_transient_failures";
const UNDECODABLE_DELIVERIES_TOTAL: &str = "notifier_intake_undecodable_deliveries_total";
const LEDGER_FAILURES_TOTAL: &str = "notifier_intake_ledger_failures_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    Transient,
    Poison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Written,
    Failed(FailureClass),
    NotAttempted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeadLetterReason {
    PayloadShapeRejected,
    RelativeLinkRejected,
    NoRecipients,
    StorageRejected,
}

impl DeadLetterReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PayloadShapeRejected => "payload_shape_rejected",
            Self::RelativeLinkRejected => "relative_link_rejected",
            Self::NoRecipients => "no_recipients",
            Self::StorageRejected => "storage_rejected",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Trace {
    command_id: Uuid,
    correlation_id: Uuid,
    causation_id: Option<Uuid>,
    actor_kind: &'static str,
    actor_id: Uuid,
}

const fn actor_kind(actor: &Actor) -> &'static str {
    match actor {
        Actor::Human(_) => "human",
        Actor::Service(_) => "service",
    }
}

#[derive(Debug)]
struct Rejection {
    reason: DeadLetterReason,
    detail: String,
}

struct Abandoned<'a> {
    trace: Trace,
    reason: DeadLetterReason,
    detail: String,
    sqlstate: Option<&'a str>,
    source_event_id: Option<Uuid>,
    recipient_ids: &'a [Uuid],
    command: &'a Value,
    delivered_count: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RawDeliver {
    source_event_id: Uuid,
    recipient_ids: Vec<Uuid>,
    template: String,
    payload: Value,
    #[serde(default)]
    link: Option<String>,
}

pub async fn bind(fabric: &Fabric) -> Result<CommandConsumer<Value>, IntakeError> {
    describe_intake_metrics();
    let consumer = fabric
        .ensure_command_consumer::<Value>(&deliver_coords(), DURABLE_NAME)
        .await?;
    tracing::info!(durable = DURABLE_NAME, "intake consumer bound");
    Ok(consumer)
}

fn describe_intake_metrics() {
    describe_counter!(
        DEAD_LETTERS_TOTAL,
        "Delivery commands abandoned on the dead-letter ledger and terminated, by stable reason code"
    );
    describe_counter!(
        TRANSIENT_FAILURES_TOTAL,
        "Deliveries held on the stream after a storage failure a retry could clear"
    );
    describe_gauge!(
        CONSECUTIVE_TRANSIENT_FAILURES,
        "Consecutive transient storage failures, reset by the first completed write — non-zero and rising means a storage outage"
    );
    describe_counter!(
        UNDECODABLE_DELIVERIES_TOTAL,
        "Deliveries of an envelope that cannot be read at all, held on the stream — every redelivery of the same frame counts again"
    );
    describe_counter!(
        LEDGER_FAILURES_TOTAL,
        "Dead-letter ledger writes that failed, degrading an abandonment to a hold — a lost grant, not a storage outage"
    );
    gauge!(CONSECUTIVE_TRANSIENT_FAILURES).set(0.0);
}

pub async fn consume(
    mut consumer: CommandConsumer<Value>,
    ingest_pool: PgPool,
    readiness: ReadinessHandle,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut consecutive_errors: u32 = 0;
    let mut transient_streak: u64 = 0;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            received = consumer.recv() => match received {
                Ok(Some(delivered)) => {
                    consecutive_errors = 0;
                    let verdict = handle(&ingest_pool, delivered).await;
                    transient_streak = observe(verdict, transient_streak);
                }
                Ok(None) => {
                    fail_loud(&readiness, &shutdown_tx, "intake stream closed (durable or stream gone)");
                    break;
                }
                Err(error) => {
                    consecutive_errors += 1;
                    tracing::warn!(%error, consecutive_errors, "intake recv error");
                    if consecutive_errors >= MAX_CONSECUTIVE_RECV_ERRORS {
                        fail_loud(&readiness, &shutdown_tx, "intake recv errors past the budget");
                        break;
                    }
                }
            },
        }
    }
    consumer.drain().await;
}

fn fail_loud(readiness: &ReadinessHandle, shutdown_tx: &watch::Sender<bool>, reason: &str) {
    tracing::error!(reason, "intake terminated abnormally — failing loud");
    readiness.set_not_ready(reason.to_owned());
    let _ = shutdown_tx.send(true);
}

fn transient_streak_after(verdict: Verdict, streak: u64) -> u64 {
    match verdict {
        Verdict::NotAttempted => streak,
        Verdict::Written | Verdict::Failed(FailureClass::Poison) => 0,
        Verdict::Failed(FailureClass::Transient) => streak + 1,
    }
}

fn observe(verdict: Verdict, streak: u64) -> u64 {
    if verdict == Verdict::Failed(FailureClass::Transient) {
        counter!(TRANSIENT_FAILURES_TOTAL).increment(1);
    }
    let streak = transient_streak_after(verdict, streak);
    gauge!(CONSECUTIVE_TRANSIENT_FAILURES).set(streak as f64);
    streak
}

async fn handle(pool: &PgPool, delivered: Delivered<IntegrationCommand<Value>>) -> Verdict {
    let (payload, trace) = match delivered.payload() {
        Ok(envelope) => (
            envelope.payload.clone(),
            Trace {
                command_id: envelope.command_id,
                correlation_id: envelope.metadata.correlation_id,
                causation_id: envelope.metadata.causation_id,
                actor_kind: actor_kind(&envelope.metadata.actor),
                actor_id: envelope.metadata.actor.id(),
            },
        ),
        Err(error) => {
            counter!(UNDECODABLE_DELIVERIES_TOTAL).increment(1);
            tracing::error!(
                %error,
                subject = delivered.subject(),
                delivered_count = ?delivered.delivered_count(),
                "undecodable command envelope, held on the stream for redelivery — a frame that cannot be recorded on the ledger is never terminated"
            );
            apply(delivered, outcome_for_undecodable_envelope()).await;
            return Verdict::NotAttempted;
        }
    };

    let delivered_count = delivered.delivered_count();
    let failure = match validate(&payload) {
        Err(rejection) => {
            let recipient_ids = traced_recipient_ids(&payload);
            let abandoned = Abandoned {
                trace,
                reason: rejection.reason,
                detail: rejection.detail,
                sqlstate: None,
                source_event_id: traced_source_event_id(&payload),
                recipient_ids: &recipient_ids,
                command: &payload,
                delivered_count,
            };
            Some(abandon(pool, &abandoned).await)
        }
        Ok(command) => match fan_out(pool, &command).await {
            Ok(written) => {
                tracing::info!(
                    source_event_id = %command.source_event_id,
                    command_id = %trace.command_id,
                    correlation_id = %trace.correlation_id,
                    recipients = command.recipient_ids.len(),
                    written,
                    "deliver command fanned out"
                );
                None
            }
            Err(error) => {
                Some(triage(pool, &command, &payload, trace, delivered_count, &error).await)
            }
        },
    };

    apply(delivered, outcome_for(failure)).await;
    match failure {
        None => Verdict::Written,
        Some(class) => Verdict::Failed(class),
    }
}

fn validate(payload: &Value) -> Result<DeliverNotification, Rejection> {
    let raw: RawDeliver = serde_json::from_value(payload.clone()).map_err(|error| Rejection {
        reason: DeadLetterReason::PayloadShapeRejected,
        detail: error.to_string(),
    })?;
    let link = match raw.link {
        Some(candidate) => Some(RelativeLink::parse(candidate).map_err(|error| Rejection {
            reason: DeadLetterReason::RelativeLinkRejected,
            detail: error.to_string(),
        })?),
        None => None,
    };
    if raw.recipient_ids.is_empty() {
        return Err(Rejection {
            reason: DeadLetterReason::NoRecipients,
            detail: "the command names no recipient".to_owned(),
        });
    }
    Ok(DeliverNotification {
        source_event_id: raw.source_event_id,
        recipient_ids: raw.recipient_ids,
        template: raw.template,
        payload: raw.payload,
        link,
    })
}

fn traced_source_event_id(payload: &Value) -> Option<Uuid> {
    payload
        .get("source_event_id")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn traced_recipient_ids(payload: &Value) -> Vec<Uuid> {
    payload
        .get("recipient_ids")
        .and_then(Value::as_array)
        .map(|ids| {
            ids.iter()
                .filter_map(Value::as_str)
                .filter_map(|id| Uuid::parse_str(id).ok())
                .collect()
        })
        .unwrap_or_default()
}

async fn triage(
    pool: &PgPool,
    command: &DeliverNotification,
    payload: &Value,
    trace: Trace,
    delivered_count: Option<i64>,
    error: &sqlx::Error,
) -> FailureClass {
    let sqlstate = sqlstate_of(error);
    match classify(error) {
        FailureClass::Transient => {
            tracing::error!(
                %error,
                source_event_id = %command.source_event_id,
                command_id = %trace.command_id,
                correlation_id = %trace.correlation_id,
                ?delivered_count,
                ?sqlstate,
                "storage write failing, commands held for redelivery"
            );
            FailureClass::Transient
        }
        FailureClass::Poison => {
            let abandoned = Abandoned {
                trace,
                reason: DeadLetterReason::StorageRejected,
                detail: error.to_string(),
                sqlstate: sqlstate.as_deref(),
                source_event_id: Some(command.source_event_id),
                recipient_ids: &command.recipient_ids,
                command: payload,
                delivered_count,
            };
            abandon(pool, &abandoned).await
        }
    }
}

async fn abandon(pool: &PgPool, abandoned: &Abandoned<'_>) -> FailureClass {
    match record_dead_letter(pool, abandoned).await {
        Ok(Some(dead_letter_id)) => {
            counter!(DEAD_LETTERS_TOTAL, "reason" => abandoned.reason.as_str()).increment(1);
            tracing::error!(
                reason = abandoned.reason.as_str(),
                detail = abandoned.detail,
                source_event_id = ?abandoned.source_event_id,
                command_id = %abandoned.trace.command_id,
                correlation_id = %abandoned.trace.correlation_id,
                recipients = abandoned.recipient_ids.len(),
                delivered_count = ?abandoned.delivered_count,
                sqlstate = ?abandoned.sqlstate,
                %dead_letter_id,
                "permanently invalid deliver command, recorded as a dead letter and terminated"
            );
            FailureClass::Poison
        }
        Ok(None) => {
            tracing::error!(
                reason = abandoned.reason.as_str(),
                detail = abandoned.detail,
                source_event_id = ?abandoned.source_event_id,
                command_id = %abandoned.trace.command_id,
                correlation_id = %abandoned.trace.correlation_id,
                delivered_count = ?abandoned.delivered_count,
                sqlstate = ?abandoned.sqlstate,
                "permanently invalid deliver command already on the ledger, terminating the redelivered frame"
            );
            FailureClass::Poison
        }
        Err(ledger_error) => {
            counter!(LEDGER_FAILURES_TOTAL).increment(1);
            tracing::error!(
                %ledger_error,
                reason = abandoned.reason.as_str(),
                detail = abandoned.detail,
                source_event_id = ?abandoned.source_event_id,
                command_id = %abandoned.trace.command_id,
                correlation_id = %abandoned.trace.correlation_id,
                sqlstate = ?abandoned.sqlstate,
                "dead-letter ledger unavailable, holding the command on the stream rather than dropping it untraced"
            );
            FailureClass::Transient
        }
    }
}

async fn record_dead_letter(
    pool: &PgPool,
    abandoned: &Abandoned<'_>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let id = Uuid::now_v7();
    let raw = serde_json::to_vec(abandoned.command).expect("a decoded payload re-serializes");
    let recorded = sqlx::query(
        "INSERT INTO dead_letters
             (id, command_id, source_event_id, recipient_ids, command, reason, sqlstate,
              correlation_id, causation_id, actor_kind, actor_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (command_id) DO NOTHING
         RETURNING id",
    )
    .bind(id)
    .bind(abandoned.trace.command_id)
    .bind(abandoned.source_event_id)
    .bind(abandoned.recipient_ids)
    .bind(raw)
    .bind(abandoned.reason.as_str())
    .bind(abandoned.sqlstate)
    .bind(abandoned.trace.correlation_id)
    .bind(abandoned.trace.causation_id)
    .bind(abandoned.trace.actor_kind)
    .bind(abandoned.trace.actor_id)
    .fetch_optional(pool)
    .await?
    .map(|row| row.get("id"));
    Ok(recorded)
}

fn outcome_for(failure: Option<FailureClass>) -> MessageOutcome {
    match failure {
        None => MessageOutcome::Ack,
        Some(FailureClass::Transient) => MessageOutcome::Nak(Some(NAK_DELAY)),
        Some(FailureClass::Poison) => MessageOutcome::Term,
    }
}

fn outcome_for_undecodable_envelope() -> MessageOutcome {
    MessageOutcome::Nak(Some(NAK_DELAY))
}

fn classify(error: &sqlx::Error) -> FailureClass {
    match error {
        sqlx::Error::Database(database_error) => {
            classify_sqlstate(database_error.code().as_deref())
        }
        _ => FailureClass::Transient,
    }
}

fn sqlstate_of(error: &sqlx::Error) -> Option<String> {
    match error {
        sqlx::Error::Database(database_error) => {
            database_error.code().map(|code| code.into_owned())
        }
        _ => None,
    }
}

fn classify_sqlstate(sqlstate: Option<&str>) -> FailureClass {
    match sqlstate {
        Some(code)
            if PERMANENTLY_INVALID_SQLSTATE_CLASSES
                .iter()
                .any(|class| code.starts_with(class)) =>
        {
            FailureClass::Poison
        }
        _ => FailureClass::Transient,
    }
}

async fn fan_out(pool: &PgPool, command: &DeliverNotification) -> Result<usize, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let written = insert_notifications(
        &mut tx,
        command.source_event_id,
        &command.recipient_ids,
        &command.template,
        &command.payload,
        command.link.as_ref(),
    )
    .await?;
    tx.commit().await?;
    Ok(written)
}

async fn apply(delivered: Delivered<IntegrationCommand<Value>>, outcome: MessageOutcome) {
    let result = match outcome {
        MessageOutcome::Ack => delivered.ack().await,
        MessageOutcome::Nak(delay) => delivered.nak(delay).await,
        MessageOutcome::Term => delivered.term().await,
        other => {
            tracing::error!(?other, "unexpected message outcome, NAKing");
            delivered.nak(Some(NAK_DELAY)).await
        }
    };
    if let Err(error) = result {
        tracing::error!(%error, ?outcome, "failed to settle message");
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    #[error("intake consumer unavailable: {0}")]
    Consumer(#[from] FabricError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wire(link: Value) -> Value {
        json!({
            "source_event_id": "0196a000-0000-7000-8000-000000000001",
            "recipient_ids": ["0196a000-0000-7000-8000-000000000002"],
            "template": "meeting_scheduled",
            "payload": {"meeting_id": "m-1"},
            "link": link,
        })
    }

    #[test]
    fn a_successful_fan_out_acks() {
        assert_eq!(outcome_for(None), MessageOutcome::Ack);
    }

    #[test]
    fn a_transient_failure_naks_for_redelivery_and_is_never_terminated() {
        assert_eq!(
            outcome_for(Some(FailureClass::Transient)),
            MessageOutcome::Nak(Some(NAK_DELAY)),
            "a storage outage holds the command on the stream, whatever the delivery count"
        );
    }

    #[test]
    fn a_permanently_invalid_write_is_terminated_as_poison() {
        assert_eq!(
            outcome_for(Some(FailureClass::Poison)),
            MessageOutcome::Term
        );
    }

    #[test]
    fn an_undecodable_envelope_is_held_never_terminated() {
        assert_eq!(
            outcome_for_undecodable_envelope(),
            MessageOutcome::Nak(Some(NAK_DELAY)),
            "no frame is destroyed without a committed ledger row, and an envelope we cannot read \
             carries no id to record"
        );
    }

    #[test]
    fn a_valid_command_survives_the_permissive_decode_unchanged() {
        let command = validate(&wire(json!("/meetings/m-1"))).expect("the frame is valid");
        assert_eq!(
            command.source_event_id.to_string(),
            "0196a000-0000-7000-8000-000000000001"
        );
        assert_eq!(command.recipient_ids.len(), 1);
        assert_eq!(command.template, "meeting_scheduled");
        assert_eq!(command.payload, json!({"meeting_id": "m-1"}));
        assert_eq!(
            command.link.map(|link| link.as_str().to_owned()),
            Some("/meetings/m-1".to_owned())
        );
    }

    #[test]
    fn an_out_of_domain_link_is_a_traceable_rejection_not_a_decode_failure() {
        for unsafe_link in ["https://evil.com", "//evil.com", "javascript:alert(1)", ""] {
            let rejection = validate(&wire(json!(unsafe_link)))
                .err()
                .unwrap_or_else(|| panic!("{unsafe_link:?} must be refused"));
            assert_eq!(
                rejection.reason,
                DeadLetterReason::RelativeLinkRejected,
                "the refusal must name the link, so the ledger row says why: {unsafe_link:?}"
            );
        }
    }

    #[test]
    fn a_payload_the_contract_does_not_describe_is_a_traceable_rejection() {
        for shape in [
            json!({"recipient_ids": [], "template": "t", "payload": {}}),
            json!({"source_event_id": "not-a-uuid", "recipient_ids": [], "template": "t", "payload": {}}),
            json!({"source_event_id": "0196a000-0000-7000-8000-000000000001", "recipient_ids": ["nope"], "template": "t", "payload": {}}),
            json!({"source_event_id": "0196a000-0000-7000-8000-000000000001", "recipient_ids": [], "template": 42, "payload": {}}),
            json!("a bare string"),
        ] {
            let rejection = validate(&shape)
                .err()
                .unwrap_or_else(|| panic!("{shape} must be refused"));
            assert_eq!(rejection.reason, DeadLetterReason::PayloadShapeRejected);
        }
    }

    #[test]
    fn a_command_naming_no_recipient_is_recorded_rather_than_silently_acked() {
        let empty = json!({
            "source_event_id": "0196a000-0000-7000-8000-000000000001",
            "recipient_ids": [],
            "template": "meeting_scheduled",
            "payload": {},
        });
        let rejection = validate(&empty).expect_err("a producer bug, not a no-op");
        assert_eq!(rejection.reason, DeadLetterReason::NoRecipients);
    }

    #[test]
    fn every_ledger_reason_has_a_stable_code() {
        for (reason, code) in [
            (
                DeadLetterReason::PayloadShapeRejected,
                "payload_shape_rejected",
            ),
            (
                DeadLetterReason::RelativeLinkRejected,
                "relative_link_rejected",
            ),
            (DeadLetterReason::NoRecipients, "no_recipients"),
            (DeadLetterReason::StorageRejected, "storage_rejected"),
        ] {
            assert_eq!(reason.as_str(), code);
        }
    }

    #[test]
    fn every_actor_kind_has_a_stable_ledger_label() {
        let id = Uuid::now_v7();
        assert_eq!(actor_kind(&Actor::Human(id.into())), "human");
        assert_eq!(actor_kind(&Actor::Service(id.into())), "service");
    }

    #[test]
    fn a_rejected_frame_still_yields_the_ids_the_ledger_needs() {
        let refused = wire(json!("https://evil.com"));
        assert_eq!(
            traced_source_event_id(&refused).map(|id| id.to_string()),
            Some("0196a000-0000-7000-8000-000000000001".to_owned())
        );
        assert_eq!(traced_recipient_ids(&refused).len(), 1);
    }

    #[test]
    fn unreadable_ids_never_block_the_ledger_row() {
        let mangled = json!({"source_event_id": 7, "recipient_ids": ["nope", 3]});
        assert_eq!(traced_source_event_id(&mangled), None);
        assert!(traced_recipient_ids(&mangled).is_empty());
        assert_eq!(traced_source_event_id(&json!("scalar")), None);
        assert!(traced_recipient_ids(&json!("scalar")).is_empty());
    }

    #[test]
    fn only_a_data_or_constraint_sqlstate_is_poison() {
        for permanently_invalid in ["22P02", "22001", "23502", "23505", "23514"] {
            assert_eq!(
                classify_sqlstate(Some(permanently_invalid)),
                FailureClass::Poison,
                "sqlstate {permanently_invalid} can never succeed on retry"
            );
        }
    }

    #[test]
    fn every_other_sqlstate_is_transient_so_nothing_is_lost() {
        for recoverable in [
            "08006", "08001", "53300", "57P01", "40001", "42501", "XX000",
        ] {
            assert_eq!(
                classify_sqlstate(Some(recoverable)),
                FailureClass::Transient,
                "sqlstate {recoverable} must be retried, not dropped"
            );
        }
        assert_eq!(classify_sqlstate(None), FailureClass::Transient);
    }

    #[test]
    fn an_unreachable_database_is_transient_not_poison() {
        for unreachable in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::PoolClosed,
            sqlx::Error::Io(std::io::Error::other("connection refused")),
        ] {
            assert_eq!(
                classify(&unreachable),
                FailureClass::Transient,
                "{unreachable:?} must never terminate a delivery command"
            );
        }
    }

    #[test]
    fn the_transient_streak_counts_consecutive_failures_and_resets_on_an_answer() {
        let mut streak = 0;
        for expected in 1..=5 {
            streak = transient_streak_after(Verdict::Failed(FailureClass::Transient), streak);
            assert_eq!(streak, expected);
        }
        assert_eq!(
            transient_streak_after(Verdict::Written, streak),
            0,
            "a completed write ends the streak"
        );
        assert_eq!(
            transient_streak_after(Verdict::Failed(FailureClass::Poison), streak),
            0,
            "storage answered — it refused the row, which is not an outage"
        );
        assert_eq!(
            transient_streak_after(Verdict::NotAttempted, streak),
            streak,
            "a frame that never reached storage says nothing about it"
        );
    }
}
