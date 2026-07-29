use std::time::Duration;

use br_notifier_contract::{DeliverNotification, deliver_coords};
use br_util_axum_readiness::{Readiness, ReadinessHandle};
use br_util_nats_fabric::{
    CommandConsumer, Delivered, Fabric, FabricError, IntegrationCommand, MessageOutcome,
};
use sqlx::{PgPool, Row};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::notification::insert_notification;

const DURABLE_NAME: &str = "svc-notifier";
const NAK_DELAY: Duration = Duration::from_secs(1);
const MAX_CONSECUTIVE_RECV_ERRORS: u32 = 10;
const STORAGE_OUTAGE_ALERT_AFTER: u32 = 3;
const STORAGE_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const STORAGE_OUTAGE_REASON: &str =
    "storage unreachable — delivery commands are held on the stream for redelivery";
const PERMANENTLY_INVALID_SQLSTATE_CLASSES: [&str; 2] = ["22", "23"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureClass {
    Transient,
    Poison,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FanOutVerdict {
    Written,
    Failed(FailureClass),
    NotAttempted,
}

#[derive(Debug, Clone, Copy)]
struct Trace {
    command_id: Uuid,
    correlation_id: Uuid,
    causation_id: Option<Uuid>,
}

pub async fn bind(fabric: &Fabric) -> Result<CommandConsumer<DeliverNotification>, IntakeError> {
    let consumer = fabric
        .ensure_command_consumer::<DeliverNotification>(&deliver_coords(), DURABLE_NAME)
        .await?;
    tracing::info!(durable = DURABLE_NAME, "intake consumer bound");
    Ok(consumer)
}

pub async fn consume(
    mut consumer: CommandConsumer<DeliverNotification>,
    ingest_pool: PgPool,
    readiness: ReadinessHandle,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut consecutive_errors: u32 = 0;
    let mut storage = StorageGauge::new(readiness.clone(), ingest_pool.clone());
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            received = consumer.recv() => match received {
                Ok(Some(delivered)) => {
                    consecutive_errors = 0;
                    let verdict = handle(&ingest_pool, delivered).await;
                    storage.record(verdict);
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

struct StorageGauge {
    readiness: ReadinessHandle,
    pool: PgPool,
    consecutive_failures: u32,
    probe: Option<JoinHandle<()>>,
    probe_spent: bool,
}

impl StorageGauge {
    fn new(readiness: ReadinessHandle, pool: PgPool) -> Self {
        Self {
            readiness,
            pool,
            consecutive_failures: 0,
            probe: None,
            probe_spent: false,
        }
    }

    fn record(&mut self, verdict: FanOutVerdict) {
        match verdict {
            FanOutVerdict::NotAttempted => {}
            FanOutVerdict::Failed(FailureClass::Transient) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures < STORAGE_OUTAGE_ALERT_AFTER {
                    return;
                }
                if self.consecutive_failures == STORAGE_OUTAGE_ALERT_AFTER {
                    tracing::error!(
                        failures = self.consecutive_failures,
                        "storage outage — the intake holds every delivery on the stream until it clears"
                    );
                }
                self.readiness.set_not_ready(STORAGE_OUTAGE_REASON);
                self.arm_probe();
            }
            FanOutVerdict::Written => {
                self.clear_outage();
                self.probe_spent = false;
            }
            FanOutVerdict::Failed(FailureClass::Poison) => self.clear_outage(),
        }
    }

    fn clear_outage(&mut self) {
        if self.consecutive_failures >= STORAGE_OUTAGE_ALERT_AFTER {
            tracing::info!("storage reachable again — intake resumed");
            restore_readiness(&self.readiness);
        }
        self.disarm_probe();
        self.consecutive_failures = 0;
    }

    fn arm_probe(&mut self) {
        if self.probe_spent {
            return;
        }
        if self
            .probe
            .as_ref()
            .is_some_and(|probe| !probe.is_finished())
        {
            return;
        }
        self.probe = Some(tokio::spawn(probe_storage(
            self.readiness.clone(),
            self.pool.clone(),
        )));
        self.probe_spent = true;
    }

    fn disarm_probe(&mut self) {
        if let Some(probe) = self.probe.take() {
            probe.abort();
        }
    }
}

impl Drop for StorageGauge {
    fn drop(&mut self) {
        self.disarm_probe();
    }
}

async fn probe_storage(readiness: ReadinessHandle, pool: PgPool) {
    loop {
        tokio::time::sleep(STORAGE_PROBE_INTERVAL).await;
        match sqlx::query("SELECT 1").execute(&pool).await {
            Ok(_) => {
                tracing::info!(
                    "storage probe answered — readiness restored without waiting for traffic"
                );
                restore_readiness(&readiness);
                return;
            }
            Err(error) => tracing::debug!(%error, "storage probe still failing"),
        }
    }
}

fn restore_readiness(readiness: &ReadinessHandle) {
    match readiness.snapshot() {
        Readiness::NotReady { reason } if reason == STORAGE_OUTAGE_REASON => readiness.set_ready(),
        _ => {}
    }
}

async fn handle(
    pool: &PgPool,
    delivered: Delivered<IntegrationCommand<DeliverNotification>>,
) -> FanOutVerdict {
    let (command, trace) = match delivered.payload() {
        Ok(envelope) => (
            envelope.payload.clone(),
            Trace {
                command_id: envelope.command_id,
                correlation_id: envelope.metadata.correlation_id,
                causation_id: envelope.metadata.causation_id,
            },
        ),
        Err(error) => {
            tracing::error!(
                %error,
                subject = delivered.subject(),
                delivered_count = ?delivered.delivered_count(),
                "terminating undecodable command (poison) — no ledger row is possible, the raw frame is not exposed by the consumer"
            );
            apply(delivered, outcome_for_undecodable()).await;
            return FanOutVerdict::NotAttempted;
        }
    };

    let delivered_count = delivered.delivered_count();
    let failure = match fan_out(pool, &command).await {
        Ok(()) => None,
        Err(error) => Some(triage(pool, &command, trace, delivered_count, &error).await),
    };

    apply(delivered, outcome_for(failure)).await;
    match failure {
        None => FanOutVerdict::Written,
        Some(class) => FanOutVerdict::Failed(class),
    }
}

async fn triage(
    pool: &PgPool,
    command: &DeliverNotification,
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
            match record_dead_letter(pool, command, trace, sqlstate.as_deref()).await {
                Ok(Some(dead_letter_id)) => {
                    tracing::error!(
                        %error,
                        source_event_id = %command.source_event_id,
                        command_id = %trace.command_id,
                        correlation_id = %trace.correlation_id,
                        recipients = command.recipient_ids.len(),
                        ?sqlstate,
                        %dead_letter_id,
                        "permanently invalid deliver command, recorded as a dead letter and terminated"
                    );
                    FailureClass::Poison
                }
                Ok(None) => {
                    tracing::error!(
                        %error,
                        source_event_id = %command.source_event_id,
                        command_id = %trace.command_id,
                        correlation_id = %trace.correlation_id,
                        ?sqlstate,
                        "permanently invalid deliver command already on the ledger, terminating the redelivered frame"
                    );
                    FailureClass::Poison
                }
                Err(ledger_error) => {
                    tracing::error!(
                        %error,
                        %ledger_error,
                        source_event_id = %command.source_event_id,
                        command_id = %trace.command_id,
                        correlation_id = %trace.correlation_id,
                        ?sqlstate,
                        "dead-letter ledger unavailable, holding the command on the stream rather than dropping it untraced"
                    );
                    FailureClass::Transient
                }
            }
        }
    }
}

async fn record_dead_letter(
    pool: &PgPool,
    command: &DeliverNotification,
    trace: Trace,
    sqlstate: Option<&str>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let id = Uuid::now_v7();
    let raw = serde_json::to_vec(command).expect("the deliver command serializes");
    let recorded = sqlx::query(
        "INSERT INTO dead_letters
             (id, command_id, source_event_id, recipient_ids, command, sqlstate,
              correlation_id, causation_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (command_id) DO NOTHING
         RETURNING id",
    )
    .bind(id)
    .bind(trace.command_id)
    .bind(command.source_event_id)
    .bind(&command.recipient_ids)
    .bind(raw)
    .bind(sqlstate)
    .bind(trace.correlation_id)
    .bind(trace.causation_id)
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

fn outcome_for_undecodable() -> MessageOutcome {
    outcome_for(Some(FailureClass::Poison))
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

async fn fan_out(pool: &PgPool, command: &DeliverNotification) -> Result<(), sqlx::Error> {
    if command.recipient_ids.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for recipient_id in &command.recipient_ids {
        insert_notification(
            &mut tx,
            command.source_event_id,
            *recipient_id,
            &command.template,
            &command.payload,
            command.link.as_ref(),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn apply(
    delivered: Delivered<IntegrationCommand<DeliverNotification>>,
    outcome: MessageOutcome,
) {
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
    fn an_undecodable_frame_is_terminated_not_acked() {
        assert_eq!(outcome_for_undecodable(), MessageOutcome::Term);
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

    fn gauge(readiness: &ReadinessHandle) -> StorageGauge {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unreachable:unreachable@127.0.0.1:1/unreachable")
            .expect("a lazy pool never dials");
        StorageGauge::new(readiness.clone(), pool)
    }

    fn sustain_outage(storage: &mut StorageGauge) {
        for _ in 0..STORAGE_OUTAGE_ALERT_AFTER {
            storage.record(FanOutVerdict::Failed(FailureClass::Transient));
        }
    }

    #[tokio::test]
    async fn a_storage_outage_takes_readiness_down_and_restores_it_on_recovery() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        assert!(!readiness.is_ready(), "a sustained outage must be visible");

        storage.record(FanOutVerdict::Written);
        assert_eq!(storage.consecutive_failures, 0);
        assert!(readiness.is_ready(), "a completed write clears the outage");
    }

    #[tokio::test]
    async fn a_brief_hiccup_never_takes_readiness_down() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        for _ in 0..STORAGE_OUTAGE_ALERT_AFTER - 1 {
            storage.record(FanOutVerdict::Failed(FailureClass::Transient));
        }
        assert!(readiness.is_ready());
    }

    #[tokio::test]
    async fn an_undecodable_frame_says_nothing_about_storage() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        storage.record(FanOutVerdict::NotAttempted);
        assert_eq!(
            storage.consecutive_failures, STORAGE_OUTAGE_ALERT_AFTER,
            "a frame that never reached storage must not clear the outage"
        );
        assert!(!readiness.is_ready());
    }

    #[tokio::test]
    async fn an_outage_that_outlives_an_external_set_ready_escalates_again() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        readiness.set_ready();

        storage.record(FanOutVerdict::Failed(FailureClass::Transient));
        assert!(
            !readiness.is_ready(),
            "escalation is idempotent, not a one-shot at the threshold"
        );
    }

    #[tokio::test]
    async fn recovery_never_clobbers_a_readiness_held_down_for_another_reason() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        readiness.set_not_ready("intake terminated abnormally");

        storage.record(FanOutVerdict::Written);
        assert!(
            !readiness.is_ready(),
            "only the storage outage may clear the storage outage"
        );
    }

    #[tokio::test]
    async fn a_sustained_outage_never_stacks_probes() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        let armed = storage.probe.as_ref().expect("the probe is armed").id();

        storage.record(FanOutVerdict::Failed(FailureClass::Transient));
        assert_eq!(
            storage
                .probe
                .as_ref()
                .expect("the probe is still armed")
                .id(),
            armed,
            "a second escalation must reuse the live probe"
        );

        storage.record(FanOutVerdict::Written);
        assert!(storage.probe.is_none(), "a real write disarms the probe");
    }

    #[tokio::test]
    async fn a_partial_outage_stays_down_after_the_probe_has_spent_its_one_restoration() {
        let readiness = ReadinessHandle::ready();
        let mut storage = gauge(&readiness);
        sustain_outage(&mut storage);
        assert!(!readiness.is_ready());

        storage.disarm_probe();
        restore_readiness(&readiness);
        assert!(readiness.is_ready(), "the probe answered and stood down");

        storage.record(FanOutVerdict::Failed(FailureClass::Transient));
        assert!(
            !readiness.is_ready(),
            "writes still failing takes readiness back down"
        );
        assert!(
            storage.probe.is_none(),
            "no second probe: reads answering while writes fail must not oscillate the endpoints"
        );

        storage.record(FanOutVerdict::Written);
        assert!(readiness.is_ready());
        assert!(
            !storage.probe_spent,
            "only a real write re-arms the silent-recovery path"
        );
    }
}
