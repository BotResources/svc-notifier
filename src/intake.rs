use std::time::Duration;

use br_notifier_contract::{DeliverNotification, deliver_coords};
use br_util_axum_readiness::ReadinessHandle;
use br_util_nats_fabric::{
    CommandConsumer, Delivered, Fabric, FabricError, IntegrationCommand, MessageOutcome,
};
use sqlx::PgPool;
use tokio::sync::watch;

use crate::notification::insert_notification;

const DURABLE_NAME: &str = "svc-notifier";
const MAX_DELIVER: i64 = 5;
const NAK_DELAY: Duration = Duration::from_secs(1);
const MAX_CONSECUTIVE_RECV_ERRORS: u32 = 10;

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
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            received = consumer.recv() => match received {
                Ok(Some(delivered)) => {
                    consecutive_errors = 0;
                    handle(&ingest_pool, delivered).await;
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

async fn handle(pool: &PgPool, delivered: Delivered<IntegrationCommand<DeliverNotification>>) {
    let command = match delivered.payload() {
        Ok(envelope) => envelope.payload.clone(),
        Err(error) => {
            tracing::error!(%error, "terminating undecodable command (poison)");
            apply(delivered, outcome_for_undecodable()).await;
            return;
        }
    };

    let fan_out = fan_out(pool, &command).await;
    let outcome = outcome_for(delivered.delivered_count(), fan_out.is_ok());
    if let Err(error) = &fan_out {
        if outcome == MessageOutcome::Term {
            tracing::error!(%error, source_event_id = %command.source_event_id, "redelivery budget exhausted, terminating command");
        } else {
            tracing::error!(%error, source_event_id = %command.source_event_id, "deliver fan-out failed, NAKing");
        }
    }

    apply(delivered, outcome).await;
}

fn outcome_for(delivered_count: Option<i64>, fan_out_ok: bool) -> MessageOutcome {
    if fan_out_ok {
        return MessageOutcome::Ack;
    }
    match delivered_count {
        Some(count) if count > MAX_DELIVER => MessageOutcome::Term,
        _ => MessageOutcome::Nak(Some(NAK_DELAY)),
    }
}

fn outcome_for_undecodable() -> MessageOutcome {
    MessageOutcome::Term
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
    fn fan_out_success_always_acks() {
        assert_eq!(outcome_for(Some(1), true), MessageOutcome::Ack);
        assert_eq!(
            outcome_for(Some(MAX_DELIVER + 9), true),
            MessageOutcome::Ack
        );
        assert_eq!(outcome_for(None, true), MessageOutcome::Ack);
    }

    #[test]
    fn transient_failure_naks_within_the_budget() {
        for count in 1..=MAX_DELIVER {
            assert_eq!(
                outcome_for(Some(count), false),
                MessageOutcome::Nak(Some(NAK_DELAY)),
                "delivery {count} is within the budget"
            );
        }
    }

    #[test]
    fn failure_past_the_budget_terminates() {
        assert_eq!(
            outcome_for(Some(MAX_DELIVER + 1), false),
            MessageOutcome::Term
        );
        assert_eq!(
            outcome_for(Some(MAX_DELIVER + 100), false),
            MessageOutcome::Term
        );
    }

    #[test]
    fn absent_delivery_count_on_failure_naks_rather_than_terminating() {
        assert_eq!(
            outcome_for(None, false),
            MessageOutcome::Nak(Some(NAK_DELAY))
        );
    }

    #[test]
    fn an_undecodable_frame_is_terminated_not_acked() {
        assert_eq!(outcome_for_undecodable(), MessageOutcome::Term);
    }
}
