use std::time::Duration;

use async_nats::jetstream::{self, consumer::PullConsumer};
use futures::StreamExt;
use sqlx::PgPool;

use br_notifier_contract::{DELIVER_SUBJECT, DeliverNotification};

use crate::notification::insert_notification;

const STREAM_NAME: &str = "NOTIFY";
const DURABLE_NAME: &str = "svc-notifier";
const MAX_DELIVER: i64 = 5;
const ACK_WAIT: Duration = Duration::from_secs(5);
const NAK_DELAY: Duration = Duration::from_secs(1);

pub async fn run(jetstream: jetstream::Context, ingest_pool: PgPool) -> Result<(), IntakeError> {
    let stream = jetstream
        .get_stream(STREAM_NAME)
        .await
        .map_err(|e| IntakeError::Stream(e.to_string()))?;

    let consumer: PullConsumer = stream
        .create_consumer_strict(jetstream::consumer::pull::Config {
            durable_name: Some(DURABLE_NAME.to_string()),
            filter_subject: DELIVER_SUBJECT.to_string(),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: MAX_DELIVER,
            ack_wait: ACK_WAIT,
            ..Default::default()
        })
        .await
        .map_err(|e| IntakeError::Consumer(e.to_string()))?;

    let mut messages = consumer
        .messages()
        .await
        .map_err(|e| IntakeError::Consumer(e.to_string()))?;

    tracing::info!(subject = DELIVER_SUBJECT, "intake consumer ready");
    while let Some(message) = messages.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, "intake message error");
                continue;
            }
        };
        handle(&ingest_pool, &message).await;
    }
    Ok(())
}

async fn handle(pool: &PgPool, message: &jetstream::Message) {
    let command: DeliverNotification = match serde_json::from_slice(&message.payload) {
        Ok(command) => command,
        Err(error) => {
            tracing::warn!(%error, "rejecting undecodable deliver command (fail-closed)");
            ack(message).await;
            return;
        }
    };

    if delivery_count(message) >= MAX_DELIVER {
        tracing::error!(
            source_event_id = %command.source_event_id,
            "redelivery budget exhausted, dropping command"
        );
        terminate(message).await;
        return;
    }

    match fan_out(pool, &command).await {
        Ok(()) => ack(message).await,
        Err(error) => {
            tracing::error!(%error, source_event_id = %command.source_event_id, "deliver fan-out failed, NAKing");
            if let Err(nak_error) = message
                .ack_with(jetstream::AckKind::Nak(Some(NAK_DELAY)))
                .await
            {
                tracing::error!(%nak_error, "failed to NAK message");
            }
        }
    }
}

fn delivery_count(message: &jetstream::Message) -> i64 {
    message.info().map(|info| info.delivered).unwrap_or(1)
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

async fn ack(message: &jetstream::Message) {
    if let Err(error) = message.ack().await {
        tracing::error!(%error, "failed to ack message");
    }
}

async fn terminate(message: &jetstream::Message) {
    if let Err(error) = message.ack_with(jetstream::AckKind::Term).await {
        tracing::error!(%error, "failed to terminate message");
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IntakeError {
    #[error("intake stream unavailable: {0}")]
    Stream(String),
    #[error("intake consumer unavailable: {0}")]
    Consumer(String),
}
