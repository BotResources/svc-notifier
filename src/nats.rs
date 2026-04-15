use std::collections::HashMap;
use std::sync::Arc;

use async_nats::jetstream;
use async_nats::jetstream::consumer::AckPolicy;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db;
use crate::graphql::types::NotificationGql;

#[derive(Debug, Deserialize)]
pub struct NotifyDeliverPayload {
    pub source_event_id: Uuid,
    pub recipient_ids: Vec<Uuid>,
    pub template: String,
    pub payload: serde_json::Value,
}

pub type UserChannels = Arc<tokio::sync::Mutex<HashMap<Uuid, broadcast::Sender<NotificationGql>>>>;

pub fn new_user_channels() -> UserChannels {
    Arc::new(tokio::sync::Mutex::new(HashMap::new()))
}

pub async fn run_consumer(jetstream: jetstream::Context, pool: PgPool, channels: UserChannels) {
    let stream = match jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: "NOTIFY".to_string(),
            subjects: vec!["notify.>".to_string()],
            storage: jetstream::stream::StorageType::File,
            retention: jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        })
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to get/create NOTIFY stream");
            return;
        }
    };

    let consumer = match stream
        .get_or_create_consumer(
            "svc-notifier",
            jetstream::consumer::pull::Config {
                durable_name: Some("svc-notifier".to_string()),
                filter_subject: "notify.deliver".to_string(),
                max_deliver: 5,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to create consumer");
            return;
        }
    };

    use futures::StreamExt;

    let mut messages = match consumer.messages().await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to open consumer messages stream");
            return;
        }
    };

    while let Some(result) = messages.next().await {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "error receiving message from stream");
                continue;
            }
        };

        match serde_json::from_slice::<NotifyDeliverPayload>(&msg.payload) {
            Ok(payload) => {
                let success = process_message(&pool, &channels, payload).await;
                if success {
                    if let Err(e) = msg.ack().await {
                        tracing::warn!(error = %e, "failed to ack message");
                    }
                } else if let Err(e) = msg
                    .ack_with(async_nats::jetstream::AckKind::Nak(None))
                    .await
                {
                    tracing::warn!(error = %e, "failed to nack message");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "malformed notify.deliver message, skipping");
                if let Err(e) = msg.ack().await {
                    tracing::warn!(error = %e, "failed to ack malformed message");
                }
            }
        }
    }

    tracing::warn!("consumer messages stream ended unexpectedly");
}

/// Returns true if all inserts succeeded, false if any DB error occurred.
///
/// Public for integration testing. In production, called only by `run_consumer`.
pub async fn process_message(
    pool: &PgPool,
    channels: &UserChannels,
    payload: NotifyDeliverPayload,
) -> bool {
    if payload.recipient_ids.is_empty() {
        tracing::warn!(
            source_event_id = %payload.source_event_id,
            template = %payload.template,
            "empty recipient_ids in notify.deliver message"
        );
        return true; // nothing to do, ack the message
    }

    let mut all_ok = true;

    for recipient_id in &payload.recipient_ids {
        match db::insert_notification(
            pool,
            payload.source_event_id,
            *recipient_id,
            &payload.template,
            &payload.payload,
        )
        .await
        {
            Ok(Some(row)) => {
                let gql = NotificationGql::from(row);
                let mut channels_guard = channels.lock().await;
                if let Some(tx) = channels_guard.get(recipient_id) {
                    if tx.receiver_count() == 0 {
                        channels_guard.remove(recipient_id);
                    } else {
                        let _ = tx.send(gql);
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    source_event_id = %payload.source_event_id,
                    recipient_id = %recipient_id,
                    "duplicate notification, skipped"
                );
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    source_event_id = %payload.source_event_id,
                    recipient_id = %recipient_id,
                    "failed to insert notification"
                );
                all_ok = false;
            }
        }
    }

    all_ok
}
