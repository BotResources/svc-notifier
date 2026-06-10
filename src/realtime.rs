use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::notification::{
    NOTIFY_CHANNEL, Notification, NotificationSignal, read_notification_unscoped,
};

/// What a subscription connection receives. Mirrors the GraphQL event union
/// field-for-field; the GraphQL layer only renames it onto the SDL types.
#[derive(Debug, Clone)]
pub enum ClientEvent {
    Added(Arc<Notification>),
    Read {
        ids: Vec<Uuid>,
        read_at: DateTime<Utc>,
    },
    Deleted {
        ids: Vec<Uuid>,
    },
}

/// Per-recipient fan-out of committed events to that recipient's open
/// subscription connections. The only in-process state; a write handled by any
/// instance reaches subscribers on this instance because the trigger is PG
/// `LISTEN/NOTIFY`, not an in-process call from the writer.
#[derive(Clone, Default)]
pub struct Subscribers {
    inner: Arc<Mutex<HashMap<Uuid, broadcast::Sender<ClientEvent>>>>,
}

impl Subscribers {
    pub fn subscribe(&self, recipient_id: Uuid) -> broadcast::Receiver<ClientEvent> {
        let mut map = self.inner.lock().expect("subscribers mutex poisoned");
        map.entry(recipient_id)
            .or_insert_with(|| broadcast::channel(256).0)
            .subscribe()
    }

    fn deliver(&self, recipient_id: Uuid, event: ClientEvent) {
        let sender = {
            let map = self.inner.lock().expect("subscribers mutex poisoned");
            map.get(&recipient_id).cloned()
        };
        if let Some(sender) = sender {
            let _ = sender.send(event);
        }
    }
}

/// Run the PG listener for the lifetime of the process. Each `pg_notify` on
/// [`NOTIFY_CHANNEL`] is decoded, turned into a [`ClientEvent`] from committed
/// state (re-reading the row for `Added`), and routed to the affected
/// recipient's local subscribers. Reconnects on listener error.
pub async fn run_listener(pool: PgPool, subscribers: Subscribers) {
    loop {
        if let Err(error) = listen_loop(&pool, &subscribers).await {
            tracing::error!(%error, "notification listener dropped, reconnecting");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

async fn listen_loop(pool: &PgPool, subscribers: &Subscribers) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(NOTIFY_CHANNEL).await?;
    loop {
        let message = listener.recv().await?;
        let signal: NotificationSignal = match serde_json::from_str(message.payload()) {
            Ok(signal) => signal,
            Err(error) => {
                tracing::warn!(%error, payload = message.payload(), "undecodable notification signal");
                continue;
            }
        };
        dispatch(pool, subscribers, signal).await;
    }
}

async fn dispatch(pool: &PgPool, subscribers: &Subscribers, signal: NotificationSignal) {
    match signal {
        NotificationSignal::Added { recipient_id, id } => {
            match read_notification_unscoped(pool, id).await {
                Ok(Some(notification)) => {
                    subscribers.deliver(recipient_id, ClientEvent::Added(Arc::new(notification)));
                }
                Ok(None) => {}
                Err(error) => tracing::error!(%error, %id, "failed to re-read added notification"),
            }
        }
        NotificationSignal::Read { recipient_id, ids } => {
            let read_at = read_at_for(pool, &ids).await.unwrap_or_else(Utc::now);
            subscribers.deliver(recipient_id, ClientEvent::Read { ids, read_at });
        }
        NotificationSignal::Deleted { recipient_id, ids } => {
            subscribers.deliver(recipient_id, ClientEvent::Deleted { ids });
        }
    }
}

async fn read_at_for(pool: &PgPool, ids: &[Uuid]) -> Option<DateTime<Utc>> {
    let first = ids.first()?;
    read_notification_unscoped(pool, *first)
        .await
        .ok()
        .flatten()
        .and_then(|notification| notification.read_at)
}
