use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::notification::{
    NOTIFY_CHANNEL, Notification, NotificationSignal, read_notification_for,
};

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

    fn active(&self, recipient_id: Uuid) -> bool {
        let mut map = self.inner.lock().expect("subscribers mutex poisoned");
        let Some(sender) = map.get(&recipient_id) else {
            return false;
        };
        if sender.receiver_count() > 0 {
            return true;
        }
        map.remove(&recipient_id);
        false
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
    if !subscribers.active(signal.recipient_id()) {
        return;
    }
    match signal {
        NotificationSignal::Added { recipient_id, id } => {
            match read_notification_for(pool, recipient_id, id).await {
                Ok(Some(notification)) => {
                    subscribers.deliver(recipient_id, ClientEvent::Added(Arc::new(notification)));
                }
                Ok(None) => {}
                Err(error) => tracing::error!(%error, %id, "failed to re-read added notification"),
            }
        }
        NotificationSignal::Read {
            recipient_id,
            ids,
            read_at,
        } => {
            subscribers.deliver(recipient_id, ClientEvent::Read { ids, read_at });
        }
        NotificationSignal::Deleted { recipient_id, ids } => {
            subscribers.deliver(recipient_id, ClientEvent::Deleted { ids });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registered(subscribers: &Subscribers) -> usize {
        subscribers
            .inner
            .lock()
            .expect("subscribers mutex poisoned")
            .len()
    }

    #[test]
    fn a_recipient_nobody_watches_is_inactive_so_no_row_is_re_read() {
        let subscribers = Subscribers::default();
        assert!(!subscribers.active(Uuid::now_v7()));
    }

    #[test]
    fn an_open_stream_makes_its_recipient_active() {
        let subscribers = Subscribers::default();
        let recipient_id = Uuid::now_v7();
        let _stream = subscribers.subscribe(recipient_id);
        assert!(subscribers.active(recipient_id));
    }

    #[test]
    fn the_last_stream_closing_evicts_the_channel_instead_of_leaking_it() {
        let subscribers = Subscribers::default();
        let recipient_id = Uuid::now_v7();
        let first = subscribers.subscribe(recipient_id);
        let second = subscribers.subscribe(recipient_id);

        drop(first);
        assert!(subscribers.active(recipient_id), "one stream is still open");

        drop(second);
        assert!(!subscribers.active(recipient_id));
        assert_eq!(
            registered(&subscribers),
            0,
            "a disconnected recipient must leave no channel behind"
        );
    }
}
