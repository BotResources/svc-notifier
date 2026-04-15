use std::pin::Pin;

use async_graphql::{Context, Subscription};
use futures::Stream;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use br_service_core::Passport;

use crate::AppState;
use crate::graphql::types::NotificationGql;
use crate::nats::UserChannels;

pub struct SubscriptionRoot;

type NotifStream = Pin<Box<dyn Stream<Item = NotificationGql> + Send>>;

fn empty_stream() -> NotifStream {
    Box::pin(futures::stream::empty())
}

#[Subscription]
impl SubscriptionRoot {
    /// Real-time notification push per user.
    ///
    /// Note: async-graphql #[Subscription] does not support Result return types.
    /// Auth failures return a closed stream and log a warning. The client sees
    /// an empty subscription that immediately completes — no items, no error payload.
    /// WebSocket-level auth rejection requires middleware outside GraphQL.
    async fn notifier_notification_added(&self, ctx: &Context<'_>) -> NotifStream {
        let passport = match ctx.data::<Passport>() {
            Ok(p) => p.clone(),
            Err(_) => {
                tracing::warn!("subscription attempted without valid Passport — rejecting");
                return empty_stream();
            }
        };
        let state = match ctx.data::<AppState>() {
            Ok(s) => s,
            Err(_) => return empty_stream(),
        };
        let channels = state.channels.clone();
        let user_id = match &passport {
            Passport::Human { user_id, .. } => *user_id,
            Passport::Service { .. } => return empty_stream(),
        };

        let mut channels_guard = channels.lock().await;
        let rx = match channels_guard.get(&user_id) {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(64);
                channels_guard.insert(user_id, tx);
                rx
            }
        };
        drop(channels_guard);

        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(item) => Some(item),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!(lagged = n, user_id = %user_id, "subscription lagged");
                None
            }
        });

        Box::pin(CleanupStream {
            inner: Box::pin(stream),
            user_id,
            channels,
        })
    }
}

/// Stream wrapper that cleans up the broadcast channel entry on drop.
struct CleanupStream {
    inner: Pin<Box<dyn Stream<Item = NotificationGql> + Send>>,
    user_id: Uuid,
    channels: UserChannels,
}

impl Stream for CleanupStream {
    type Item = NotificationGql;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl Drop for CleanupStream {
    fn drop(&mut self) {
        let user_id = self.user_id;
        let channels = self.channels.clone();
        tokio::spawn(async move {
            let mut guard = channels.lock().await;
            if let Some(tx) = guard.get(&user_id)
                && tx.receiver_count() == 0
            {
                guard.remove(&user_id);
            }
        });
    }
}
