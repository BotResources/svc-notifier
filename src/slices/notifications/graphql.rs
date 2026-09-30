use async_graphql::{Context, Error, ID, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::graphql::forbidden;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use super::mutations::{DeleteNotifications, MarkAllAsRead, MarkAsRead};
use super::view::{NotificationView, NotificationsView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = NotificationViewUnion;
    delta = NotificationDelta {
        reset = NotificationReset,
        upsert = NotificationUpsert,
        remove = NotificationRemove
    };
    Notification => service_engine::view::ViewProjector<NotificationsView> => NotificationView,
}

/// Notifications belong to people: a machine identity gets FORBIDDEN and no data.
fn recipient<'a>(ctx: &'a Context<'_>) -> Result<&'a AppPrincipal> {
    let principal = ctx.data::<AppPrincipal>()?;
    if principal.is_machine() {
        return Err(forbidden());
    }
    Ok(principal)
}

/// An id that is no UUID names no notification: it is ignored like an unknown one.
fn uuid_of(id: &ID) -> Option<Uuid> {
    Uuid::parse_str(&id.0).ok()
}

#[derive(Default)]
pub struct NotificationsQuery;

#[Object]
impl NotificationsQuery {
    async fn notifier_notifications(&self, ctx: &Context<'_>) -> Result<Vec<NotificationView>> {
        recipient(ctx)?;
        Query::<AppPrincipal>::new(ctx)?
            .fetch_view_window::<NotificationsView>(&())
            .await
    }
}

#[derive(Default)]
pub struct NotificationsMutation;

#[Object]
impl NotificationsMutation {
    async fn notifier_mark_as_read(
        &self,
        ctx: &Context<'_>,
        notification_id: ID,
    ) -> Result<MutationAck> {
        recipient(ctx)?;
        let ids = uuid_of(&notification_id);
        match ids {
            Some(id) => {
                service_engine::ack::<AppPrincipal, MarkAsRead>(ctx, MarkAsRead { id }).await
            }
            None => Ok(MutationAck::ok()),
        }
    }

    async fn notifier_mark_all_as_read(&self, ctx: &Context<'_>) -> Result<MutationAck> {
        recipient(ctx)?;
        service_engine::ack::<AppPrincipal, MarkAllAsRead>(ctx, MarkAllAsRead).await
    }

    async fn notifier_delete_notification(
        &self,
        ctx: &Context<'_>,
        notification_id: ID,
    ) -> Result<MutationAck> {
        recipient(ctx)?;
        let ids = uuid_of(&notification_id).into_iter().collect();
        service_engine::ack::<AppPrincipal, DeleteNotifications>(ctx, DeleteNotifications { ids })
            .await
    }

    async fn notifier_delete_notifications(
        &self,
        ctx: &Context<'_>,
        ids: Vec<ID>,
    ) -> Result<MutationAck> {
        recipient(ctx)?;
        let ids = ids.iter().filter_map(uuid_of).collect();
        service_engine::ack::<AppPrincipal, DeleteNotifications>(ctx, DeleteNotifications { ids })
            .await
    }
}

#[derive(Default)]
pub struct NotificationsSubscription;

#[Subscription]
impl NotificationsSubscription {
    async fn notifier_notification_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<NotificationDelta>>> {
        // ENGINE-GAP: the engine refuses an attach above `window_capacity` (default 10,000)
        // with WINDOW_TOO_LARGE; 1.0 had no such bound and the target contract has no page
        // argument, so a recipient above it gets no stream at all.
        recipient(ctx).map_err(|_: Error| forbidden())?;
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(
                NotificationsView::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| NotificationDelta::from_delta(&delta)))
    }
}
