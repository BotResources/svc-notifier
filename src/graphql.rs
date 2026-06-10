use async_graphql::{
    Context, Error, ErrorExtensions, ID, Object, Result, SimpleObject, Subscription, Union,
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use sqlx::PgPool;
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

use br_core_auth::Passport;

use crate::notification::{
    Notification, delete_notifications, list_notifications, mark_all_as_read, mark_as_read,
    unread_count,
};
use crate::realtime::{ClientEvent, Subscribers};

pub struct AppState {
    pub app_pool: PgPool,
    pub subscribers: Subscribers,
}

struct Recipient(Uuid);

fn recipient(ctx: &Context<'_>) -> Result<Recipient> {
    match ctx.data::<Passport>()? {
        Passport::Human { user_id, .. } => Ok(Recipient(*user_id)),
        Passport::Service { .. } => Err(coded("FORBIDDEN")),
    }
}

fn coded(code: &str) -> Error {
    Error::new(code.to_string()).extend_with(|_, e| e.set("code", code))
}

fn db_error(error: sqlx::Error) -> Error {
    tracing::error!(%error, "database error");
    coded("INTERNAL")
}

#[derive(SimpleObject)]
#[graphql(name = "Notification")]
pub struct NotificationNode {
    pub id: ID,
    pub template: String,
    pub payload: serde_json::Value,
    pub link: Option<String>,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl From<&Notification> for NotificationNode {
    fn from(notification: &Notification) -> Self {
        Self {
            id: ID(notification.id.to_string()),
            template: notification.template.clone(),
            payload: notification.payload.clone(),
            link: notification
                .link
                .as_ref()
                .map(|link| link.as_str().to_string()),
            read_at: notification.read_at,
            created_at: notification.created_at,
        }
    }
}

#[derive(SimpleObject)]
pub struct NotificationConnection {
    pub nodes: Vec<NotificationNode>,
    pub has_next_page: bool,
}

#[derive(SimpleObject)]
pub struct NotificationAdded {
    pub notification: NotificationNode,
}

#[derive(SimpleObject)]
pub struct NotificationsRead {
    pub ids: Vec<ID>,
    pub read_at: DateTime<Utc>,
}

#[derive(SimpleObject)]
pub struct NotificationsDeleted {
    pub ids: Vec<ID>,
}

#[derive(Union)]
pub enum NotifierNotificationEvent {
    NotificationAdded(NotificationAdded),
    NotificationsRead(NotificationsRead),
    NotificationsDeleted(NotificationsDeleted),
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn notifier_notifications(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 20)] first: i32,
        after: Option<ID>,
    ) -> Result<NotificationConnection> {
        let caller = recipient(ctx)?;
        let after = parse_id(after)?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        let page = list_notifications(&mut tx, first as i64, after)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(NotificationConnection {
            nodes: page.nodes.iter().map(NotificationNode::from).collect(),
            has_next_page: page.has_next_page,
        })
    }

    async fn notifier_unread_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let caller = recipient(ctx)?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        let count = unread_count(&mut tx).await.map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(count as i32)
    }
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn notifier_mark_as_read(&self, ctx: &Context<'_>, notification_id: ID) -> Result<bool> {
        let caller = recipient(ctx)?;
        let id = require_id(&notification_id)?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        match mark_as_read(&mut tx, caller.0, id)
            .await
            .map_err(db_error)?
        {
            Some(_) => {
                tx.commit().await.map_err(db_error)?;
                Ok(true)
            }
            None => Err(coded("NOT_FOUND")),
        }
    }

    async fn notifier_mark_all_as_read(&self, ctx: &Context<'_>) -> Result<bool> {
        let caller = recipient(ctx)?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        mark_all_as_read(&mut tx, caller.0)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(true)
    }

    async fn notifier_delete_notification(
        &self,
        ctx: &Context<'_>,
        notification_id: ID,
    ) -> Result<bool> {
        let caller = recipient(ctx)?;
        let id = require_id(&notification_id)?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        let deleted = delete_notifications(&mut tx, caller.0, &[id])
            .await
            .map_err(db_error)?;
        if deleted.is_empty() {
            return Err(coded("NOT_FOUND"));
        }
        tx.commit().await.map_err(db_error)?;
        Ok(true)
    }

    async fn notifier_delete_notifications(&self, ctx: &Context<'_>, ids: Vec<ID>) -> Result<bool> {
        let caller = recipient(ctx)?;
        let ids = ids.iter().map(require_id).collect::<Result<Vec<_>>>()?;
        let mut tx = scoped_tx(ctx, &caller).await?;
        delete_notifications(&mut tx, caller.0, &ids)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(true)
    }
}

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn notifier_notification_events(
        &self,
        ctx: &Context<'_>,
    ) -> impl Stream<Item = NotifierNotificationEvent> + use<> {
        let receiver = match (ctx.data::<AppState>(), ctx.data::<Passport>()) {
            (Ok(state), Ok(Passport::Human { user_id, .. })) => {
                Some(state.subscribers.subscribe(*user_id))
            }
            _ => None,
        };
        futures::stream::iter(receiver)
            .flat_map(BroadcastStream::new)
            .filter_map(|event| async move { event.ok().map(into_union) })
    }
}

fn into_union(event: ClientEvent) -> NotifierNotificationEvent {
    match event {
        ClientEvent::Added(notification) => {
            NotifierNotificationEvent::NotificationAdded(NotificationAdded {
                notification: NotificationNode::from(notification.as_ref()),
            })
        }
        ClientEvent::Read { ids, read_at } => {
            NotifierNotificationEvent::NotificationsRead(NotificationsRead {
                ids: ids.into_iter().map(|id| ID(id.to_string())).collect(),
                read_at,
            })
        }
        ClientEvent::Deleted { ids } => {
            NotifierNotificationEvent::NotificationsDeleted(NotificationsDeleted {
                ids: ids.into_iter().map(|id| ID(id.to_string())).collect(),
            })
        }
    }
}

async fn scoped_tx<'a>(
    ctx: &Context<'a>,
    caller: &Recipient,
) -> Result<sqlx::Transaction<'a, sqlx::Postgres>> {
    let state = ctx.data::<AppState>()?;
    let mut tx = state.app_pool.begin().await.map_err(db_error)?;
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(caller.0.to_string())
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
    Ok(tx)
}

fn parse_id(id: Option<ID>) -> Result<Option<Uuid>> {
    match id {
        Some(id) => Ok(Some(require_id(&id)?)),
        None => Ok(None),
    }
}

fn require_id(id: &ID) -> Result<Uuid> {
    Uuid::parse_str(id.as_str()).map_err(|_| coded("BAD_REQUEST"))
}
