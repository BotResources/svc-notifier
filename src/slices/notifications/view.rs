use async_graphql::{ID, SimpleObject};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector};
use uuid::Uuid;

use super::aggregate::{Notification, NotificationRow};
use super::store::{self, NotificationStore};
use crate::kernel::AppPrincipal;

/// The notification as the recipient sees it: exactly the target SDL type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SimpleObject)]
#[graphql(name = "Notification")]
#[serde(rename_all = "camelCase")]
pub struct NotificationView {
    pub id: ID,
    pub template: String,
    pub payload: serde_json::Value,
    pub link: Option<String>,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Default)]
pub struct NotificationsView;

impl NotificationsView {
    pub const NAME: ProjectorName = ProjectorName::from_static("notifications");
}

impl Projector for NotificationsView {
    type Principal = AppPrincipal;
    type Noun = Notification;
    type Store = NotificationStore;
    type Query = ();
    type Out = NotificationView;
    // The second enforcement layer: the recipient cohort, applied by the engine at populate,
    // at render and at delta routing.
    type Visibility = Notification;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, AppPrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        let mut conn = cx.pool().acquire().await?;
        let keys =
            store::ids_of_recipient(&mut conn, cx.principal().recipient(), cx.limit_all()).await?;
        // Newest first, and an open head: a new notification enters the window.
        // ENGINE-GAP: the order holds for the one-shot query (`fetch_view_window` keeps the
        // population order) but NOT for the stream's first payload: the engine builds the
        // Reset from a map keyed by the key bytes, so it lists the views by id, not by
        // population order. `scenarios_stream::a_new_stream_starts_with_the_whole_list`
        // is red on exactly that assertion; a UUIDv7 id sorts oldest first.
        Ok(Population::Ordered {
            keys,
            open_head: true,
        })
    }

    fn project(
        row: &NotificationRow,
        _principal: &AppPrincipal,
    ) -> Result<NotificationView, EngineError> {
        Ok(NotificationView {
            id: ID(row.id.to_string()),
            template: row.template.clone(),
            payload: row.payload.clone(),
            link: row.link.clone(),
            read_at: row.read_at,
            created_at: row.created_at,
        })
    }
}
