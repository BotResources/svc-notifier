use async_graphql::SimpleObject;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::db::NotificationRow;

#[derive(Debug, Clone, SimpleObject)]
pub struct NotificationGql {
    pub id: Uuid,
    pub template: String,
    pub payload: serde_json::Value,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl From<NotificationRow> for NotificationGql {
    fn from(row: NotificationRow) -> Self {
        Self {
            id: row.id,
            template: row.template,
            payload: row.payload,
            read_at: row.read_at,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, SimpleObject)]
pub struct NotificationConnection {
    pub nodes: Vec<NotificationGql>,
    pub has_next_page: bool,
}
