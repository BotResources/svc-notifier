use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, Postgres, Row};
use uuid::Uuid;

use br_notifier_contract::RelativeLink;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HydrationError {
    #[error("notification_link_corrupt")]
    LinkCorrupt,
}

#[derive(Debug, Clone)]
pub struct Notification {
    pub id: Uuid,
    pub template: String,
    pub payload: serde_json::Value,
    pub link: Option<RelativeLink>,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl Notification {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, HydrationError> {
        let stored_link: Option<String> = row.get("link");
        let link = match stored_link {
            Some(raw) => Some(RelativeLink::parse(raw).map_err(|_| HydrationError::LinkCorrupt)?),
            None => None,
        };
        Ok(Self {
            id: row.get("id"),
            template: row.get("template"),
            payload: row.get("payload"),
            link,
            read_at: row.get("read_at"),
            created_at: row.get("created_at"),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationSignal {
    Added {
        recipient_id: Uuid,
        id: Uuid,
    },
    Read {
        recipient_id: Uuid,
        ids: Vec<Uuid>,
        read_at: DateTime<Utc>,
    },
    Deleted {
        recipient_id: Uuid,
        ids: Vec<Uuid>,
    },
}

pub const NOTIFY_CHANNEL: &str = "notification_events";

pub async fn set_rls_user(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    user_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(user_id.to_string())
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn signal<'e, E>(executor: E, signal: &NotificationSignal) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let payload = serde_json::to_string(signal).expect("signal serialization cannot fail");
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(NOTIFY_CHANNEL)
        .bind(payload)
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn insert_notification(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    source_event_id: Uuid,
    recipient_id: Uuid,
    template: &str,
    payload: &serde_json::Value,
    link: Option<&RelativeLink>,
) -> Result<Option<Uuid>, sqlx::Error> {
    let id = Uuid::now_v7();
    let inserted: Option<Uuid> = sqlx::query(
        "INSERT INTO notifications (id, source_event_id, recipient_id, template, payload, link)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (source_event_id, recipient_id) DO NOTHING
         RETURNING id",
    )
    .bind(id)
    .bind(source_event_id)
    .bind(recipient_id)
    .bind(template)
    .bind(payload)
    .bind(link.map(RelativeLink::as_str))
    .fetch_optional(&mut **tx)
    .await?
    .map(|row| row.get("id"));

    if let Some(new_id) = inserted {
        signal(
            &mut **tx,
            &NotificationSignal::Added {
                recipient_id,
                id: new_id,
            },
        )
        .await?;
    }
    Ok(inserted)
}

pub struct Page {
    pub nodes: Vec<Notification>,
    pub has_next_page: bool,
}

pub async fn list_notifications(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    first: i64,
    after: Option<Uuid>,
) -> Result<Page, sqlx::Error> {
    let limit = first.clamp(1, 100);
    let rows = sqlx::query(
        "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
         FROM notifications
         WHERE $1::uuid IS NULL
            OR (created_at, id) < (
                SELECT created_at, id FROM notifications WHERE id = $1
            )
         ORDER BY created_at DESC, id DESC
         LIMIT $2",
    )
    .bind(after)
    .bind(limit + 1)
    .fetch_all(&mut **tx)
    .await?;

    let has_next_page = rows.len() as i64 > limit;
    let nodes = rows
        .iter()
        .take(limit as usize)
        .map(Notification::from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(corrupt)?;
    Ok(Page {
        nodes,
        has_next_page,
    })
}

pub async fn unread_count(tx: &mut sqlx::Transaction<'_, Postgres>) -> Result<i64, sqlx::Error> {
    let row = sqlx::query("SELECT COUNT(*) AS n FROM notifications WHERE read_at IS NULL")
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.get("n"))
}

pub async fn mark_as_read(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
    id: Uuid,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let row = sqlx::query(
        "UPDATE notifications
         SET read_at = COALESCE(read_at, now())
         WHERE id = $1
         RETURNING read_at",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let read_at: Option<DateTime<Utc>> = match row {
        Some(row) => row.get("read_at"),
        None => return Ok(None),
    };
    if let Some(read_at) = read_at {
        signal(
            &mut **tx,
            &NotificationSignal::Read {
                recipient_id,
                ids: vec![id],
                read_at,
            },
        )
        .await?;
        return Ok(Some(read_at));
    }
    Ok(None)
}

pub async fn mark_all_as_read(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
) -> Result<(Vec<Uuid>, DateTime<Utc>), sqlx::Error> {
    let read_at = Utc::now();
    let rows = sqlx::query(
        "UPDATE notifications
         SET read_at = $1
         WHERE read_at IS NULL
         RETURNING id",
    )
    .bind(read_at)
    .fetch_all(&mut **tx)
    .await?;
    let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    if !ids.is_empty() {
        signal(
            &mut **tx,
            &NotificationSignal::Read {
                recipient_id,
                ids: ids.clone(),
                read_at,
            },
        )
        .await?;
    }
    Ok((ids, read_at))
}

pub async fn delete_notifications(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
    ids: &[Uuid],
) -> Result<Vec<Uuid>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query("DELETE FROM notifications WHERE id = ANY($1) RETURNING id")
        .bind(ids)
        .fetch_all(&mut **tx)
        .await?;
    let deleted: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    if !deleted.is_empty() {
        signal(
            &mut **tx,
            &NotificationSignal::Deleted {
                recipient_id,
                ids: deleted.clone(),
            },
        )
        .await?;
    }
    Ok(deleted)
}

pub async fn read_notification_for(
    pool: &sqlx::PgPool,
    recipient_id: Uuid,
    id: Uuid,
) -> Result<Option<Notification>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_rls_user(&mut tx, recipient_id).await?;
    let row = sqlx::query(
        "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
         FROM notifications WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let notification = match row {
        Some(row) => Some(Notification::from_row(&row).map_err(corrupt)?),
        None => None,
    };
    tx.commit().await?;
    Ok(notification)
}

fn corrupt(error: HydrationError) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signal_serializes_with_a_type_tag() {
        let recipient_id = Uuid::now_v7();
        let id = Uuid::now_v7();
        let value = serde_json::to_value(NotificationSignal::Added { recipient_id, id }).unwrap();
        assert_eq!(value["type"], "added");
        assert_eq!(value["recipient_id"], json!(recipient_id));
        assert_eq!(value["id"], json!(id));

        let read_at = Utc::now();
        let value = serde_json::to_value(NotificationSignal::Read {
            recipient_id,
            ids: vec![id],
            read_at,
        })
        .unwrap();
        assert_eq!(value["type"], "read");
        assert_eq!(value["ids"], json!([id]));
        assert_eq!(value["read_at"], json!(read_at));

        let value = serde_json::to_value(NotificationSignal::Deleted {
            recipient_id,
            ids: vec![id],
        })
        .unwrap();
        assert_eq!(value["type"], "deleted");
    }
}
