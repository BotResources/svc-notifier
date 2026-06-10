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

/// The authoritative state of one delivered notification, hydrated from its row.
/// Hydration re-validates the stored `link` through the contract newtype, so a
/// row that somehow holds an unsafe link refuses to load rather than serve it —
/// the same fail-closed barrier the contract applies at intake, re-applied on
/// read. The `Unread → Read` lifecycle is the presence of `read_at`; its
/// irreversibility is enforced in SQL (no path ever clears it).
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

/// The small fact a write emits on `notification_events` (the PG channel),
/// in the same transaction as the write. Carries no notification content —
/// the listener re-reads current PG state to build the client event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationSignal {
    Added { recipient_id: Uuid, id: Uuid },
    Read { recipient_id: Uuid, ids: Vec<Uuid> },
    Deleted { recipient_id: Uuid, ids: Vec<Uuid> },
}

pub const NOTIFY_CHANNEL: &str = "notification_events";

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

/// Insert one notification, deduplicated on `(source_event_id, recipient_id)`.
/// First write wins: a conflicting row is left untouched and `None` is returned,
/// so a redelivered or duplicated command never double-applies and never pushes
/// a second event. Emits an `Added` signal only when a row is actually created.
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

/// Newest-first page under the caller's RLS context. `after` is the last id of
/// the previous page; pagination walks the `(created_at DESC, id DESC)` order.
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

/// Mark one notification read under the caller's RLS context. Idempotent: an
/// already-read row keeps its original `read_at` and is still reported affected,
/// so the ack stays `true`. Returns `None` when the id is foreign or unknown.
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
            },
        )
        .await?;
        return Ok(Some(read_at));
    }
    Ok(None)
}

/// Mark every unread notification of the caller read, emitting exactly one bulk
/// `Read` signal carrying all affected ids — empty when nothing was unread.
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
            },
        )
        .await?;
    }
    Ok((ids, read_at))
}

/// Hard-delete the caller's notifications among `ids`. Foreign ids are invisible
/// to RLS, hence silently skipped; the returned ids and the emitted `Deleted`
/// signal carry only rows actually removed, so a foreign id can never be probed.
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

/// Re-read one notification across RLS (system path) for the realtime listener
/// to build the `Added` client event from committed state.
pub async fn read_notification_unscoped(
    pool: &sqlx::PgPool,
    id: Uuid,
) -> Result<Option<Notification>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
         FROM notifications WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(Notification::from_row(&row).map_err(corrupt)?)),
        None => Ok(None),
    }
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

        let value = serde_json::to_value(NotificationSignal::Read {
            recipient_id,
            ids: vec![id],
        })
        .unwrap();
        assert_eq!(value["type"], "read");
        assert_eq!(value["ids"], json!([id]));

        let value = serde_json::to_value(NotificationSignal::Deleted {
            recipient_id,
            ids: vec![id],
        })
        .unwrap();
        assert_eq!(value["type"], "deleted");
    }
}
