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

impl NotificationSignal {
    pub const fn recipient_id(&self) -> Uuid {
        match self {
            Self::Added { recipient_id, .. }
            | Self::Read { recipient_id, .. }
            | Self::Deleted { recipient_id, .. } => *recipient_id,
        }
    }
}

pub const NOTIFY_CHANNEL: &str = "notification_events";

pub const SIGNAL_ID_CHUNK: usize = 150;

pub const SIGNALS_PER_STATEMENT: usize = 150;

async fn signal<'e, E>(executor: E, signals: &[NotificationSignal]) -> Result<(), sqlx::Error>
where
    E: Executor<'e, Database = Postgres>,
{
    let payloads: Vec<String> = signals
        .iter()
        .map(|signal| serde_json::to_string(signal).expect("signal serialization cannot fail"))
        .collect();
    sqlx::query("SELECT pg_notify($1, payload) FROM unnest($2::text[]) AS payload")
        .bind(NOTIFY_CHANNEL)
        .bind(&payloads)
        .execute(executor)
        .await?;
    Ok(())
}

async fn signal_all(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    signals: &[NotificationSignal],
) -> Result<(), sqlx::Error> {
    for batch in signals.chunks(SIGNALS_PER_STATEMENT) {
        signal(&mut **tx, batch).await?;
    }
    Ok(())
}

async fn signal_read(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
    ids: &[Uuid],
    read_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let signals: Vec<NotificationSignal> = ids
        .chunks(SIGNAL_ID_CHUNK)
        .map(|chunk| NotificationSignal::Read {
            recipient_id,
            ids: chunk.to_vec(),
            read_at,
        })
        .collect();
    signal_all(tx, &signals).await
}

async fn signal_deleted(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
    ids: &[Uuid],
) -> Result<(), sqlx::Error> {
    let signals: Vec<NotificationSignal> = ids
        .chunks(SIGNAL_ID_CHUNK)
        .map(|chunk| NotificationSignal::Deleted {
            recipient_id,
            ids: chunk.to_vec(),
        })
        .collect();
    signal_all(tx, &signals).await
}

pub async fn insert_notifications(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    source_event_id: Uuid,
    recipient_ids: &[Uuid],
    template: &str,
    payload: &serde_json::Value,
    link: Option<&RelativeLink>,
) -> Result<usize, sqlx::Error> {
    let ids: Vec<Uuid> = recipient_ids.iter().map(|_| Uuid::now_v7()).collect();
    let rows = sqlx::query(
        "INSERT INTO notifications (id, source_event_id, recipient_id, template, payload, link)
         SELECT fan_out.id, $2, fan_out.recipient_id, $4, $5, $6
         FROM unnest($1::uuid[], $3::uuid[]) AS fan_out(id, recipient_id)
         ON CONFLICT (source_event_id, recipient_id) DO NOTHING
         RETURNING id, recipient_id",
    )
    .bind(&ids)
    .bind(source_event_id)
    .bind(recipient_ids)
    .bind(template)
    .bind(payload)
    .bind(link.map(RelativeLink::as_str))
    .fetch_all(&mut **tx)
    .await?;

    let signals: Vec<NotificationSignal> = rows
        .iter()
        .map(|row| NotificationSignal::Added {
            recipient_id: row.get("recipient_id"),
            id: row.get("id"),
        })
        .collect();
    signal_all(tx, &signals).await?;
    Ok(signals.len())
}

pub struct Page {
    pub nodes: Vec<Notification>,
    pub has_next_page: bool,
}

pub async fn list_notifications(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
    first: i64,
    after: Option<Uuid>,
) -> Result<Page, sqlx::Error> {
    let limit = first.clamp(1, 100);
    let rows = sqlx::query(
        "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
         FROM notifications
         WHERE recipient_id = $1
           AND ($2::uuid IS NULL
                OR (created_at, id) < (
                    SELECT created_at, id FROM notifications
                    WHERE id = $2 AND recipient_id = $1
                ))
         ORDER BY created_at DESC, id DESC
         LIMIT $3",
    )
    .bind(recipient_id)
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

pub async fn unread_count(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    recipient_id: Uuid,
) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT COUNT(*) AS n FROM notifications
         WHERE read_at IS NULL AND recipient_id = $1",
    )
    .bind(recipient_id)
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
         WHERE id = $1 AND recipient_id = $2
         RETURNING read_at",
    )
    .bind(id)
    .bind(recipient_id)
    .fetch_optional(&mut **tx)
    .await?;
    let read_at: Option<DateTime<Utc>> = match row {
        Some(row) => row.get("read_at"),
        None => return Ok(None),
    };
    if let Some(read_at) = read_at {
        signal_read(tx, recipient_id, &[id], read_at).await?;
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
         WHERE read_at IS NULL AND recipient_id = $2
         RETURNING id",
    )
    .bind(read_at)
    .bind(recipient_id)
    .fetch_all(&mut **tx)
    .await?;
    let ids: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    if !ids.is_empty() {
        signal_read(tx, recipient_id, &ids, read_at).await?;
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
    let rows = sqlx::query(
        "DELETE FROM notifications WHERE id = ANY($1) AND recipient_id = $2 RETURNING id",
    )
    .bind(ids)
    .bind(recipient_id)
    .fetch_all(&mut **tx)
    .await?;
    let deleted: Vec<Uuid> = rows.iter().map(|row| row.get("id")).collect();
    if !deleted.is_empty() {
        signal_deleted(tx, recipient_id, &deleted).await?;
    }
    Ok(deleted)
}

pub async fn read_notification_for(
    pool: &sqlx::PgPool,
    recipient_id: Uuid,
    id: Uuid,
) -> Result<Option<Notification>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT set_config('app.current_user_id', $1, true)")
        .bind(recipient_id.to_string())
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query(
        "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
         FROM notifications WHERE id = $1 AND recipient_id = $2",
    )
    .bind(id)
    .bind(recipient_id)
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

    const PG_NOTIFY_PAYLOAD_LIMIT: usize = 8000;

    #[test]
    fn a_full_signal_chunk_stays_under_the_pg_notify_payload_limit() {
        let recipient_id = Uuid::now_v7();
        let ids: Vec<Uuid> = (0..SIGNAL_ID_CHUNK).map(|_| Uuid::now_v7()).collect();

        let read = serde_json::to_string(&NotificationSignal::Read {
            recipient_id,
            ids: ids.clone(),
            read_at: Utc::now(),
        })
        .unwrap();
        assert!(
            read.len() < PG_NOTIFY_PAYLOAD_LIMIT,
            "a bulk read signal of {SIGNAL_ID_CHUNK} ids serialises to {} bytes — over the limit \
             the whole transaction is aborted by PostgreSQL",
            read.len()
        );

        let deleted =
            serde_json::to_string(&NotificationSignal::Deleted { recipient_id, ids }).unwrap();
        assert!(
            deleted.len() < PG_NOTIFY_PAYLOAD_LIMIT,
            "a bulk delete signal of {SIGNAL_ID_CHUNK} ids serialises to {} bytes",
            deleted.len()
        );
    }
}
