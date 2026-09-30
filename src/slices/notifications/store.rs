use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::aggregate::NotificationRow;

// The 1.0 table, unchanged: id, source_event_id, recipient_id, template, payload, link,
// read_at, created_at.
const COLUMNS: &str =
    "id, source_event_id, recipient_id, template, payload, link, read_at, created_at";

fn row_of(row: &sqlx::postgres::PgRow) -> NotificationRow {
    NotificationRow {
        id: row.get("id"),
        source_event_id: row.get("source_event_id"),
        recipient_id: row.get("recipient_id"),
        template: row.get("template"),
        payload: row.get("payload"),
        link: row.get("link"),
        read_at: row.get::<Option<DateTime<Utc>>, _>("read_at"),
        created_at: row.get("created_at"),
    }
}

pub struct NotificationStore;

impl Persistence for NotificationStore {
    type Aggregate = NotificationRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Self::row_lock(conn, "notifications", key)
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, NotificationRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(&format!(
                "SELECT {COLUMNS} FROM notifications WHERE id = ANY($1)"
            ))
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .iter()
                .map(|row| {
                    let notification = row_of(row);
                    (notification.id, notification)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        notification: &'a NotificationRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            // Only `read_at` ever changes after delivery.
            sqlx::query("UPDATE notifications SET read_at = $2 WHERE id = $1")
                .bind(notification.id)
                .bind(notification.read_at)
                .execute(conn)
                .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        notification: &'a NotificationRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(&format!(
                "INSERT INTO notifications ({COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            ))
            .bind(notification.id)
            .bind(notification.source_event_id)
            .bind(notification.recipient_id)
            .bind(&notification.template)
            .bind(&notification.payload)
            .bind(&notification.link)
            .bind(notification.read_at)
            .bind(notification.created_at)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM notifications WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for NotificationRow {
    type Store = NotificationStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

/// The recipient's notifications, newest first, bounded by what the caller answers.
pub async fn ids_of_recipient(
    conn: &mut PgConnection,
    recipient: Uuid,
    limit: i64,
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT id FROM notifications WHERE recipient_id = $1 \
         ORDER BY created_at DESC, id DESC LIMIT $2",
    )
    .bind(recipient)
    .bind(limit)
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn unread_ids_of_recipient(
    conn: &mut PgConnection,
    recipient: Uuid,
) -> Result<Vec<Uuid>, EngineError> {
    let rows =
        sqlx::query("SELECT id FROM notifications WHERE recipient_id = $1 AND read_at IS NULL")
            .bind(recipient)
            .fetch_all(conn)
            .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

/// A notification a 1.0 store already holds for this (source event, recipient) carries a
/// random id, not the deterministic one: the natural key is the dedup of last resort.
pub async fn exists_for_source(
    conn: &mut PgConnection,
    source_event_id: Uuid,
    recipient: Uuid,
) -> Result<bool, EngineError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM notifications WHERE source_event_id = $1 AND recipient_id = $2)",
    )
    .bind(source_event_id)
    .bind(recipient)
    .fetch_one(conn)
    .await?)
}
