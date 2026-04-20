use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NotificationRow {
    pub id: Uuid,
    pub source_event_id: Uuid,
    pub recipient_id: Uuid,
    pub template: String,
    pub payload: serde_json::Value,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

pub async fn insert_notification(
    pool: &PgPool,
    source_event_id: Uuid,
    recipient_id: Uuid,
    template: &str,
    payload: &serde_json::Value,
) -> Result<Option<NotificationRow>, sqlx::Error> {
    let id = Uuid::now_v7();
    sqlx::query_as::<_, NotificationRow>(
        r#"
        INSERT INTO notifications (id, source_event_id, recipient_id, template, payload)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (source_event_id, recipient_id) DO NOTHING
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(source_event_id)
    .bind(recipient_id)
    .bind(template)
    .bind(payload)
    .fetch_optional(pool)
    .await
}

pub async fn list_notifications(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    first: i64,
    after: Option<Uuid>,
) -> Result<Vec<NotificationRow>, sqlx::Error> {
    if let Some(cursor) = after {
        sqlx::query_as::<_, NotificationRow>(
            r#"
            SELECT n.* FROM notifications n
            WHERE n.created_at < (SELECT created_at FROM notifications WHERE id = $1)
            ORDER BY n.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(cursor)
        .bind(first)
        .fetch_all(&mut **tx)
        .await
    } else {
        sqlx::query_as::<_, NotificationRow>(
            "SELECT * FROM notifications ORDER BY created_at DESC LIMIT $1",
        )
        .bind(first)
        .fetch_all(&mut **tx)
        .await
    }
}

pub async fn unread_count(tx: &mut sqlx::Transaction<'_, Postgres>) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications WHERE read_at IS NULL")
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.0)
}

pub async fn notification_exists(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<bool, sqlx::Error> {
    let row: (bool,) = sqlx::query_as("SELECT EXISTS(SELECT 1 FROM notifications WHERE id = $1)")
        .bind(id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(row.0)
}

pub async fn mark_as_read(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result =
        sqlx::query("UPDATE notifications SET read_at = NOW() WHERE id = $1 AND read_at IS NULL")
            .bind(id)
            .execute(&mut **tx)
            .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn mark_all_as_read(
    tx: &mut sqlx::Transaction<'_, Postgres>,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query("UPDATE notifications SET read_at = NOW() WHERE read_at IS NULL")
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected())
}

pub async fn delete_notification(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM notifications WHERE id = $1")
        .bind(id)
        .execute(&mut **tx)
        .await?;
    Ok(result.rows_affected() > 0)
}
