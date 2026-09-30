// Storage assertions: what Postgres holds after a scenario. Read through the
// harness assertion connection, never written — every state is built by the
// producer's deliver command or by the recipient's own mutations.
// GAP: this file names the 1.0 table and columns (`notifications`). The engine
// version may store notifications differently; this is the one place to adapt.
use br_notifier_contract::DeliverNotification;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{RECOVERY_TIMEOUT, TestContext, TestStack, deliver, relative_link};

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct NotificationRecord {
    pub id: Uuid,
    pub source_event_id: Uuid,
    pub recipient_id: Uuid,
    pub template: String,
    pub payload: Value,
    pub link: Option<String>,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

const COLUMNS: &str =
    "id, source_event_id, recipient_id, template, payload, link, read_at, created_at";

impl TestStack {
    pub async fn rows_for(&self, recipient: Uuid) -> Vec<NotificationRecord> {
        sqlx::query_as::<_, NotificationRecord>(&format!(
            "SELECT {COLUMNS} FROM notifications WHERE recipient_id = $1 ORDER BY created_at, id"
        ))
        .bind(recipient)
        .fetch_all(&self.owner_pool)
        .await
        .expect("failed to read the notifications of a recipient (assertion connection)")
    }

    pub async fn rows_for_source(&self, source_event_id: Uuid) -> Vec<NotificationRecord> {
        sqlx::query_as::<_, NotificationRecord>(&format!(
            "SELECT {COLUMNS} FROM notifications WHERE source_event_id = $1 ORDER BY created_at, id"
        ))
        .bind(source_event_id)
        .fetch_all(&self.owner_pool)
        .await
        .expect("failed to read the notifications of a source event (assertion connection)")
    }

    pub async fn row(&self, id: Uuid) -> Option<NotificationRecord> {
        sqlx::query_as::<_, NotificationRecord>(&format!(
            "SELECT {COLUMNS} FROM notifications WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.owner_pool)
        .await
        .expect("failed to read a notification (assertion connection)")
    }

    // Waits for a fact the scenario expects, then names what was missing when it
    // never came — a timeout is a failure with a reason, not a silent false.
    pub async fn eventually<F, Fut>(&self, what: &str, predicate: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        assert!(
            br_test_harness::wait_until(RECOVERY_TIMEOUT, predicate).await,
            "not reached within {RECOVERY_TIMEOUT:?}: {what}"
        );
    }
}

// One delivery, as a producer makes it: a fresh source event, one recipient.
// Returns the stored notification once the write has committed, so the Given
// that follows can name it and so `created_at` values never tie.
pub async fn deliver_to(
    ctx: &TestContext,
    recipient: Uuid,
    template: &str,
    payload: Value,
) -> NotificationRecord {
    deliver_command(ctx, deliver(&[recipient], template, payload)).await
}

// The same, with everything a view can carry: a link and nested, non-ASCII
// payload data — so a scenario that judges "only this field changed" compares
// views that have other fields worth losing.
pub async fn deliver_rich_to(
    ctx: &TestContext,
    recipient: Uuid,
    template: &str,
) -> NotificationRecord {
    let command = DeliverNotification {
        link: relative_link(&format!("/meetings/{template}")),
        ..deliver(
            &[recipient],
            template,
            json!({
                "meeting": { "id": template, "title": "Réunion — été", "attendees": ["a", "b"] }
            }),
        )
    };
    deliver_command(ctx, command).await
}

async fn deliver_command(ctx: &TestContext, command: DeliverNotification) -> NotificationRecord {
    let source = command.source_event_id;
    let template = command.template.clone();
    ctx.stack.publish_deliver(&command).await;
    ctx.stack
        .eventually(&format!("the delivery {template} is stored"), || async {
            !ctx.stack.rows_for_source(source).await.is_empty()
        })
        .await;
    ctx.stack
        .rows_for_source(source)
        .await
        .into_iter()
        .next()
        .expect("the stored notification")
}
