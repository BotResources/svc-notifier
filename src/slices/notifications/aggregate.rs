use chrono::{DateTime, Utc};
use serde::Serialize;
use service_engine::Cohort;
use service_engine::name::NounName;
use service_engine::visibility::{Cohorts, Visibility};
use service_engine::wire::Noun;
use uuid::Uuid;

use crate::kernel::AppPrincipal;

const RECIPIENT: &str = "recipient";

/// Fixed namespace of the deterministic notification id (UUIDv5).
const ID_NAMESPACE: Uuid = Uuid::from_u128(0x6e6f_7469_6669_6572_2d31_2e31_0000_0001);

pub struct Notification;

impl Noun for Notification {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("notification");
}

/// What a delta says caused it (the `cause` of an Upsert / Remove). Small by design.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Cause {
    Delivered,
    Read,
    Deleted,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NotificationRow {
    pub id: Uuid,
    pub source_event_id: Uuid,
    pub recipient_id: Uuid,
    pub template: String,
    pub payload: serde_json::Value,
    pub link: Option<String>,
    pub read_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl NotificationRow {
    /// One source event and one recipient always name the same notification: the
    /// dedup rule is the identity itself, so a second delivery finds it and stops.
    pub fn id_for(source_event_id: Uuid, recipient_id: Uuid) -> Uuid {
        let mut name = [0u8; 32];
        name[..16].copy_from_slice(source_event_id.as_bytes());
        name[16..].copy_from_slice(recipient_id.as_bytes());
        Uuid::new_v5(&ID_NAMESPACE, &name)
    }

    pub fn deliver(
        source_event_id: Uuid,
        recipient_id: Uuid,
        template: String,
        payload: serde_json::Value,
        link: Option<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            id: Self::id_for(source_event_id, recipient_id),
            source_event_id,
            recipient_id,
            template,
            payload,
            link,
            read_at: None,
            created_at: now,
        }
    }

    /// Read is final and idempotent: `false` means nothing changed, so nothing is announced.
    pub fn mark_read(&mut self, now: DateTime<Utc>) -> bool {
        if self.read_at.is_some() {
            return false;
        }
        self.read_at = Some(now);
        true
    }

    pub fn belongs_to(&self, recipient: Uuid) -> bool {
        self.recipient_id == recipient
    }
}

impl Visibility for Notification {
    type Row = NotificationRow;
    type Principal = AppPrincipal;

    fn cohorts(row: &NotificationRow) -> Cohorts {
        vec![Cohort::uuid(RECIPIENT, row.recipient_id)]
    }

    fn memberships(principal: &AppPrincipal) -> Cohorts {
        vec![Cohort::uuid(RECIPIENT, principal.recipient())]
    }
}
