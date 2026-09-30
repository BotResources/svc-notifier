// The wire of notifier 1.1, exactly as the target SDL states it: one list query
// with no arguments, four ack-only mutations, one delta stream. There is no
// unread count and no pagination anywhere in this suite.
use br_core_auth::{Passport, PassportBuilder};
use br_notifier_contract::{DeliverNotification, RelativeLink};
use br_test_harness::verdict;
use serde_json::{Value, json};
use uuid::Uuid;

pub const LIST_QUERY: &str =
    "query { notifierNotifications { id template payload link readAt createdAt } }";

pub const MARK_AS_READ: &str =
    "mutation($id: ID!) { notifierMarkAsRead(notificationId: $id) { success } }";
pub const MARK_ALL_AS_READ: &str = "mutation { notifierMarkAllAsRead { success } }";
pub const DELETE_ONE: &str =
    "mutation($id: ID!) { notifierDeleteNotification(notificationId: $id) { success } }";
pub const DELETE_MANY: &str =
    "mutation($ids: [ID!]!) { notifierDeleteNotifications(ids: $ids) { success } }";

// Reset first (the whole list, newest first), then Upsert / Remove deltas.
// The delta type names are the ones the engine build declares with
// `subscription_union!` (src/slices/notifications/graphql.rs): they match this document.
pub const DELTAS_SUBSCRIPTION: &str = r#"subscription {
  notifierNotificationDeltas {
    __typename
    ... on NotificationReset {
      revision
      views { ... on Notification { id template payload link readAt createdAt } }
    }
    ... on NotificationUpsert {
      revision
      view { ... on Notification { id template payload link readAt createdAt } }
    }
    ... on NotificationRemove { revision projector key }
  }
}"#;

pub fn make_passport(user_id: Uuid) -> Passport {
    PassportBuilder::new().user_id(user_id).build()
}

// A person acting as another one: the Passport names the person acted as
// (`user_id`) and the real actor (`impersonator`). The notifications are the real
// actor's.
pub fn make_impersonating_passport(admin_id: Uuid, acted_as_id: Uuid) -> Passport {
    PassportBuilder::new()
        .user_id(acted_as_id)
        .impersonator(admin_id)
        .build()
}

pub fn make_service_passport(service_account_id: Uuid) -> Passport {
    PassportBuilder::new()
        .user_id(service_account_id)
        .build_service()
}

// A deliver request with a fresh source event and no link.
pub fn deliver(recipients: &[Uuid], template: &str, payload: Value) -> DeliverNotification {
    DeliverNotification {
        source_event_id: Uuid::now_v7(),
        recipient_ids: recipients.to_vec(),
        template: template.to_string(),
        payload,
        link: None,
    }
}

pub fn relative_link(path: &str) -> Option<RelativeLink> {
    Some(RelativeLink::parse(path).expect("a relative path the contract accepts"))
}

// A mutation confirms success and nothing else: no notification, no state.
pub fn expect_ack_only(response: &Value, field: &str) {
    verdict::expect_ack(response, field);
    assert_eq!(
        response["data"],
        json!({ field: { "success": true } }),
        "{field} must answer exactly the acknowledgement: {response}"
    );
}

// A machine identity is refused by code and gets no data on the field.
pub fn expect_forbidden(response: &Value, field: &str) {
    assert!(
        response["data"][field].is_null(),
        "{field}: a refused caller must get no data: {response}"
    );
    let code = verdict::expect_code_shaped(response, &format!("{field} refusal"));
    assert_eq!(
        code, "FORBIDDEN",
        "{field}: the refusal code must be FORBIDDEN: {response}"
    );
}

// The list query, as the ids it returned, in the order served.
pub fn listed(response: &Value) -> &Vec<Value> {
    response["data"]["notifierNotifications"]
        .as_array()
        .unwrap_or_else(|| panic!("notifierNotifications must be a plain list: {response}"))
}
