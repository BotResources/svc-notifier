// A recipient's session on the delta stream, folded the way a client folds it.
//
// The contract: a Reset carrying the whole list first, then Upsert (the whole
// notification) and Remove (the id of the removed one) deltas. A scenario judges
// the FINAL FOLDED VIEW — never the number of events, because a bulk action may
// arrive as one delta per notification or as a single Reset. The raw deltas are
// kept only for the scenarios whose subject is the wire itself (the first
// payload, the identity of a removal, silence).
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{SseOutcome, SseSubscription};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::time::Instant;

use super::{QUIET_WINDOW, RECOVERY_TIMEOUT, SSE_TIMEOUT, ServiceInstance};

#[derive(Debug, Clone)]
pub enum Delta {
    Reset(Vec<Value>),
    Upsert(Value),
    // The key exactly as the wire carries it.
    Remove(Value),
    // The lane notices the engine may inject; Notifier gives them no meaning.
    Lanes,
}

pub struct Session {
    sub: SseSubscription,
    view: BTreeMap<String, Value>,
    first_payload: Vec<Value>,
    deltas: Vec<Delta>,
}

fn id_of(notification: &Value) -> String {
    notification["id"]
        .as_str()
        .unwrap_or_else(|| panic!("a notification carries a string id: {notification}"))
        .to_owned()
}

// An instant the wire carries as an RFC 3339 string.
pub fn instant(value: &Value) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(
        value
            .as_str()
            .unwrap_or_else(|| panic!("a timestamp is an RFC 3339 string: {value}")),
    )
    .unwrap_or_else(|error| panic!("a timestamp is an RFC 3339 string: {value}: {error}"))
    .with_timezone(&Utc)
}

fn parse(data: &Value) -> Delta {
    let delta = &data["notifierNotificationDeltas"];
    let array = |value: &Value| value.as_array().cloned().unwrap_or_default();
    match delta["__typename"].as_str() {
        Some("NotificationReset") => Delta::Reset(array(&delta["views"])),
        Some("NotificationUpsert") => Delta::Upsert(delta["view"].clone()),
        Some("NotificationRemove") => Delta::Remove(delta["key"].clone()),
        Some("LanesPaused" | "LanesResumed") => Delta::Lanes,
        other => panic!("unknown delta {other:?} on the stream: {data}"),
    }
}

impl Session {
    // Opens the stream and reads its first payload, which must be a Reset.
    pub async fn open(instance: &ServiceInstance, passport: &Passport) -> Self {
        let mut sub = instance.subscribe(passport).await;
        let first = sub
            .expect_event("the first payload of the stream", SSE_TIMEOUT)
            .await;
        let Delta::Reset(views) = parse(&first) else {
            panic!("the first payload of the stream must be a Reset: {first}");
        };
        let view = views
            .iter()
            .map(|view| (id_of(view), view.clone()))
            .collect();
        Self {
            sub,
            view,
            first_payload: views,
            deltas: Vec::new(),
        }
    }

    // The views of the first payload, in the order the wire served them.
    pub fn first_payload(&self) -> &[Value] {
        &self.first_payload
    }

    pub fn deltas(&self) -> &[Delta] {
        &self.deltas
    }

    // The folded view: every notification currently held, by id.
    pub fn view(&self) -> &BTreeMap<String, Value> {
        &self.view
    }

    pub fn get(&self, id: impl ToString) -> Option<&Value> {
        self.view.get(&id.to_string())
    }

    // The ids named by every Remove seen so far. A key that is not a bare JSON
    // string is a wire violation and fails here.
    pub fn removed_ids(&self) -> Vec<String> {
        self.deltas
            .iter()
            .filter_map(|delta| match delta {
                Delta::Remove(key) => Some(
                    key.as_str()
                        .unwrap_or_else(|| {
                            panic!("a Remove key is the notification id as a bare string: {key}")
                        })
                        .to_owned(),
                ),
                _ => None,
            })
            .collect()
    }

    // Every notification id the wire ever showed: in the first payload, in a Reset,
    // in an Upsert. A removed one stays in the set — it was shown.
    pub fn ids_seen(&self) -> BTreeSet<String> {
        let mut seen: BTreeSet<String> = self.first_payload.iter().map(id_of).collect();
        for delta in &self.deltas {
            match delta {
                Delta::Reset(views) => seen.extend(views.iter().map(id_of)),
                Delta::Upsert(view) => {
                    seen.insert(id_of(view));
                }
                Delta::Remove(_) | Delta::Lanes => {}
            }
        }
        seen
    }

    // Ids the wire showed a second time WITHOUT a new window in between. A Reset
    // is a whole window: it replaces everything shown so far, so the ids it lists
    // are not "seen again" — a stream may legally start over. Inside a window
    // nothing is legal twice: an id listed twice in one Reset, or an Upsert of an
    // id the current window already showed, is a double. A Remove takes the id out
    // of the window (a later Upsert of it is a new arrival).
    pub fn doubled_ids(&self) -> Vec<String> {
        fn open_window(views: &[Value], window: &mut BTreeSet<String>, doubled: &mut Vec<String>) {
            window.clear();
            for id in views.iter().map(id_of) {
                if !window.insert(id.clone()) {
                    doubled.push(id);
                }
            }
        }
        let mut doubled = Vec::new();
        let mut window = BTreeSet::new();
        open_window(&self.first_payload, &mut window, &mut doubled);
        for delta in &self.deltas {
            match delta {
                Delta::Reset(views) => open_window(views, &mut window, &mut doubled),
                Delta::Upsert(view) => {
                    let id = id_of(view);
                    if !window.insert(id.clone()) {
                        doubled.push(id);
                    }
                }
                Delta::Remove(key) => {
                    if let Some(id) = key.as_str() {
                        window.remove(id);
                    }
                }
                Delta::Lanes => {}
            }
        }
        doubled
    }

    fn apply(&mut self, data: &Value) {
        let delta = parse(data);
        match &delta {
            Delta::Reset(views) => {
                self.view = views
                    .iter()
                    .map(|view| (id_of(view), view.clone()))
                    .collect();
            }
            Delta::Upsert(view) => {
                self.view.insert(id_of(view), view.clone());
            }
            Delta::Remove(key) => {
                if let Some(id) = key.as_str() {
                    self.view.remove(id);
                }
            }
            Delta::Lanes => {}
        }
        self.deltas.push(delta);
    }

    // Folds everything that arrives until the stream has been quiet for `quiet`.
    pub async fn pump(&mut self, quiet: Duration) {
        let deadline = Instant::now() + RECOVERY_TIMEOUT * 2;
        loop {
            assert!(
                Instant::now() < deadline,
                "the stream never went quiet: {} deltas folded",
                self.deltas.len()
            );
            match self.sub.next_outcome(quiet).await {
                SseOutcome::Event(data) => self.apply(&data),
                SseOutcome::Timeout => return,
                SseOutcome::Closed => panic!("the service closed the stream"),
                other => panic!("unexpected stream outcome: {other:?}"),
            }
        }
    }

    // Waits until the folded view satisfies `holds`, then proves it still does
    // after a quiet window: settled means "right and stable", not "right once".
    pub async fn settle(&mut self, what: &str, holds: impl Fn(&BTreeMap<String, Value>) -> bool) {
        let deadline = Instant::now() + RECOVERY_TIMEOUT;
        while !holds(&self.view) {
            assert!(
                Instant::now() < deadline,
                "the folded view never settled ({what}); it holds: {:#?}",
                self.view
            );
            match self.sub.next_outcome(Duration::from_millis(500)).await {
                SseOutcome::Event(data) => self.apply(&data),
                SseOutcome::Timeout => {}
                SseOutcome::Closed => panic!("the service closed the stream ({what})"),
                other => panic!("unexpected stream outcome ({what}): {other:?}"),
            }
        }
        self.pump(QUIET_WINDOW).await;
        assert!(
            holds(&self.view),
            "the folded view settled then moved on ({what}); it holds: {:#?}",
            self.view
        );
    }

    // Silence as a behaviour: nothing at all arrives for the quiet window.
    pub async fn expect_no_delta(&mut self, what: &str) {
        let before = self.deltas.len();
        self.pump(QUIET_WINDOW).await;
        assert!(
            self.deltas.len() == before,
            "expected no delta ({what}), got: {:#?}",
            &self.deltas[before..]
        );
    }
}
