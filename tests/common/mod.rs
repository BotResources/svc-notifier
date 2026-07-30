// each tests/*.rs is a separate binary; shared helpers look dead from binaries that skip them
#![allow(dead_code)]

use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use br_core_auth::{Passport, PassportBuilder, PassportHeader};
use br_core_integration::{Actor, EventMetadata, UserId};
use br_notifier_contract::DeliverNotification;
use br_notifier_publisher::NotifierPublisher;
use br_test_harness::{
    BootOutcome, FabricTestNats, GraphqlClient, SpawnedProcess, SseSubscription,
};
use br_util_nats_fabric::IntegrationCommand;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

static PORT_COUNTER: OnceLock<AtomicU16> = OnceLock::new();

// Ok(None) while the table does not exist yet (first run, before migrations);
// Ok(Some(granted)) once it does.
async fn ledger_insert_granted(pool: &PgPool) -> Result<Option<bool>, sqlx::Error> {
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT has_table_privilege('svc_notifier_ingest', 'dead_letters', 'INSERT')
         WHERE to_regclass('dead_letters') IS NOT NULL",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|row| row.0))
}

fn next_port() -> u16 {
    PORT_COUNTER
        .get_or_init(|| {
            let base = std::env::var("TEST_PORT_BASE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(9100);
            AtomicU16::new(base)
        })
        .fetch_add(1, Ordering::SeqCst)
}

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const FAIL_LOUD_WINDOW: Duration = Duration::from_secs(10);
pub const CONSUME_WAIT: Duration = Duration::from_secs(3);
pub const SSE_TIMEOUT: Duration = Duration::from_secs(5);
pub const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

pub const LEGACY_SUBJECT: &str = "notify.deliver";

// Mirrors, verbatim, the intake's transient-failure log message in
// `src/intake.rs` (`triage`, the FailureClass::Transient arm). The outage
// scenarios count its occurrences to observe redeliveries from outside the
// process, so the two strings are coupled and must move together — renaming the
// log line without renaming this constant turns s07c green for the wrong reason.
pub const STORAGE_HELD_LOG_MARKER: &str = "storage write failing, commands held for redelivery";

// Mirrors the two poison-path log messages in `src/intake.rs` (`abandon`). Same
// coupling rule as STORAGE_HELD_LOG_MARKER: rename there, rename here. Every
// scenario asserting one of these is zero also asserts a sibling marker is
// non-zero in the same run, so a renamed log line can never pass for silence.
pub const DEAD_LETTER_RECORDED_LOG_MARKER: &str = "recorded as a dead letter and terminated";
pub const DEAD_LETTER_REPEATED_LOG_MARKER: &str = "already on the ledger";
pub const LEDGER_UNAVAILABLE_LOG_MARKER: &str = "dead-letter ledger unavailable";

// Mirrors `NAK_DELAY` in `src/intake.rs`: the redelivery delay a held frame is
// NAKed with. A frame the intake terminated never comes back, so waiting out
// several of these cycles is how a scenario proves a `term()` from outside the
// process — there is no other observable difference between "terminated" and
// "not redelivered yet".
pub const NAK_DELAY: Duration = Duration::from_secs(1);
pub const TERM_OBSERVATION_WINDOW: Duration = Duration::from_secs(NAK_DELAY.as_secs() * 4);

// The number of consecutive held redeliveries a scenario waits for before it
// calls a storage outage observable. It mirrors no service constant — the
// readiness escalation that used to own one was removed (operator decision) and
// the condition now lives on `/metrics`, where the alerting threshold is a
// deployment decision. This is the suite's own bar for "the gauge is rising".
pub const OUTAGE_ALERT_THRESHOLD: usize = 3;

// The intake metrics the operator alerts on (`/metrics`, Prometheus exposition).
// Same coupling rule as the log markers: renaming one in `src/intake.rs` without
// renaming it here silently retires an alert.
pub const DEAD_LETTERS_TOTAL_METRIC: &str = "notifier_intake_dead_letters_total";
pub const TRANSIENT_FAILURES_TOTAL_METRIC: &str = "notifier_intake_transient_failures_total";
pub const CONSECUTIVE_TRANSIENT_FAILURES_METRIC: &str =
    "notifier_intake_consecutive_transient_failures";
// The ledger's own failure counter. It exists so that a lost INSERT grant — one
// service's broken posture — cannot be read off the transient counter as a
// PostgreSQL outage: the two conditions have different runbooks.
pub const LEDGER_FAILURES_TOTAL_METRIC: &str = "notifier_intake_ledger_failures_total";

// The stable dead-letter reason codes (`src/intake.rs`, `DeadLetterReason`).
// The ledger's `reason` column is an operator contract: a runbook greps these.
pub const REASON_RELATIVE_LINK_REJECTED: &str = "relative_link_rejected";
pub const REASON_PAYLOAD_SHAPE_REJECTED: &str = "payload_shape_rejected";
pub const REASON_NO_RECIPIENTS: &str = "no_recipients";
pub const REASON_STORAGE_REJECTED: &str = "storage_rejected";

// The intake logs JetStream's own redelivery counter on both settlement paths a
// scenario cares about: a held frame (transient failure) and an abandoned one.
// Reading the broker's counter out of the log beats counting our own log lines
// in both directions — an outage scenario needs it to rise past any budget, and
// a termination scenario needs it to stay at one, which is an absence stated as
// a positive number and therefore immune to a renamed log line. The service logs
// JSON, so the field reads `"delivered_count":"Some(N)"`.
const DELIVERED_COUNT_FIELD: &str = "\"delivered_count\":\"Some(";

pub struct TestStack {
    pub owner_pool: PgPool,
    nats: FabricTestNats,
    service_owner_url: String,
    app_url: String,
    ingest_url: String,
}

pub struct ServiceInstance {
    pub port: u16,
    pub base_url: String,
    process: SpawnedProcess,
    graphql: GraphqlClient,
}

pub struct TestContext {
    pub stack: TestStack,
    pub instance: ServiceInstance,
}

pub struct BareBootResult {
    pub outcome: BootOutcome,
    pub logs: String,
}

impl TestStack {
    pub async fn up() -> Self {
        let _ = dotenvy::from_filename(".env.test");

        let owner_url = std::env::var("DATABASE_URL_OWNER")
            .unwrap_or_else(|_| "postgres://owner:owner@localhost:5432/svc_notifier_test".into());
        let service_owner_url = std::env::var("DATABASE_URL_SERVICE_OWNER").unwrap_or_else(|_| {
            "postgres://svc_notifier_owner:svc_notifier_owner@localhost:5432/svc_notifier_test"
                .into()
        });
        let app_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://svc_notifier_app:svc_notifier_app@localhost:5432/svc_notifier_test".into()
        });
        let ingest_url = std::env::var("DATABASE_URL_INGEST")
            .unwrap_or_else(|_| "postgres://svc_notifier_ingest:svc_notifier_ingest@localhost:5432/svc_notifier_test".into());

        let owner_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&owner_url)
            .await
            .expect("failed to connect owner (assertion) pool");

        sqlx::query("DELETE FROM notifications")
            .execute(&owner_pool)
            .await
            .ok();
        sqlx::query("DELETE FROM dead_letters")
            .execute(&owner_pool)
            .await
            .ok();
        // s21 takes the ledger's INSERT grant away to prove the intake holds a
        // command it cannot trace. A panic between its revoke and its restore
        // would otherwise leave the grant off for good: every later run of s20
        // would fail and s21 itself would pass for the wrong reason. Restoring
        // it here — idempotently, before every scenario — makes the fixture
        // self-healing rather than trusting an unwind. It is a no-op on a fresh
        // database, where the migration issues the grant itself.
        sqlx::query("GRANT INSERT ON dead_letters TO svc_notifier_ingest")
            .execute(&owner_pool)
            .await
            .ok();
        // …and verify the repair actually held. The GRANT above is best-effort
        // (`.ok()`) because it runs before the very first migration has created
        // the table; once the table exists, a silently-failed repair would let
        // every ledger scenario pass for the wrong reason (nothing recorded,
        // because nothing could be). Skipped while the table is still absent.
        match ledger_insert_granted(&owner_pool).await {
            Ok(Some(granted)) => assert!(
                granted,
                "the ingest role must be able to write dead_letters before a scenario starts — \
                 the self-healing GRANT did not take"
            ),
            Ok(None) => {}
            Err(error) => panic!(
                "could not verify the ledger INSERT grant, so no ledger scenario can be trusted \
                 in this run: {error}"
            ),
        }

        let nats = FabricTestNats::start().await;

        Self {
            owner_pool,
            nats,
            service_owner_url,
            app_url,
            ingest_url,
        }
    }

    pub async fn spawn_instance(&self, with_nats: bool) -> ServiceInstance {
        let port = next_port();
        let base_url = format!("http://localhost:{port}");
        let port_str = port.to_string();
        let nats_url = self.nats.url();

        let mut envs: Vec<(&str, &str)> = vec![
            ("PORT", &port_str),
            ("DATABASE_URL_OWNER", &self.service_owner_url),
            ("DATABASE_URL", &self.app_url),
            ("RUST_LOG", "warn"),
        ];
        if with_nats {
            envs.push(("NATS_URL", &nats_url));
            envs.push(("DATABASE_URL_INGEST", &self.ingest_url));
        } else {
            envs.push(("NATS_URL", ""));
        }

        let mut process = SpawnedProcess::spawn(env!("CARGO_BIN_EXE_svc-notifier"), &[], &envs);
        if let Err(reason) = process
            .wait_for_http_ok(&format!("{base_url}/readyz"), STARTUP_TIMEOUT)
            .await
        {
            panic!("svc-notifier did not become healthy on port {port}: {reason}");
        }
        if with_nats {
            // /readyz answers before the intake consumer is bound, and the bind
            // is announced at INFO while the fixture runs the service at `warn`
            // — every scenario that counts log markers depends on that level, so
            // the wait cannot be turned into a log signal without changing what
            // the whole suite reads. A settle window it is, until the service
            // gates readiness on the bind.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        ServiceInstance {
            port,
            base_url: base_url.clone(),
            process,
            graphql: GraphqlClient::new(&base_url),
        }
    }

    pub async fn publish_deliver(&self, command: &DeliverNotification) {
        NotifierPublisher::new(self.nats.fabric())
            .deliver(command, default_metadata())
            .await
            .expect("publish the deliver command over the fabric");
    }

    // Publishes a deliver command under a caller-chosen envelope command_id, so a
    // scenario can replay the exact same frame (what a term() the broker never
    // registered looks like) instead of a fresh command. Still typed coordinates
    // through the Fabric — never a hand-built subject.
    pub async fn publish_deliver_envelope(
        &self,
        command_id: Uuid,
        command: &DeliverNotification,
        trace: Trace,
    ) {
        self.publish_payload_envelope(
            command_id,
            &serde_json::to_value(command).expect("a typed command serializes"),
            trace,
        )
        .await;
    }

    // The refused-payload vehicle. A producer bug does not arrive as a typed
    // `DeliverNotification` — it arrives as a well-formed envelope carrying a
    // payload the contract refuses (an out-of-domain link, a shape the contract
    // does not describe, no recipient at all). `Fabric::publish_command` is
    // generic over the payload, so a raw `serde_json::Value` rides the very same
    // typed coordinates as a compliant command — no raw subject, no new harness
    // affordance, and the frame is byte-for-byte what a misbehaving producer
    // would put on the wire.
    pub async fn publish_payload_envelope(&self, command_id: Uuid, payload: &Value, trace: Trace) {
        let envelope = IntegrationCommand::new(
            command_id,
            br_notifier_contract::deliver_command_type(),
            br_notifier_contract::DELIVER_VERSION,
            Utc::now(),
            trace.metadata(),
            payload.clone(),
        );
        self.nats
            .fabric()
            .publish_command(&br_notifier_contract::deliver_coords(), &envelope)
            .await
            .expect("publish the deliver envelope over the fabric");
    }

    pub async fn publish_dead_subject(&self, subject: &str, bytes: &[u8]) {
        self.nats.publish_dead_subject(subject, bytes).await;
    }

    pub async fn notification_rows(&self) -> Vec<NotificationRecord> {
        sqlx::query_as::<_, NotificationRecord>(
            "SELECT id, source_event_id, recipient_id, template, payload, link, read_at, created_at
             FROM notifications ORDER BY created_at, id",
        )
        .fetch_all(&self.owner_pool)
        .await
        .expect("failed to read notification rows (assertion connection)")
    }

    pub async fn rows_for(&self, recipient_id: Uuid) -> Vec<NotificationRecord> {
        self.notification_rows()
            .await
            .into_iter()
            .filter(|row| row.recipient_id == recipient_id)
            .collect()
    }

    pub async fn dead_letters(&self) -> Vec<DeadLetterRecord> {
        sqlx::query_as::<_, DeadLetterRecord>(
            "SELECT id, command_id, source_event_id, recipient_ids, command, reason, sqlstate,
                    correlation_id, causation_id, actor_kind, actor_id, recorded_at
             FROM dead_letters ORDER BY recorded_at, id",
        )
        .fetch_all(&self.owner_pool)
        .await
        .expect("failed to read dead letters (assertion connection)")
    }

    pub async fn revoke_ledger_writes(&self) {
        sqlx::query("REVOKE INSERT ON dead_letters FROM svc_notifier_ingest")
            .execute(&self.owner_pool)
            .await
            .expect("failed to revoke the ledger INSERT grant");
    }

    pub async fn restore_ledger_writes(&self) {
        sqlx::query("GRANT INSERT ON dead_letters TO svc_notifier_ingest")
            .execute(&self.owner_pool)
            .await
            .expect("failed to restore the ledger INSERT grant");
    }

    pub async fn count_rows(&self) -> usize {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications")
            .fetch_one(&self.owner_pool)
            .await
            .expect("failed to count notifications");
        row.0 as usize
    }

    pub async fn wait_until<F, Fut>(&self, timeout: Duration, predicate: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        br_test_harness::wait_until(timeout, predicate).await
    }

    pub async fn shutdown(self) {
        self.nats.shutdown().await;
    }
}

impl TestContext {
    pub async fn setup() -> Self {
        let stack = TestStack::up().await;
        let instance = stack.spawn_instance(true).await;
        Self { stack, instance }
    }
}

pub async fn seed_one(ctx: &TestContext, recipient: Uuid, template: &str) -> Uuid {
    let before = ctx.stack.rows_for(recipient).await.len();
    ctx.stack
        .publish_deliver(&deliver(&[recipient], template, json!({})))
        .await;
    assert!(
        ctx.stack
            .wait_until(RECOVERY_TIMEOUT, || async {
                ctx.stack.rows_for(recipient).await.len() == before + 1
            })
            .await,
        "seeding through the intake failed for template {template}"
    );
    ctx.stack
        .rows_for(recipient)
        .await
        .into_iter()
        .find(|row| row.template == template)
        .expect("seeded row must exist")
        .id
}

pub async fn spawn_against_bare_broker(nats_url: &str) -> BareBootResult {
    let _ = dotenvy::from_filename(".env.test");
    let port = next_port();
    let base_url = format!("http://localhost:{port}");
    let port_str = port.to_string();
    let service_owner_url = dsn(
        "DATABASE_URL_SERVICE_OWNER",
        "postgres://svc_notifier_owner:svc_notifier_owner@localhost:5432/svc_notifier_test",
    );
    let app_url = dsn(
        "DATABASE_URL",
        "postgres://svc_notifier_app:svc_notifier_app@localhost:5432/svc_notifier_test",
    );
    let ingest_url = dsn(
        "DATABASE_URL_INGEST",
        "postgres://svc_notifier_ingest:svc_notifier_ingest@localhost:5432/svc_notifier_test",
    );

    let envs: Vec<(&str, &str)> = vec![
        ("PORT", &port_str),
        ("DATABASE_URL_OWNER", &service_owner_url),
        ("DATABASE_URL", &app_url),
        ("DATABASE_URL_INGEST", &ingest_url),
        ("NATS_URL", nats_url),
        ("RUST_LOG", "warn"),
    ];

    let mut process = SpawnedProcess::spawn(env!("CARGO_BIN_EXE_svc-notifier"), &[], &envs);
    let outcome = process
        .await_boot(&format!("{base_url}/readyz"), FAIL_LOUD_WINDOW)
        .await;
    let logs = process.logs();
    process.shutdown().await;
    BareBootResult { outcome, logs }
}

fn dsn(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl ServiceInstance {
    pub async fn graphql(&self, passport: &Passport, query: &str, vars: Value) -> Value {
        self.graphql.query(passport, query, vars).await
    }

    pub async fn graphql_unauthenticated(&self, query: &str) -> (reqwest::StatusCode, Value) {
        self.graphql.query_unauthenticated(query, json!({})).await
    }

    pub async fn graphql_bad_passport(&self, query: &str, header: &str) -> reqwest::StatusCode {
        let (status, _) = self
            .graphql
            .query_with_passport_header(header, query, json!({}))
            .await;
        status
    }

    pub async fn subscribe(&self, passport: &Passport) -> SseSubscription {
        SseSubscription::open(&self.base_url, passport, EVENTS_SUBSCRIPTION).await
    }

    // The refusal counterpart of `subscribe`. A subscription the service refuses
    // answers one SSE frame carrying a GraphQL error and then ends the stream —
    // `SseSubscription` is a handle over a *live* stream and fails loud on an
    // error frame, so the refusal verdict is read from the raw response instead.
    // Returns the frame's payload, ready for `verdict::expect_code_shaped`.
    pub async fn subscribe_refused(&self, passport: &Passport) -> Value {
        // A refused subscription ends its stream; a subscription that was
        // *accepted* would hold the connection open forever. The bounded read is
        // therefore part of the assertion: a hang means the caller was let in.
        let response = tokio::time::timeout(
            SSE_TIMEOUT,
            self.graphql.post_raw(
                "/graphql",
                &[
                    ("X-Passport", passport.to_header().as_str()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": EVENTS_SUBSCRIPTION }),
            ),
        )
        .await;
        let (status, body) = response.unwrap_or_else(|_| {
            panic!("the subscription was not refused — the stream stayed open past {SSE_TIMEOUT:?}")
        });
        assert!(
            status.is_success(),
            "a refused subscription is a GraphQL verdict on an open stream, not a transport \
             failure: {status} {body}"
        );
        // The refusal rides an SSE body, which `post_raw` hands back as a JSON
        // string; anything else (a JSON error object, say) is rendered whole so
        // the panic below shows what actually came back instead of "".
        let body = body
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| body.to_string());
        let frames: Vec<Value> = body
            .split("\n\n")
            .filter_map(|block| {
                let data = block
                    .lines()
                    .find_map(|line| line.strip_prefix("data:"))?
                    .trim();
                serde_json::from_str::<Value>(data).ok()
            })
            .collect();
        assert_eq!(
            frames.len(),
            1,
            "a refused subscription answers exactly one frame, then ends: {body}"
        );
        frames.into_iter().next().expect("one frame")
    }

    // The lagging counterpart of `subscribe`. `SseSubscription` is the handle
    // for a *healthy* stream: it fails loud the moment a frame carries
    // `errors`, which is exactly the terminal frame a disconnected subscriber
    // must be served. The request is the one the harness handle makes — POST
    // /graphql, `Accept: text/event-stream`, forged Passport — and only the
    // reading discipline differs: nothing is read until the scenario asks, so
    // the events pile up behind the socket the way they do for a client that
    // stopped keeping up.
    pub async fn open_unread_events(&self, passport: &Passport) -> UnreadSseSession {
        let response = reqwest::Client::new()
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", passport.to_header().as_str())
            .header("Accept", "text/event-stream")
            .json(&json!({ "query": EVENTS_SUBSCRIPTION }))
            .send()
            .await
            .expect("open the notification event stream");
        assert!(
            response.status().is_success(),
            "opening the event stream must succeed, got {}",
            response.status()
        );
        UnreadSseSession {
            response,
            buffer: String::new(),
        }
    }

    // Reads one sample out of the Prometheus exposition: the value of `name`
    // whose label set contains every (key, value) in `labels`. None when the
    // metric was never touched — an untouched counter is simply absent, which is
    // itself an assertable fact.
    pub async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let (_, body) = self.get("/metrics").await;
        body.lines()
            .filter(|line| !line.starts_with('#'))
            .filter_map(|line| {
                let (head, value) = line.rsplit_once(' ')?;
                let (metric, rendered_labels) = match head.split_once('{') {
                    Some((metric, rest)) => (metric, rest.trim_end_matches('}')),
                    None => (head, ""),
                };
                (metric == name).then_some((rendered_labels.to_string(), value.to_string()))
            })
            .find(|(rendered_labels, _)| {
                labels
                    .iter()
                    .all(|(key, value)| rendered_labels.contains(&format!("{key}=\"{value}\"")))
            })
            .and_then(|(_, value)| value.parse().ok())
    }

    pub async fn metric_or_zero(&self, name: &str, labels: &[(&str, &str)]) -> f64 {
        self.metric(name, labels).await.unwrap_or(0.0)
    }

    pub async fn get(&self, path: &str) -> (reqwest::StatusCode, String) {
        self.graphql.get_raw(path).await
    }

    pub fn logs(&self) -> String {
        self.process.logs()
    }

    pub fn log_hits(&self, marker: &str) -> usize {
        self.logs().matches(marker).count()
    }

    pub fn max_delivered_count(&self) -> i64 {
        self.logs()
            .split(DELIVERED_COUNT_FIELD)
            .skip(1)
            .filter_map(|tail| tail.split(')').next()?.trim().parse::<i64>().ok())
            .max()
            .unwrap_or(0)
    }

    pub fn unread_count(value: &Value) -> i64 {
        value["data"]["notifierUnreadCount"]
            .as_i64()
            .unwrap_or_else(|| panic!("no unreadCount in response: {value}"))
    }
}

// One SSE frame as it comes off an unread session.
enum FrameRead {
    // A `next` frame carrying the GraphQL response payload — data, errors, or
    // both, exactly as the client would parse it.
    Payload(Value),
    // The service closed the stream.
    Ended,
    // The window elapsed with the stream still open and silent.
    Quiet,
}

// A live SSE session a scenario deliberately does not read while facts pile up
// behind it — the shape a lagging client takes on the wire. Opened by
// `ServiceInstance::open_unread_events`. Every read is bounded: a session that
// is never disconnected must fail the scenario, never hang it.
pub struct UnreadSseSession {
    response: reqwest::Response,
    buffer: String,
}

impl UnreadSseSession {
    // Reads the session up to its terminal verdict. Frames served before it are
    // counted and discarded — a lagged session is served whatever the transport
    // had already absorbed before the loss — and the first frame carrying
    // `errors` is returned whole, ready for `verdict::expect_code_shaped`.
    pub async fn expect_verdict(&mut self, what: &str, window: Duration) -> (usize, Value) {
        let mut served = 0;
        loop {
            match self.read(window).await {
                FrameRead::Payload(payload) if payload["errors"] != Value::Null => {
                    return (served, payload);
                }
                FrameRead::Payload(_) => served += 1,
                FrameRead::Ended => panic!(
                    "{what}: the stream ended without a verdict after {served} event(s) — a \
                     client cut off in silence cannot tell a lost fact from an empty inbox"
                ),
                FrameRead::Quiet => panic!(
                    "{what}: no verdict within {window:?} after {served} event(s) — the session \
                     is still open and serving, so nothing was ever lost"
                ),
            }
        }
    }

    // Reads one served event, failing loud on a verdict, a close or silence.
    // The step that turns "the session heard nothing" into "the session was
    // there to hear it" before a scenario starves it.
    pub async fn expect_event(&mut self, what: &str, window: Duration) -> Value {
        match self.read(window).await {
            FrameRead::Payload(payload) if payload["errors"] == Value::Null => payload,
            FrameRead::Payload(payload) => {
                panic!("{what}: expected an event, got a verdict: {payload}")
            }
            FrameRead::Ended => panic!("{what}: the stream ended instead of serving an event"),
            FrameRead::Quiet => panic!("{what}: no event within {window:?}"),
        }
    }

    // Proves the stream is *closed*, not merely quiet: a disconnection the
    // client must act on, rather than an idle connection it would keep folding
    // into.
    pub async fn expect_end(&mut self, what: &str, window: Duration) {
        match self.read(window).await {
            FrameRead::Ended => {}
            FrameRead::Payload(payload) => {
                panic!("{what}: the stream served another frame after its verdict: {payload}")
            }
            FrameRead::Quiet => panic!(
                "{what}: the stream stayed open for {window:?} after its verdict — a truncated \
                 session must be closed, not left dangling"
            ),
        }
    }

    async fn read(&mut self, window: Duration) -> FrameRead {
        let deadline = tokio::time::Instant::now() + window;
        loop {
            if let Some(block) = self.take_block() {
                match Self::payload(&block) {
                    Some(payload) => return FrameRead::Payload(payload),
                    // A keep-alive comment or any non-`next` framing carries no
                    // fact; it is not an event and not an end.
                    None => continue,
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return FrameRead::Quiet;
            }
            match tokio::time::timeout(remaining, self.response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buffer.push_str(&String::from_utf8_lossy(&chunk)),
                Ok(Ok(None)) => return FrameRead::Ended,
                Ok(Err(error)) => panic!("the event stream errored at the transport: {error}"),
                Err(_) => return FrameRead::Quiet,
            }
        }
    }

    fn take_block(&mut self) -> Option<String> {
        let block_end = self.buffer.find("\n\n")?;
        let block = self.buffer[..block_end].to_string();
        self.buffer = self.buffer[block_end + 2..].to_string();
        Some(block)
    }

    fn payload(block: &str) -> Option<Value> {
        let mut event_type = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                event_type = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                data = Some(value.trim().to_string());
            }
        }
        if event_type.as_deref() != Some("next") {
            return None;
        }
        serde_json::from_str(&data?).ok()
    }
}

pub fn notifier_event(event: &Value) -> &Value {
    &event["notifierNotificationEvents"]
}

pub const EVENTS_SUBSCRIPTION: &str = r#"subscription {
  notifierNotificationEvents {
    __typename
    ... on NotificationAdded { notification { id template payload link readAt createdAt } }
    ... on NotificationsRead { ids readAt }
    ... on NotificationsDeleted { ids }
  }
}"#;

pub const LIST_QUERY: &str = r#"query {
  notifierNotifications {
    nodes { id template payload link readAt createdAt }
    hasNextPage
  }
}"#;

pub const UNREAD_QUERY: &str = "{ notifierUnreadCount }";

// The same list query, cursor-driven. `LIST_QUERY` takes the default page (20)
// and is the right shape for a scenario with a handful of notifications; an
// inbox larger than one page is only readable in full by walking the cursor,
// which is exactly what a client rebuilding its state after a disconnection
// does.
pub const PAGE_QUERY: &str = r#"query($first: Int!, $after: ID) {
  notifierNotifications(first: $first, after: $after) {
    nodes { id template payload link readAt createdAt }
    hasNextPage
  }
}"#;

// The largest page the read layer will serve — it clamps `first`, so asking for
// more than this is not a way around the cursor.
const MAX_PAGE: usize = 100;

// A bound on the walk itself: a `hasNextPage` that never turns false is a bug
// the scenario must fail on, not loop on.
const MAX_PAGES_WALKED: usize = 64;

// Walks the caller's whole inbox the way a client must — page by page, following
// the cursor until the service says there is no next page — and returns the ids
// in the order they were served.
pub async fn listed_ids(instance: &ServiceInstance, passport: &Passport) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..MAX_PAGES_WALKED {
        let page = instance
            .graphql(
                passport,
                PAGE_QUERY,
                json!({"first": MAX_PAGE, "after": after}),
            )
            .await;
        let connection = &page["data"]["notifierNotifications"];
        let nodes = connection["nodes"]
            .as_array()
            .unwrap_or_else(|| panic!("no nodes in {page}"));
        ids.extend(nodes.iter().map(|node| {
            node["id"]
                .as_str()
                .unwrap_or_else(|| panic!("a node carries a string id: {node}"))
                .to_owned()
        }));
        let has_next_page = connection["hasNextPage"]
            .as_bool()
            .unwrap_or_else(|| panic!("no hasNextPage in {page}"));
        if !has_next_page {
            return ids;
        }
        after = ids.last().cloned();
    }
    panic!(
        "the notification list never stopped claiming a next page after {MAX_PAGES_WALKED} pages"
    )
}

#[derive(Debug, sqlx::FromRow)]
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

#[derive(Debug, sqlx::FromRow)]
pub struct DeadLetterRecord {
    pub id: Uuid,
    pub command_id: Uuid,
    // Nullable since migration 0003: a payload the contract refuses may carry no
    // readable source_event_id, and the ledger row must land regardless.
    pub source_event_id: Option<Uuid>,
    pub recipient_ids: Vec<Uuid>,
    pub command: Vec<u8>,
    pub reason: String,
    pub sqlstate: Option<String>,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    // Nullable since migration 0004: the columns are additive, so a row written
    // before the migration carries no actor. A row the current intake writes
    // always names one — "re-emit the command verbatim" is only half an audit
    // trail without "and here is who asked for it".
    pub actor_kind: Option<String>,
    pub actor_id: Option<Uuid>,
    pub recorded_at: DateTime<Utc>,
}

impl DeadLetterRecord {
    pub fn command(&self) -> Value {
        serde_json::from_slice(&self.command).expect("the recorded command is the producer's JSON")
    }
}

// The producer's causality triple, carried verbatim on the envelope metadata.
// A dead-letter row claims to let an operator re-emit the command as it was, so
// scenarios pin what the ledger recorded against what was published.
#[derive(Debug, Clone, Copy)]
pub struct Trace {
    pub actor_id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
}

impl Trace {
    pub fn caused_by(causation_id: Uuid) -> Self {
        Self {
            actor_id: Uuid::now_v7(),
            correlation_id: Uuid::now_v7(),
            causation_id: Some(causation_id),
        }
    }

    pub fn fresh() -> Self {
        Self {
            actor_id: Uuid::now_v7(),
            correlation_id: Uuid::now_v7(),
            causation_id: None,
        }
    }

    fn metadata(self) -> EventMetadata {
        let metadata = EventMetadata::new(
            Actor::Human(UserId::from(self.actor_id)),
            self.correlation_id,
        );
        match self.causation_id {
            Some(causation_id) => metadata.with_causation(causation_id),
            None => metadata,
        }
    }
}

pub fn make_passport(user_id: Uuid) -> Passport {
    PassportBuilder::new().user_id(user_id).build()
}

pub fn make_impersonating_passport(admin_id: Uuid, impersonated_id: Uuid) -> Passport {
    PassportBuilder::new()
        .user_id(impersonated_id)
        .impersonator(admin_id)
        .build()
}

pub fn make_service_passport(service_account_id: Uuid) -> Passport {
    PassportBuilder::new()
        .user_id(service_account_id)
        .build_service()
}

pub fn deliver(recipients: &[Uuid], template: &str, payload: Value) -> DeliverNotification {
    DeliverNotification {
        source_event_id: Uuid::now_v7(),
        recipient_ids: recipients.to_vec(),
        template: template.to_string(),
        payload,
        link: None,
    }
}

fn default_metadata() -> EventMetadata {
    EventMetadata::new(Actor::Human(UserId::from(Uuid::now_v7())), Uuid::now_v7())
}

pub struct PausedPostgres {
    container: String,
}

impl PausedPostgres {
    pub fn pause() -> Self {
        let container = find_postgres_container_by_published_port_then_any();
        run_docker(&["pause", &container]);
        Self { container }
    }
}

impl Drop for PausedPostgres {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["unpause", &self.container])
            .output();
    }
}

fn find_postgres_container_by_published_port_then_any() -> String {
    let port = postgres_host_port();
    docker_ps_first_postgres(&["--filter", &format!("publish={port}")])
        .or_else(|| docker_ps_first_postgres(&[]))
        .unwrap_or_else(|| {
            panic!(
                "no running postgres container found (looked for published port {port}, then any) — start docker-compose.test.yml first"
            )
        })
}

fn docker_ps_first_postgres(filters: &[&str]) -> Option<String> {
    let mut args = vec!["ps"];
    args.extend_from_slice(filters);
    args.extend_from_slice(&["--format", "{{.Names}}\t{{.Image}}"]);
    let output = Command::new("docker")
        .args(&args)
        .output()
        .expect("docker ps failed — the outage scenarios need the docker CLI");
    let listing = String::from_utf8_lossy(&output.stdout);
    listing
        .lines()
        .find(|line| line.contains("postgres"))
        .map(|line| line.split('\t').next().unwrap_or_default().to_string())
}

fn postgres_host_port() -> String {
    let _ = dotenvy::from_filename(".env.test");
    std::env::var("DATABASE_URL_OWNER")
        .ok()
        .and_then(|url| {
            let authority = url.rsplit('@').next()?.to_string();
            let port = authority.split('/').next()?.split(':').nth(1)?.to_string();
            (!port.is_empty()).then_some(port)
        })
        .unwrap_or_else(|| "5432".to_string())
}

fn run_docker(args: &[&str]) {
    let output = Command::new("docker")
        .args(args)
        .output()
        .expect("docker command failed to start");
    assert!(
        output.status.success(),
        "docker {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}
