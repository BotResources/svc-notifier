// Shared e2e harness. Each `tests/*.rs` is compiled as a separate binary, so
// helpers shared here appear dead to clippy from binaries that don't use them.
#![allow(dead_code)]

use std::process::{Child, Command};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_nats::jetstream;
use br_core_auth::{AuthMethod, Passport, PassportHeader};
use br_notifier_contract::{DELIVER_SUBJECT, DeliverNotification};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

// TEST_PORT_BASE lets several checkouts run the suite on one machine
// without their spawned instances colliding (default base: 9100).
static PORT_COUNTER: OnceLock<AtomicU16> = OnceLock::new();

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
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(200);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONSUME_WAIT: Duration = Duration::from_secs(3);
pub const SSE_TIMEOUT: Duration = Duration::from_secs(5);
pub const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

pub const STREAM_NAME: &str = "NOTIFY";
pub const DURABLE_NAME: &str = "svc-notifier";
pub const LEGACY_SUBJECT: &str = "notify.deliver";

/// Shared infra (Postgres + NATS) with a clean slate: empty notifications
/// table, freshly recreated stream (which also drops durable consumers and
/// any pending messages from previous tests).
pub struct TestStack {
    pub owner_pool: PgPool,
    pub nats_client: async_nats::Client,
    pub jetstream: jetstream::Context,
    service_owner_url: String,
    app_url: String,
    ingest_url: String,
    nats_url: String,
}

/// One running svc-notifier process.
pub struct ServiceInstance {
    pub port: u16,
    pub base_url: String,
    child: Child,
}

/// Convenience: a stack plus a single instance — the default scenario shape.
pub struct TestContext {
    pub stack: TestStack,
    pub instance: ServiceInstance,
}

impl TestStack {
    pub async fn up() -> Self {
        let _ = dotenvy::from_filename(".env.test");

        // The superuser DSN is HARNESS-ONLY (assertion connection, state
        // reset). The service under test gets `service_owner_url` — a
        // non-superuser migration role mirroring the production posture, so
        // any service code path reading through the owner pool is subject to
        // FORCE RLS in tests exactly as it would be in production.
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
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());

        let owner_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&owner_url)
            .await
            .expect("failed to connect owner (assertion) pool");

        sqlx::query("DELETE FROM notifications")
            .execute(&owner_pool)
            .await
            .ok();

        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("failed to connect to NATS");
        let jetstream = jetstream::new(nats_client.clone());

        let _ = jetstream.delete_stream(STREAM_NAME).await;
        jetstream
            .create_stream(jetstream::stream::Config {
                name: STREAM_NAME.to_string(),
                subjects: vec!["notifier.cmd.>".to_string(), "notify.>".to_string()],
                storage: jetstream::stream::StorageType::File,
                retention: jetstream::stream::RetentionPolicy::Limits,
                ..Default::default()
            })
            .await
            .expect("failed to create NOTIFY stream");

        Self {
            owner_pool,
            nats_client,
            jetstream,
            service_owner_url,
            app_url,
            ingest_url,
            nats_url,
        }
    }

    /// Spawn a real svc-notifier process. `with_nats: false` starts the
    /// instance without intake (serving GraphQL only) — used to prove pushes
    /// derive from committed PG state, not from the consuming process.
    pub async fn spawn_instance(&self, with_nats: bool) -> ServiceInstance {
        let port = next_port();
        let base_url = format!("http://localhost:{port}");

        let bin_path = env!("CARGO_BIN_EXE_svc-notifier");
        let mut command = Command::new(bin_path);
        command
            .env("PORT", port.to_string())
            .env("DATABASE_URL_OWNER", &self.service_owner_url)
            .env("DATABASE_URL", &self.app_url)
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if with_nats {
            command
                .env("NATS_URL", &self.nats_url)
                .env("DATABASE_URL_INGEST", &self.ingest_url);
        } else {
            command.env_remove("NATS_URL");
        }
        let child = command.spawn().expect("failed to spawn svc-notifier");

        let instance = ServiceInstance {
            port,
            base_url,
            child,
        };
        instance.wait_for_health().await;
        if with_nats {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        instance
    }

    /// Publish a typed deliver command on the contract subject — the only
    /// legitimate way to seed notifications.
    pub async fn publish_deliver(&self, command: &DeliverNotification) {
        let bytes = serde_json::to_vec(command).expect("failed to serialize deliver command");
        self.publish_raw(DELIVER_SUBJECT, bytes).await;
    }

    /// Publish arbitrary bytes on an arbitrary subject (malformed payloads,
    /// invalid links, legacy subject).
    pub async fn publish_raw(&self, subject: &str, bytes: Vec<u8>) {
        self.jetstream
            .publish(subject.to_string(), bytes.into())
            .await
            .expect("failed to publish to NATS")
            .await
            .expect("failed to get publish ack");
    }

    /// Exact rows, via the dedicated assertion connection (bypasses RLS).
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

    pub async fn count_rows(&self) -> usize {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications")
            .fetch_one(&self.owner_pool)
            .await
            .expect("failed to count notifications");
        row.0 as usize
    }

    /// State of the service's durable consumer — the NATS envelope.
    pub async fn consumer_info(&self) -> Option<async_nats::jetstream::consumer::Info> {
        let stream = self.jetstream.get_stream(STREAM_NAME).await.ok()?;
        let mut consumer: jetstream::consumer::PullConsumer =
            stream.get_consumer(DURABLE_NAME).await.ok()?;
        consumer.info().await.ok().cloned()
    }

    pub async fn stream_message_count(&self) -> u64 {
        let mut stream = self
            .jetstream
            .get_stream(STREAM_NAME)
            .await
            .expect("failed to get stream");
        stream
            .info()
            .await
            .expect("failed to get stream info")
            .state
            .messages
    }

    /// Poll until `predicate` is true or `timeout` elapses; returns success.
    pub async fn wait_until<F, Fut>(&self, timeout: Duration, mut predicate: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if predicate().await {
                return true;
            }
            if tokio::time::Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

impl TestContext {
    pub async fn setup() -> Self {
        let stack = TestStack::up().await;
        let instance = stack.spawn_instance(true).await;
        Self { stack, instance }
    }
}

impl ServiceInstance {
    async fn wait_for_health(&self) {
        let url = format!("{}/health", self.base_url);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();

        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "svc-notifier did not become healthy within {:?} on port {}",
                    STARTUP_TIMEOUT, self.port
                );
            }
            if let Ok(resp) = client.get(&url).send().await
                && resp.status().is_success()
            {
                return;
            }
            tokio::time::sleep(STARTUP_POLL_INTERVAL).await;
        }
    }

    /// Authenticated GraphQL request (forged Passport — we are inside the
    /// trust boundary the gateway normally establishes).
    pub async fn graphql(&self, passport: &Passport, query: &str, vars: Value) -> Value {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", passport.to_header())
            .json(&json!({ "query": query, "variables": vars }))
            .send()
            .await
            .expect("GraphQL request failed");

        resp.json::<Value>()
            .await
            .expect("failed to parse GraphQL response")
    }

    pub async fn graphql_unauthenticated(&self, query: &str) -> (reqwest::StatusCode, Value) {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .json(&json!({ "query": query, "variables": {} }))
            .send()
            .await
            .expect("unauthenticated request failed");

        let status = resp.status();
        let body = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, body)
    }

    pub async fn graphql_bad_passport(&self, query: &str, header: &str) -> reqwest::StatusCode {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", header)
            .json(&json!({ "query": query, "variables": {} }))
            .send()
            .await
            .expect("bad passport request failed");

        resp.status()
    }

    /// Open the notification event-union subscription over SSE.
    pub async fn subscribe(&self, passport: &Passport) -> Subscription {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", passport.to_header())
            .header("Accept", "text/event-stream")
            .json(&json!({ "query": EVENTS_SUBSCRIPTION }))
            .send()
            .await
            .expect("subscription request failed");

        assert!(
            resp.status().is_success(),
            "subscription request returned {}",
            resp.status()
        );

        Subscription {
            stream: Box::pin(resp.bytes_stream()),
            buffer: String::new(),
        }
    }

    pub fn unread_count(value: &Value) -> i64 {
        value["data"]["notifierUnreadCount"]
            .as_i64()
            .unwrap_or_else(|| panic!("no unreadCount in response: {value}"))
    }
}

impl Drop for ServiceInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

/// A live SSE subscription; `next_event` returns the unwrapped
/// `notifierNotificationEvents` payload of the next pushed event.
/// A GraphQL error on the stream fails the scenario loudly.
pub struct Subscription {
    stream: std::pin::Pin<Box<dyn futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>,
    buffer: String,
}

impl Subscription {
    pub async fn next_event(&mut self, timeout: Duration) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(block) = self.take_block() {
                if let Some(event) = Self::parse_block(&block) {
                    return Some(event);
                }
                continue;
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(remaining, self.stream.next()).await {
                Ok(Some(Ok(bytes))) => {
                    self.buffer.push_str(&String::from_utf8_lossy(&bytes));
                }
                _ => return None,
            }
        }
    }

    pub async fn expect_event(&mut self, what: &str) -> Value {
        self.next_event(SSE_TIMEOUT)
            .await
            .unwrap_or_else(|| panic!("expected subscription event: {what}, got none"))
    }

    pub async fn expect_silence(&mut self, what: &str) {
        if let Some(event) = self.next_event(CONSUME_WAIT).await {
            panic!("expected no subscription event ({what}), got: {event}");
        }
    }

    fn take_block(&mut self) -> Option<String> {
        let block_end = self.buffer.find("\n\n")?;
        let block = self.buffer[..block_end].to_string();
        self.buffer = self.buffer[block_end + 2..].to_string();
        Some(block)
    }

    fn parse_block(block: &str) -> Option<Value> {
        let mut event_type = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(val) = line.strip_prefix("event:") {
                event_type = Some(val.trim().to_string());
            } else if let Some(val) = line.strip_prefix("data:") {
                data = Some(val.trim().to_string());
            }
        }
        if event_type.as_deref() != Some("next") {
            return None;
        }
        let payload: Value = serde_json::from_str(&data?).ok()?;
        if payload["errors"] != Value::Null {
            panic!("subscription stream returned errors: {}", payload["errors"]);
        }
        let event = payload["data"]["notifierNotificationEvents"].clone();
        (event != Value::Null).then_some(event)
    }
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

pub fn make_passport(user_id: Uuid) -> Passport {
    Passport::Human {
        user_id,
        is_super_admin: false,
        is_active: true,
        claims: json!({}),
        auth_method: AuthMethod::Jwt,
        impersonator: None,
    }
}

pub fn make_service_passport(service_account_id: Uuid) -> Passport {
    Passport::Service {
        service_account_id,
        claims: json!({}),
    }
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

/// Pauses the Postgres container; unpauses on drop so a panicking scenario
/// cannot poison the rest of the suite.
pub struct PausedPostgres {
    container: String,
}

impl PausedPostgres {
    pub fn pause() -> Self {
        let container = find_postgres_container();
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

fn find_postgres_container() -> String {
    let port = postgres_host_port();
    // Published-port filter disambiguates between several local stacks; a
    // host-networked container (the CI shape) publishes nothing, so fall
    // back to the name/image scan — unambiguous on a single-stack runner.
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
