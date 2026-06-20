// each tests/*.rs is a separate binary; shared helpers look dead from binaries that skip them
#![allow(dead_code)]

use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use br_core_auth::{Passport, PassportBuilder};
use br_core_integration::{Actor, EventMetadata, UserId};
use br_notifier_contract::DeliverNotification;
use br_notifier_publisher::NotifierPublisher;
use br_test_harness::{
    BootOutcome, FabricTestNats, GraphqlClient, SpawnedProcess, SseSubscription,
};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

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
const FAIL_LOUD_WINDOW: Duration = Duration::from_secs(10);
pub const CONSUME_WAIT: Duration = Duration::from_secs(3);
pub const SSE_TIMEOUT: Duration = Duration::from_secs(5);
pub const RECOVERY_TIMEOUT: Duration = Duration::from_secs(30);

pub const DURABLE_NAME: &str = "svc-notifier";
pub const LEGACY_SUBJECT: &str = "notify.deliver";

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

    pub async fn get(&self, path: &str) -> (reqwest::StatusCode, String) {
        self.graphql.get_raw(path).await
    }

    pub fn unread_count(value: &Value) -> i64 {
        value["data"]["notifierUnreadCount"]
            .as_i64()
            .unwrap_or_else(|| panic!("no unreadCount in response: {value}"))
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
    PassportBuilder::new().user_id(user_id).build()
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
