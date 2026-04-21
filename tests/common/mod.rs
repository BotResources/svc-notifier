// Each `tests/*.rs` is compiled as a separate binary, so helpers shared here
// appear dead to clippy from the perspective of binaries that don't use them.
#![allow(dead_code)]

use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use async_nats::jetstream;
use br_core_auth::Passport;
use br_core_auth::PassportHeader;
use reqwest::StatusCode;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(9100);

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const STARTUP_POLL_INTERVAL: Duration = Duration::from_millis(200);
const HTTP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TestContext {
    pub port: u16,
    pub base_url: String,
    pub owner_pool: PgPool,
    pub nats_client: async_nats::Client,
    pub jetstream: jetstream::Context,
    child: Child,
}

impl TestContext {
    pub async fn setup() -> Self {
        let _ = dotenvy::from_filename(".env.test");

        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let base_url = format!("http://localhost:{port}");

        let owner_url = std::env::var("DATABASE_URL_OWNER")
            .unwrap_or_else(|_| "postgres://owner:owner@localhost:5432/svc_notifier_test".into());
        let app_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://svc_notifier_app:svc_notifier_app@localhost:5432/svc_notifier_test".into()
        });
        let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
        let ingest_url = std::env::var("DATABASE_URL_INGEST")
            .unwrap_or_else(|_| "postgres://svc_notifier_ingest:svc_notifier_ingest@localhost:5432/svc_notifier_test".into());

        // Owner pool for seeding and verification (bypasses RLS)
        let owner_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&owner_url)
            .await
            .expect("failed to connect owner pool");

        // Reset DB state
        sqlx::query("DELETE FROM notifications")
            .execute(&owner_pool)
            .await
            .ok(); // table may not exist yet on first run

        // Connect NATS
        let nats_client = async_nats::connect(&nats_url)
            .await
            .expect("failed to connect to NATS");
        let jetstream = jetstream::new(nats_client.clone());

        // Spawn the service binary
        let bin_path = env!("CARGO_BIN_EXE_svc-notifier");
        let child = Command::new(bin_path)
            .env("PORT", port.to_string())
            .env("DATABASE_URL_OWNER", &owner_url)
            .env("DATABASE_URL", &app_url)
            .env("DATABASE_URL_INGEST", &ingest_url)
            .env("NATS_URL", &nats_url)
            .env("RUST_LOG", "warn")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn svc-notifier");

        let ctx = Self {
            port,
            base_url,
            owner_pool,
            nats_client,
            jetstream,
            child,
        };

        // Wait for health
        ctx.wait_for_health().await;

        // Give the NATS consumer time to initialize after the service is healthy
        tokio::time::sleep(Duration::from_secs(2)).await;

        ctx
    }

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

    /// Send an authenticated GraphQL query.
    pub async fn graphql_query(&self, passport: &Passport, query: &str, vars: Value) -> Value {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", passport.to_header())
            .json(&serde_json::json!({
                "query": query,
                "variables": vars,
            }))
            .send()
            .await
            .expect("GraphQL request failed");

        resp.json::<Value>()
            .await
            .expect("failed to parse GraphQL response")
    }

    /// Send an unauthenticated request to the GraphQL endpoint.
    pub async fn graphql_query_unauthenticated(&self, query: &str) -> (StatusCode, Value) {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .json(&serde_json::json!({
                "query": query,
                "variables": {},
            }))
            .send()
            .await
            .expect("unauthenticated request failed");

        let status = resp.status();
        let body = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, body)
    }

    /// Send a request with a malformed X-Passport header.
    pub async fn graphql_query_bad_passport(&self, query: &str, header: &str) -> StatusCode {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .unwrap();

        let resp = client
            .post(format!("{}/graphql", self.base_url))
            .header("X-Passport", header)
            .json(&serde_json::json!({
                "query": query,
                "variables": {},
            }))
            .send()
            .await
            .expect("bad passport request failed");

        resp.status()
    }

    /// Publish a message to NATS JetStream.
    pub async fn nats_publish(&self, subject: &str, payload: &Value) {
        // Ensure the NOTIFY stream exists
        self.jetstream
            .get_or_create_stream(jetstream::stream::Config {
                name: "NOTIFY".to_string(),
                subjects: vec!["notify.>".to_string()],
                storage: jetstream::stream::StorageType::File,
                retention: jetstream::stream::RetentionPolicy::Limits,
                ..Default::default()
            })
            .await
            .expect("failed to get/create NOTIFY stream");

        let bytes = serde_json::to_vec(payload).expect("failed to serialize payload");
        self.jetstream
            .publish(subject.to_string(), bytes.into())
            .await
            .expect("failed to publish to NATS")
            .await
            .expect("failed to get publish ack");
    }

    /// Reset the notifications table (delete all rows via owner pool).
    pub async fn reset_notifications(&self) {
        sqlx::query("DELETE FROM notifications")
            .execute(&self.owner_pool)
            .await
            .expect("failed to reset notifications");
    }

    /// Insert a notification directly via owner pool (bypasses RLS).
    pub async fn insert_notification(
        &self,
        id: Uuid,
        source_event_id: Uuid,
        recipient_id: Uuid,
        template: &str,
        payload: &Value,
    ) {
        sqlx::query(
            "INSERT INTO notifications (id, source_event_id, recipient_id, template, payload) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(id)
        .bind(source_event_id)
        .bind(recipient_id)
        .bind(template)
        .bind(payload)
        .execute(&self.owner_pool)
        .await
        .expect("failed to insert notification");
    }

    /// Count notifications in DB via owner pool (bypasses RLS).
    pub async fn count_notifications(&self) -> i64 {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM notifications")
            .fetch_one(&self.owner_pool)
            .await
            .expect("failed to count notifications");
        row.0
    }

    /// Count notifications for a specific recipient via owner pool.
    pub async fn count_notifications_for(&self, recipient_id: Uuid) -> i64 {
        let row: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM notifications WHERE recipient_id = $1")
                .bind(recipient_id)
                .fetch_one(&self.owner_pool)
                .await
                .expect("failed to count notifications for recipient");
        row.0
    }

    /// Create a Human Passport for testing.
    pub fn make_passport(user_id: Uuid, is_super_admin: bool) -> Passport {
        Passport::Human {
            user_id,
            is_super_admin,
            is_active: true,
            claims: serde_json::json!({}),
        }
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
