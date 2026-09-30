// The real infrastructure of one scenario: Postgres (assertion connection),
// NATS JetStream (Fabric provisioner) and the spawned service binary. Nothing
// here is mocked and nothing lives in production code.
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use br_core_auth::{Passport, PassportHeader};
use br_core_integration::{Actor, EventMetadata, UserId};
use br_notifier_contract::DeliverNotification;
use br_notifier_publisher::NotifierPublisher;
use br_test_harness::{
    FabricTestNats, FixedStream, GraphqlClient, SpawnedNats, SpawnedProcess, SseSubscription,
};
use br_util_nats_fabric::IntegrationCommand;
use chrono::Utc;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use super::{DELTAS_SUBSCRIPTION, RECOVERY_TIMEOUT, SSE_TIMEOUT, engine_nats};

static PORT_COUNTER: OnceLock<AtomicU16> = OnceLock::new();

const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

// The durable consumer the service binds on the command stream (src/intake.rs).
const INTAKE_DURABLE: &str = "svc-notifier";

// The one runtime role of the engine build (see `spawn_instance`).
const APP_ROLE: &str = "svc_notifier_ingest";

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

fn dsn(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

pub struct TestStack {
    // The assertion connection: superuser, harness-only, never handed to the
    // service. It reads storage and, once per scenario, clears the table of the
    // previous scenario (see `up`); no notification is ever written through it.
    pub owner_pool: PgPool,
    nats: FabricTestNats,
    // Kept for its lifetime: the broker dies with the stack.
    _server: SpawnedNats,
    service_owner_url: String,
    ingest_url: String,
}

pub struct ServiceInstance {
    pub base_url: String,
    process: SpawnedProcess,
    graphql: GraphqlClient,
}

pub struct TestContext {
    pub stack: TestStack,
    pub instance: ServiceInstance,
}

impl TestStack {
    pub async fn up() -> Self {
        let _ = dotenvy::from_filename(".env.test");

        let owner_url = dsn(
            "DATABASE_URL_OWNER",
            "postgres://owner:owner@localhost:5432/svc_notifier_test",
        );
        let owner_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&owner_url)
            .await
            .expect("failed to connect the owner (assertion) pool");

        // ENGINE ACCOMMODATION: the engine's first migration creates its own schema
        // (`service_engine`), which needs CREATE on the database. The 1.0 provisioning
        // (scripts/init-db.sql) gives the migration owner CREATE on `public` only; in
        // production the CNPG owner owns the database, so this is a fixture-only gap.
        sqlx::query(
            "DO $$ BEGIN EXECUTE format('GRANT CREATE ON DATABASE %I TO svc_notifier_owner', \
             current_database()); END $$",
        )
        .execute(&owner_pool)
        .await
        .expect("failed to let the migration owner create the engine schema");

        // A clean slate between scenarios, best effort: the table only exists once
        // a first run has migrated. Every scenario also scopes its assertions to
        // the ids it minted itself, so a leftover row can never make one pass.
        // (The engine build stores notifications in the same 1.0 table.)
        sqlx::query("DELETE FROM notifications")
            .execute(&owner_pool)
            .await
            .ok();

        // ENGINE ACCOMMODATION: the streams the engine binds carry a bounded max_age (see
        // `engine_nats`); the harness provisioner alone would create them unbounded.
        let server = SpawnedNats::start().await;
        engine_nats::provision(&server.url()).await;

        Self {
            owner_pool,
            nats: FabricTestNats::connect(&server.url()).await,
            _server: server,
            service_owner_url: dsn(
                "DATABASE_URL_SERVICE_OWNER",
                "postgres://svc_notifier_owner:svc_notifier_owner@localhost:5432/svc_notifier_test",
            ),
            ingest_url: dsn(
                "DATABASE_URL_INGEST",
                "postgres://svc_notifier_ingest:svc_notifier_ingest@localhost:5432/svc_notifier_test",
            ),
        }
    }

    pub async fn spawn_instance(&self) -> ServiceInstance {
        let port = next_port();
        let base_url = format!("http://localhost:{port}");
        let port_str = port.to_string();
        let nats_url = self.nats.url();

        // ENGINE OPS CONTRACT (adaptation of the axum boot): the engine binary runs
        // `migrate` to completion under the owner DSN, then `serve` on ONE runtime role
        // (`APP_ROLE` / `DATABASE_URL`). The 1.0 app and ingest roles collapse to one:
        // the ingest role, whose 1.0 policy already admits every row. Migrations are
        // idempotent, so each spawn re-runs them.
        let migrate_envs: Vec<(&str, &str)> = vec![
            ("DATABASE_URL_OWNER", &self.service_owner_url),
            ("APP_ROLE", APP_ROLE),
            ("RUST_LOG", "warn"),
        ];
        let migrated = br_test_harness::run_once(
            env!("CARGO_BIN_EXE_svc-notifier"),
            &["migrate"],
            &migrate_envs,
            STARTUP_TIMEOUT,
        )
        .await
        .expect("the migrate command could not run");
        assert!(
            migrated.status.success(),
            "migrate failed: {}{}",
            String::from_utf8_lossy(&migrated.stdout),
            String::from_utf8_lossy(&migrated.stderr)
        );

        let envs: Vec<(&str, &str)> = vec![
            ("PORT", &port_str),
            ("DATABASE_URL", &self.ingest_url),
            ("APP_ROLE", APP_ROLE),
            ("NATS_URL", &nats_url),
            ("ENGINE_CHANNEL", "notifier_engine"),
            ("HOSTNAME", "notifier-test-pod"),
            ("RUST_LOG", "warn"),
        ];

        let mut process =
            SpawnedProcess::spawn(env!("CARGO_BIN_EXE_svc-notifier"), &["serve"], &envs);
        if let Err(reason) = process
            .wait_for_http_ok(&format!("{base_url}/readyz"), STARTUP_TIMEOUT)
            .await
        {
            panic!("svc-notifier did not become healthy on port {port}: {reason}");
        }
        // No settle window: the command stream retains anything published from the
        // moment the intake durable is bound, so a request sent right after /readyz can
        // be delayed, never missed.

        ServiceInstance {
            base_url: base_url.clone(),
            process,
            graphql: GraphqlClient::new(&base_url),
        }
    }

    // The producer's front door: the typed deliver command over the Fabric. It is
    // the only way a notification is ever created — every Given goes through it.
    // Waits until the service has taken at least `count` commands off the stream,
    // read on the durable of the service as the broker counts it. It proves the
    // service RECEIVED a request — not that it handled it (the storage may be
    // frozen) — and it is the fence before an absence is asserted: silence only
    // means something once the frames have reached the service.
    pub async fn wait_commands_received(&self, count: u64) {
        let received = br_test_harness::wait_until(RECOVERY_TIMEOUT, || async {
            self.nats
                .consumer_delivered(FixedStream::Cmd, INTAKE_DURABLE)
                .await
                >= count
        })
        .await;
        assert!(
            received,
            "the service did not take {count} command(s) within {RECOVERY_TIMEOUT:?}"
        );
    }

    pub async fn publish_deliver(&self, command: &DeliverNotification) {
        NotifierPublisher::new(self.nats.fabric())
            .deliver(command, default_metadata())
            .await
            .expect("publish the deliver command over the fabric");
    }

    // The refused-request vehicle. A producer bug does not arrive as a typed
    // `DeliverNotification` (the contract refuses it at construction): it arrives
    // as a well-formed envelope whose payload breaks the rules. The frame rides
    // the same typed coordinates as a compliant command — no raw subject.
    pub async fn publish_raw_deliver(&self, payload: &Value) {
        let envelope = IntegrationCommand::new(
            Uuid::now_v7(),
            br_notifier_contract::deliver_command_type(),
            br_notifier_contract::DELIVER_VERSION,
            Utc::now(),
            default_metadata(),
            payload.clone(),
        );
        self.nats
            .fabric()
            .publish_command(&br_notifier_contract::deliver_coords(), &envelope)
            .await
            .expect("publish the raw deliver envelope over the fabric");
    }
}

impl TestContext {
    pub async fn setup() -> Self {
        let stack = TestStack::up().await;
        let instance = stack.spawn_instance().await;
        Self { stack, instance }
    }
}

impl ServiceInstance {
    pub async fn graphql(&self, passport: &Passport, query: &str, vars: Value) -> Value {
        self.graphql.query(passport, query, vars).await
    }

    pub async fn graphql_unauthenticated(&self, query: &str) -> (reqwest::StatusCode, Value) {
        self.graphql.query_unauthenticated(query, json!({})).await
    }

    pub async fn graphql_with_header(
        &self,
        header: &str,
        query: &str,
    ) -> (reqwest::StatusCode, Value) {
        self.graphql
            .query_with_passport_header(header, query, json!({}))
            .await
    }

    pub async fn get(&self, path: &str) -> (reqwest::StatusCode, String) {
        self.graphql.get_raw(path).await
    }

    pub async fn subscribe(&self, passport: &Passport) -> SseSubscription {
        SseSubscription::open(&self.base_url, passport, DELTAS_SUBSCRIPTION)
            .await
            .with_logs(&self.process)
    }

    // The refusal counterpart of `subscribe`. A refused subscription answers one
    // frame carrying a GraphQL error and ends; `SseSubscription` is a handle over a
    // live stream and fails loud on an error frame, so the refusal is read from the
    // raw response. A subscription that was *accepted* holds the connection open
    // forever, so the bounded read is part of the assertion.
    // The engine answers a refused subscription the same way over SSE: exactly one
    // `next` frame carrying the error, then `complete`.
    pub async fn subscribe_refused(&self, passport: &Passport) -> Value {
        let response = tokio::time::timeout(
            SSE_TIMEOUT,
            self.graphql.post_raw(
                "/graphql",
                &[
                    ("X-Passport", passport.to_header().as_str()),
                    ("Accept", "text/event-stream"),
                ],
                json!({ "query": DELTAS_SUBSCRIPTION }),
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
}

fn default_metadata() -> EventMetadata {
    EventMetadata::new(Actor::Human(UserId::from(Uuid::now_v7())), Uuid::now_v7())
}
