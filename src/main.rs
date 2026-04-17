use std::net::SocketAddr;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use svc_notifier::AppState;
use svc_notifier::graphql::{self, AppSchema};
use svc_notifier::nats;

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("schema") {
        print!("{}", graphql::sdl());
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    dotenvy::dotenv().ok();

    let nats_url = std::env::var("NATS_URL").ok();
    let port: u16 = std::env::var("PORT")
        .expect("PORT must be set")
        .parse()
        .expect("PORT must be a valid u16");

    // 1. Migration pool (owner role) — run migrations, grant access, then drop.
    let migration_url = std::env::var("DATABASE_URL_OWNER")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("DATABASE_URL_OWNER or DATABASE_URL must be set");
    let migration_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&migration_url)
        .await
        .expect("failed to connect to database for migrations");

    // Precondition: roles svc_notifier_app and svc_notifier_ingest must be
    // provisioned before first startup. See scripts/init-db.sql for reference.
    sqlx::migrate!("./migrations")
        .run(&migration_pool)
        .await
        .expect("failed to run migrations");

    // Grant access to svc_notifier_app role (non-fatal if role doesn't exist yet).
    if let Err(e) =
        br_service_core::grant_app_access(&migration_pool, "svc_notifier_app").await
    {
        tracing::warn!(error = %e, "failed to grant svc_notifier_app access");
    }

    // Grant minimal access to svc_notifier_ingest: only INSERT + SELECT on notifications.
    for sql in [
        "GRANT USAGE ON SCHEMA public TO svc_notifier_ingest",
        "GRANT INSERT, SELECT ON TABLE notifications TO svc_notifier_ingest",
    ] {
        if let Err(e) = sqlx::query(sql).execute(&migration_pool).await {
            tracing::warn!(error = %e, "failed to grant svc_notifier_ingest access");
        }
    }
    migration_pool.close().await;

    // 2. App pool (svc_notifier_app) — subject to user-scoped RLS, used by GraphQL.
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    let channels = nats::new_user_channels();
    let state = AppState {
        pool: pool.clone(),
        channels: channels.clone(),
    };

    // NATS consumer (optional — graceful degradation)
    if let Some(nats_url) = nats_url {
        // 3. Ingest pool (svc_notifier_ingest) — only created when NATS intake is enabled.
        let ingest_url = std::env::var("DATABASE_URL_INGEST")
            .expect("DATABASE_URL_INGEST must be set when NATS_URL is provided");
        let ingest_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&ingest_url)
            .await
            .expect("failed to connect ingest pool");

        let nats_user = std::env::var("NATS_USER").ok();
        let nats_password = std::env::var("NATS_PASSWORD").ok();

        let nats_connect_result = if let (Some(user), Some(pass)) = (nats_user, nats_password) {
            async_nats::ConnectOptions::with_user_and_password(user, pass)
                .connect(&nats_url)
                .await
        } else {
            async_nats::connect(&nats_url).await
        };

        match nats_connect_result {
            Ok(nats_client) => {
                let jetstream = async_nats::jetstream::new(nats_client);
                let consumer_pool = ingest_pool;
                let consumer_channels = channels.clone();
                tokio::spawn(async move {
                    nats::run_consumer(jetstream, consumer_pool, consumer_channels).await;
                });
                tracing::info!("NATS consumer started on notify.deliver");
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to connect to NATS — running without NATS intake");
            }
        }
    } else {
        tracing::warn!("NATS_URL not set — running without NATS intake");
    }

    let schema = graphql::build_schema(state.clone());

    let graphql_routes = Router::new()
        .route("/graphql", post(graphql_handler))
        .route("/graphql/playground", get(graphql_playground))
        .layer(axum::middleware::from_fn(
            br_service_core::passport_header_middleware,
        ));

    let app = Router::new()
        .merge(graphql_routes)
        .route("/health", get(health))
        .route("/schema", get(schema_sdl))
        .with_state(schema);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("svc-notifier listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server error");
}

async fn graphql_handler(
    State(schema): State<AppSchema>,
    passport: Option<axum::Extension<br_service_core::Passport>>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Response {
    let mut request = req.into_inner();
    if let Some(axum::Extension(p)) = passport {
        request = request.data(p);
    }

    // SSE subscriptions: gateway sends subscription operations as HTTP POST
    // with Accept: text/event-stream.
    let wants_sse = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.contains("text/event-stream"));

    if wants_sse {
        let stream = schema.execute_stream(request);
        let sse = stream.map(|resp| {
            let json = serde_json::to_string(&resp).unwrap_or_default();
            Ok::<_, std::convert::Infallible>(Event::default().event("next").data(json))
        });
        return Sse::new(sse)
            .keep_alive(KeepAlive::default())
            .into_response();
    }

    // Queries/mutations: keep standard async-graphql-axum behavior.
    let resp: GraphQLResponse = schema.execute(request).await.into();
    resp.into_response()
}

async fn graphql_playground() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn health() -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({"status": "ok"})),
    )
}

async fn schema_sdl(State(schema): State<AppSchema>) -> impl IntoResponse {
    schema.sdl()
}
