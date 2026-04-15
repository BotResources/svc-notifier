use std::net::SocketAddr;

use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
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

    sqlx::migrate!("./migrations")
        .run(&migration_pool)
        .await
        .expect("failed to run migrations");

    // Grant access to app role (non-fatal if role doesn't exist yet).
    let app_role = std::env::var("APP_ROLE").unwrap_or_else(|_| "app".to_string());
    if let Err(e) = br_service_core::grant_app_access(&migration_pool, &app_role).await {
        tracing::warn!(error = %e, "failed to grant app access");
    }
    migration_pool.close().await;

    // 2. App pool (app role) — subject to RLS, used for runtime.
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
                let consumer_pool = pool.clone();
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
        .route_service("/graphql/ws", GraphQLSubscription::new(schema.clone()))
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
    req: GraphQLRequest,
) -> GraphQLResponse {
    let mut request = req.into_inner();
    if let Some(axum::Extension(p)) = passport {
        request = request.data(p);
    }
    schema.execute(request).await.into()
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
