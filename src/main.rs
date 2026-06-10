mod graphql;
mod intake;
mod notification;
mod realtime;

use std::time::Duration;

use async_graphql::http::GraphiQLSource;
use async_graphql::{Request, Schema, SchemaBuilder};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use br_core_auth::Passport;
use br_util_axum_auth::passport_header_middleware;
use futures::StreamExt;
use sqlx::PgPool;
use tokio::net::TcpListener;

use graphql::{AppState, MutationRoot, QueryRoot, SubscriptionRoot};
use realtime::{Subscribers, run_listener};

type NotifierSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

#[derive(Clone)]
struct HttpState {
    schema: NotifierSchema,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if std::env::args().nth(1).as_deref() == Some("schema") {
        println!("{}", schema_builder().finish().sdl());
        return Ok(());
    }

    run_migrations().await?;

    let app_url = std::env::var("DATABASE_URL")?;
    let app_pool = br_util_postgres::init_pool(&app_url).await?;

    let subscribers = Subscribers::default();
    tokio::spawn(run_listener(app_pool.clone(), subscribers.clone()));

    if let Ok(nats_url) = std::env::var("NATS_URL") {
        let ingest_url = std::env::var("DATABASE_URL_INGEST")
            .map_err(|_| "DATABASE_URL_INGEST is required when NATS_URL is set")?;
        let ingest_pool = ingest_pool(&ingest_url).await?;
        let jetstream = connect_jetstream(&nats_url).await?;
        tokio::spawn(async move {
            if let Err(error) = intake::run(jetstream, ingest_pool).await {
                tracing::error!(%error, "intake terminated");
            }
        });
    }

    let schema = schema_builder()
        .data(AppState {
            app_pool,
            subscribers,
        })
        .finish();

    let state = HttpState { schema };
    let app = Router::new()
        .route("/health", get(health))
        .route("/sdl", get(schema_sdl))
        .route("/graphql/playground", get(playground))
        .route(
            "/graphql",
            get(playground)
                .post(graphql_handler)
                .layer(axum::middleware::from_fn(passport_header_middleware)),
        )
        .with_state(state);

    let port: u16 = std::env::var("PORT")?.parse()?;
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "svc-notifier listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn schema_builder() -> SchemaBuilder<QueryRoot, MutationRoot, SubscriptionRoot> {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
}

async fn ingest_pool(url: &str) -> Result<PgPool, Box<dyn std::error::Error>> {
    br_util_postgres::validate_database_tls(url)?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::Executor::execute(conn, "SET statement_timeout = '3s'").await?;
                Ok(())
            })
        })
        .connect(url)
        .await?;
    Ok(pool)
}

async fn run_migrations() -> Result<(), Box<dyn std::error::Error>> {
    let owner_pool = br_util_postgres::init_migration_pool().await?;
    sqlx::migrate!("./migrations").run(&owner_pool).await?;
    owner_pool.close().await;
    Ok(())
}

async fn connect_jetstream(
    nats_url: &str,
) -> Result<async_nats::jetstream::Context, Box<dyn std::error::Error>> {
    let client = match (std::env::var("NATS_USER"), std::env::var("NATS_PASSWORD")) {
        (Ok(user), Ok(password)) => {
            async_nats::ConnectOptions::with_user_and_password(user, password)
                .connect(nats_url)
                .await?
        }
        _ => async_nats::connect(nats_url).await?,
    };
    Ok(async_nats::jetstream::new(client))
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn schema_sdl(State(state): State<HttpState>) -> impl IntoResponse {
    state.schema.sdl()
}

async fn playground() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn graphql_handler(
    State(state): State<HttpState>,
    Extension(passport): Extension<Passport>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let request: Request = match serde_json::from_slice::<GraphQLRequestBody>(&body) {
        Ok(parsed) => parsed.into_request(),
        Err(error) => {
            return (StatusCode::BAD_REQUEST, format!("invalid request: {error}")).into_response();
        }
    };
    let request = request.data(passport);

    if wants_event_stream(&headers) {
        let stream = state.schema.execute_stream(request).map(|response| {
            let json = serde_json::to_string(&response).expect("response serialization");
            Ok::<Event, std::convert::Infallible>(Event::default().event("next").data(json))
        });
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
            .into_response()
    } else {
        Json(state.schema.execute(request).await).into_response()
    }
}

fn wants_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GraphQLRequestBody {
    query: String,
    #[serde(default)]
    operation_name: Option<String>,
    #[serde(default)]
    variables: serde_json::Value,
}

impl GraphQLRequestBody {
    fn into_request(self) -> Request {
        let mut request = Request::new(self.query);
        if let Some(name) = self.operation_name {
            request = request.operation_name(name);
        }
        if !self.variables.is_null() {
            request = request.variables(async_graphql::Variables::from_json(self.variables));
        }
        request
    }
}
