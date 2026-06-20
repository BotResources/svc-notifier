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
use br_util_axum_readiness::{ReadinessHandle, readiness_route};
use br_util_nats_fabric::{Fabric, NatsAuth};
use br_util_observability::{
    http_metrics_layer, init_logging, init_metrics, liveness_route, metrics_route,
};
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
    init_logging("svc-notifier");

    if std::env::args().nth(1).as_deref() == Some("schema") {
        println!("{}", schema_builder().finish().sdl());
        return Ok(());
    }

    let metrics = init_metrics("svc-notifier")?;
    let readiness = ReadinessHandle::not_ready("starting up");

    run_migrations().await?;

    let app_url = std::env::var("DATABASE_URL")?;
    let app_pool = br_util_postgres::init_pool(&app_url).await?;

    let subscribers = Subscribers::default();
    tokio::spawn(run_listener(app_pool.clone(), subscribers.clone()));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    if let Some(nats_url) = std::env::var("NATS_URL").ok().filter(|url| !url.is_empty()) {
        let ingest_url = std::env::var("DATABASE_URL_INGEST")
            .map_err(|_| "DATABASE_URL_INGEST is required when NATS_URL is set")?;
        let ingest_pool = ingest_pool(&ingest_url).await?;
        let fabric = connect_fabric(&nats_url).await?;
        let consumer = intake::bind(&fabric).await?;
        tokio::spawn(intake::consume(
            consumer,
            ingest_pool,
            readiness.clone(),
            shutdown_tx.clone(),
            shutdown_rx.clone(),
        ));
    }

    let schema = schema_builder()
        .data(AppState {
            app_pool,
            subscribers,
        })
        .finish();

    let state = HttpState { schema };
    let app = Router::new()
        .route("/livez", liveness_route())
        .route("/readyz", readiness_route(readiness.clone()))
        .route("/metrics", metrics_route(metrics))
        .route("/sdl", get(schema_sdl))
        .route("/graphql/playground", get(playground))
        .route(
            "/graphql",
            get(playground)
                .post(graphql_handler)
                .layer(axum::middleware::from_fn(passport_header_middleware)),
        )
        .layer(http_metrics_layer())
        .with_state(state);

    let port: u16 = std::env::var("PORT")?.parse()?;
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    readiness.set_ready();
    tracing::info!(port, "svc-notifier listening");
    let intake_failed = shutdown_rx.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_rx))
        .await?;
    let intake_terminated_abnormally = *intake_failed.borrow();
    let _ = shutdown_tx.send(true);
    if intake_terminated_abnormally {
        return Err("intake terminated abnormally — exiting non-zero so K8s reschedules".into());
    }
    Ok(())
}

async fn shutdown_signal(mut intake_down: tokio::sync::watch::Receiver<bool>) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    let intake_failed = async {
        let _ = intake_down.wait_for(|down| *down).await;
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = intake_failed => {},
    }
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

async fn connect_fabric(nats_url: &str) -> Result<Fabric, Box<dyn std::error::Error>> {
    let fabric = match (std::env::var("NATS_USER"), std::env::var("NATS_PASSWORD")) {
        (Ok(user), Ok(password)) => {
            Fabric::connect_with(nats_url, &NatsAuth { user, password }).await?
        }
        _ => Fabric::connect(nats_url).await?,
    };
    Ok(fabric)
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
