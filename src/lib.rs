pub mod db;
pub mod error;
pub mod graphql;
pub mod nats;

use sqlx::PgPool;

use crate::nats::UserChannels;

/// Migrator for use by `#[sqlx::test]` in integration tests.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub channels: UserChannels,
}
