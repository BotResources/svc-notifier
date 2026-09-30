use sqlx::migrate::Migrator;

/// The 1.0 migrations, untouched: the engine applies its own set first, then this one,
/// on the same `_sqlx_migrations` ledger.
pub fn migrator() -> Migrator {
    sqlx::migrate!("./migrations")
}
