use service_engine::config::EngineConfig;
use service_engine::{BootPlan, run_service};
use svc_notifier::slices::{MutationRoot, QueryRoot, SubscriptionRoot};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::from_env()?
        .with_service("notifier")
        .with_service_version(env!("CARGO_PKG_VERSION"));

    run_service(BootPlan {
        component: "svc-notifier",
        libraries: Vec::new(),
        service_migrator: svc_notifier::db::migrator(),
        config,
        query: QueryRoot::default(),
        mutation: MutationRoot::default(),
        subscription: SubscriptionRoot::default(),
        declare_scopes: false,
        register: svc_notifier::register::all,
    })
    .await?;
    Ok(())
}
