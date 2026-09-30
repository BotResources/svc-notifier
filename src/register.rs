use futures_util::future::BoxFuture;
use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;
use crate::kernel::principal::AppPrincipalResolver;

pub fn all(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_principal_resolver(AppPrincipalResolver)?;
    engine.register_reaction_principal(
        |_pg, actor| -> BoxFuture<'_, Result<AppPrincipal, EngineError>> {
            Box::pin(async move { Ok(AppPrincipal::from_actor(actor)) })
        },
    )?;
    crate::slices::register(engine)
}
