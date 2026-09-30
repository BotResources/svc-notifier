//! The one slice: a private notification, from the producer's deliver command to the
//! recipient's live list. Aggregate, store, view, reaction, mutations and GraphQL
//! capability all live here.
mod aggregate;
pub mod graphql;
mod mutations;
mod reactions;
mod store;
mod view;
mod wire;

use service_engine::Engine;
use service_engine::error::EngineError;

use crate::kernel::AppPrincipal;

/// The consumer name of the intake on the command stream (the 1.0 durable, kept).
pub const INTAKE_DURABLE: &str = "svc-notifier";

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_view(view::NotificationsView)?;
    engine.register_mutation::<mutations::MarkAsRead, _>(mutations::mark_as_read)?;
    engine.register_mutation::<mutations::MarkAllAsRead, _>(mutations::mark_all_as_read)?;
    engine
        .register_mutation::<mutations::DeleteNotifications, _>(mutations::delete_notifications)?;
    engine.register_reaction::<wire::InboundDeliver, _, _>(INTAKE_DURABLE, reactions::deliver)?;
    engine.register_schema_slice(service_engine::graphql::SliceFragment::derive::<
        graphql::NotificationsQuery,
        graphql::NotificationsMutation,
        graphql::NotificationsSubscription,
    >("notifications"))?;
    Ok(())
}
