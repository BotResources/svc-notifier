use std::time::Duration;

use sqlx::PgPool;

use br_notifier_contract::{DeliverNotification, declare_command_coords};
use br_util_nats_fabric::{Delivery, Fabric, FabricError, IntegrationCommand, MessageOutcome};

pub const DURABLE_NAME: &str = "svc-notifier";
const NAK_DELAY: Duration = Duration::from_secs(1);

pub async fn run(fabric: Fabric, ingest_pool: PgPool) -> Result<(), FabricError> {
    let coords = declare_command_coords();
    fabric
        .run_commands::<DeliverNotification, _, _, _>(
            &coords,
            DURABLE_NAME,
            |delivery| handle(&ingest_pool, delivery),
            on_poison,
        )
        .await
}

pub async fn verify(fabric: &Fabric) -> Result<(), FabricError> {
    fabric
        .verify_command_durable(&declare_command_coords(), DURABLE_NAME)
        .await
}

async fn handle(
    pool: &PgPool,
    delivery: Delivery<IntegrationCommand<DeliverNotification>>,
) -> MessageOutcome {
    let command = delivery.envelope.payload;
    match fan_out(pool, &command).await {
        Ok(()) => MessageOutcome::Ack,
        Err(error) => {
            tracing::error!(
                %error,
                source_event_id = %command.source_event_id,
                "deliver fan-out failed, NAKing for redelivery"
            );
            MessageOutcome::Nak(Some(NAK_DELAY))
        }
    }
}

fn on_poison(error: FabricError) {
    tracing::warn!(%error, "rejecting undecodable deliver command (fail-closed, terminated)");
}

async fn fan_out(pool: &PgPool, command: &DeliverNotification) -> Result<(), sqlx::Error> {
    if command.recipient_ids.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    for recipient_id in &command.recipient_ids {
        crate::notification::insert_notification(
            &mut tx,
            command.source_event_id,
            *recipient_id,
            &command.template,
            &command.payload,
            command.link.as_ref(),
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}
