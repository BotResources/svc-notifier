use br_notifier_contract::{
    DELIVER_VERSION, DeliverNotification, deliver_command_type, deliver_coords,
};
use br_util_nats_fabric::{EventMetadata, Fabric, IntegrationCommand};
use chrono::Utc;
use uuid::Uuid;

use crate::error::PublishError;

pub struct NotifierPublisher<'a> {
    fabric: &'a Fabric,
}

impl<'a> NotifierPublisher<'a> {
    pub fn new(fabric: &'a Fabric) -> Self {
        Self { fabric }
    }

    pub async fn deliver(
        &self,
        command: &DeliverNotification,
        metadata: EventMetadata,
    ) -> Result<(), PublishError> {
        let envelope = IntegrationCommand::new(
            Uuid::now_v7(),
            deliver_command_type(),
            DELIVER_VERSION,
            Utc::now(),
            metadata,
            command.clone(),
        );
        self.fabric
            .publish_command(&deliver_coords(), &envelope)
            .await
            .map_err(PublishError::Publish)
    }
}
