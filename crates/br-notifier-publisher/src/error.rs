use br_util_nats_fabric::FabricError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("could not publish the deliver command: {0}")]
    Publish(#[from] FabricError),
}
