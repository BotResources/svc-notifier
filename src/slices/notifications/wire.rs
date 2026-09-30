use br_notifier_contract::{DeliverNotification, deliver_coords};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};

/// The producer's deliver command, decoded from the envelope payload. The contract
/// type refuses a link that is not a relative path at deserialization, so a request
/// that breaks the offer never reaches the handler: it is dead-lettered as a whole.
pub struct InboundDeliver(pub DeliverNotification);

impl ReactionMessage for InboundDeliver {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(deliver_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundDeliver)
    }
}
