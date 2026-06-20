use br_core_integration::{Actor, EventMetadata, UserId};
use br_notifier_contract::{DeliverNotification, RelativeLink, deliver_coords};
use br_notifier_publisher::NotifierPublisher;
use br_test_harness::FabricTestNats;
use br_util_nats_fabric::command_subject;
use std::time::Duration;
use uuid::Uuid;

fn metadata() -> EventMetadata {
    EventMetadata::new(Actor::Human(UserId::from(Uuid::now_v7())), Uuid::now_v7())
}

#[test]
fn deliver_coords_render_to_the_fixed_v1_command_subject() {
    assert_eq!(
        command_subject(&deliver_coords()),
        "integration.cmd.notifier.notification.deliver.v1"
    );
}

#[tokio::test]
#[ignore = "real-infra: needs `nats-server` on PATH"]
async fn published_deliver_command_is_consumed_on_the_deliver_coords() {
    let nats = FabricTestNats::start().await;
    let durable = nats.durable("notifier-publisher-itest");

    let command = DeliverNotification {
        source_event_id: Uuid::now_v7(),
        recipient_ids: vec![Uuid::now_v7(), Uuid::now_v7()],
        template: "meeting_scheduled".to_string(),
        payload: serde_json::json!({"meeting_id": "m-1"}),
        link: Some(RelativeLink::parse("/meetings/m-1").unwrap()),
    };

    let mut consumer = nats
        .fabric()
        .ensure_command_consumer::<DeliverNotification>(&deliver_coords(), &durable)
        .await
        .expect("bind the deliver command consumer on the fixed INTEGRATION_CMD stream");

    NotifierPublisher::new(nats.fabric())
        .deliver(&command, metadata())
        .await
        .expect("publish the deliver command over the fabric");

    let delivered = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
        .await
        .expect("a delivery arrives within the deadline")
        .expect("recv does not error")
        .expect("the published command is delivered, not stream end");

    let envelope = delivered.payload().expect("the envelope decodes");
    assert_eq!(envelope.payload, command);
    assert_eq!(
        delivered.subject(),
        "integration.cmd.notifier.notification.deliver.v1"
    );
    delivered.ack().await.expect("ack the consumed command");

    consumer.drain().await;
    nats.shutdown().await;
}
