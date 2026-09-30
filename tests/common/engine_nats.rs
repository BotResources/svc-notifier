// Engine accommodation for the broker: the engine refuses to run against an
// INTEGRATION_CMD stream with an unlimited `max_age` (its message-claim retention can
// never cover it). The harness provisioner creates the fixed streams unbounded, so the
// broker of a scenario is provisioned here first, with the same subjects and a bounded
// age; `FabricTestNats::connect` then finds them and creates nothing.
use std::time::Duration;

use async_nats::jetstream::stream::Config;

const MAX_AGE: Duration = Duration::from_secs(1800);

pub async fn provision(url: &str) {
    let client = async_nats::connect(url)
        .await
        .expect("connect the scenario broker");
    let jetstream = async_nats::jetstream::new(client);
    for (name, subject) in [
        ("INTEGRATION_CMD", "integration.cmd.>"),
        ("INTEGRATION_EVT", "integration.evt.>"),
    ] {
        jetstream
            .create_stream(Config {
                name: name.to_string(),
                subjects: vec![subject.to_string()],
                duplicate_window: Duration::from_secs(120),
                max_age: MAX_AGE,
                ..Default::default()
            })
            .await
            .unwrap_or_else(|error| panic!("declare {name}: {error}"));
    }
}
