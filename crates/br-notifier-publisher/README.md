# br-notifier-publisher

The producer-side kit for [`svc-notifier`](https://github.com/BotResources/svc-notifier):
a thin wrapper over the real [`br-util-nats-fabric`](https://github.com/BotResources/br-rust-common)
that publishes a typed [`DeliverNotification`](https://github.com/BotResources/svc-notifier)
(from `br-notifier-contract`) as an integration command on the deliver
coordinates. A producer never hand-builds a subject string or touches
`async_nats` — it builds the typed command and hands it to the Fabric.

## Surface

- **`NotifierPublisher::new(&Fabric)`** — binds to an already-connected Fabric.
- **`NotifierPublisher::deliver(&DeliverNotification, EventMetadata)`** — wraps the
  command in an `IntegrationCommand` envelope (fresh UUIDv7 command id, command
  type `notification.deliver`, version `1`, the caller's metadata) and publishes
  it via `Fabric::publish_command` on `deliver_coords()`. The Fabric renders the
  fixed v1 subject `integration.cmd.notifier.notification.deliver.v1` on the
  `INTEGRATION_CMD` stream.
- **`PublishError`** — the single typed error this crate surfaces (a publish
  failure carries the underlying Fabric error text).

The `EventMetadata` (actor, correlation id, optional causation id) is the
producer's to supply, so the command carries its provenance.

## Usage

```toml
[dependencies]
br-notifier-publisher = { git = "https://github.com/BotResources/svc-notifier", package = "br-notifier-publisher", tag = "br-notifier-publisher/v0.2.0", version = "0.2.0" }
```

License: Apache-2.0.
