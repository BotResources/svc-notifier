# br-notifier-contract

The published language of [`svc-notifier`](https://github.com/BotResources/svc-notifier):
the typed command producers use to request a notification delivery. Owned by the
receiver, versioned and tagged independently of the service.

## Surface

- **`DeliverNotification`** — the v1 deliver command **payload**. One message fans
  out to one notification per recipient. Delivery is idempotent on
  `(source_event_id, recipient_id)`: the first command wins, duplicates are
  silently skipped. It travels as the `payload: T` of an
  `IntegrationCommand<DeliverNotification>` (the `br-core-integration` envelope:
  `command_id`, `command_type`, `version`, `issued_at`, `metadata`, `payload`).
- **`declare_command_coords() -> CommandCoords`** — the typed command coordinates
  (receiver/bc `notifier`, aggregate `notification`, verb `deliver`, v1). The
  `br-util-nats-fabric` Fabric renders them to the subject
  `integration.cmd.notifier.notification.deliver.v1` on the fixed `INTEGRATION_CMD`
  stream and validates them against the v1 grammar — there is no freestyle subject
  string and the stream name is not caller-chosen.
- **`RelativeLink`** — a fail-closed, same-domain relative URL, validated at
  construction **and** on deserialization. Accepts a path rooted at `/`
  (query string and fragment allowed); rejects absolute URLs, schemes
  (`https:`, `javascript:`, …), scheme-relative `//host`, backslashes,
  whitespace, control characters, and empty input. `RelativeLinkError` speaks
  stable codes. An unsafe `link` makes the whole enveloped command fail to
  deserialize, so the intake rejects it (fail-closed).

### `command_id` vs `source_event_id` — do not conflate

The envelope's **`command_id`** identifies the *message* (transport-level
de-duplication / tracing). The payload's **`source_event_id`** is the
*idempotency key* — it identifies the originating domain event the notification is
keyed on, and is what `(source_event_id, recipient_id)` dedups against. A producer
that retries a delivery keeps the same `source_event_id` (so the row is not
duplicated) even if it mints a fresh `command_id`.

A conforming producer builds an `IntegrationCommand<DeliverNotification>` and
publishes it via the Fabric on the coordinates from `declare_command_coords()`. See
the [service README](https://github.com/BotResources/svc-notifier#readme) for the
full intake semantics.

## Usage

```toml
[dependencies]
br-notifier-contract = { git = "https://github.com/BotResources/svc-notifier", package = "br-notifier-contract", tag = "br-notifier-contract/v0.2.0", version = "0.2.0" }
```

License: Apache-2.0.
