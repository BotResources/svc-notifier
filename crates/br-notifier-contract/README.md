# br-notifier-contract

The published language of [`svc-notifier`](https://github.com/BotResources/svc-notifier):
the typed command producers use to request a notification delivery. Owned by the
receiver, versioned and tagged independently of the service.

## Surface

- **`DeliverNotification`** — the v1 deliver command. One message fans out to one
  notification per recipient. Delivery is idempotent on
  `(source_event_id, recipient_id)`: the first command wins, duplicates are
  silently skipped.
- **`deliver_coords()`** — the typed `CommandCoords` (from `br-core-integration`)
  the command is published on: receiver `notifier`, aggregate `notification`,
  verb `deliver`, version `1`. The NATS Fabric renders these to the fixed v1
  subject `integration.cmd.notifier.notification.deliver.v1` on the
  `INTEGRATION_CMD` stream. This crate is transport-agnostic: it owns the
  coordinates, not the rendered subject string (the Fabric owns rendering).
  The segment values are also exposed as `DELIVER_RECEIVER` / `DELIVER_AGGREGATE`
  / `DELIVER_VERB` / `DELIVER_VERSION`.
- **`RelativeLink`** — a fail-closed, same-domain relative URL, validated at
  construction **and** on deserialization. Accepts a path rooted at `/`
  (query string and fragment allowed); rejects absolute URLs, schemes
  (`https:`, `javascript:`, …), scheme-relative `//host`, backslashes,
  whitespace, control characters, and empty input. `RelativeLinkError` speaks
  stable codes.

The wire format is pinned by tests: a producer that serializes a
`DeliverNotification` to JSON and publishes it on the subject the Fabric renders
from `deliver_coords()` is a conforming producer. The
[`br-notifier-publisher`](https://github.com/BotResources/svc-notifier) crate is
the supported way to publish it over the Fabric. See the
[service README](https://github.com/BotResources/svc-notifier#readme) for the
full intake semantics.

## Usage

```toml
[dependencies]
br-notifier-contract = { git = "https://github.com/BotResources/svc-notifier", package = "br-notifier-contract", tag = "br-notifier-contract/v0.2.0", version = "0.2.0" }
```

License: Apache-2.0.
