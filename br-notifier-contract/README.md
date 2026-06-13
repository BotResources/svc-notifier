# br-notifier-contract

The published language of [`svc-notifier`](https://github.com/BotResources/svc-notifier):
the typed command producers use to request a notification delivery. Owned by the
receiver, versioned and tagged independently of the service.

## Surface

- **`DeliverNotification`** — the v1 deliver command. One message fans out to one
  notification per recipient. Delivery is idempotent on
  `(source_event_id, recipient_id)`: the first command wins, duplicates are
  silently skipped.
- **`DELIVER_SUBJECT`** — the JetStream subject the command is published on:
  `notifier.cmd.notification.deliver.v1`.
- **`RelativeLink`** — a fail-closed, same-domain relative URL, validated at
  construction **and** on deserialization. Accepts a path rooted at `/`
  (query string and fragment allowed); rejects absolute URLs, schemes
  (`https:`, `javascript:`, …), scheme-relative `//host`, backslashes,
  whitespace, control characters, and empty input. `RelativeLinkError` speaks
  stable codes.

The wire format is pinned by tests: a producer that serializes a
`DeliverNotification` to JSON and publishes it on `DELIVER_SUBJECT` is a
conforming producer. See the
[service README](https://github.com/BotResources/svc-notifier#readme) for the
full intake semantics.

## Usage

```toml
[dependencies]
br-notifier-contract = { git = "https://github.com/BotResources/svc-notifier", package = "br-notifier-contract", tag = "br-notifier-contract/v0.1.0", version = "0.1.0" }
```

License: MIT.
