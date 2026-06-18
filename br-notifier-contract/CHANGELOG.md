# Changelog

All notable changes to `br-notifier-contract` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
It is versioned independently of the `svc-notifier` service crate.

## 0.2.0

Breaking. Adopts the enveloped Fabric command wire on the v1 Project NATS Fabric
grammar; every producer moves in lockstep.

### Changed
- **BREAKING — wire envelope.** `DeliverNotification` is now the `payload: T` body
  of an `IntegrationCommand<DeliverNotification>` (envelope: `command_id`,
  `command_type`, `version`, `issued_at`, `metadata`, `payload`), not a bare
  top-level message. Its own fields (`source_event_id`, `recipient_ids`,
  `template`, `payload`, optional fail-closed `link`) are unchanged.
- **BREAKING — subject + stream.** Replaced the freestyle `DELIVER_SUBJECT` const
  (`notifier.cmd.notification.deliver.v1`) and its convention test with a typed
  `declare_command_coords() -> CommandCoords` (receiver/bc `notifier`, aggregate
  `notification`, verb `deliver`, v1). The Fabric renders the subject
  `integration.cmd.notifier.notification.deliver.v1` on the fixed `INTEGRATION_CMD`
  stream; there is no caller-chosen subject string.

### Added
- `br-core-integration` dependency (pure contracts) for `CommandCoords`.

### Notes
- `source_event_id` stays the idempotency key, and lives **inside the payload** —
  distinct from the envelope's transport-level `command_id`. Producers must not
  conflate the two: `command_id` identifies the message, `source_event_id`
  identifies the originating event the notification is keyed on.

### Changed (0.2.0, non-wire)
- Relicensed from MIT to Apache-2.0.
- Stripped doc-comments from `lib.rs` per the no-comments doctrine.

## 0.1.0

### Added
- `DeliverNotification` — the typed v1 deliver command
  (`source_event_id`, `recipient_ids`, `template`, `payload`, optional `link`).
  `link` is omitted from the wire when absent and optional on deserialization.
- `RelativeLink` — fail-closed newtype for same-domain relative URLs, validated
  at construction and on deserialization: must be rooted at `/`; rejects absolute
  URLs and schemes, scheme-relative `//host`, backslashes, whitespace, control
  characters, and empty input. Typed `RelativeLinkError` with stable error codes.
- `DELIVER_SUBJECT` = `notifier.cmd.notification.deliver.v1` (the
  `{bc}.cmd.{aggregate}.{command}.v{N}` convention).
- Unit tests as the contract's spec: `RelativeLink` accept/reject security
  vectors, exact wire-format pin with round-trip, subject convention check.
