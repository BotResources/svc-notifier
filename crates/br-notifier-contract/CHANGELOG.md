# Changelog

All notable changes to `br-notifier-contract` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
It is versioned independently of the `svc-notifier` service crate.

## [Unreleased]

## 0.3.0

### Changed
- **Bump `br-core-integration` `v1.0.2` → `v1.1.0`** (with the matching
  `version = "1.1.0"` next to the tag). `deliver_coords()` returns
  `br_core_integration::CommandCoords` in its public signature, so the library
  version that coordinate type resolves to is part of this crate's public API —
  hence the minor bump rather than a patch. The coordinate values, the
  `DeliverNotification` / `RelativeLink` wire, `deliver_command_type()` and the
  rendered subject are all unchanged; no source change.

## 0.2.0

### Added
- `deliver_coords()` — typed `CommandCoords` (from `br-core-integration`) for the
  v1 deliver command (receiver `notifier`, aggregate `notification`, verb
  `deliver`, version `1`). The Fabric renders these to
  `integration.cmd.notifier.notification.deliver.v1` on the `INTEGRATION_CMD`
  stream. Exposed alongside the segment constants `DELIVER_RECEIVER`,
  `DELIVER_AGGREGATE`, `DELIVER_VERB`, `DELIVER_VERSION`.
- `deliver_command_type()` — the envelope `command_type` (`{aggregate}.{verb}`),
  derived from the same segment constants as `deliver_coords()` so the publisher's
  envelope and the coordinates share one source and cannot drift.
- `br-core-integration` dependency for the coordinate newtypes. The crate stays
  transport-agnostic: it owns the coordinates, never a NATS client or the
  rendered subject string.

### Removed
- `DELIVER_SUBJECT` (the pre-v1 `notifier.cmd.notification.deliver.v1` raw subject
  string). The published subject is now derived by the Fabric from
  `deliver_coords()` on the fixed v1 grammar; a raw subject string is no longer
  part of the contract. **Breaking** — hence the minor bump from 0.1.0 (pre-1.0).

### Changed
- Relicensed from MIT to Apache-2.0.
- Stripped doc-comments from `lib.rs` per the no-comments doctrine; the contract
  surface is unchanged otherwise.
- The wire payload of `DeliverNotification` and `RelativeLink` is unchanged: the
  command stays v1.
- Replaced the regex-based subject-convention test with a test asserting
  `deliver_coords()` render to the expected fixed v1 subject.

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
