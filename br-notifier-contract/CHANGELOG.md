# Changelog

All notable changes to `br-notifier-contract` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
It is versioned independently of the `svc-notifier` service crate.

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
