# Changelog

All notable changes to `br-notifier-publisher` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
It is versioned independently of the `svc-notifier` service crate.

## [Unreleased]

## 0.2.1

### Changed

- Dependency-only patch: br-rust-common pins `v1.1.0` → **`v1.2.0`** and
  `br-notifier-contract` `0.3.0` → **`0.3.1`** (single-source alignment with the
  v1.2.0 consumer wave; no API change).

## 0.2.0

### Changed
- **Bump `br-util-nats-fabric` `v1.0.2` → `v1.1.0`** (and the `br-core-integration`
  / `br-test-harness` dev-deps to `v1.1.0`), with the matching
  `version = "1.1.0"` next to each tag. `NotifierPublisher` exposes
  `br_util_nats_fabric::Fabric`, `EventMetadata` and `FabricError` in its public
  signatures, so the library version those types resolve to is part of this
  crate's public API — hence the minor bump rather than a patch. No source or
  behavior change; the published `IntegrationCommand`, its payload and its
  coordinates are unchanged.
- Bump the `br-notifier-contract` path/tag dependency to `0.3.0`.

## 0.1.0

### Added
- `NotifierPublisher` — a thin producer over `br-util-nats-fabric` that publishes a
  typed `DeliverNotification` as an `IntegrationCommand` on `deliver_coords()`
  (the fixed v1 subject `integration.cmd.notifier.notification.deliver.v1` on the
  `INTEGRATION_CMD` stream). No raw `async_nats`, no hand-built subject.
- `PublishError` — the typed publish error.
- Real-infra test: publish through the Fabric and consume the command back on the
  deliver coordinates, asserting the payload round-trips and the rendered subject
  is the fixed v1 subject.
