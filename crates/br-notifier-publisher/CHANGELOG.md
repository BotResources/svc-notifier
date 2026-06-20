# Changelog

All notable changes to `br-notifier-publisher` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
It is versioned independently of the `svc-notifier` service crate.

## [Unreleased]

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
