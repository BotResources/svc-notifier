# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- `br-notifier-contract` 0.1.0 — the service's published language, as a sibling
  workspace crate with its own version, changelog and (upcoming) tag line. Producers
  depend on it instead of hand-rolling the deliver payload. The service does not
  consume it yet — that lands with the subject migration. See
  `br-notifier-contract/CHANGELOG.md`.

### Changed
- Repository converted to a two-crate Cargo workspace (`svc-notifier` +
  `br-notifier-contract`). The service crate is unchanged; root-level cargo
  commands cover both crates via `default-members`.
- README rewritten as the service's contract: target-state sections are explicitly
  marked `[target]` until the implementation lands (subject migration, `link`,
  subscription event union, bulk delete, LISTEN/NOTIFY realtime).
- README now matches the code where it previously did not: subscriptions are served
  over SSE on `POST /graphql` (there is no `/graphql/ws` WebSocket route), the
  required `DATABASE_URL_INGEST` variable is documented, and the unread `APP_ROLE`
  variable is gone (role names are fixed: `svc_notifier_app`, `svc_notifier_ingest`).

### Removed
- `docs/domain.md` — its staged hexagonal plan is superseded; the notification
  lifecycle and behavior inventory it carried are absorbed into the README (its
  open questions on delete semantics and template allow-listing survive there).

## 0.2.0

### Added
- GitHub Actions CI: `check` (fmt, clippy, unit tests, helm lint),
  `integration` (Postgres 17 + NATS JetStream, full P1-P4 harness),
  `audit`, `auto-tag` on version bump.
- GitHub Actions CD: multi-arch image (linux/amd64 + linux/arm64) and
  Helm OCI chart published on `v*` tag push.
- Helm chart `br-svc-notifier` (autonomous plugin model): Deployment,
  Service, ServiceAccount, Postgres DSNs via existing Secret, optional
  NATS credentials Secret.
- Runtime-only `Dockerfile` (debian:bookworm-slim, ~80 MB). The binary is
  compiled outside Docker via `cross` and copied in — no more
  `--mount=type=ssh` required for image builds.
- `scripts/publish.sh` + `scripts/lib/*.sh` — local/CI publish pipeline
  mirrored from `svc-auth`. Supports `--dry-run`, `--local-image`,
  `--check-only`, `--skip-checks`.
- `.dockerignore` and `CHANGELOG.md`.

### Changed
- Bumped version 0.1.0 → 0.2.0. This release marks the arrival of the CI
  gate and the plugin-autonomous packaging; the service itself has no
  behavioral changes relative to 0.1.0.
- `tests/common/mod.rs` declared `#![allow(dead_code)]` at module level
  (shared helpers are not used by every test binary, triggering clippy
  false positives under `--all-targets -D warnings`).
- Collapsed nested `if` blocks in `tests/common/mod.rs` and
  `tests/p4_subscriptions.rs` using let-chains to satisfy
  `clippy::collapsible_if`.

## 0.1.0

- Initial internal release. See git history for details.
