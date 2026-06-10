# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- `br-notifier-contract` 0.1.0 — the service's published language, as a sibling
  workspace crate with its own version, changelog and tag line. Producers
  depend on it instead of hand-rolling the deliver payload. The service does not
  consume it yet — that lands with the subject migration. See
  `br-notifier-contract/CHANGELOG.md`.
- The e2e suite is rewritten as named behavior scenarios
  (`tests/scenarios_*.rs`), each pinning the three external envelopes (NATS
  ack/NAK/redelivery + consumer state, exact PG rows via a dedicated assertion
  connection, the GraphQL view of a forged Passport). New coverage: dedup
  first-wins as contract, fail-closed link rejection, legacy-subject
  retirement, DB-outage NAK/recovery/exhaustion, partial-redelivery
  idempotence, cross-session read/delete propagation, single bulk event for
  `markAllAsRead`, bulk delete RLS semantics, reconnect
  (subscribe-then-snapshot), and a two-instance scenario proving pushes derive
  from committed PG state. Scenarios pinning target behavior fail red until
  the implementation lands (spec-first).
- `deny.toml` + cargo-deny, cargo-machete, cargo-semver-checks (contract
  crate), per-crate changelog check, shellcheck and trufflehog jobs in CI,
  aligned with the platform CI standard.
- `scripts/setup-branch-protection.sh` — declarative required-checks
  management for `main`; the e2e job is a required check.

### Changed
- CI triggers on `pull_request` only (plus `workflow_dispatch`), with a
  `cargo fmt` auto-fix gate fronting every Rust job — no more double runs,
  no CI on pushes to `main`.
- CD is restructured image-first/tag-after: `detect-bump` (per crate) →
  publish image + chart → create `{crate}/v{version}` tag + GitHub Release.
  The auto-tag job moves out of CI into CD; `publish.sh` no longer requires a
  pre-existing tag (the tag is created after a successful publish, never
  before). The contract crate is released as a tag only.
- Direct-SQL test seeding is removed: every scenario seeds through the real
  NATS intake.
- `scripts/lib/*.sh` pass shellcheck (`cd` failure guards, exported
  `CRATE_NAME`).

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
- **The entire service implementation** (`src/`, `migrations/`) — deliberately:
  the repository becomes a spec-first playground for a from-scratch rebuild.
  The README and the red e2e scenario suite are the contract; `src/main.rs` is
  a stub that exits non-zero. Runtime dependencies were cleared with it — the
  test harness keeps its own dev-dependencies.

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
