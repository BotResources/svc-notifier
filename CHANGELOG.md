# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.5.1

### Changed
- **Explicit version on every `br-rust-common` pin.** The form is now
  `{ git, package, tag = "v0.8.0", version = "0.8.0" }` on all five deps and the
  `br-core-auth` dev-dep — a bare `{ git, tag }` pin is, to cargo, a wildcard
  (`*`); the version makes the requirement explicit and readable. `[bans]
  wildcards` flips `allow` → `deny`, with `allow-wildcard-paths = true` and
  `publish = false` so the in-workspace `br-notifier-contract` path dep stays
  exempt, so the pin form cannot silently regress to a wildcard. No `Cargo.lock`
  change, no runtime change.

## 0.5.0

A reuse pass on the shared library: bump every `br-rust-common` pin to the unified
`v0.8.0` and adopt the lib's RLS, observability and readiness helpers in place of
hand-rolled code. No change to the GraphQL surface or the `br-notifier-contract`
wire format; the probe-endpoint change is an operational contract for the chart.

### Changed
- **Shared-lib bump → unified `v0.8.0`.** `br-core-auth`, `br-util-axum-auth` and
  `br-util-postgres` (and the `br-core-auth` dev-dep) move from
  `tag = "br-util-postgres-v0.7.0"` to `tag = "v0.8.0"` — one coherent set, one
  tag. Adds `br-util-observability` and `br-util-axum-readiness` at the same tag.
- **GraphQL RLS context via the shared `br_util_postgres::set_rls_context`.** The
  resolver path now threads the real `Passport` from the auth middleware to the
  transaction boundary and calls the shared helper, replacing the hand-rolled
  `SELECT set_config('app.current_user_id', …, true)`. The helper sets five
  transaction-local `app.*` GUCs; the notifications policy reads only
  `app.current_user_id`, so the extra GUCs are inert. The realtime listener has no
  `Passport` (its recipient is synthesized from the `pg_notify` signal) and keeps
  its single manual `set_config` — fabricating a fake identity to reach the helper
  would be a security smell.
- **Observability via `br-util-observability`.** `init_logging("svc-notifier")`
  replaces the hand-rolled `tracing_subscriber` JSON setup (and the
  `tracing-subscriber` dependency is dropped). `init_metrics` + `metrics_route`
  (`/metrics`) + `http_metrics_layer` add a Prometheus exposition with process and
  HTTP collectors and anonymized labels.
- **Probe endpoints `/health` → `/livez` + `/readyz`** (BREAKING for the chart).
  `/health` is removed. `/livez` (always-200 liveness, `br-util-observability`) and
  `/readyz` (`br-util-axum-readiness`, `503` until boot work completes, then `200`)
  replace it. The chart's liveness probe moves from a TCP-port check to
  `httpGet /livez` and the readiness probe to `/readyz` (`values.yaml` gains
  `probes.liveness.path`); chart `version`/`appVersion` bump to 0.5.0 in lockstep.

### Tests
- **The service-passport rejection is now proven on the query/mutation surface.**
  `scenarios_authn::service_passport_queries_and_mutations_are_forbidden` asserts a
  `Passport::Service` gets a `FORBIDDEN` verdict (no result) on both reads and writes,
  backing the README's "rejected before any work" claim. The query/mutation guard is
  extracted as a named `require_human` so its authZ intent is not mistaken for dead code.

### Notes
- **`br_core_integration::DurableConsumer` evaluated and declined for the intake.**
  Its public consume methods force the integration envelope; svc-notifier consumes
  a bare `DeliverNotification` on a contract-owned subject, so the hand-rolled
  `consumer.messages()` loop is kept (see README → Infra debt).

## 0.4.1

A chart-only patch: a generic knob for adding labels to the rendered Service,
so a GitOps consumer no longer has to patch the Service out-of-band. No code,
contract, or behavior change.

### Added
- **Chart: `service.labels`** — a map (default `{}`) merged onto the Service's
  `metadata.labels` on top of the standard chart labels (which are never
  overridden), rendered with a `with`-block guard like `service.annotations`.
  This covers the case where an external controller discovers the Service by
  label — e.g. labels matched by a service-discovery selector such as a
  federation gateway composer that enumerates subgraph Services. The chart
  `version`/`appVersion` are bumped to 0.4.1 in lockstep with the crate.

## 0.4.0

A scoped pre-deployment fix: align the SDL route with the gateway composer's
hard contract and pull in the strict-by-default database-TLS posture from the
shared lib.

### Changed (BREAKING)
- **SDL route renamed `/schema` → `/sdl`.** The GraphQL gateway composer polls
  every subgraph at `GET {base_url}/sdl`; serving the SDL elsewhere gets the
  subgraph rejected at composition. There is no alias — one route, one truth. A
  new e2e scenario (`s05_sdl_route_serves_the_schema_for_the_gateway_composer`)
  pins it: `GET /sdl` returns the SDL and `GET /schema` is now 404.
- **Shared-lib bump `v0.4.0` → br-core-auth 0.6.2 / br-util-axum-auth 0.4.2 /
  br-util-postgres 0.7.0.** Database-TLS validation is now **unconditionally
  strict**: the `Environment` enum and the `allow_insecure` /
  `ALLOW_INSECURE_DATABASE` blanket bypass are gone. A plaintext DSN to any
  remote (non-loopback) host is refused at startup unless the host is declared
  in `TRUSTED_NETWORK_HOSTS` (the deliberate per-host opt-out for an
  intra-namespace, network-isolated CNPG database) or the DSN enforces TLS
  (`sslmode=require`/`verify-ca`/`verify-full`). `svc-notifier` dropped all
  environment-mode logic from `main.rs` accordingly. br-core-auth 0.6.x also
  tightens `Passport` deserialization (strict serde); the valid wire format is
  byte-identical, so no behavior change for well-formed passports.

### Added
- **Chart: `postgres.trustedNetworkHosts`** — a list of DB hosts allowed over
  plaintext, rendered as the `TRUSTED_NETWORK_HOSTS` env var when non-empty
  (default empty = TLS required for any remote host). This is what lets a K3s
  deployment boot against intra-namespace CNPG over plaintext under the 0.7.0
  lib.
- **Chart: `extraEnv`** — a generic escape hatch for extra container env entries
  rendered verbatim.

### Fixed
- **Chart comment corrected to match the code** (doc-must-match-code). The
  `values.yaml` Postgres-roles comment claimed the owner role "backs the
  LISTEN/NOTIFY listener, which re-reads committed rows across recipients" —
  the pre-review behavior. In reality the listener runs on the
  `svc_notifier_app` pool and re-reads each signalled row under the recipient's
  own RLS scope; the owner role is used for migrations + grants at boot only and
  never at runtime.

## 0.3.0

The from-scratch rebuild: the service is reimplemented against the README
contract and the e2e scenario suite, which now passes green against real
Postgres and real NATS JetStream. Built as a single crate — capability files
(`notification`, `intake`, `graphql`, `realtime`) with types, SQL, resolvers
and IO inline.

### Fixed (code review)
- **Realtime listener now reads under RLS, never via a privileged role.** The
  listener's row re-reads ran on the migrations owner pool, which under `FORCE`
  row-level security has no applicable policy — it returned zero rows on any
  non-superuser owner (i.e. CNPG production), silently dropping every `Added`
  push. The listener now runs on the `svc_notifier_app` pool and scopes each
  re-read to the signal's recipient, obeying the same policy as a user-facing
  read; it works on an instance running without NATS (no ingest role available).
- **`Read` signal carries `read_at`.** The read fact now ships the exact
  `read_at` the write committed, eliminating a second listener re-read and the
  fabricated `Utc::now()` fallback that could push a wrong timestamp.
- **Durable consumer creation is reconciled fail-loud.** Intake switched from
  `get_or_create_consumer` (which silently tolerates a divergent delivery config)
  to `create_consumer_strict`: the consumer is created with the exact config or
  startup aborts if one exists with a different config. The remaining
  deployment-vs-service ownership gap is recorded under README "Infra debt".
- **GraphQL errors speak codes, not language, and never leak sqlx.** Error
  messages are now the stable code itself (the `code` extension is unchanged);
  the database-error path logs server-side and returns only `INTERNAL`, no longer
  interpolating the raw sqlx message (column/constraint names) into the response.
- **RLS context bound to the resolved caller.** `scoped_tx` sets
  `app.current_user_id` from the typed `Recipient` resolved at the entry point
  rather than re-reading the Passport, making "the row touched belongs to the
  authenticated caller" a fact carried by the type.
- Removed the SDL-rendering placeholder pool (the `schema` subcommand builds the
  schema without runtime data) and all source doc-comments (intent lives in
  names, types and this README). Migration drops `IF NOT EXISTS`
  on the objects it owns (table + indexes), keeping `DROP POLICY IF EXISTS`.

### Added
- `br-notifier-contract` 0.1.0 — the service's published language, as a sibling
  workspace crate with its own version, changelog and tag line. Producers
  depend on it instead of hand-rolling the deliver payload. The service now
  consumes it: intake deserializes `DeliverNotification` straight from the
  contract type. See `br-notifier-contract/CHANGELOG.md`.
- **Intake** — a durable JetStream pull consumer (`consumer.messages()`, no
  polling) bound to the deployment-provisioned `NOTIFY` stream, filtering the
  contract subject `notifier.cmd.notification.deliver.v1` only. One command
  fans out one row per recipient in a single transaction; dedup is
  first-wins on `(source_event_id, recipient_id)` via `ON CONFLICT DO NOTHING`.
  A malformed message — including a command whose `link` the contract rejects
  fail-closed — is acked with an error log and never persisted. A database
  failure NAKs for redelivery (`max_deliver` 5); the budget's final slot is
  terminated without a write attempt, so an exhausted command is cleanly
  dropped and no late write lands after recovery.
- **GraphQL surface** — `notifier`-prefixed root fields: `notifierNotifications`
  (newest-first pagination), `notifierUnreadCount`, ack-only mutations
  (`notifierMarkAsRead`, `notifierMarkAllAsRead`, `notifierDeleteNotification`,
  `notifierDeleteNotifications`), and the `notifierNotificationEvents`
  subscription (the `NotifierNotificationEvent` union: `NotificationAdded`,
  `NotificationsRead`, `NotificationsDeleted`). Mutations return verdicts, never
  state; `NOT_FOUND`/`FORBIDDEN`/`BAD_REQUEST` carry a stable `code` extension.
  Served over SSE on `POST /graphql` with `Accept: text/event-stream`.
- **Realtime via PG `LISTEN/NOTIFY`** — every write emits `pg_notify` in the
  same transaction as the state it announces; a per-instance listener re-reads
  committed rows and routes typed events to that recipient's in-process
  subscriptions. Correctness is replica-count-independent (proven by the
  two-instance scenario).
- **Authorization** — Passport middleware (401 on missing/malformed header);
  resolvers open a transaction-local RLS context (`br_util_postgres`) so a
  recipient only ever sees or touches their own rows; `FORCE`d RLS with two
  least-privilege roles (`svc_notifier_app` user-scoped, `svc_notifier_ingest`
  insert + RETURNING). Service passports are refused (never a recipient).
- **Migrations** run at startup under the owner role, which then closes; the
  migration grants the two runtime roles their least-privilege access.
- `deny.toml` + cargo-deny, cargo-machete, cargo-semver-checks (contract
  crate), per-crate changelog check, shellcheck and trufflehog jobs in CI,
  aligned with the platform CI standard.
- `scripts/setup-branch-protection.sh` — declarative required-checks
  management for `main`; the e2e job is a required check.
- The e2e suite is rewritten as named behavior scenarios
  (`tests/scenarios_*.rs`), each pinning the three external envelopes (NATS
  ack/NAK/redelivery + consumer state, exact PG rows via a dedicated assertion
  connection, the GraphQL view of a forged Passport). Coverage: dedup
  first-wins as contract, fail-closed link rejection, legacy-subject
  retirement, DB-outage NAK/recovery/exhaustion, partial-redelivery
  idempotence, cross-session read/delete propagation, single bulk event for
  `markAllAsRead`, bulk delete RLS semantics, reconnect
  (subscribe-then-snapshot), and a two-instance scenario proving pushes derive
  from committed PG state.

### Changed
- Repository converted to a two-crate Cargo workspace (`svc-notifier` +
  `br-notifier-contract`); root-level cargo commands cover both via
  `default-members`.
- sqlx uses the `tls-rustls` backend, dropping the `rsa`/`native-tls` chain.
- Chart `br-svc-notifier`: declares `strategy.type: Recreate`, ships default
  `node.kubernetes.io/unreachable` and `node.kubernetes.io/not-ready`
  tolerations (NoExecute, 30s) for a fast reschedule, and keeps `replicaCount`
  a knob defaulting to 1. `appVersion`/`version` bumped to 0.3.0.
- README rewritten as the service's contract, with every `[target]` marker and
  the spec-status banner removed now that the implementation matches: SSE on
  `POST /graphql` (no WebSocket route), `DATABASE_URL_INGEST` documented, fixed
  role names, the `link` field, the subscription event union, bulk delete and
  `LISTEN/NOTIFY` realtime are all live.
- CI triggers on `pull_request` only (plus `workflow_dispatch`), with a
  `cargo fmt` auto-fix gate fronting every Rust job.
- CD is restructured image-first/tag-after: `detect-bump` (per crate) →
  publish image + chart → create `{crate}/v{version}` tag + GitHub Release.
  The contract crate is released as a tag only.
- Direct-SQL test seeding is removed: every scenario seeds through the real
  NATS intake.
- `scripts/lib/*.sh` pass shellcheck (`cd` failure guards, exported
  `CRATE_NAME`).

### Removed
- `docs/domain.md` — its staged refactor plan is superseded; the notification
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
- `scripts/publish.sh` + `scripts/lib/*.sh` — local/CI publish pipeline.
  Supports `--dry-run`, `--local-image`, `--check-only`, `--skip-checks`.
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
