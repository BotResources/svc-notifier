# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 1.0.3

### Fixed

- **The realtime listener is supervised like the intake.** Only the intake had a
  supervisor: the listener — the other half of "everything is live" — ran on a
  detached task where a panic died silently, leaving the process answering
  `/readyz` 200 with nobody feeding the subscribers. It holds reachable panics of
  its own (four mutex `expect`s on the subscriber registry). One `supervise`
  now covers both tasks: panic or cancellation takes readiness DOWN and exits
  non-zero.
- **`template` is validated instead of abandoned.** A template no renderer could
  key on — empty, whitespace-only, or carrying a control character — used to reach
  the `INSERT`. The NUL case failed there on a SQLSTATE class `22` and was recorded
  as a `storage_rejected` accident; the others were stored as unrenderable
  notifications. Both are now refused up front with a fifth stable reason code,
  `template_rejected`, closing the main poison vector by the front door. The rule
  stays narrow — any non-empty, control-free string is still accepted (dots,
  accents, CJK, surrounding whitespace), because the allowed-template *list* is
  per-project configuration, not a service rule.
- **A replayed `notifierMarkAsRead` no longer re-announces the read.** The
  mutation emitted `NotificationsRead` on every call, including when the
  notification was already read, so a client decrementing its unread badge per
  announcement drifted negative. The transition is now detected in one statement
  and the fact is emitted only when the state actually changed; the replay is
  still acked (idempotent, not an error) and `read_at` never moves.
- **A stale pagination cursor is refused instead of answered with an empty page.**
  Paging from a notification that has since been deleted matched no anchor row and
  returned zero nodes — which reads to a client as "you have reached the end",
  silently hiding everything after it. It now returns `NOT_FOUND`; the recovery is
  to restart from the first page.

### Changed

- The unbounded-hold bar in `s07c` sits at three times the retired five-delivery
  budget instead of just above it. At six redeliveries it coincided with the old
  ceiling, so a *different* bounded budget reintroduced at, say, ten would have
  satisfied it and the scenario would have gone green over the regression it
  exists to catch.
- The poison vehicle in `s20`/`s21` moved from an unstorable `template` to an
  unstorable `payload` (a NUL inside the JSON, which `jsonb` refuses with SQLSTATE
  22P05). Those scenarios are about a write **PostgreSQL** refuses; with the
  template now refused up front by the service, the old vehicle no longer reached
  the `INSERT` at all and would have quietly re-pointed them at the new refusal
  path.
- The undecodable-envelope ceiling is documented accurately. The previous note
  claimed held frames fill the consumer's `max_ack_pending` (256) and freeze the
  intake. They do not: `nak` is an explicit settlement, so a held frame is not
  delivered-but-unsettled, and this loop never holds more than one unsettled
  delivery. The real bound is arithmetic — N stuck envelopes cost N redeliveries
  per second of loop turns and log volume, taxing throughput — and
  `notifier_intake_undecodable_deliveries_total` rising against a flat
  `..._dead_letters_total` is the condition to alert on. No behaviour changed; the
  runbook did, because the old number would have sent an operator after the wrong
  thing.

- **A storage outage can no longer drop a delivery command.** The intake used to
  terminate a frame once `delivered_count` passed a fixed budget of five, so a
  Postgres outage lasting past the fifth redelivery silently destroyed the
  request: no notification, no debt recorded, and the producer's ack already
  consumed. The budget is retired. The fan-out failure now carries a **failure
  class** instead of a bare boolean: a **transient** failure (unreachable storage,
  pool timeout, connection loss, any SQLSTATE a retry could clear) is NAKed for
  redelivery *forever*, so JetStream holds the frame until storage returns; only a
  **poison** frame — a refused payload, or a permanently invalid write (SQLSTATE
  class `22` data exception / `23` integrity constraint violation) — is terminated,
  and only after a `dead_letters` row is committed,
  because retrying it can never succeed. An envelope that cannot be decoded at all
  is *never* terminated: it is NAKed and held forever (see below). Classification
  is conservative: anything
  unrecognised is transient. A sustained outage is visible to operators through
  **metrics, not readiness**: `notifier_intake_consecutive_transient_failures`
  (gauge, reset by the first completed write) plus
  `notifier_intake_transient_failures_total`. Readiness deliberately stays UP
  during a storage outage — taking it DOWN removes the pod from the Service
  endpoints and severs the queries and subscriptions that are still healthy, and
  since every replica shares one PostgreSQL they would all drop together, turning
  a write-side outage into a total one. The never-lose guarantee is bounded by the
  `INTEGRATION_CMD` retention declared deployment-side (7 days / 512 MiB in
  production today) — an outage approaching that is an operator incident the
  gauge raises long in advance, provided it is alerted on.
- **No command is terminated without a committed ledger row — no exception.**
  Terminating destroyed the request — for *every* recipient of the fan-out at
  once — with nothing but a log line. A new `dead_letters` table records the
  abandonment `reason`, the `source_event_id`, the full `recipient_ids` list, the
  command payload and the SQLSTATE **before** the frame is terminated; if the
  ledger write itself fails,
  the command is NAKed rather than terminated, so the intake never drops a request
  it cannot account for. What is stored is the payload's **JSON value,
  re-serialized faithfully** — the same data, with object keys in serde's order and
  numbers normalized, not the producer's original byte sequence. The column is
  `BYTEA` as a defensive choice, not a necessity: `serde_json` output is valid
  UTF-8 without NUL, so `TEXT`/`JSONB` would accept it; `BYTEA` keeps the ledger
  writable whatever a future encoding change produces, and costs nothing since
  nothing queries inside the stored command. The table is
  `FORCE`-RLS with a single ingest-write policy and carries **no** grant to
  `svc_notifier_app` — abandoned commands are operator data, read with the owner
  role (see the runbook note in the README for what each role actually sees). The
  producer's `correlation_id` / `causation_id` are kept as UUID columns so an
  abandonment stays joinable to the trace that caused it, its `actor_kind` /
  `actor_id` name **who** issued the refused command (migration `0004`), and the
  ledger is
  deduplicated on the envelope's **`command_id`** (unique index + `ON CONFLICT DO
  NOTHING`) so a `term()` the broker never registered cannot inflate the audit.
  The key is `command_id`, not `source_event_id`, on purpose: the business dedup
  key is `(source_event_id, recipient_id)`, so one source event may legitimately
  arrive as several deliver commands (chunked recipients, a late addition). Keyed
  on the source event, the first poison chunk would be recorded and every later
  one terminated with **no trace at all** — the same loss class this release
  exists to close, moved onto the poison path. **Retention is deliberately not
  decided** — no purge ships with this change; it is an operator call recorded as
  an open question in the README.
- **A refused payload is traced, not silently destroyed.** The intake used to
  terminate any frame the contract refused to deserialize on a log line alone —
  no ledger row, not even the ids in the log. Two real triggers made that a
  silent-loss hole: an out-of-domain `link` (the spec's first-class business
  refusal, enforced at deserialization by `RelativeLink`) and any contract skew
  that made *all* of a producer's frames undecodable. The consumer now
  deserializes the `IntegrationCommand` envelope with a **raw JSON payload** and
  validates that payload strictly in the service, so the `command_id`,
  `correlation_id`, the issuing actor, the ids it can read and the payload's JSON
  value are recovered
  **before** the frame is judged. A refused payload — unsafe `link`
  (`relative_link_rejected`), a shape the contract does not describe
  (`payload_shape_rejected`), or **no recipient at all** (`no_recipients`, until
  now acked in silence as a no-op) — is recorded on the ledger and terminated
  only after that row commits, exactly like a SQLSTATE 22/23 poison, with the same
  degrade-to-transient when the ledger is unwritable. The published contract is
  unchanged: producers using `br-notifier-publisher` still cannot construct an
  unsafe link — only the receiver became decode-tolerant, in order to trace what it
  refuses. The one residual class, an envelope that is not valid JSON at all (a
  typed publisher cannot emit one), is **NAKed and held forever, never
  terminated**: it carries no id to key a ledger row on, and a frame that cannot
  be accounted for is not destroyed. Every one of its redeliveries is counted by
  `notifier_intake_undecodable_deliveries_total` (a per-delivery counter, hence
  the `deliveries` in the name: one stuck frame makes it climb forever, which is
  precisely the alert).
- **A large fan-out no longer races its own `ack_wait`.** The intake inserted one
  row and emitted one `pg_notify` per recipient, serialized inside a single
  transaction on a one-at-a-time consume loop; at ~1000 recipients that work
  approached the consumer's 30s `ack_wait` and invited a redelivery *during* the
  write. The fan-out is now a single set-based
  `INSERT ... SELECT ... FROM unnest($1::uuid[], $3::uuid[]) AS fan_out(id,
  recipient_id) ON CONFLICT DO NOTHING RETURNING` — the two arrays are zipped by
  one `unnest`, so a length mismatch fails the statement instead of NULL-padding a
  row,
  and the `Added` announcements ride batched `pg_notify` statements (150 payloads
  per statement) in the same transaction. Dedup semantics are unchanged: ids stay
  client-minted UUIDv7, first write wins, a replay alters nothing.
- **A lagging subscriber is disconnected on a verdict it can read, instead of
  silently truncated.** The subscription discarded `BroadcastStream`'s `Lagged(n)`,
  so a client whose buffer
  overflowed kept folding a stream that had lost facts and diverged from the
  server forever — against the spec's "a reconnecting client rebuilds state
  without loss". The stream now emits a **terminal error** and ends on the first
  lag: `INVALID_STATE` with the stable reason code **`subscription_lagged`** and a
  `lost_events` param carrying the number of facts skipped (which used to be
  discarded outright, and is now logged too). Ending the stream silently was not
  enough — an Apollo client cannot tell a normal completion from a truncation, and
  would sit on a stale cache instead of resnapshotting. `INVALID_STATE` is the
  closest code in the closed `br-util-graphql` `ErrorCode` set: the session's state
  is no longer usable, which is neither a client input error (`BAD_USER_INPUT`) nor
  a service fault (`INTERNAL`); the reason code is what clients key on. The
  contract for the client is: on `subscription_lagged`, reconnect and re-run the
  snapshot protocol. A `Passport::Service` on the
  subscription is now a `FORBIDDEN` verdict too, like every query and mutation,
  instead of an empty stream that completes immediately.
- **The listener no longer re-reads a row nobody is watching.** Every `Added`
  signal made the single PG-listener loop open a transaction, set the RLS context,
  `SELECT` the row and commit — four round trips — *before* discovering that the
  recipient has no open subscription, in which case the event was dropped. On a
  large fan-out to mostly-offline recipients that serialized work moved the
  bottleneck straight from the write path onto the realtime path. The dispatch now
  asks `Subscribers::active` first and skips the re-read entirely when nobody is
  connected. The same call **evicts** the recipient's broadcast channel when its
  last receiver is gone: the registry used to keep one channel per recipient that
  ever subscribed, for the life of the process (principle 20 — cleanup when idle).
- **Bulk mutations no longer break above ~200 notifications.** `pg_notify` caps
  its payload at 8000 bytes and errors above it — inside the write transaction,
  that error aborted the **whole mutation**, so `notifierMarkAllAsRead` /
  `notifierDeleteNotifications` failed outright on a large inbox. Bulk `Read` /
  `Deleted` facts are now emitted in chunks of 150 ids **within the same
  transaction**: still atomic with the write, one announcement per chunk instead
  of one per row. A client folding by id is unaffected.
- **The application filters by recipient, not only RLS.** `mark_as_read`,
  `mark_all_as_read`, `delete_notifications` and the listener's row re-read
  received the caller's `recipient_id` but used it only to address the broadcast,
  leaving row-level security as the single barrier. They now carry an explicit
  `recipient_id = $caller` predicate as well (principle 15: enforce at both
  layers), so a loosened policy or a query issued outside a scoped transaction
  cannot silently become a cross-recipient write. The two **reads** —
  `notifierNotifications` (including its pagination-cursor lookup) and
  `notifierUnreadCount` — now carry it too: they were the surfaces the 1.0
  isolation breach leaked through and were the last statements relying on RLS
  alone.
- **The intake task is supervised.** It ran on a detached tokio task: a panic or
  cancellation killed the consumer while the process kept answering `/readyz` 200
  and delivery commands piled up unconsumed — precisely the "healthy probe over a
  dead intake" the boot-time stream gate exists to prevent. `supervise_intake`
  now awaits the task and fails loud (readiness DOWN + non-zero exit) if it ever
  dies.
- **Impersonation no longer exposes the impersonated user's notifications.** An
  administrator impersonating a user saw *that user's* notifications in the list,
  the unread count and the live stream, because the recipient was read from the
  passport's `user_id` and the impersonator field was ignored. Recipient
  resolution is now centralised in one function that prefers `impersonator_id()`
  when present — the acting human always sees and acts on their own notifications
  — and both the GraphQL scoping and the RLS session variable
  (`app.current_user_id`) are fed from that single resolution, so the application
  layer and the database layer can never diverge. Mark-as-read and delete follow:
  reaching for an impersonated user's notification is a `NOT_FOUND`, never a
  silent cross-user write.

### Added

- **Dead-letter retention — 90 days, purged by the service** (operator decision).
  The ledger is a diagnostic surface, not an archive: each row carries the
  producer's command in full, so arbitrary — possibly personal — producer data
  accumulated forever. A daily task deletes every row past
  `DEAD_LETTER_RETENTION_DAYS` (90) and logs how many it removed, running through
  the **ingest** role, never the owner; migration `0002` grants that role its
  `DELETE`. A failed pass is logged as an error and retried at the next tick, and
  deliberately does **not** take readiness DOWN — housekeeping stopping is not the
  process failing to serve — while the task itself is watched, so its death is
  loud. `notifier_intake_dead_letters_total` is the alertable counter: it counts
  committed ledger rows only, so a replayed frame hitting the `ON CONFLICT` no-op
  never inflates it.
- e2e `scenarios_intake::s32` — the retention edge, asserted a day either side of
  the 90-day window, plus the failure posture: with the `DELETE` grant revoked the
  pass says so in the logs, `/readyz` stays UP and the read surface keeps serving.
- e2e `scenarios_refusal::s31` — an unusable `template` (empty, NUL) is on the
  ledger under `template_rejected`, with no SQLSTATE, nothing stored and nothing
  announced. The storage-refusal series stays flat: the refusal is the service's,
  not the database's.
- The suite's silence proofs are now proofs. Nine `expect_silence` assertions rode
  sessions never shown to be live — a stream the service never registered is
  silent for the wrong reason, and those assertions would have held with the
  isolation they defend removed. Each one now proves its session first, the way
  s25 already did: where the scenario already sends the watched recipient a
  notification, that push is the proof (the seed simply moves after the
  subscribe); everywhere else a `subscribe_live` warm-up publishes one real
  delivery, reads it off the stream, deletes it through the recipient's own
  mutation and reads that too — handing the inbox back exactly as it found it, so
  the scenarios that count that very inbox can use it.
- e2e `scenarios_surface::s26` — the list is newest-first, asserted as a sequence
  and across a cursor walk. Every other scenario sorts before comparing, so
  inverting the `ORDER BY` stayed green everywhere; this is the only place the
  spec's ordering clause is pinned.
- e2e `scenarios_surface::s27` — marking read twice: one fact, one `read_at`, an
  acked replay, and never a return to unread.
- e2e `scenarios_surface::s28` — a cursor whose notification was deleted is
  refused by code, and restarting from the top shows what it would have hidden.
- e2e `scenarios_surface::s29` — row-level security proven **alone**: reads and a
  blanket `UPDATE` issued as the RLS-subject app role with **no** recipient
  predicate at all, with no context, the wrong identity, and the right one. The
  application predicates added earlier could otherwise mask a broken policy.
- e2e `scenarios_surface::s30` — the published edge as a closed world: the exact
  set of query, mutation and subscription root fields, no create-shaped verb
  anywhere, every field BC-prefixed. It proves by absence that a client can never
  make a notification exist.
- e2e `scenarios_impersonation::s19b` — both bulk mutations under impersonation.
- Migration `0002_dead_letters.sql` — the abandonment ledger for terminated
  delivery commands (RLS forced, ingest-write policy only, unique on
  `command_id`).
- Migration `0003_dead_letter_reason.sql` — the ledger's stable `reason` code
  (indexed), and a nullable `source_event_id` for the refused payloads that carry
  no readable one.
- Migration `0004_dead_letter_actor.sql` — the envelope's `actor_kind` /
  `actor_id` on the ledger (indexed on the id), so an abandonment names **who**
  issued the command and not only which trace it belonged to. Additive and
  nullable: table-level `INSERT`/`SELECT` grants to `svc_notifier_ingest` cover
  new columns, and the RLS posture is untouched.
- Prometheus metrics on the intake, the replacement for the readiness escalation:
  `notifier_intake_dead_letters_total{reason}`,
  `notifier_intake_transient_failures_total`,
  `notifier_intake_consecutive_transient_failures` (gauge, initialised to `0` at
  bind so the series exists before the first outage),
  `notifier_intake_undecodable_deliveries_total` and
  `notifier_intake_ledger_failures_total` (a failed ledger write — a revoked grant,
  say — must not read as a Postgres outage on the transient counter), on the
  existing `br-util-observability` `/metrics` wiring. Every series carries a HELP
  description (`describe_counter!` / `describe_gauge!`).
- e2e `scenarios_outage::s07c` — an outage held past the retired five-delivery
  budget, gated on **JetStream's own** `delivered_count` rather than on counting
  service log lines: the frame keeps being NAKed and, when Postgres returns, the
  notification is delivered exactly once, a subscriber that opened *before* the
  outage receives the `NotificationAdded`, and no dead letter is written.
- e2e `scenarios_intake::s20` — two distinct poison commands sharing one
  `source_event_id` (the chunked-recipients shape) leave **two** audit lines, each
  naming its own recipients; the stored command round-trips to the same JSON value;
  replaying the same envelope adds no line.
- e2e `scenarios_intake::s21` — with the ledger unwritable (the ingest role's
  INSERT grant revoked), a poison command is NAKed and held, never terminated,
  and is traced and terminated only once the ledger comes back.
- e2e `scenarios_refusal` — the executable proof of the refusal ledger:
  `s22` an out-of-domain `link` refuses the whole request and leaves a trace,
  `s23` a payload the contract does not describe is recorded with whatever ids are
  readable, `s24` a command naming nobody is a producer bug on the ledger and not
  a silent no-op, and `s21b` a refused payload is held — never terminated — while
  the ledger is unwritable.
- e2e `scenarios_impersonation` (s17–s19b) — list, unread count, subscription
  stream, mark-as-read, delete, and **both bulk mutations** (`markAllAsRead`,
  `deleteNotifications` — the only ones with no id to target) under an
  impersonated passport.
- Unit tests on the new intake failure classification (transient never
  terminates; only SQLSTATE `22`/`23` is poison; an unreachable database is
  transient), on the permissive decode and its refusals (a valid command survives
  it unchanged; an unsafe link, an undescribed shape and a recipient-less command
  each map to their stable ledger reason; a refused frame still yields the ids the
  ledger needs; an unreadable envelope is held, never terminated), on the
  ledger's actor-kind labels, on the
  transient-failure streak, on `graphql::resolve_recipient`, on the
  `subscription_lagged` verdict a truncated stream ends with, on the subscriber
  registry (an unwatched recipient is inactive, the last stream closing evicts its
  channel), and on the
  `pg_notify` chunk payload bound.

## 1.0.2

### Changed

- Dependency-only patch: `br-rust-common` pins v1.1.0 → **v1.2.0** (picks up the
  NoResponders-recoverable consumer recovery and the supervised
  `PublishedLanguageConsumer::run()`; `WatchHealth` now starts `Degraded` and is
  written only by the supervised loop) and `br-test-harness` v1.1.0 → **v1.1.2**.
  No svc-notifier surface change. Part of the v1.2.0 consumer wave unblocking
  be-botresources#242.

## 1.0.1

A dependency patch: bump the shared library and harness to the unified
`v1.1.0`. The service's wire is unchanged — the NATS subjects, the
`IntegrationCommand` envelope, the GraphQL surface and the `LISTEN/NOTIFY`
realtime path are byte-for-byte identical; the pins are internal to the binary.

### Changed
- **Bump `br-rust-common` `v1.0.2` → `v1.1.0`** (and `br-test-harness` →
  `v1.1.0`), with the matching `version = "1.1.0"` next to each tag. The
  `v1.1.0` delta is additive: it hardens the `br-util-nats-fabric` run-loop with
  automatic recovery from transient consumer errors (rebind + capped backoff; a
  missed consumer heartbeat now retries indefinitely instead of surfacing an
  error, and a non-transient run-loop error is budgeted before it fails loud).
  The intake consumer (`intake::consume`, over the Fabric `CommandConsumer`)
  inherits this directly — a transient missed heartbeat no longer surfaces from
  `recv()`, so it no longer counts against the service's consecutive-recv-error
  budget and no longer risks tearing the intake down, which was the known
  abnormal-termination mode. There is no service code change: the poison budget
  (`max_deliver` 5 + `term()`), the `ack_wait` grace and the consecutive-recv-
  error budget are all unchanged.
- Chart `version`/`appVersion` bumped to 1.0.1 in lockstep with the crate (no
  template change).

### Security
- **`crossbeam-epoch` `0.9.18` → `0.9.20`** (`cargo update`) to clear
  RUSTSEC-2026-0204. Transitive dependency; no source or behavior change.

## 1.0.0

**BREAKING — the external NATS envelope changed.** The deliver command is now
consumed from the fixed `INTEGRATION_CMD` stream at the coordinate
`integration.cmd.notifier.notification.deliver.v1`, carried in the standard
`IntegrationCommand` envelope, replacing the previous bespoke subject/stream.
Producers must publish through `br-notifier-publisher` (or the equivalent Fabric
coordinates); the old wire no longer reaches the service. This break to the
service's public contract is the reason for the major version bump.

### Fixed
- **Fail-loud on a missing `INTEGRATION_CMD` stream now gates readiness.** The
  intake consumer is bound in `main` (`intake::bind`) **before** `/readyz` is set
  ready; on bind failure (e.g. the fixed stream is absent) the process exits
  non-zero and never serves readiness, instead of the previous detached
  log-and-die task that left `/readyz` returning 200 over a dead intake
  (Security Invariant #6). Proven service-level by `s15` (a real svc-notifier
  spawned against a broker missing the stream stays not-ready / exits non-zero).
- **Undecodable commands are terminated, not acked.** A frame whose payload fails
  to decode (invalid JSON, or a contract-rejected unsafe `link`) now resolves to
  `term` (poison handling, Security Invariant #6) instead of `ack` — acking a
  poison message falsely signals successful processing and drops the poison
  signal. Unit-tested in `intake::tests`.
- **Abnormal intake termination after boot now fails loud.** If the intake
  `recv()` loop ends unexpectedly while the service is live — the stream/durable
  vanishing (`Ok(None)`), or `recv()` erroring past a consecutive-error budget —
  the loop logs `tracing::error!`, flips `/readyz` to not-ready, and triggers
  process shutdown for a non-zero exit so K8s reschedules. Previously the spawned
  task simply ended while `/readyz` kept returning 200 over a dead intake (the
  runtime counterpart of the boot-time Security Invariant #6 fix). The intentional
  shutdown path (SIGTERM/ctrl-c) stays silent and exits zero as before.

### Changed
- **`IntakeError::Consumer` / `PublishError::Publish` carry the typed
  `FabricError`** (`#[from]`) instead of a stringified message, preserving the
  error kind across the boundary.
- **The deliver `command_type` derives from the contract.**
  `br-notifier-contract` now exposes `deliver_command_type()`
  (`{aggregate}.{verb}`); `br-notifier-publisher` builds the envelope from it
  (and `DELIVER_VERSION`) so the coordinates and the envelope cannot drift.
- **README corrected to match the code.** The RLS paragraph no longer claims a
  removed `br_util_postgres::set_rls_context` helper setting five `app.*` GUCs —
  both the GraphQL `scoped_tx` and the listener set the single
  `app.current_user_id` GUC inline (the lib removed the helper; the GUC shape is a
  per-project seam). The intake-semantics and Fabric sections describe the
  undecodable → `term` contract and the readiness-gated bind. The Tests section
  records that the malformed-frame / invalid-`link` decisions (`s04`/`s05`) are
  proven by unit tests + the contract's deserialization tests; a live-intake e2e
  of a deliberately-corrupt frame is intentionally not added (it would require a
  raw-publish foot-gun the operator ruled out; a compliant `br-notifier-publisher`
  producer cannot emit such a frame by construction).
- **All NATS access now goes through `br-util-nats-fabric` — no direct `async_nats`
  anywhere in the repo (production or tests).** The intake binds a Fabric
  create-or-bind durable consumer on the deliver coordinates (rendered to
  `integration.cmd.notifier.notification.deliver.v1` on the fixed `INTEGRATION_CMD`
  stream), replacing the hand-rolled `async_nats` connect + `create_consumer_strict`
  + `consumer.messages()` loop. The connection is `Fabric::connect` /
  `connect_with`. The deliver command travels as the standard `IntegrationCommand`
  envelope (its `payload` is the unchanged `DeliverNotification`).
- **`br-rust-common` git pins bumped `v0.11.0` → `v1.0.2`** (and `br-test-harness`
  → `v1.0.2`), with the matching `version = "1.0.2"` next to each tag.
- **`scoped_tx` sets the RLS context inline.** `br_util_postgres::set_rls_context`
  was removed in the v1.0.x lib; the GraphQL read path now sets
  `app.current_user_id` transaction-local from the recipient (Service passports
  are rejected from the recipient surface as before). Behavior is unchanged for
  human recipients.
- **Graceful shutdown.** The intake loop runs under a `tokio::select!` over a
  shutdown watch channel and `drain()`s the consumer on SIGTERM/ctrl-c, leaving
  un-acked frames un-acked (at-least-once preserved). `axum::serve` uses
  `with_graceful_shutdown`.
- **An empty `NATS_URL` is treated as unset** (no intake), matching the k8s
  empty-env idiom.
- Repository is now a three-crate workspace; `br-notifier-contract` moved under
  `crates/`, joined by the new `br-notifier-publisher`.

### Added
- **`br-notifier-publisher`** (0.1.0) — the producer kit: a thin `NotifierPublisher`
  over the Fabric that publishes a typed `DeliverNotification`. The e2e harness
  publishes test commands through it (never raw `async_nats`).

### Removed
- The `async-nats` dependency (production and dev). `grep async_nats` over the repo
  returns nothing.

## 0.6.0

### Fixed
- **GraphQL error code `BAD_REQUEST` → `BAD_USER_INPUT`.** A malformed `ID`
  argument (an unparseable notification id) returned the `code` extension
  `BAD_REQUEST`, which is not in the published `ErrorCode` contract the gateway
  and frontend bind to. It now returns `BAD_USER_INPUT`. `FORBIDDEN`,
  `NOT_FOUND` and the internal/database mapping are unchanged, and the error
  *shape* (`extensions: { code }`) is identical — only the one wrong code is
  corrected.

### Changed
- **Adopt `br-util-graphql` for edge errors.** The hand-rolled `coded` /
  `db_error` helpers are replaced by `br_util_graphql::EdgeError`; resolvers
  return `Result<_, EdgeError>` and rely on the crate's
  `From<EdgeError> for async_graphql::Error`. Internal/database failures map to
  `EdgeError::internal` (detail logged, never returned to the client).
- **Missing-context error codes.** Paths where `Passport` or `AppState` is
  absent (server misconfiguration only) now emit `code: INTERNAL` instead of
  async-graphql's code-less default — a strict improvement, surfaced here for
  honesty.
- **Bump `br-rust-common` to `v0.11.0`.** All five prod deps and the
  `br-core-auth` dev-dep move from `tag = "v0.10.0"` to `tag = "v0.11.0"` with
  the matching `version = "0.11.0"`; adds `br-util-graphql` (`graphql` feature)
  at the same pin. The full e2e suite passes against real Postgres + NATS.

### Removed
- **Doc-comments stripped from `br-notifier-contract`.** The `//!` / `///`
  rustdoc on `lib.rs` is removed (house no-comments rule); the surviving intent
  already lives in `br-notifier-contract/README.md`.

## 0.5.2

A dependency-and-metadata patch: bump the shared library and pick up the
Apache-2.0 relicense. No code, contract, or behavior change.

### Changed
- **Bump `br-rust-common` to `v0.10.0`.** All five deps (`br-core-auth`,
  `br-util-axum-auth`, `br-util-axum-readiness`, `br-util-observability`,
  `br-util-postgres`) and the `br-core-auth` dev-dep move from `tag = "v0.8.0"`
  to `tag = "v0.10.0"`, with the matching `version = "0.10.0"` kept next to each
  `tag` (a tag-only pin reads as a wildcard `*` and fails `wildcards = "deny"`).
  The `v0.8.0 → v0.10.0` delta is additive for the consumed crates — the only
  source changes in the range touched `br-core-integration`,
  `br-util-scope-declaration` and `br-util-graphql`, none of which svc-notifier
  consumes — so no API breakage and no runtime change. The full e2e suite passes
  against real Postgres + NATS.
- Relicensed from MIT to Apache-2.0.
- Chart `version`/`appVersion` bumped to 0.5.2 in lockstep with the crate (no
  template change).

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
