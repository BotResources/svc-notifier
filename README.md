# svc-notifier

> [!IMPORTANT]
> **This repository is maintained for BotResources and its authorized clients.**
> It is published under MIT and made available read-only for visibility. The MIT
> license governs your rights to use, modify, and fork the code; the rest of this
> notice describes our operational stance, not a legal restriction.
>
> **We do not accept external pull requests, issues, or support requests.**
> Issues and Discussions are disabled. PRs from accounts that are not on the
> internal contributor allowlist will be closed without review. Forks are
> permitted by MIT and we do not (and cannot) prevent them; we simply do not
> monitor, support, or accept contributions from forks outside the BR commercial
> relationship.
>
> - Clients with a commercial relationship: contact your BR account manager.
> - Security reports: see [SECURITY.md](SECURITY.md) (private email channel).
> - This is not a community-supported project. No support is provided through
>   GitHub.

Standalone notification service. Producers publish a typed deliver command on NATS
JetStream; svc-notifier persists one notification per recipient in PostgreSQL under
row-level security and serves recipients through a GraphQL subgraph — list and unread
count, ack-only mutations, and a real-time subscription stream.

## Architecture

```
producers ──notifier.cmd.notification.deliver.v1──▶ NATS JetStream
                                                        │ durable pull consumer
                                                        ▼
                                                  PostgreSQL (RLS)  ◀── the single
                                                        │               source of truth
                                                   pg_notify on commit
                                                        ▼
recipients ◀──GraphQL: queries / mutations / subscription stream──┘
```

- **Intake (NATS JetStream)** — a durable pull consumer (`consumer.messages()`, no
  polling) fans each deliver command out to one row per recipient. It consumes
  `notifier.cmd.notification.deliver.v1` only; nothing listens on the legacy
  `notify.deliver` subject. The `NOTIFY` stream is treated as
  deployment-provisioned infrastructure — the service binds to it and never
  creates it. The **durable consumer is declared by the service at boot**, with
  its delivery contract (`filter_subject`, `max_deliver`, `ack_wait`, explicit
  ack) reconciled fail-loud: a consumer already present with a *different* config
  aborts startup rather than running on a silently divergent contract (see "Infra
  debt" below).
- **Truth (PostgreSQL)** — notifications live in one table, deduplicated by
  `(source_event_id, recipient_id)`, protected by forced row-level security.
- **Surface (GraphQL subgraph)** — recipient-facing, composed behind a gateway.
  Root fields are prefixed `notifier*`. When a gateway composer discovers
  subgraphs by enumerating Kubernetes Services by label, set the discovery
  labels via the chart key `service.labels` (a generic map merged onto the
  Service's `metadata.labels`); the chart never hard-codes any specific
  selector.
- **Realtime** — every push derives from committed PostgreSQL state via
  `LISTEN/NOTIFY` (see "Realtime architecture"); no in-process broadcast from the
  writer.

## The published language — `br-notifier-contract`

This repository is a two-crate workspace: the `svc-notifier` service and
[`br-notifier-contract`](br-notifier-contract/), the service's published language.
The contract crate is owned by the receiver (this service), versioned and tagged
independently, and is what producers depend on — never on the service crate.

- Subject: `DELIVER_SUBJECT` = `notifier.cmd.notification.deliver.v1` — the only
  subject the service consumes.
- Command: `DeliverNotification { source_event_id, recipient_ids, template, payload,
  link: Option<RelativeLink> }`. Producers serialize it with serde and publish it on
  the subject — no helper needed, and the contract crate stays free of any NATS
  dependency.
- `RelativeLink` is a fail-closed newtype: a same-domain relative URL, i.e. a path
  rooted at `/` (query and fragment allowed). It rejects absolute URLs and schemes
  (`https:`, `javascript:`, …), scheme-relative `//host`, backslashes, whitespace,
  control characters, and empty input — at construction *and* on deserialization.
  Its unit tests are the authoritative accept/reject vectors.

Wire format (pinned by a round-trip test in the contract crate):

```json
{
  "source_event_id": "0196a000-0000-7000-8000-000000000001",
  "recipient_ids": ["0196a000-0000-7000-8000-000000000002"],
  "template": "meeting_scheduled",
  "payload": { "meeting_id": "m-1" },
  "link": "/meetings/m-1"
}
```

`link` is optional and omitted from the wire when absent.

### Intake semantics (the receiver's promises)

- **Fan-out**: one command produces one notification per entry in `recipient_ids`.
- **Dedup, first wins** (contractual): `(source_event_id, recipient_id)` is unique.
  A redelivered or duplicated command — even with a different payload — never creates
  a second row and never updates the first one.
- **Empty `recipient_ids`**: a no-op; the message is acked.
- **Malformed message** (not valid JSON for the command): acked with an error log —
  never NAKed, so a poison message cannot cause a redelivery storm. Nothing is persisted.
- **Invalid `link`**: the whole message is rejected fail-closed — the command fails to
  deserialize (the contract's `RelativeLink` rejects an unsafe link), so it takes the
  malformed-message path: acked with an error log, zero rows persisted (no partial
  fan-out), nothing reaches any recipient.
- **Database failure mid-batch**: the message is NAKed and redelivered (up to the
  consumer's `max_deliver`, currently 5). Redelivery completes the remaining
  recipients; already-inserted recipients are not duplicated (the dedup constraint
  makes the fan-out idempotent). The final delivery the budget allows is the
  give-up slot: it is terminated without a further write attempt, so an exhausted
  command is cleanly dropped — no late write lands after recovery, and the
  documented recovery is the producer re-emitting the same `source_event_id`.

`template` is a routing/rendering key and `payload` is producer data. The service
validates neither against an allowed list or schema today (see "Open questions");
consumers must treat both as untrusted data (see "Security notes").

## GraphQL surface

Every root field is prefixed `notifier`. Queries and mutations require an
authenticated `Passport` (injected by the gateway); all resolver work runs in a
transaction carrying the caller's row-level-security context, so a recipient can
only ever see or touch their own notifications.

### Queries

- `notifierNotifications(first: Int = 20, after: ID): NotificationConnection` —
  newest-first pagination (`nodes`, `hasNextPage`).
- `notifierUnreadCount: Int!`

The notification type carries `id`, `template`, `payload`, `link`, `readAt`,
`createdAt`.

### Mutations — ack-only

Mutations return `Boolean` acknowledgments (or a structured error), never state.
State reaches clients through the subscription stream; the frontend folds events
into its snapshot instead of refetching.

- `notifierMarkAsRead(notificationId: ID!): Boolean!` — idempotent: marking an
  already-read notification acks `true`. An id that does not exist *for the caller*
  (foreign or unknown — RLS makes them indistinguishable) is a `NOT_FOUND` error.
- `notifierMarkAllAsRead: Boolean!`
- `notifierDeleteNotification(notificationId: ID!): Boolean!` — hard delete today
  (hard vs soft is an open question — see "Open questions"; the bulk variant below
  inherits whatever is decided); `NOT_FOUND` under the same rule as above.
- `notifierDeleteNotifications(ids: [ID!]!): Boolean!` — bulk delete.
  Ids not owned by the caller are invisible to it, hence untouched — contractually:
  they are silently skipped, the mutation acks, and they are absent from the emitted
  event. Foreign ids can never be probed through this mutation.

### Subscription

- `notifierNotificationEvents: NotifierNotificationEvent!` — the full notification
  event union, bulk-shaped:

  ```graphql
  union NotifierNotificationEvent =
      NotificationAdded        # { notification: Notification! }
    | NotificationsRead        # { ids: [ID!]!, readAt: DateTime! }
    | NotificationsDeleted     # { ids: [ID!]! }
  ```

  Every state change reaches the stream of every session of the affected recipient:
  a single mark-as-read is a `NotificationsRead` with one id; `markAllAsRead` emits
  exactly one bulk event carrying all affected ids; deletes likewise. The
  subscription payload exposes the same data as the underlying event — a field
  dropped between the event and the subscription is a bug.

Subscriptions are served over SSE: the gateway POSTs the subscription operation to
`/graphql` with `Accept: text/event-stream`. There is no WebSocket endpoint.
Service passports are rejected (notifications are recipient-scoped, and a service
is never a recipient). A subscription without a valid passport currently yields an
empty stream that completes immediately — the limitation (no error payload) is an
async-graphql constraint, logged server-side.

### Reconnect protocol (contract)

The stream has no cursor or replay. To lose nothing across a disconnect:

1. open the subscription **first**,
2. then query the snapshot (`notifierNotifications`, `notifierUnreadCount`),
3. fold events into the snapshot, deduplicating by notification id — an event may
   describe a change the snapshot already contains; applying it twice must be a no-op.

This is not an edge case: every deploy restarts the single service instance
(`Recreate` strategy), so every live subscription drops and reconnects on every
release. Frontends must implement this protocol, not treat reconnection as an error.

## Realtime architecture

PostgreSQL is the single source of truth; the subscription stream is **fed by PG
`LISTEN/NOTIFY`**, never by an in-process broadcast from the writer:

- Every write path (NATS intake, mutations) emits `pg_notify` **in the same
  transaction** as the write. The signal therefore fires on commit only — uncommitted
  or rolled-back state can never be pushed — and carries event type, recipient,
  ids, and the `read_at` timestamp on a read fact (small, far under the NOTIFY
  payload limit). The read fact carries `read_at` directly so the listener never
  re-reads to learn it.
- Each service instance runs a PG listener under the `svc_notifier_app` role
  (the always-present application connection — an instance without NATS still
  feeds its subscribers from PostgreSQL). For an `Added` fact it re-reads the new
  row from PostgreSQL to build the client event (what is pushed is what the truth
  says); the re-read runs in a transaction scoped to the signal's recipient, so it
  obeys the same row-level-security policy as every user-facing read — the listener
  has no privileged, RLS-bypassing read path. The event is then routed to that
  recipient's local subscription connections. This re-read is the only builder of an
  `Added` push, so its recipient-isolation is proven at the envelope by
  `scenarios_intake::s02_multi_recipient_fans_out_one_row_each_and_isolates_subscribers`
  (each recipient's `Added` carries only its own row) and
  `scenarios_surface::s06_rls_isolates_recipients_on_query_count_and_stream`
  (a foreign recipient gets no `Added` push at all).
- The only in-process state is the strictly per-connection registry of open
  subscriptions. Correctness is therefore replica-count-independent: a write handled
  by one instance reaches subscribers connected to any instance.
- The deployment default remains **one replica** (`Recreate` strategy — see the
  chart); scaling out is a measured-need decision, not a correctness requirement.

## Authorization model

- The service does **authorization only, never authentication**. The gateway
  validates credentials, strips any client-supplied `X-Passport`, and injects the
  resolved one; network policy blocks direct external access to the service. The
  passport middleware decodes that header — it is trustworthy *only* behind such a
  gateway, never on an exposed port.
- **A service passport is rejected before any work.** Every query and mutation guards
  on the passport kind first: a `Passport::Service` is a `FORBIDDEN` error returned
  *before* a transaction is opened (notifications are recipient-scoped, and a service
  is never a recipient). Proven by
  `scenarios_authn::service_passport_queries_and_mutations_are_forbidden`.
- **Row-level security as the authorization backstop**: resolvers open a transaction,
  inject the caller's transaction-local RLS context, and the policies restrict every
  select/update/delete to `recipient_id = current user`. RLS is `FORCE`d — even the
  table owner cannot bypass it. The GraphQL path injects the context with the shared
  `br_util_postgres::set_rls_context(tx, passport)`, threading the real `Passport`
  from the auth middleware to the transaction boundary; the helper sets five
  transaction-local `app.*` GUCs and the notifications policy reads only
  `app.current_user_id`. The realtime listener has **no** `Passport` — its recipient
  is synthesized from the `pg_notify` signal — so it keeps a single manual
  `set_config('app.current_user_id', …, true)`; fabricating a fake identity just to
  call the shared helper would be a security smell, not reuse.
- **Two runtime PostgreSQL roles, least privilege**:
  - `svc_notifier_app` — GraphQL resolvers *and* the realtime listener's row
    re-reads; user-scoped by the RLS policies above. The listener scopes its
    re-read to the signal's recipient, so it reads exactly the rows that recipient
    could read — no role bypasses RLS at runtime.
  - `svc_notifier_ingest` — the NATS consumer (a system component, not a user);
    INSERT plus the SELECT needed for `RETURNING`, no user-scoped read path.
  - Migrations run at startup under a separate owner role, then that pool closes;
    the owner role is never used for a runtime read.

## Security notes

- **`template` and `payload` are untrusted producer data.** Render them as data —
  never HTML-interpolate them, never treat `template` as a path or a format string.
- **`link` is the only navigable field** and is constrained by `RelativeLink` to a
  same-domain relative URL, validated fail-closed at intake and at the
  type level in the contract crate. Frontends should still bind it to router
  navigation, not to raw `href` interpolation.
- **One-shot secrets do not exist here**: notifications must never carry secrets;
  anything published on the deliver subject ends up readable by its recipients.

## Notification lifecycle

```
deliver command ──▶ Unread ──▶ Read        (idempotent, irreversible — no unread)
                      │          │
                      └──────────┴──▶ Deleted   (hard delete, row removed)
```

Notifications are created by the intake only — no GraphQL mutation creates one.
`Unread → Read` is recipient-driven and idempotent; there is no read→unread
transition. Delete is hard today (see "Open questions").

**No affordances, by design.** The lifecycle above has no interesting preconditions:
every action is always available on a notification the caller can see. The service
ships no per-action availability metadata, and none should be added — there is
nothing for it to say.

## Running locally

```sh
cp .env.test .env      # matches the docker-compose harness; or export the variables below
docker compose -f docker-compose.test.yml up -d   # Postgres 17 + NATS JetStream
cargo run                                          # the service
cargo run -- schema                                # print the GraphQL SDL and exit
```

| Variable              | Required                  | Description                                            |
|-----------------------|---------------------------|--------------------------------------------------------|
| `PORT`                | yes                       | HTTP listen port                                       |
| `DATABASE_URL`        | yes                       | DSN for the `svc_notifier_app` role (RLS-scoped)       |
| `DATABASE_URL_OWNER`  | no (falls back to `DATABASE_URL`) | DSN for migrations + grants (owner role)       |
| `DATABASE_URL_INGEST` | when `NATS_URL` is set    | DSN for the `svc_notifier_ingest` role (NATS consumer) |
| `NATS_URL`            | no                        | NATS server URL; omit to run without intake            |
| `NATS_USER`           | no                        | NATS username                                          |
| `NATS_PASSWORD`       | no                        | NATS password                                          |
| `RUST_LOG`            | no (default `info`)       | Tracing filter (structured JSON logs)                  |
| `TRUSTED_NETWORK_HOSTS` | per remote plaintext DB host | Comma-separated DB hosts reachable over plaintext (see below) |

**Database TLS.** The shared `br-util-postgres` lib is strict by default: a
plaintext DSN to any remote (non-loopback) host is **refused at startup** unless
that host is declared in `TRUSTED_NETWORK_HOSTS`, or the DSN itself enforces TLS
(`sslmode=require` / `verify-ca` / `verify-full`). The platform runs
CloudNativePG intra-namespace over plaintext behind a default-deny
`NetworkPolicy`, so a K3s/Kubernetes deployment lists the CNPG service host in
`TRUSTED_NETWORK_HOSTS` (chart key `postgres.trustedNetworkHosts`) — a
deliberate per-host opt-out that trusts the network segment, not transport TLS.
Loopback hosts always pass with no declaration. (There is no `ALLOW_INSECURE_DATABASE`
blanket bypass and no environment mode — strictness is unconditional.)

The roles must exist before first startup;
[`scripts/init-db.sql`](scripts/init-db.sql) is the reference bootstrap (the test
compose mounts it automatically). Migrations run at service startup. The role
model mirrors production: the migration owner role (`DATABASE_URL_OWNER`) is
**migration-only** — RLS-exempt (`BYPASSRLS`) so migrations and future data
backfills always work, and never used by any runtime path; runtime access goes
through the `svc_notifier_app` / `svc_notifier_ingest` roles only. In the test
harness the service under test runs with exactly these roles
(`DATABASE_URL_SERVICE_OWNER` in `.env.test`); the compose superuser is
harness-only and is never handed to the service.

| Path                  | Method | Description                                          |
|-----------------------|--------|------------------------------------------------------|
| `/graphql`            | POST   | GraphQL endpoint; SSE subscriptions via `Accept: text/event-stream` |
| `/graphql/playground` | GET    | GraphiQL UI                                          |
| `/livez`              | GET    | Liveness — always `200` (`br-util-observability`); the chart points the liveness probe here |
| `/readyz`             | GET    | Readiness (`br-util-axum-readiness`) — `200` once boot work succeeds, `503` while starting; the chart points the readiness probe here |
| `/metrics`            | GET    | Prometheus exposition (`br-util-observability`) — process + HTTP collectors, anonymized labels |
| `/sdl`                | GET    | GraphQL SDL (the gateway composer polls this path)   |

## Tests

- **Unit tests** live next to the code; the contract crate's tests are its spec
  (`cargo test -p br-notifier-contract`).
- **End-to-end tests are the specification.** Named behavior scenarios
  (`tests/scenarios_*.rs`), each pinning the three external envelopes — what
  happens on NATS (ack/NAK/redelivery, consumer state), in PostgreSQL (exact
  rows, asserted through a dedicated assertion connection, never through the
  app), and on the GraphQL surface (what a recipient's session observes:
  query, unread count, subscription push). They cover cross-session event
  propagation, reconnect, redelivery idempotence, fail-closed link rejection,
  DB-outage NAK/recovery/exhaustion, and a two-instance scenario proving
  pushes derive from committed PG state. All seeding goes through the real
  intake — there is no direct-SQL seeding path.
- The suite runs against real Postgres and real NATS JetStream — no infra
  mocks. The outage scenarios additionally need the `docker` CLI (they pause
  the Postgres container). Bring the harness up first:

```sh
docker compose -f docker-compose.test.yml up -d
cargo test --tests --no-fail-fast
```

The scenario suite is the service's definition of done: it passes green
against the real harness.

## Versioning & release

Two independently versioned crates:

- `svc-notifier` — the service. Released as an image + chart:
  `ghcr.io/botresources/br-svc-notifier:{version}` and
  `oci://ghcr.io/botresources/charts/br-svc-notifier:{version}`.
- `br-notifier-contract` — the published language, consumed by producers as a git
  dependency. A change here is a contract change and follows semver strictly.

Release flow — image-first, tag-after, per crate:

1. Bump the crate's version in its `Cargo.toml` and add the matching heading
   to its `CHANGELOG.md` (Keep a Changelog) in the same PR.
2. CI gates the PR (`pull_request` is the only CI trigger): fmt auto-fix gate,
   clippy + unit tests, the e2e scenario suite, cargo-audit, cargo-deny,
   cargo-machete, semver-checks on the contract crate, per-crate changelog
   presence, shellcheck, secret scan. All of them are required checks on
   `main`, managed declaratively by
   [`scripts/setup-branch-protection.sh`](scripts/setup-branch-protection.sh).
3. On merge, CD detects the version bump per crate, publishes the service
   image + chart **first**, then creates the tag `{crate}/v{version}` and the
   GitHub Release — a tag is a receipt that the version shipped, never a
   promise. The contract crate has no artifact: its release *is* the
   `br-notifier-contract/v{version}` tag.

No manual tagging, no manual image/chart push.

Local pipeline: `./scripts/publish.sh --check-only` (fmt, clippy, unit tests, helm
lint), `--local-image`, `--dry-run` — see the script header.

## Infra debt

- **The durable consumer is declared by the service, not by the deployment.**
  Doctrine prefers declared-by-deployment infrastructure with the service binding
  fail-loud (`get_consumer`). Today the service still creates the consumer at boot,
  because the test harness recreates the stream per scenario and does not declare a
  consumer. The mitigation is that creation is **declarative and reconciled**:
  `create_consumer_strict` either creates the consumer with the exact delivery
  config or, if one already exists with a *different* config, fails startup — there
  is no silent config drift. Moving the consumer declaration into the deployment
  (and flipping the bind to `get_consumer`) is owed; until then this is the
  defensible middle.
- **`br_core_integration::DurableConsumer` (v0.8.0) was evaluated and declined for
  the intake.** Its public `run_commands` / `run_events` deserialize every message
  into the integration envelope (`IntegrationCommand<T>` / `IntegrationEvent<T>`);
  the only payload-agnostic path (`run_inner`) is private. svc-notifier consumes a
  **bare** `DeliverNotification` on `notifier.cmd.notification.deliver.v1` — a
  wire-format frozen and tested in `br-notifier-contract` and depended on by
  producers — so adopting the wrapper would mean re-enveloping the published
  contract, a breaking change for no benefit. The intake keeps its hand-rolled
  `consumer.messages()` loop (IO inline by design), which also preserves its
  own redelivery-budget and fail-closed-on-poison policy. Re-evaluate if the lib
  later exposes a payload-agnostic `run` (the `bind` side already fits once the
  consumer is deployment-declared).

## Open questions

- **Hard vs soft delete** — delete currently removes the row. Soft delete would
  enable a trash/undo UX and tombstones; decide before any cascade-on-user-deletion
  work.
- **Allowed-template list** — the service accepts any `template` string. The list of
  valid templates is per-project policy and belongs in configuration; it must never
  be hard-coded in the generic contract crate.

## License & contributing

MIT — see [LICENSE](LICENSE). This repository does not accept external
contributions or support requests; see [CONTRIBUTING.md](CONTRIBUTING.md) and
[SUPPORT.md](SUPPORT.md). Report security issues privately via
[SECURITY.md](SECURITY.md).
