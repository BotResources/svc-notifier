# svc-notifier

> [!IMPORTANT]
> **This repository is maintained for BotResources and its authorized clients.**
> It is published under Apache-2.0 and made available read-only for visibility
> and dependency consumption. The Apache-2.0 license governs your rights to
> use, modify, and fork the code; the rest of this notice describes our
> operational stance, not a legal restriction.
>
> **We do not accept external pull requests, issues, or support requests.**
> Issues and Discussions are disabled. PRs from accounts that are not on the
> internal contributor allowlist will be closed without review. The GitHub
> fork button is disabled — you may still fork under Apache-2.0, but we
> provide no support outside the BR commercial relationship.
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
producers ──integration.cmd.notifier.notification.deliver.v1──▶ NATS JetStream
   (br-notifier-publisher / Fabric)                     │ (INTEGRATION_CMD)
                                                        │ Fabric durable consumer
                                                        ▼
                                                  PostgreSQL (RLS)  ◀── the single
                                                        │               source of truth
                                                   pg_notify on commit
                                                        ▼
recipients ◀──GraphQL: queries / mutations / subscription stream──┘
```

- **Intake (NATS, via the Project NATS Fabric)** — all NATS access goes through
  `br-util-nats-fabric`; the service holds no `async_nats` handle. A Fabric
  create-or-bind durable consumer (no polling — it parks at zero CPU) fans each
  deliver command out to one row per recipient. It binds the deliver coordinates
  (receiver `notifier`, aggregate `notification`, verb `deliver`, v1), which the
  Fabric renders to `integration.cmd.notifier.notification.deliver.v1` on the
  fixed `INTEGRATION_CMD` stream; nothing listens on the legacy `notify.deliver`
  subject. The fixed stream is deployment-provisioned infrastructure — the
  service binds to it and **fails loud** if it is absent, never creating it. The
  **durable consumer is created-or-bound by the Fabric at boot** on the
  Fabric-owned config (explicit ack, `deliver_policy = All`, the rendered
  coordinate filter); two replicas naming the same durable converge.
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

## The published language — `br-notifier-contract` + `br-notifier-publisher`

This repository is a workspace: the `svc-notifier` service plus two crates under
[`crates/`](crates/) — [`br-notifier-contract`](crates/br-notifier-contract/), the
service's published language, and [`br-notifier-publisher`](crates/br-notifier-publisher/),
the producer kit. The contract crate is owned by the receiver (this service),
versioned and tagged independently, and is what producers depend on — never on the
service crate.

- Coordinates: `deliver_coords()` — receiver `notifier`, aggregate `notification`,
  verb `deliver`, v1. The Fabric renders them to
  `integration.cmd.notifier.notification.deliver.v1` on the `INTEGRATION_CMD`
  stream. The contract crate is transport-agnostic: it owns the coordinates, not
  a raw subject string, and pulls in no NATS client.
- Command: `DeliverNotification { source_event_id, recipient_ids, template, payload,
  link: Option<RelativeLink> }`. Producers build the typed command and publish it
  via `br-notifier-publisher`'s `NotifierPublisher` over the Fabric (wrapped in the
  standard `IntegrationCommand` envelope) — no hand-built subject, no `async_nats`.
- `RelativeLink` is a fail-closed newtype: a same-domain relative URL, i.e. a path
  rooted at `/` (query and fragment allowed). It rejects absolute URLs and schemes
  (`https:`, `javascript:`, …), scheme-relative `//host`, backslashes, whitespace,
  control characters, and empty input — at construction *and* on deserialization.
  Its unit tests are the authoritative accept/reject vectors.

`DeliverNotification` payload wire format (pinned by a round-trip test in the
contract crate; on the bus it travels as the `payload` of the standard
`IntegrationCommand` envelope):

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
- **Malformed message** (not valid JSON for the command): **terminated** as poison
  (`term`) with an error log — never NAKed (no redelivery storm) and never acked (an
  ack would falsely tell JetStream the frame was processed). Nothing is persisted.
- **Invalid `link`**: the whole message is rejected fail-closed — the command fails to
  deserialize (the contract's `RelativeLink` rejects an unsafe link), so it takes the
  undecodable-message path: terminated with an error log, zero rows persisted (no
  partial fan-out), nothing reaches any recipient.
- **Dead-letter ledger**: a command that decodes but can never be written is
  recorded in `dead_letters` — `source_event_id`, the **full** `recipient_ids`
  list (a poison write abandons every recipient at once, so every one of them is
  named), the raw command bytes, the SQLSTATE, the producer's `correlation_id` /
  `causation_id` and a timestamp — **before** the frame is terminated. The ledger
  is deduplicated on the envelope's **`command_id`** (unique index + `ON CONFLICT
  DO NOTHING`): one audit line per abandoned **command**, not per frame, so a
  `term()` the broker never registered cannot inflate the audit, and the first
  trace wins. Deduplicating on `source_event_id` would be **wrong and lossy**: the
  business dedup key is `(source_event_id, recipient_id)`, so one source event may
  legitimately arrive as several deliver commands (recipients sent in chunks, a
  recipient added late — the shape `s14` exercises). Keyed on the source event,
  the first poison chunk would be recorded and every later one terminated with no
  trace at all. `source_event_id` keeps a plain (non-unique) index for ops
  lookups. If the ledger write itself fails, the command is NAKed
  instead of terminated: the intake never drops a request it cannot account for,
  even at the cost of an infinite hold (which the `/readyz` DOWN signal makes
  loud). The command is stored as `BYTEA`, not `JSONB`/`TEXT`, precisely because
  the byte sequence that made the notification write impossible would make a
  text-typed ledger write impossible too. An **undecodable** frame gets no ledger
  row: the Fabric's `Delivered` surface exposes no raw bytes, so there is nothing
  faithful to record — the error log is the only trace, and widening that would
  need a `br-util-nats-fabric` change.
- **Database failure mid-batch**: the message is NAKed and redelivered — **without
  any budget**. A transient storage failure (unreachable database, pool timeout,
  connection loss, any SQLSTATE that a retry could clear) is NAKed for as long as
  the outage lasts, so an accepted delivery command is never dropped: JetStream
  holds the frame and redelivers it when storage returns. Redelivery completes the
  remaining recipients; already-inserted recipients are not duplicated (the dedup
  constraint makes the fan-out idempotent). Only a **permanently invalid** write —
  SQLSTATE class `22` (data exception) or `23` (integrity constraint violation),
  which no retry can clear — is terminated as poison, and never before its
  abandonment has been recorded in the **dead-letter ledger** (below).
- **The never-lose guarantee is bounded by the stream's retention**, which is a
  deployment-side declaration, not a service-side one: `INTEGRATION_CMD` is
  `retention: limits` with `maxAge` 168h (7 days) and `maxBytes` 512 MiB in
  production today. A storage outage lasting days — or a stream saturating its
  byte limit and discarding old frames — would evict a held command despite the
  service holding it correctly. That is an **operator incident**, not a normal
  mode: the `/readyz` DOWN signal below raises it hours to days before the
  retention edge is anywhere near.
- **Outage visibility.** After three consecutive transient failures the service
  reports `/readyz` DOWN with an operator-facing reason, so a pod that cannot
  write leaves the load-balancer rotation instead of quietly piling work up.
  Readiness is restored by whichever comes first: the next successful write, or a
  bounded storage probe armed at escalation (`SELECT 1` every 5s, stops on the
  first answer, at most one in flight). The probe is the one place this service
  polls rather than subscribes — PostgreSQL offers no push channel for "I am back",
  and the probe exists so a quiet service does not stay out of rotation waiting for
  traffic it will not receive. It only ever clears a `NotReady` it recognises as its
  own (the storage-outage reason), so it can never mask an unrelated fail-loud.

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
- **Bulk facts are chunked to stay under the NOTIFY payload limit.** PostgreSQL
  caps a `pg_notify` payload at 8000 bytes and raises an error above it — inside
  the write transaction, that error would **abort the whole mutation**. A bulk
  `Read` / `Deleted` fact covering more ids than `SIGNAL_ID_CHUNK` (150) is
  therefore emitted as several signals **within the same transaction**: still
  atomic with the write, still one announcement per chunk rather than one per row.
  A client folding by id is unaffected; a client asserting "exactly one event per
  bulk mutation" is only correct below the chunk size. The bound is pinned by a
  unit test on the serialized payload, not by arithmetic in a comment.
- **Known gap — a signal emitted while the listener is reconnecting is lost.**
  `LISTEN/NOTIFY` has no replay: if the listener's connection drops and a commit
  lands in the ~1s reconnect window, the row exists and no one is told until the
  client next fetches a snapshot. `s07c` proves the announcement survives a
  storage outage that freezes the connection (the common case), but it cannot
  prove the hard-restart case, where the listener's socket is actually killed.
  Closing it needs a catch-up watermark on reconnect — a design decision, not a
  patch; it is not in this service today.
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
- **The recipient is the acting human, never the impersonated one.** A notification
  belongs to the human who is really acting: on an impersonated passport the
  recipient resolves to `impersonator_id()`, not to `user_id`. An administrator
  impersonating a user keeps seeing and acting on their *own* notifications —
  list, unread count, live stream, mark-as-read and delete alike. Recipient
  resolution happens in **one** place (`graphql::resolve_recipient`), and both the
  application-layer scoping and the RLS context (`app.current_user_id`) are fed
  from it, so the two layers can never disagree. Proven by
  `scenarios_impersonation` (s17–s19) and by unit tests on `resolve_recipient`.
- **Defence in depth: the application also filters by recipient.** Every write and
  re-read that knows the caller's identity carries an explicit
  `recipient_id = $caller` predicate in its `WHERE`, on top of the RLS policy —
  `mark_as_read`, `mark_all_as_read`, `delete_notifications` and the listener's
  row re-read. RLS is the backstop, not the only barrier: a future migration that
  loosens a policy, or a query issued outside a scoped transaction, must not
  silently become a cross-recipient write.
- **Row-level security as the authorization backstop**: resolvers open a transaction,
  inject the caller's transaction-local RLS context, and the policies restrict every
  select/update/delete to `recipient_id = current user`. RLS is `FORCE`d — even the
  table owner cannot bypass it. Both write/read paths set a **single** transaction-local
  GUC, `app.current_user_id`, and the notifications policy reads only that GUC. The
  GraphQL path (`scoped_tx`) sets it inline with
  `set_config('app.current_user_id', $recipient, true)` — the shared
  `br_util_postgres::set_rls_context` helper was removed in the v1.0.x lib, and the
  GUC *shape* is a per-project seam, so the service owns this one line. The realtime
  listener has **no** `Passport` — its recipient is synthesized from the `pg_notify`
  signal — and sets the same single GUC the same inline way.
- **Two runtime PostgreSQL roles, least privilege**:
  - `svc_notifier_app` — GraphQL resolvers *and* the realtime listener's row
    re-reads; user-scoped by the RLS policies above. The listener scopes its
    re-read to the signal's recipient, so it reads exactly the rows that recipient
    could read — no role bypasses RLS at runtime.
  - `svc_notifier_ingest` — the NATS consumer (a system component, not a user);
    INSERT plus the SELECT needed for `RETURNING`, no user-scoped read path. It is
    also the only runtime role that may write `dead_letters`; that table carries no
    grant to `svc_notifier_app` at all — an abandoned command is operator data, not
    user data, and is read with the owner role.
    **Runbook — read `dead_letters` as the owner role.** The table is `FORCE` RLS
    with a single `svc_notifier_ingest` policy and no grant at all to
    `svc_notifier_app`. Verified behaviour, per role: `svc_notifier_app` →
    `ERROR: permission denied for table dead_letters` (loud, no grant);
    `svc_notifier_owner` (`BYPASSRLS`) → reads every row — **this is the ops
    path**; `svc_notifier_ingest` → reads every row too (its policy is
    `USING (true)`), but it is the service's own runtime role, not an operator
    seat. The trap to know about is the *third* case, which no role hits today but
    the next one might: a role granted `SELECT` **without** a matching policy gets
    **zero rows, silently** under `FORCE` RLS — no error. So never read this table
    through a freshly-granted role and conclude "nothing was abandoned"; an empty
    result under anything but the owner means "not allowed to see", not "empty".
    Decode the payload with `convert_from(command, 'UTF8')`.
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
| `NATS_URL`            | no                        | NATS server URL; omit or leave empty to run without intake |
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
  (`tests/scenarios_*.rs`), each pinning the external envelopes — what
  happens in PostgreSQL (exact rows, asserted through a dedicated assertion
  connection, never through the app) and on the GraphQL surface (what a
  recipient's session observes: query, unread count, subscription push). They
  cover cross-session event propagation, reconnect, redelivery idempotence,
  DB-outage NAK/recovery (including an outage longer than the retired redelivery
  budget), impersonation isolation, a two-instance scenario proving pushes
  derive from committed PG state, and **service-level fail-loud** when the fixed
  `INTEGRATION_CMD` stream is absent (`s15`: a real svc-notifier spawned against a
  broker missing the stream exits non-zero and never serves `/readyz`). All
  seeding goes through the real intake (the `NotifierPublisher`) — there is no
  direct-SQL seeding path.
- **Malformed-frame / invalid-`link` at the intake (proven without a live-intake
  e2e, by design):** the decode → `term` (poison) and fail-closed-`link` decisions
  are proven by **unit tests** (`intake::tests`, including the undecodable → `term`
  case) and by `br-notifier-contract`'s deserialization tests (the contract rejects
  an unsafe `link` fail-closed). A live-intake e2e of a deliberately-corrupt frame
  is **intentionally not added**: it would require a raw-publish helper on the
  harness, which the operator has ruled out as a foot-gun that defeats the
  typed-coordinates discipline — and a compliant producer using
  `br-notifier-publisher` cannot emit such a frame by construction (it only ships a
  typed `DeliverNotification`). This is the proportionate proof, not deferred debt;
  it would only be revisited if a non-lib / non-Rust producer were introduced.
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

Three independently versioned crates:

- `svc-notifier` — the service. Released as an image + chart:
  `ghcr.io/botresources/br-svc-notifier:{version}` and
  `oci://ghcr.io/botresources/charts/br-svc-notifier:{version}`.
- `br-notifier-contract` — the published language, consumed by producers as a git
  dependency. A change here is a contract change and follows semver strictly.
  Released as the `br-notifier-contract/v{version}` tag.
- `br-notifier-publisher` — the producer-side kit (`crates/br-notifier-publisher`)
  that wraps a typed `DeliverNotification` in the integration-command envelope and
  publishes it over the Fabric. Consumed by producers as a git dependency.
  Released as the `br-notifier-publisher/v{version}` tag.

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
   promise. The contract and publisher crates have no artifact: their release
   *is* the `br-notifier-contract/v{version}` / `br-notifier-publisher/v{version}`
   tag, created when the crate's version bumps on `main`.

No manual tagging, no manual image/chart push.

Local pipeline: `./scripts/publish.sh --check-only` (fmt, clippy, unit tests, helm
lint), `--local-image`, `--dry-run` — see the script header.

## NATS Fabric

All NATS access goes through `br-util-nats-fabric` — the service holds no
`async_nats` handle, builds no subject string, and names no stream. The fixed
`INTEGRATION_CMD` stream is deployment-provisioned; the service binds it
fail-loud and never creates it. The durable consumer (named `svc-notifier`) is
**created-or-bound by the Fabric** at boot on the Fabric-owned config — this is
sanctioned (a durable carries the service's processing semantics, not infra),
and two replicas naming the same durable converge.

The bind **gates readiness**: it happens *before* `/readyz` is set ready. If the
fixed stream is absent the bind errors, the process exits non-zero, and the
service never becomes ready — `/readyz` stays 503. A dead intake can never sit
behind a healthy readiness probe (Security Invariant #6: fail loud → readiness
DOWN).

The intake task is **supervised** for the same reason. It runs on its own tokio
task, and a task that panics or is cancelled dies silently: the process would
keep serving `/readyz` 200 while delivery commands piled up unconsumed on the
stream — the exact "healthy probe over a dead intake" shape the boot gate exists
to prevent. `supervise_intake` awaits the task handle and, on a panic or a
cancellation, takes readiness DOWN and triggers the shutdown that makes the
process exit non-zero. This is also what makes the ledger's one `expect` (the
command re-serialization in `record_dead_letter`) safe to keep: it cannot become
a silent stall.

- **Envelope.** The command travels as the standard `IntegrationCommand`
  envelope; its `payload` is the `DeliverNotification` whose wire format is frozen
  and tested in `br-notifier-contract`. Producers publish through
  `br-notifier-publisher`, which wraps the payload in the envelope.
- **Poison handling — by failure class, never by a delivery count.** A frame is
  terminated only when retrying it is pointless *by nature*: an **undecodable**
  frame (invalid JSON, or a payload the contract rejects fail-closed — e.g. an
  unsafe `link`) and a **permanently invalid write** (SQLSTATE class `22` / `23`),
  the latter only once it has been recorded in the `dead_letters` ledger.
  Terminating is never acking: an ack would lie to JetStream that the frame was
  processed and would hide the poison from advisories. Every other fan-out failure
  is **transient** and is NAKed with a short delay for redelivery, however long the
  outage lasts (within the stream's retention) — the intake has no delivery budget,
  because a producer's ack must mean the notification will be delivered.
  Classification is conservative: when in doubt, transient. The decode / transient /
  poison outcome logic is unit-tested directly in `intake::tests`, and the ledger is
  pinned end-to-end by
  `scenarios_intake::s20_a_permanently_unwritable_command_is_dead_lettered_before_it_is_terminated`.
- **`ack_wait`.** The intake uses the Fabric default (30s). Redelivery is driven by
  the 1s NAK delay, not by `ack_wait`, so an outage longer than the retired
  five-delivery budget is exercised end-to-end in
  `scenarios_outage::s07c_an_outage_past_the_retired_budget_still_delivers_exactly_once`.

## Open questions

- **Hard vs soft delete** — delete currently removes the row. Soft delete would
  enable a trash/undo UX and tombstones; decide before any cascade-on-user-deletion
  work.
- **Allowed-template list** — the service accepts any `template` string. The list of
  valid templates is per-project policy and belongs in configuration; it must never
  be hard-coded in the generic contract crate.
- **`dead_letters` retention / TTL — undecided, operator's call.** The ledger has
  **no purge today**: a row lives until someone deletes it. Each row stores the
  producer's command verbatim, and `payload` is arbitrary producer data that may
  well be personal, so this is a retention question, not just a disk question.
  It pairs with the `INTEGRATION_CMD` retention noted under "Intake semantics":
  that one bounds how long a *held* command survives, this one how long an
  *abandoned* one is kept. Both are deployment-side decisions the service must not
  take on its own.

## License & contributing

Apache-2.0 — see [LICENSE](LICENSE). This repository does not accept external
contributions or support requests; see [CONTRIBUTING.md](CONTRIBUTING.md) and
[SUPPORT.md](SUPPORT.md). Report security issues privately via
[SECURITY.md](SECURITY.md).
