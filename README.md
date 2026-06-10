# svc-notifier

Standalone notification service. Producers publish a typed deliver command on NATS
JetStream; svc-notifier persists one notification per recipient in PostgreSQL under
row-level security and serves recipients through a GraphQL subgraph — list and unread
count, ack-only mutations, and a real-time subscription stream.

> **Spec status.** This README is the service's contract, parts of it written ahead of
> the code on purpose: the work is sequenced contract → failing end-to-end specs →
> implementation. Anything marked **[target]** is specified but **not implemented yet**;
> unmarked statements describe current behavior. The markers disappear as the
> implementation lands — a README claim that silently diverges from the code is
> treated as a bug, not a detail.

## Architecture

```
producers ──notifier.cmd.notification.deliver.v1──▶ NATS JetStream
                                                        │ durable pull consumer
                                                        ▼
                                                  PostgreSQL (RLS)  ◀── the single
                                                        │               source of truth
                                          [target] pg_notify on commit
                                                        ▼
recipients ◀──GraphQL: queries / mutations / subscription stream──┘
```

- **Intake (NATS JetStream)** — a durable pull consumer (`consumer.messages()`, no
  polling) fans each deliver command out to one row per recipient. It consumes the
  subject `notify.deliver` today; **[target]** it consumes
  `notifier.cmd.notification.deliver.v1` only, and nothing listens on the legacy
  subject. The consumer currently creates the `NOTIFY` stream if it is absent; treat
  the stream as deployment-provisioned infrastructure regardless.
- **Truth (PostgreSQL)** — notifications live in one table, deduplicated by
  `(source_event_id, recipient_id)`, protected by forced row-level security.
- **Surface (GraphQL subgraph)** — recipient-facing, composed behind a gateway.
  Root fields are prefixed `notifier*`.
- **Realtime** — today the NATS consumer pushes new notifications to in-process
  subscriber channels. **[target]** All pushes derive from committed PostgreSQL state
  via `LISTEN/NOTIFY` (see "Realtime architecture").

## The published language — `br-notifier-contract`

This repository is a two-crate workspace: the `svc-notifier` service and
[`br-notifier-contract`](br-notifier-contract/), the service's published language.
The contract crate is owned by the receiver (this service), versioned and tagged
independently, and is what producers depend on — never on the service crate.

- Subject: `DELIVER_SUBJECT` = `notifier.cmd.notification.deliver.v1` **[target:
  the service still consumes `notify.deliver` today]**.
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
- **[target] Invalid `link`**: the whole message is rejected fail-closed — acked with
  an error log, zero rows persisted (no partial fan-out), nothing reaches any recipient.
- **Database failure mid-batch**: the message is NAKed and redelivered (up to the
  consumer's `max_deliver`, currently 5). Redelivery completes the remaining
  recipients; already-inserted recipients are not duplicated (the dedup constraint
  makes the fan-out idempotent).

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

The notification type carries `id`, `template`, `payload`, `readAt`, `createdAt`
**[target: plus `link`]**.

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
- **[target]** `notifierDeleteNotifications(ids: [ID!]!): Boolean!` — bulk delete.
  Ids not owned by the caller are invisible to it, hence untouched — contractually:
  they are silently skipped, the mutation acks, and they are absent from the emitted
  event. Foreign ids can never be probed through this mutation.

### Subscription

- Today: `notifierNotificationAdded: Notification!` — new notifications only. A
  change made in one session (e.g. mark-as-read) does **not** reach other sessions.
- **[target]** `notifierNotificationEvents: NotifierNotificationEvent!` — the full
  notification event union, bulk-shaped, replacing `notifierNotificationAdded`:

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

## Realtime architecture **[target]**

PostgreSQL is the single source of truth; the subscription stream is **fed by PG
`LISTEN/NOTIFY`**, never by an in-process broadcast from the writer:

- Every write path (NATS intake, mutations) emits `pg_notify` **in the same
  transaction** as the write. The signal therefore fires on commit only — uncommitted
  or rolled-back state can never be pushed — and carries only event type, recipient
  and ids (small, far under the NOTIFY payload limit).
- Each service instance runs a PG listener, re-reads the affected rows from
  PostgreSQL to build the client event (what is pushed is what the truth says), and
  routes it to its local subscription connections.
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
- **Row-level security as the authorization backstop**: resolvers open a transaction,
  inject the caller's transaction-local RLS context, and the policies restrict every
  select/update/delete to `recipient_id = current user`. RLS is `FORCE`d — even the
  table owner cannot bypass it.
- **Two runtime PostgreSQL roles, least privilege**:
  - `svc_notifier_app` — GraphQL resolvers; user-scoped by the RLS policies above.
  - `svc_notifier_ingest` — the NATS consumer (a system component, not a user);
    INSERT plus the SELECT needed for `RETURNING`, no user-scoped read path.
  - Migrations run at startup under a separate owner role, then that pool closes.

## Security notes

- **`template` and `payload` are untrusted producer data.** Render them as data —
  never HTML-interpolate them, never treat `template` as a path or a format string.
- **`link` is the only navigable field** and is constrained by `RelativeLink` to a
  same-domain relative URL, validated fail-closed at intake **[target]** and at the
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

The two runtime roles must exist before first startup;
[`scripts/init-db.sql`](scripts/init-db.sql) is the reference bootstrap (the test
compose mounts it automatically). Migrations run at service startup.

| Path                  | Method | Description                                          |
|-----------------------|--------|------------------------------------------------------|
| `/graphql`            | POST   | GraphQL endpoint; SSE subscriptions via `Accept: text/event-stream` |
| `/graphql/playground` | GET    | GraphiQL UI                                          |
| `/health`             | GET    | Health check                                         |
| `/schema`             | GET    | GraphQL SDL                                          |

## Tests

- **Unit tests** live next to the code; the contract crate's tests are its spec
  (`cargo test -p br-notifier-contract`).
- **End-to-end tests** run against real Postgres and real NATS JetStream — no infra
  mocks. Bring the harness up first:

```sh
docker compose -f docker-compose.test.yml up -d
cargo test --tests
```

**[target]** The e2e suite is being rewritten as named behavior scenarios, each
pinning the three external envelopes — what happens on NATS (ack/NAK/redelivery),
in PostgreSQL (exact rows, asserted through a dedicated connection), and on the
GraphQL surface (what a recipient's session observes) — including cross-session
event propagation, reconnect, redelivery idempotence, and a two-instance scenario
proving pushes derive from committed PG state.

## Versioning & release

Two independently versioned crates:

- `svc-notifier` — the service. Released as an image + chart:
  `ghcr.io/botresources/br-svc-notifier:{version}` and
  `oci://ghcr.io/botresources/charts/br-svc-notifier:{version}`.
- `br-notifier-contract` — the published language, consumed by producers as a git
  dependency. A change here is a contract change and follows semver strictly.

Release flow: bump the crate's version in its `Cargo.toml`, add the matching
heading to its `CHANGELOG.md` (Keep a Changelog), merge to `main`. CI gates
(fmt, clippy, unit + integration tests, audit) then tags automatically — the
service tag `v{version}` today, **[target]** per-crate tags `{crate}/v{version}`.
No manual tagging, no manual image/chart push.

Local pipeline: `./scripts/publish.sh --check-only` (fmt, clippy, unit tests, helm
lint), `--local-image`, `--dry-run` — see the script header.

## Open questions

- **Hard vs soft delete** — delete currently removes the row. Soft delete would
  enable a trash/undo UX and tombstones; decide before any cascade-on-user-deletion
  work.
- **Allowed-template list** — the service accepts any `template` string. The list of
  valid templates is per-project policy and belongs in configuration; it must never
  be hard-coded in the generic contract crate.
