# svc-notifier

Standalone notification service. Receives events via NATS JetStream, persists notifications to PostgreSQL with row-level security, and exposes a GraphQL API with real-time subscriptions.

## Architecture

- **NATS intake** -- Consumes `notify.deliver` messages from the `NOTIFY` JetStream stream, inserts notifications, and pushes to active WebSocket subscribers.
- **GraphQL API** -- Queries (list, unread count), mutations (mark read, delete), and subscriptions (real-time push).
- **RLS** -- All GraphQL resolvers use transaction-local RLS via `br-util-postgres`. Users can only access their own notifications.

See [`docs/domain.md`](docs/domain.md) for the notification lifecycle, the
business-rule inventory, and the planned move to a hexagonal layout.

## Environment variables

| Variable             | Required | Default | Description                                   |
|----------------------|----------|---------|-----------------------------------------------|
| `PORT`               | yes      |         | HTTP listen port                              |
| `DATABASE_URL`       | yes      |         | PostgreSQL connection string (app role)        |
| `DATABASE_URL_OWNER` | no       |         | PostgreSQL connection string for migrations    |
| `APP_ROLE`           | no       | `app`   | PostgreSQL role to grant table access to       |
| `NATS_URL`           | no       |         | NATS server URL (omit to run without intake)   |
| `NATS_USER`          | no       |         | NATS username for authenticated connections    |
| `NATS_PASSWORD`      | no       |         | NATS password for authenticated connections    |
| `RUST_LOG`           | no       | `info`  | Tracing filter                                |

## NATS contract

Subject: `notify.deliver`

```json
{
  "source_event_id": "uuid",
  "recipient_ids": ["uuid", "..."],
  "template": "meeting_scheduled",
  "payload": { "arbitrary": "json" }
}
```

Duplicate delivery is safe -- the `(source_event_id, recipient_id)` unique constraint deduplicates.

## GraphQL API

### Queries

- `notifierNotifications(first: Int = 20, after: ID): NotificationConnection` -- paginated list
- `notifierUnreadCount: Int!` -- count of unread notifications

### Mutations

- `notifierMarkAsRead(notificationId: ID!): Boolean!`
- `notifierMarkAllAsRead: Boolean!`
- `notifierDeleteNotification(notificationId: ID!): Boolean!`

### Subscriptions

- `notifierNotificationAdded: NotificationGql!` -- real-time push via WebSocket

## Endpoints

| Path                   | Method | Description          |
|------------------------|--------|----------------------|
| `/graphql`             | POST   | GraphQL endpoint     |
| `/graphql/playground`  | GET    | GraphiQL UI          |
| `/graphql/ws`          | WS     | GraphQL subscriptions|
| `/health`              | GET    | Health check         |
| `/schema`              | GET    | GraphQL SDL          |

## Release

Version is sourced from `Cargo.toml`. Artifacts:

- Image: `ghcr.io/botresources/br-svc-notifier:{version}` (linux/amd64 + linux/arm64)
- Chart: `oci://ghcr.io/botresources/charts/br-svc-notifier:{version}`

Flow:

1. Bump `version` in `Cargo.toml`.
2. Add a `## {version}` heading (Keep a Changelog) to `CHANGELOG.md`.
3. Merge to `main`. CI runs `check` + `integration` + `audit`; on success, `auto-tag` pushes `v{version}`.
4. CD is triggered by the tag: cross-compiles both arches, publishes image + chart.

No manual tagging; no manual image/chart push.

## Local CI

- `./scripts/publish.sh --check-only` — fmt, clippy, unit tests, helm lint.
- `./scripts/publish.sh --local-image` — build a runnable image for the host arch (no push).
- `./scripts/publish.sh --dry-run` — native release build only, no Docker, no push.
- `./scripts/publish.sh` — full publish. Requires `GHCR_TOKEN` and a tag matching `Cargo.toml` already on `main`.

Integration tests (`tests/p1_*..p4_*`) need Postgres + NATS JetStream:

```sh
docker compose -f docker-compose.test.yml up -d
cargo test --tests
```
