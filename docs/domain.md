# Notification Domain — Plan

> Status: **plan**, not yet implemented. The current codebase has no `domain/`
> layer; business rules are smeared across SQL (RLS, constraints), the NATS
> consumer (`src/nats.rs`), and GraphQL handlers (`src/graphql/*.rs`).
>
> This document captures the lifecycle, the rule inventory, and the target
> hexagonal layout we want to migrate towards. It is the source of truth for
> "what notifier should be"; the code lags it for now.

## 1. Notification lifecycle

```
   [source_event]
        │
        ▼
   Created ─────► Skipped  (dedup, opt-out, unknown template, inactive recipient)
        │
        ▼
   Unread ──┬──► Read
            │      │
            └──────┴──► Deleted (hard today; soft on the table)
                              │
                              └──► Purged (TTL — not implemented)
```

A `Notification` is materialised from an upstream event (`source_event_id`,
`recipient_id`, `template`, `payload`). Once persisted, it transitions through
a small state machine driven by recipient actions. Push to connected clients is
a side effect of `Created`, not a state.

## 2. Business rule inventory

Status legend: ✅ enforced · ⚠ partial · ❌ missing.

### Creation

| Rule | Today | Where |
|---|---|---|
| R1. Idempotent on `(source_event_id, recipient_id)` | ✅ | SQL `UNIQUE` |
| R2. Empty `recipient_ids` ⇒ no-op ack | ✅ | `src/nats.rs` |
| R3. `template` ∈ allowed list | ❌ | — |
| R4. `payload` matches template schema | ❌ | — |
| R5. Reject if recipient inactive / deleted | ❌ | — |
| R6. Reject if recipient opted out of template | ❌ | — (no prefs) |
| R7. Time-window dedup (e.g. 3 likes/min ⇒ 1 notif) | ❌ | — |

### Lifecycle

| Rule | Today | Where |
|---|---|---|
| R8. States = {Unread, Read, Deleted} | ✅ implicit | `read_at IS NULL` |
| R9. No Read → Unread transition | ✅ | mutation absent |
| R10. `mark_as_read` idempotent | ✅ | `src/graphql/mutations.rs` |
| R11. Hard vs. soft delete | ⚠ | hard delete; choice undocumented |

### Authorization

| Rule | Today | Where |
|---|---|---|
| R12. Only recipient can read / mutate own notifs | ✅ | RLS policies |
| R13. Service accounts forbidden via GraphQL | ⚠ | only enforced in subscription |
| R14. Impersonator behaviour (admin-as-user) | ❌ | — (open question) |
| R15. PAT-scoped restrictions | ❌ | — |

### Delivery / routing

| Rule | Today | Where |
|---|---|---|
| R16. WS push only when recipient connected | ✅ | `src/nats.rs` + `src/graphql/subscriptions.rs` |
| R17. Lagged subscriber ⇒ drop with warn | ✅ | `src/graphql/subscriptions.rs` |
| R18. Fanout to email / push when offline | ❌ | — |
| R19. Should impersonator also receive a copy? | ❌ | — |

### Retention

| Rule | Today | Where |
|---|---|---|
| R20. TTL on notifications | ❌ | — (live forever) |
| R21. Auto-purge Read notifs after N days | ❌ | — |

### Audit

| Rule | Today | Where |
|---|---|---|
| R22. Mutations log actor + impersonator | ❌ | — |
| R23. Creation logs source event | ✅ | `source_event_id` column |

### Compliance

| Rule | Today | Where |
|---|---|---|
| R24. User deletion cascades to notifs | ❌ | — |

## 3. Why this matters now

Two pressures push us past "RLS = domain":

1. **`br-rust-common 0.4.0`** exposes `auth_method` (Jwt / Pat) and
   `impersonator` on `Passport::Human`. Today we have nowhere to consume them.
   R14, R15, R19, R22 are all directly enabled by that change and have no
   natural home.
2. **Untrusted ingest payloads** (R3, R4) — anything published on
   `notify.deliver` lands in the table verbatim. The first malformed payload
   that reaches a GraphQL consumer will reveal that we have no validation
   layer.

## 4. Target hexagonal layout

```
src/
├── domain/                     # pure, no IO, no framework
│   ├── notification.rs         # entity + Status enum + transitions
│   ├── recipient.rs            # value object
│   ├── template.rs             # value object — allowed list + payload schema (R3, R4)
│   ├── creation.rs             # R1–R7  → fn create(...) -> Result<Notification, SkipReason>
│   ├── lifecycle.rs            # R8–R11 → fn mark_as_read / delete
│   ├── authorization.rs        # R12–R15 → fn can_view / can_mutate(passport, notif)
│   ├── routing.rs              # R18–R19 → fn route(event, actor) -> Vec<Channel>
│   ├── retention.rs            # R20–R21
│   └── audit.rs                # R22 → AuditEntry value object
├── ports/                      # traits the domain depends on
│   ├── repository.rs           # NotificationRepository
│   ├── realtime.rs             # RealtimePublisher
│   ├── prefs.rs                # UserPreferences (R6)
│   ├── user_directory.rs       # UserDirectory (R5)
│   ├── clock.rs                # Clock
│   └── audit_sink.rs           # AuditSink
├── adapters/                   # driven side — implementations of ports
│   ├── pg_repository.rs        # = today's src/db.rs
│   ├── broadcast_realtime.rs   # = today's UserChannels
│   └── …
└── inbound/                    # driving side
    ├── nats_consumer.rs        # = today's src/nats.rs, calls domain::creation::create
    └── graphql/                # = today's src/graphql/, calls domain::lifecycle / ::authorization
```

The contract: `domain/` and `ports/` know nothing about Postgres, NATS, axum,
async-graphql. Adapters and inbound modules are the only places that import
those crates.

## 5. Migration plan

Incremental — do **not** refactor everything in one PR.

### Stage 0 — Scaffolding (no behaviour change)

- Create `src/domain/notification.rs` with `Status` enum (`Unread { since }`,
  `Read { at }`) and a `Notification` struct holding the existing fields.
- Create `src/domain/lifecycle.rs` with `fn mark_as_read(n: Notification, now: DateTime<Utc>) -> Notification`.
- Wire `src/graphql/mutations.rs` to call it (still talking to the same DB).

~50 lines, zero behaviour change. Establishes the layout.

### Stage 1 — Template validation (R3, R4)

- `src/domain/template.rs` with an enum or registry of allowed templates +
  a `validate_payload(template, payload) -> Result<(), TemplateError>`.
- Hook into `src/nats.rs::process_message` before insert; reject with a
  warn-and-ack on invalid templates.

This is the first rule that genuinely has nowhere else to live.

### Stage 2 — Audit (R22)

- `src/domain/audit.rs` with `AuditEntry { actor, impersonator, action, target, at }`.
- `trait AuditSink` in `src/ports/`.
- Tracing-only adapter to start. Wire it into the three GraphQL mutations.

Small, but unblocks the v0.4.0 `Passport.impersonator` info we currently drop.

### Stage 3 — User directory + preferences (R5, R6)

- `trait UserDirectory` (is_active) and `trait UserPreferences` (is_opted_in).
- Adapter likely calls another internal service. Out of scope until product
  asks for opt-out / muting.

### Stage 4+ — Retention, fanout, soft delete, …

Driven by product asks, not architecture for its own sake.

## 6. Open questions

- **R11**: hard vs. soft delete. Hard is simpler; soft enables "trash" UX and
  GDPR-style tombstones. Decide before R24 (cascade-on-user-deletion) lands.
- **R14 / R19**: impersonation routing. See in-flight discussion — currently
  leaning on **audit-only** (Stage 2) plus a publisher-side convention to
  fan out `recipient_ids = [user_id, impersonator_id].dedup()` rather than
  any notifier-side merge.
- **R3**: where does the allowed template list live? In notifier (rigid,
  tightly coupled) or in a shared crate / registry (more decoupled, more
  ceremony). Probably notifier-local at first; promote to shared crate on
  the second consumer.
