-- Create runtime roles. The compose POSTGRES_USER ("owner") is a SUPERUSER and
-- is reserved for the TEST HARNESS ONLY (assertion connection, state reset) —
-- the service under test never sees it. Four roles (namespaced to the service):
--   owner               — superuser; harness-only, never handed to the service
--   svc_notifier_owner  — migrations + grants ONLY. NOT a superuser, but
--                         BYPASSRLS: exempt from row-level security so that
--                         migrations (including future data backfills) always
--                         work. It must NEVER be used for anything else — no
--                         runtime read or write path may borrow it; runtime
--                         access goes through svc_notifier_app / _ingest.
--   svc_notifier_app    — GraphQL user-facing, subject to user-scoped RLS
--   svc_notifier_ingest — NATS consumer, system component, dedicated policies
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_notifier_owner') THEN
        CREATE ROLE svc_notifier_owner LOGIN PASSWORD 'svc_notifier_owner' BYPASSRLS;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_notifier_app') THEN
        CREATE ROLE svc_notifier_app LOGIN PASSWORD 'svc_notifier_app';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_notifier_ingest') THEN
        CREATE ROLE svc_notifier_ingest LOGIN PASSWORD 'svc_notifier_ingest';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE svc_notifier_test TO svc_notifier_owner;
GRANT CONNECT ON DATABASE svc_notifier_test TO svc_notifier_app;
GRANT CONNECT ON DATABASE svc_notifier_test TO svc_notifier_ingest;

GRANT USAGE ON SCHEMA public TO svc_notifier_owner, svc_notifier_app, svc_notifier_ingest;
GRANT CREATE ON SCHEMA public TO svc_notifier_owner;
