-- Create runtime roles. The owner role is created by POSTGRES_USER in docker-compose.
-- Three roles (namespaced to the service):
--   owner              — migrations + grants (not runtime)
--   svc_notifier_app   — GraphQL user-facing, subject to user-scoped RLS
--   svc_notifier_ingest — NATS consumer, system component, dedicated RLS policies
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_notifier_app') THEN
        CREATE ROLE svc_notifier_app LOGIN PASSWORD 'svc_notifier_app';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'svc_notifier_ingest') THEN
        CREATE ROLE svc_notifier_ingest LOGIN PASSWORD 'svc_notifier_ingest';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE svc_notifier_test TO svc_notifier_app;
GRANT CONNECT ON DATABASE svc_notifier_test TO svc_notifier_ingest;
