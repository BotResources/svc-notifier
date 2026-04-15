-- Create the app role used at runtime (subject to RLS).
-- The owner role is created by POSTGRES_USER in docker-compose.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app') THEN
        CREATE ROLE app LOGIN PASSWORD 'app';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE svc_notifier_test TO app;
