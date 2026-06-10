CREATE TABLE notifications (
    id              UUID PRIMARY KEY,
    source_event_id UUID NOT NULL,
    recipient_id    UUID NOT NULL,
    template        TEXT NOT NULL,
    payload         JSONB NOT NULL,
    link            TEXT,
    read_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX notifications_source_recipient_uniq
    ON notifications (source_event_id, recipient_id);

CREATE INDEX notifications_recipient_created_idx
    ON notifications (recipient_id, created_at DESC, id DESC);

ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE notifications FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS notifications_app_scope ON notifications;
CREATE POLICY notifications_app_scope ON notifications
    TO svc_notifier_app
    USING (recipient_id = current_setting('app.current_user_id')::uuid)
    WITH CHECK (recipient_id = current_setting('app.current_user_id')::uuid);

DROP POLICY IF EXISTS notifications_ingest_write ON notifications;
CREATE POLICY notifications_ingest_write ON notifications
    TO svc_notifier_ingest
    USING (true)
    WITH CHECK (true);

GRANT USAGE ON SCHEMA public TO svc_notifier_app;
GRANT SELECT, UPDATE, DELETE ON notifications TO svc_notifier_app;

GRANT USAGE ON SCHEMA public TO svc_notifier_ingest;
GRANT SELECT, INSERT ON notifications TO svc_notifier_ingest;
