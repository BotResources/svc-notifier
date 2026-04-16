CREATE TABLE notifications (
    id              UUID PRIMARY KEY,
    source_event_id UUID NOT NULL,
    recipient_id    UUID NOT NULL,
    template        TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    read_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_event_id, recipient_id)
);

CREATE INDEX idx_notifications_recipient ON notifications (recipient_id, created_at DESC);
CREATE INDEX idx_notifications_unread ON notifications (recipient_id) WHERE read_at IS NULL;

ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE notifications FORCE ROW LEVEL SECURITY;

-- svc_notifier_app: user-scoped via RLS transaction-local context.
-- Used by GraphQL resolvers (queries, mutations, subscriptions).
CREATE POLICY app_insert ON notifications FOR INSERT TO svc_notifier_app WITH CHECK (true);
CREATE POLICY app_select ON notifications FOR SELECT TO svc_notifier_app
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);
CREATE POLICY app_update ON notifications FOR UPDATE TO svc_notifier_app
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);
CREATE POLICY app_delete ON notifications FOR DELETE TO svc_notifier_app
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);

-- svc_notifier_ingest: NATS consumer (system component, not user-facing).
-- INSERT + SELECT only. SELECT is needed for INSERT ... RETURNING *.
CREATE POLICY ingest_insert ON notifications FOR INSERT TO svc_notifier_ingest WITH CHECK (true);
CREATE POLICY ingest_select ON notifications FOR SELECT TO svc_notifier_ingest USING (true);
