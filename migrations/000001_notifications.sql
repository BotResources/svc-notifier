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

-- INSERT is intentionally permissive: only the NATS consumer inserts.
-- No GraphQL mutation performs INSERT. Tighten this policy if that contract changes.
CREATE POLICY notifications_insert ON notifications FOR INSERT WITH CHECK (true);
CREATE POLICY notifications_select ON notifications FOR SELECT
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);
CREATE POLICY notifications_update ON notifications FOR UPDATE
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);
CREATE POLICY notifications_delete ON notifications FOR DELETE
    USING (recipient_id = current_setting('app.current_user_id', true)::uuid);
