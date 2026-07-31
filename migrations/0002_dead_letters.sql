CREATE TABLE dead_letters (
    id              UUID PRIMARY KEY,
    command_id      UUID NOT NULL,
    source_event_id UUID NOT NULL,
    recipient_ids   UUID[] NOT NULL,
    command         BYTEA NOT NULL,
    sqlstate        TEXT,
    correlation_id  UUID NOT NULL,
    causation_id    UUID,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX dead_letters_command_uniq ON dead_letters (command_id);

CREATE INDEX dead_letters_source_event_idx ON dead_letters (source_event_id);

CREATE INDEX dead_letters_recorded_idx ON dead_letters (recorded_at DESC, id DESC);

CREATE INDEX dead_letters_correlation_idx ON dead_letters (correlation_id);

ALTER TABLE dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE dead_letters FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS dead_letters_ingest_write ON dead_letters;
CREATE POLICY dead_letters_ingest_write ON dead_letters
    TO svc_notifier_ingest
    USING (true)
    WITH CHECK (true);

GRANT SELECT, INSERT, DELETE ON dead_letters TO svc_notifier_ingest;
