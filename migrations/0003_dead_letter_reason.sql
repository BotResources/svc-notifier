ALTER TABLE dead_letters ALTER COLUMN source_event_id DROP NOT NULL;

ALTER TABLE dead_letters ADD COLUMN reason TEXT NOT NULL DEFAULT 'storage_rejected';

ALTER TABLE dead_letters ALTER COLUMN reason DROP DEFAULT;

CREATE INDEX dead_letters_reason_idx ON dead_letters (reason);
