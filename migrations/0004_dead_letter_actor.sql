ALTER TABLE dead_letters ADD COLUMN actor_kind TEXT;

ALTER TABLE dead_letters ADD COLUMN actor_id UUID;

CREATE INDEX dead_letters_actor_idx ON dead_letters (actor_id);
