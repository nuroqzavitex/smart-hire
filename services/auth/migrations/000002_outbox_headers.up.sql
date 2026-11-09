ALTER TABLE outbox ADD COLUMN headers jsonb NOT NULL DEFAULT '{}';
