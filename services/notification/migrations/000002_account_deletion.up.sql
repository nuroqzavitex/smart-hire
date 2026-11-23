-- Account deletion (ADR 0014): a deleted user's replica row stays as a
-- tombstone without address, so later events notify nobody, and the service
-- answers user.data_purged through its own outbox (pkg/outbox schema).
ALTER TABLE users ADD COLUMN deleted boolean NOT NULL DEFAULT false;

CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    headers jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;
