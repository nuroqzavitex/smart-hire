CREATE TABLE candidates (
    user_id uuid PRIMARY KEY,
    full_name text NOT NULL,
    phone text,
    location text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cvs (
    id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL,
    object_key text NOT NULL UNIQUE,
    file_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('UPLOADING','UPLOADED','PARSED','FAILED')),
    attempt_id text,
    parsed_data jsonb,
    parse_error text,
    size_bytes bigint,
    consent_at timestamptz NOT NULL,
    parsed_at timestamptz,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX cvs_candidate ON cvs (candidate_id, created_at DESC, id DESC);

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

CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
