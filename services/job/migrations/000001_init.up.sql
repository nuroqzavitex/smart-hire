CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE FUNCTION unaccent_immutable(text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT public.unaccent('public.unaccent'::regdictionary, $1) $$;

CREATE FUNCTION text_array_join(text[]) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT array_to_string($1, ' ') $$;

CREATE FUNCTION lower_text_array(text[]) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT coalesce(array_agg(lower(x)), '{}') FROM unnest($1) AS x $$;

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    recruiter_id uuid NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    required_skills text[] NOT NULL DEFAULT '{}',
    nice_to_have_skills text[] NOT NULL DEFAULT '{}',
    level text NOT NULL CHECK (level IN ('intern','junior','middle','senior','lead')),
    location text NOT NULL,
    salary_min int, salary_max int,
    status text NOT NULL CHECK (status IN ('draft','open','closed')),
    version bigint NOT NULL DEFAULT 1,
    published_at timestamptz, deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', unaccent_immutable(title)), 'A') ||
        setweight(to_tsvector('simple', unaccent_immutable(text_array_join(required_skills))), 'B') ||
        setweight(to_tsvector('simple', unaccent_immutable(description)), 'C')) STORED
);
CREATE INDEX jobs_search_gin ON jobs USING GIN (search_vector) WHERE status = 'open' AND deleted_at IS NULL;
CREATE INDEX jobs_open_published ON jobs (published_at DESC, id DESC) WHERE status = 'open' AND deleted_at IS NULL;
CREATE INDEX jobs_recruiter ON jobs (recruiter_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX jobs_skills_gin ON jobs USING GIN (lower_text_array(required_skills));

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
