CREATE TABLE applications (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    recruiter_id uuid NOT NULL,
    cv_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING','SCREENING','SCREENED','NEEDS_MANUAL_REVIEW',
        'INTERVIEW','OFFERED','HIRED','REJECTED','CANCELLED')),
    reason text NOT NULL DEFAULT '',
    score numeric(5,2),
    score_breakdown jsonb,
    model_version text NOT NULL DEFAULT '',
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT applications_idempotency_key UNIQUE (candidate_id, idempotency_key)
);
-- One live application per (job, candidate); a CANCELLED one does not block
-- applying again.
CREATE UNIQUE INDEX applications_live_job_candidate ON applications (job_id, candidate_id)
    WHERE status <> 'CANCELLED';
CREATE INDEX applications_candidate ON applications (candidate_id, created_at DESC, id DESC);
CREATE INDEX applications_recruiter ON applications (recruiter_id, created_at DESC, id DESC);
CREATE INDEX applications_job ON applications (job_id, created_at DESC, id DESC);

CREATE TABLE application_history (
    id bigserial PRIMARY KEY,
    application_id uuid NOT NULL REFERENCES applications (id),
    from_status text,
    to_status text NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL DEFAULT '',
    at timestamptz NOT NULL
);
CREATE INDEX application_history_app ON application_history (application_id, id);

-- Submit saga state (spec §6.4, ADR 0011). next_run_at is also the worker's
-- lease; version guards writes after a lease expired.
CREATE TABLE saga_instances (
    id uuid PRIMARY KEY,
    application_id uuid NOT NULL UNIQUE REFERENCES applications (id),
    job_id uuid NOT NULL,
    step text NOT NULL CHECK (step IN ('RESERVE_QUOTA','SCORE','RELEASE_QUOTA')),
    state text NOT NULL CHECK (state IN ('RUNNING','COMPENSATING','COMPLETED','COMPENSATED')),
    attempts int NOT NULL DEFAULT 0,
    next_run_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    step_started_at timestamptz NOT NULL,
    cancel_reason text NOT NULL DEFAULT '',
    last_error text NOT NULL DEFAULT '',
    version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE INDEX saga_instances_due ON saga_instances (next_run_at) WHERE state IN ('RUNNING','COMPENSATING');
CREATE INDEX saga_instances_running_job ON saga_instances (job_id) WHERE state = 'RUNNING';
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
