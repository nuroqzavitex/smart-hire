-- Daily application quota held by the application saga (ADR 0011).
CREATE TABLE daily_quota (
    candidate_id uuid NOT NULL,
    day date NOT NULL,
    used int NOT NULL CHECK (used >= 0),
    PRIMARY KEY (candidate_id, day)
);

-- One reservation per application. A RELEASED row without a prior
-- reservation is a tombstone: it stops a late ReserveQuota from taking quota.
CREATE TABLE quota_reservations (
    application_id uuid PRIMARY KEY,
    candidate_id uuid,
    day date,
    state text NOT NULL CHECK (state IN ('RESERVED', 'RELEASED')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
