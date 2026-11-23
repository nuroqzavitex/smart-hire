-- Account-deletion saga (spec §6.5, ADR 0014). Auth only tracks which services
-- confirmed the purge; rows hold no PII and outlive the user as the proof.
CREATE TABLE deletion_requests (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    role text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'COMPLETED', 'STUCK')),
    expected_services text[] NOT NULL,
    purged_services text[] NOT NULL DEFAULT '{}',
    requested_at timestamptz NOT NULL,
    last_published_at timestamptz NOT NULL,
    completed_at timestamptz
);
-- At most one open request per user.
CREATE UNIQUE INDEX deletion_requests_open_user ON deletion_requests (user_id) WHERE status <> 'COMPLETED';
CREATE INDEX deletion_requests_sweep ON deletion_requests (last_published_at) WHERE status <> 'COMPLETED';

CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
