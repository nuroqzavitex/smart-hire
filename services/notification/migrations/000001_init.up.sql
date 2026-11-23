-- Replicas from user.registered and job.* (ADR 0012). users holds the email
-- address (PII) so notifications can be mailed; M4b purges it on deletion.
CREATE TABLE users (
    user_id uuid PRIMARY KEY,
    email text NOT NULL DEFAULT '',
    role text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- deleted marks a tombstone so a late job.updated cannot bring a job back.
CREATE TABLE jobs (
    job_id uuid PRIMARY KEY,
    recruiter_id uuid,
    title text NOT NULL DEFAULT '',
    version bigint NOT NULL,
    deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE notifications (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    type text NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    data jsonb NOT NULL DEFAULT '{}',
    read_at timestamptz,
    created_at timestamptz NOT NULL
);
CREATE INDEX notifications_user ON notifications (user_id, created_at DESC, id DESC);
CREATE INDEX notifications_unread ON notifications (user_id) WHERE read_at IS NULL;

-- Email outbox: written in the notification's transaction, sent by a loop
-- that leases rows through next_attempt_at.
CREATE TABLE email_outbox (
    id uuid PRIMARY KEY,
    notification_id uuid NOT NULL REFERENCES notifications (id),
    to_addr text NOT NULL,
    subject text NOT NULL,
    html_body text NOT NULL,
    text_body text NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    attempts int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    last_error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    sent_at timestamptz
);
CREATE INDEX email_outbox_due ON email_outbox (next_attempt_at) WHERE status = 'PENDING';

CREATE TABLE processed_events (
    event_id text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
