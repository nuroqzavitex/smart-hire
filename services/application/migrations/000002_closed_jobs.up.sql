-- Jobs seen closed or deleted through job.closed / job.deleted (both are
-- terminal). Submit and the RESERVE_QUOTA step check it, so an application
-- racing the event is still caught (ADR 0011).
CREATE TABLE closed_jobs (
    job_id uuid PRIMARY KEY,
    closed_at timestamptz NOT NULL
);
