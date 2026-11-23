-- name: CreateSaga :exec
INSERT INTO saga_instances (id, application_id, job_id, step, state, attempts, next_run_at,
    deadline_at, step_started_at, cancel_reason, last_error, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- name: ClaimSagas :many
-- Leases due sagas: next_run_at moves past the lease so no other worker
-- claims them until it expires; version changes so a previous holder's
-- write is rejected.
UPDATE saga_instances SET next_run_at = sqlc.arg(lease_until), attempts = attempts + 1,
    version = version + 1, updated_at = sqlc.arg(now)
WHERE id IN (
    SELECT id FROM saga_instances
    WHERE state IN ('RUNNING', 'COMPENSATING') AND next_run_at <= sqlc.arg(now)
    ORDER BY next_run_at
    LIMIT sqlc.arg(batch)
    FOR UPDATE SKIP LOCKED)
RETURNING *;

-- name: SaveSaga :execrows
UPDATE saga_instances SET step = $3, state = $4, attempts = $5, next_run_at = $6,
    step_started_at = $7, cancel_reason = $8, last_error = $9, version = version + 1, updated_at = $10
WHERE id = $1 AND version = $2;

-- name: GetSagaByApplication :one
SELECT * FROM saga_instances WHERE application_id = $1;

-- name: CancelRunningSagasForJob :execrows
UPDATE saga_instances SET state = 'COMPENSATING', step = 'RELEASE_QUOTA', cancel_reason = sqlc.arg(reason),
    next_run_at = sqlc.arg(now), attempts = 0, version = version + 1, updated_at = sqlc.arg(now)
WHERE job_id = sqlc.arg(job_id) AND state = 'RUNNING';

-- name: CountSagasByState :many
SELECT state, count(*)::int AS n FROM saga_instances GROUP BY state;
