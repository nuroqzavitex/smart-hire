-- name: CreateApplication :exec
INSERT INTO applications (id, job_id, candidate_id, recruiter_id, cv_id, status, reason, score,
    score_breakdown, model_version, idempotency_key, request_hash, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);

-- name: GetApplication :one
SELECT * FROM applications WHERE id = $1;

-- name: GetApplicationForUpdate :one
SELECT * FROM applications WHERE id = $1 FOR UPDATE;

-- name: GetApplicationByKey :one
SELECT * FROM applications WHERE candidate_id = $1 AND idempotency_key = $2;

-- name: UpdateApplication :exec
UPDATE applications SET status = $2, reason = $3, score = $4, score_breakdown = $5,
    model_version = $6, version = $7, updated_at = $8
WHERE id = $1;

-- name: ListApplications :many
-- Newest first; every filter is optional, the cursor is (created_at, id) of
-- the last row of the previous page.
SELECT * FROM applications
WHERE (sqlc.narg(candidate_id)::uuid IS NULL OR candidate_id = sqlc.narg(candidate_id))
  AND (sqlc.narg(recruiter_id)::uuid IS NULL OR recruiter_id = sqlc.narg(recruiter_id))
  AND (sqlc.narg(job_id)::uuid IS NULL OR job_id = sqlc.narg(job_id))
  AND (sqlc.narg(status)::text IS NULL OR status = sqlc.narg(status))
  AND (sqlc.narg(after_at)::timestamptz IS NULL
       OR (created_at, id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(row_limit);

-- name: InsertHistory :exec
INSERT INTO application_history (application_id, from_status, to_status, actor, reason, at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListHistory :many
SELECT from_status, to_status, actor, reason, at FROM application_history
WHERE application_id = $1 ORDER BY id;
