-- name: CreateDeletionRequest :exec
INSERT INTO deletion_requests (id, user_id, role, status, expected_services, purged_services,
    requested_at, last_published_at, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: GetOpenDeletionRequestByUser :one
SELECT id, user_id, role, status, expected_services, purged_services, requested_at, last_published_at, completed_at
FROM deletion_requests WHERE user_id = $1 AND status <> 'COMPLETED';

-- name: GetDeletionRequest :one
SELECT id, user_id, role, status, expected_services, purged_services, requested_at, last_published_at, completed_at
FROM deletion_requests WHERE id = $1;

-- name: GetDeletionRequestForUpdate :one
SELECT id, user_id, role, status, expected_services, purged_services, requested_at, last_published_at, completed_at
FROM deletion_requests WHERE id = $1 FOR UPDATE;

-- name: UpdateDeletionRequest :exec
UPDATE deletion_requests SET status = $2, purged_services = $3, last_published_at = $4, completed_at = $5
WHERE id = $1;

-- name: ClaimDeletionSweep :many
SELECT id, user_id, role, status, expected_services, purged_services, requested_at, last_published_at, completed_at
FROM deletion_requests
WHERE status <> 'COMPLETED'
  AND (last_published_at < sqlc.arg(republish_before)::timestamptz
       OR (status = 'PENDING' AND requested_at < sqlc.arg(stuck_before)::timestamptz))
ORDER BY last_published_at
LIMIT sqlc.arg(lim)
FOR UPDATE SKIP LOCKED;

-- name: CountDeletionRequestsByStatus :many
SELECT status, count(*)::bigint AS n FROM deletion_requests GROUP BY status;
