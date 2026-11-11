-- name: InsertCV :exec
INSERT INTO cvs (id, candidate_id, object_key, file_name, status, attempt_id, parsed_data, parse_error, size_bytes, consent_at, parsed_at, version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);

-- name: GetCV :one
SELECT id, candidate_id, object_key, file_name, status, attempt_id, parsed_data, parse_error, size_bytes, consent_at, parsed_at, version, created_at, updated_at
FROM cvs WHERE id = $1;

-- name: GetCVForUpdate :one
SELECT id, candidate_id, object_key, file_name, status, attempt_id, parsed_data, parse_error, size_bytes, consent_at, parsed_at, version, created_at, updated_at
FROM cvs WHERE id = $1 FOR UPDATE;

-- name: UpdateCV :execrows
UPDATE cvs SET status = $2, attempt_id = $3, parsed_data = $4, parse_error = $5, size_bytes = $6,
    parsed_at = $7, version = $8, updated_at = $9
WHERE id = $1;

-- name: ListCVsByCandidate :many
SELECT id, candidate_id, object_key, file_name, status, attempt_id, parsed_data, parse_error, size_bytes, consent_at, parsed_at, version, created_at, updated_at
FROM cvs
WHERE candidate_id = sqlc.arg(candidate_id)
  AND (sqlc.narg(after_id)::uuid IS NULL OR
       (created_at, id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(lim);
