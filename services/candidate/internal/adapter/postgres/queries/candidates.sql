-- name: UpsertCandidate :one
INSERT INTO candidates (user_id, full_name, phone, location, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (user_id) DO UPDATE SET full_name = EXCLUDED.full_name, phone = EXCLUDED.phone,
    location = EXCLUDED.location, updated_at = EXCLUDED.updated_at
RETURNING user_id, full_name, phone, location, created_at, updated_at;

-- name: GetCandidate :one
SELECT user_id, full_name, phone, location, created_at, updated_at
FROM candidates WHERE user_id = $1;
