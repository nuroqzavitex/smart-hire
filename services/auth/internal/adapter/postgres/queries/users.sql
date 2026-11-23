-- name: CreateUser :exec
INSERT INTO users (id, email, password_hash, role, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetUserByEmail :one
SELECT id, email, password_hash, role, status, created_at, updated_at
FROM users WHERE email = $1;

-- name: GetUserByID :one
SELECT id, email, password_hash, role, status, created_at, updated_at
FROM users WHERE id = $1;

-- name: SetUserStatus :execrows
UPDATE users SET status = $2, updated_at = $3 WHERE id = $1;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: DeleteUserRegisteredOutbox :exec
-- user.registered carries the email; drop it with the account (ADR 0014).
DELETE FROM outbox WHERE aggregate_id = sqlc.arg(user_id)::text AND event_type = 'user.registered';
