-- name: CreateUser :exec
INSERT INTO users (id, email, password_hash, role, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: GetUserByEmail :one
SELECT id, email, password_hash, role, status, created_at, updated_at
FROM users WHERE email = $1;

-- name: GetUserByID :one
SELECT id, email, password_hash, role, status, created_at, updated_at
FROM users WHERE id = $1;
