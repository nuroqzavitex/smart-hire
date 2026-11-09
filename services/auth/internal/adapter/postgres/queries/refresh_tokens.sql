-- name: CreateRefreshToken :exec
INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: GetRefreshTokenByHashForUpdate :one
SELECT id, user_id, family_id, token_hash, expires_at, revoked_at, replaced_by, created_at
FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE;

-- name: RevokeRefreshToken :exec
UPDATE refresh_tokens SET revoked_at = $2, replaced_by = $3 WHERE id = $1;

-- name: RevokeRefreshTokenFamily :exec
UPDATE refresh_tokens SET revoked_at = $2 WHERE family_id = $1 AND revoked_at IS NULL;
