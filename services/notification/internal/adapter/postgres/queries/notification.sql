-- name: UpsertUser :exec
-- A tombstone (deleted account) is never overwritten.
INSERT INTO users (user_id, email, role, updated_at) VALUES ($1, $2, $3, now())
ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email, role = EXCLUDED.role, updated_at = now()
WHERE NOT users.deleted;

-- name: GetUser :one
SELECT user_id, email, role, deleted FROM users WHERE user_id = $1;

-- name: TombstoneUser :exec
INSERT INTO users (user_id, email, role, deleted, updated_at) VALUES ($1, '', '', true, now())
ON CONFLICT (user_id) DO UPDATE SET email = '', role = '', deleted = true, updated_at = now();

-- name: ClearJobRecruiter :exec
UPDATE jobs SET recruiter_id = NULL WHERE recruiter_id = $1;

-- name: DeleteUserEmails :exec
DELETE FROM email_outbox WHERE notification_id IN (SELECT id FROM notifications WHERE user_id = $1);

-- name: DeleteUserNotifications :execrows
DELETE FROM notifications WHERE user_id = $1;

-- name: UpsertJob :execrows
-- Writes only when the stored version is older (or absent).
INSERT INTO jobs (job_id, recruiter_id, title, version, deleted) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (job_id) DO UPDATE SET recruiter_id = COALESCE(EXCLUDED.recruiter_id, jobs.recruiter_id),
    title = CASE WHEN EXCLUDED.deleted THEN jobs.title ELSE EXCLUDED.title END,
    version = EXCLUDED.version, deleted = EXCLUDED.deleted
WHERE jobs.version < EXCLUDED.version;

-- name: GetJob :one
SELECT job_id, recruiter_id, title, version, deleted FROM jobs WHERE job_id = $1;

-- name: CreateNotification :exec
INSERT INTO notifications (id, user_id, type, title, body, data, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListNotifications :many
SELECT * FROM notifications
WHERE user_id = sqlc.arg(user_id)
  AND (NOT sqlc.arg(unread_only)::bool OR read_at IS NULL)
  AND (sqlc.narg(after_at)::timestamptz IS NULL
       OR (created_at, id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(row_limit);

-- name: CountUnread :one
SELECT count(*)::int FROM notifications WHERE user_id = $1 AND read_at IS NULL;

-- name: MarkRead :one
UPDATE notifications SET read_at = COALESCE(read_at, sqlc.arg(at)::timestamptz)
WHERE id = sqlc.arg(id) AND user_id = sqlc.arg(user_id)
RETURNING *;

-- name: EnqueueEmail :exec
INSERT INTO email_outbox (id, notification_id, to_addr, subject, html_body, text_body, status, attempts, next_attempt_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', 0, $7, $8);

-- name: ClaimDueEmails :many
UPDATE email_outbox SET next_attempt_at = sqlc.arg(lease_until)
WHERE id IN (
    SELECT e.id FROM email_outbox e
    WHERE e.status = 'PENDING' AND e.next_attempt_at <= sqlc.arg(now)
    ORDER BY e.next_attempt_at
    LIMIT sqlc.arg(batch)
    FOR UPDATE SKIP LOCKED)
RETURNING *;

-- name: MarkEmailSent :exec
UPDATE email_outbox SET status = 'SENT', sent_at = $2, attempts = attempts + 1, last_error = '' WHERE id = $1;

-- name: MarkEmailRetry :exec
UPDATE email_outbox SET attempts = $2, next_attempt_at = $3, last_error = $4 WHERE id = $1;

-- name: MarkEmailFailed :exec
UPDATE email_outbox SET status = 'FAILED', attempts = $2, last_error = $3 WHERE id = $1;
