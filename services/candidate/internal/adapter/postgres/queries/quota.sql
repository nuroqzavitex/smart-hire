-- name: GetReservationForUpdate :one
SELECT application_id, candidate_id, day, state FROM quota_reservations
WHERE application_id = $1 FOR UPDATE;

-- name: IncrementQuota :one
-- Takes one unit unless the day's quota is used up (no row returned then).
INSERT INTO daily_quota (candidate_id, day, used) VALUES (sqlc.arg(candidate_id), sqlc.arg(day), 1)
ON CONFLICT (candidate_id, day) DO UPDATE SET used = daily_quota.used + 1
WHERE daily_quota.used < sqlc.arg(quota_limit)::int
RETURNING used;

-- name: DecrementQuota :exec
UPDATE daily_quota SET used = used - 1
WHERE candidate_id = $1 AND day = $2 AND used > 0;

-- name: InsertReservation :execrows
INSERT INTO quota_reservations (application_id, candidate_id, day, state)
VALUES ($1, $2, $3, $4)
ON CONFLICT (application_id) DO NOTHING;

-- name: MarkReservationReleased :exec
UPDATE quota_reservations SET state = 'RELEASED', updated_at = now()
WHERE application_id = $1;

-- name: DeleteReservation :exec
DELETE FROM quota_reservations WHERE application_id = $1;

-- name: DeleteCandidateQuota :exec
DELETE FROM daily_quota WHERE candidate_id = $1;

-- name: DeleteCandidateReservations :exec
DELETE FROM quota_reservations WHERE candidate_id = $1;
