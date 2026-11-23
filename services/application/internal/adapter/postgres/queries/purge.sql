-- Account deletion (ADR 0014). Run in one transaction, in this order.

-- name: CompensateCandidateSagas :execrows
-- Ends the running sagas of the candidate; the version bump makes any write
-- of a worker that still holds a lease fail its version guard.
UPDATE saga_instances s
SET state = 'COMPENSATED', cancel_reason = 'account_deleted', version = s.version + 1,
    next_run_at = sqlc.arg(now)::timestamptz, updated_at = sqlc.arg(now)::timestamptz
FROM applications a
WHERE s.application_id = a.id AND a.candidate_id = sqlc.arg(user_id)::uuid
  AND s.state IN ('RUNNING', 'COMPENSATING');

-- name: CancelCandidateApplications :execrows
-- Cancels the candidate's live applications and records the history rows.
WITH live AS (
    SELECT id, status FROM applications
    WHERE candidate_id = sqlc.arg(user_id)::uuid AND status NOT IN ('HIRED', 'REJECTED', 'CANCELLED')
    FOR UPDATE
), cancelled AS (
    UPDATE applications a
    SET status = 'CANCELLED', reason = 'account_deleted', version = a.version + 1,
        updated_at = sqlc.arg(now)::timestamptz
    FROM live WHERE a.id = live.id
    RETURNING a.id
)
INSERT INTO application_history (application_id, from_status, to_status, actor, reason, at)
SELECT live.id, live.status, 'CANCELLED', 'system', 'account_deleted', sqlc.arg(now)::timestamptz
FROM live JOIN cancelled ON cancelled.id = live.id;

-- name: AnonymiseHistoryActor :exec
UPDATE application_history SET actor = 'deleted-user' WHERE actor = sqlc.arg(user_id)::text;

-- name: PseudonymiseCandidateApplications :execrows
-- A fresh random candidate_id per application keeps the statistics but drops
-- the link between the applications and to the person.
UPDATE applications
SET candidate_id = gen_random_uuid(), cv_id = '00000000-0000-0000-0000-000000000000',
    idempotency_key = 'purged:' || id::text, request_hash = '', score_breakdown = NULL
WHERE candidate_id = sqlc.arg(user_id)::uuid;

-- name: AnonymiseRecruiterApplications :execrows
UPDATE applications SET recruiter_id = '00000000-0000-0000-0000-000000000000'
WHERE recruiter_id = sqlc.arg(user_id)::uuid;
