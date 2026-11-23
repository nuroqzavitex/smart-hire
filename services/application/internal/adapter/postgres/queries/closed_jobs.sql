-- name: MarkJobClosed :exec
INSERT INTO closed_jobs (job_id, closed_at) VALUES ($1, $2) ON CONFLICT (job_id) DO NOTHING;

-- name: IsJobClosed :one
SELECT EXISTS (SELECT 1 FROM closed_jobs WHERE job_id = $1);
