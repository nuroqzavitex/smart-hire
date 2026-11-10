-- name: InsertJob :exec
INSERT INTO jobs (id, recruiter_id, title, description, required_skills, nice_to_have_skills, level, location, salary_min, salary_max, status, version, published_at, deleted_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);

-- name: GetJob :one
SELECT id, recruiter_id, title, description, required_skills, nice_to_have_skills, level, location, salary_min, salary_max, status, version, published_at, deleted_at, created_at, updated_at
FROM jobs WHERE id = $1 AND deleted_at IS NULL;

-- name: GetJobForUpdate :one
SELECT id, recruiter_id, title, description, required_skills, nice_to_have_skills, level, location, salary_min, salary_max, status, version, published_at, deleted_at, created_at, updated_at
FROM jobs WHERE id = $1 AND deleted_at IS NULL FOR UPDATE;

-- name: UpdateJob :execrows
UPDATE jobs SET title = $2, description = $3, required_skills = $4, nice_to_have_skills = $5,
    level = $6, location = $7, salary_min = $8, salary_max = $9, status = $10, version = $11,
    published_at = $12, deleted_at = $13, updated_at = $14
WHERE id = $1;

-- name: SearchOpenJobs :many
WITH q AS (SELECT websearch_to_tsquery('simple', unaccent_immutable(sqlc.arg(q)::text)) AS tsq)
SELECT j.id, j.recruiter_id, j.title, j.description, j.required_skills, j.nice_to_have_skills, j.level, j.location, j.salary_min, j.salary_max, j.status, j.version, j.published_at, j.deleted_at, j.created_at, j.updated_at, ts_rank(j.search_vector, q.tsq)::real AS rank
FROM jobs j, q
WHERE j.status = 'open' AND j.deleted_at IS NULL AND j.search_vector @@ q.tsq
  AND (sqlc.narg(skills)::text[] IS NULL OR lower_text_array(j.required_skills) && sqlc.narg(skills)::text[])
  AND (sqlc.narg(location)::text IS NULL
       OR lower(unaccent_immutable(j.location)) = lower(unaccent_immutable(sqlc.narg(location)::text)))
  AND (sqlc.narg(level)::text IS NULL OR j.level = sqlc.narg(level)::text)
  AND (sqlc.narg(salary_min)::int IS NULL OR j.salary_max IS NULL OR j.salary_max >= sqlc.narg(salary_min)::int)
  AND (sqlc.narg(after_id)::uuid IS NULL OR
       (ts_rank(j.search_vector, q.tsq)::real, j.published_at, j.id)
         < (sqlc.narg(after_rank)::real, sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY rank DESC, j.published_at DESC, j.id DESC
LIMIT sqlc.arg(lim);

-- name: ListOpenJobs :many
SELECT j.id, j.recruiter_id, j.title, j.description, j.required_skills, j.nice_to_have_skills, j.level, j.location, j.salary_min, j.salary_max, j.status, j.version, j.published_at, j.deleted_at, j.created_at, j.updated_at
FROM jobs j
WHERE j.status = 'open' AND j.deleted_at IS NULL
  AND (sqlc.narg(skills)::text[] IS NULL OR lower_text_array(j.required_skills) && sqlc.narg(skills)::text[])
  AND (sqlc.narg(location)::text IS NULL
       OR lower(unaccent_immutable(j.location)) = lower(unaccent_immutable(sqlc.narg(location)::text)))
  AND (sqlc.narg(level)::text IS NULL OR j.level = sqlc.narg(level)::text)
  AND (sqlc.narg(salary_min)::int IS NULL OR j.salary_max IS NULL OR j.salary_max >= sqlc.narg(salary_min)::int)
  AND (sqlc.narg(after_id)::uuid IS NULL OR
       (j.published_at, j.id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY j.published_at DESC, j.id DESC
LIMIT sqlc.arg(lim);

-- name: ListJobsByRecruiter :many
SELECT id, recruiter_id, title, description, required_skills, nice_to_have_skills, level, location, salary_min, salary_max, status, version, published_at, deleted_at, created_at, updated_at
FROM jobs
WHERE recruiter_id = $1 AND deleted_at IS NULL
  AND (sqlc.narg(status)::text IS NULL OR status = sqlc.narg(status)::text)
  AND (sqlc.narg(after_id)::uuid IS NULL OR
       (created_at, id) < (sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(lim);
