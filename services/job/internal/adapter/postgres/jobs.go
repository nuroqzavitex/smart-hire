package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

// JobRepo implements app.JobRepo.
type JobRepo struct{ pool *pgxpool.Pool }

// NewJobRepo returns a JobRepo over pool.
func NewJobRepo(pool *pgxpool.Pool) *JobRepo { return &JobRepo{pool: pool} }

// Create inserts j.
func (r *JobRepo) Create(ctx context.Context, j domain.Job) error {
	err := queries(ctx, r.pool).InsertJob(ctx, sqlc.InsertJobParams{
		ID: j.ID, RecruiterID: j.RecruiterID, Title: j.Title, Description: j.Description,
		RequiredSkills: nonNil(j.RequiredSkills), NiceToHaveSkills: nonNil(j.NiceToHaveSkills),
		Level: string(j.Level), Location: j.Location,
		SalaryMin: toInt4(j.SalaryMin), SalaryMax: toInt4(j.SalaryMax),
		Status: string(j.Status), Version: j.Version,
		PublishedAt: j.PublishedAt, DeletedAt: j.DeletedAt,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// Get returns a non-deleted job.
func (r *JobRepo) Get(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	row, err := queries(ctx, r.pool).GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, mapJobNotFound(err, "get job")
	}
	return toJob(row), nil
}

// GetForUpdate is Get with a row lock (SELECT ... FOR UPDATE); call it inside a tx.
func (r *JobRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	row, err := queries(ctx, r.pool).GetJobForUpdate(ctx, id)
	if err != nil {
		return domain.Job{}, mapJobNotFound(err, "get job for update")
	}
	return toJob(sqlc.GetJobRow(row)), nil
}

// Update writes every mutable field of j.
func (r *JobRepo) Update(ctx context.Context, j domain.Job) error {
	n, err := queries(ctx, r.pool).UpdateJob(ctx, sqlc.UpdateJobParams{
		ID: j.ID, Title: j.Title, Description: j.Description,
		RequiredSkills: nonNil(j.RequiredSkills), NiceToHaveSkills: nonNil(j.NiceToHaveSkills),
		Level: string(j.Level), Location: j.Location,
		SalaryMin: toInt4(j.SalaryMin), SalaryMax: toInt4(j.SalaryMax),
		Status: string(j.Status), Version: j.Version,
		PublishedAt: j.PublishedAt, DeletedAt: j.DeletedAt, UpdatedAt: j.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	if n == 0 {
		return domain.ErrJobNotFound
	}
	return nil
}

// Search lists open jobs, ranked by text relevance when f.Query is set.
// Ordering is (rank,) published_at, id, all descending, matching app.Cursor.
func (r *JobRepo) Search(ctx context.Context, f app.SearchFilter, after *app.Cursor, limit int) ([]app.SearchHit, error) {
	q := queries(ctx, r.pool)
	var skills []string
	if len(f.Skills) > 0 {
		skills = f.Skills
	}
	loc := toText(f.Location)
	var level pgtype.Text
	if f.Level != nil {
		level = pgtype.Text{String: string(*f.Level), Valid: true}
	}
	sal := toInt4(f.SalaryMin)
	var afterID *uuid.UUID
	var afterAt *time.Time
	var afterRank pgtype.Float4
	if after != nil {
		afterID, afterAt = &after.ID, &after.At
		if after.Rank != nil {
			afterRank = pgtype.Float4{Float32: *after.Rank, Valid: true}
		}
	}
	if f.Query != "" {
		rows, err := q.SearchOpenJobs(ctx, sqlc.SearchOpenJobsParams{
			Q: f.Query, Skills: skills, Location: loc, Level: level, SalaryMin: sal,
			AfterID: afterID, AfterRank: afterRank, AfterAt: afterAt, Lim: int32(limit), //nolint:gosec // limit is capped by the app layer
		})
		if err != nil {
			return nil, fmt.Errorf("search jobs: %w", err)
		}
		hits := make([]app.SearchHit, len(rows))
		for i, row := range rows {
			hits[i] = app.SearchHit{Rank: row.Rank, Job: toJob(sqlc.GetJobRow{
				ID: row.ID, RecruiterID: row.RecruiterID, Title: row.Title, Description: row.Description,
				RequiredSkills: row.RequiredSkills, NiceToHaveSkills: row.NiceToHaveSkills,
				Level: row.Level, Location: row.Location, SalaryMin: row.SalaryMin, SalaryMax: row.SalaryMax,
				Status: row.Status, Version: row.Version, PublishedAt: row.PublishedAt, DeletedAt: row.DeletedAt,
				CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
			})}
		}
		return hits, nil
	}
	rows, err := q.ListOpenJobs(ctx, sqlc.ListOpenJobsParams{
		Skills: skills, Location: loc, Level: level, SalaryMin: sal,
		AfterID: afterID, AfterAt: afterAt, Lim: int32(limit), //nolint:gosec // limit is capped by the app layer
	})
	if err != nil {
		return nil, fmt.Errorf("list open jobs: %w", err)
	}
	hits := make([]app.SearchHit, len(rows))
	for i, row := range rows {
		hits[i] = app.SearchHit{Job: toJob(sqlc.GetJobRow(row))}
	}
	return hits, nil
}

// ListByRecruiter lists a recruiter's non-deleted jobs, newest first.
func (r *JobRepo) ListByRecruiter(ctx context.Context, recruiterID uuid.UUID, status *domain.Status, after *app.Cursor, limit int) ([]domain.Job, error) {
	p := sqlc.ListJobsByRecruiterParams{RecruiterID: recruiterID, Lim: int32(limit)} //nolint:gosec // limit is capped by the app layer
	if status != nil {
		p.Status = pgtype.Text{String: string(*status), Valid: true}
	}
	if after != nil {
		p.AfterID, p.AfterAt = &after.ID, &after.At
	}
	rows, err := queries(ctx, r.pool).ListJobsByRecruiter(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list jobs by recruiter: %w", err)
	}
	jobs := make([]domain.Job, len(rows))
	for i, row := range rows {
		jobs[i] = toJob(sqlc.GetJobRow(row))
	}
	return jobs, nil
}

func mapJobNotFound(err error, op string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrJobNotFound
	}
	return fmt.Errorf("%s: %w", op, err)
}

func toJob(r sqlc.GetJobRow) domain.Job {
	return domain.Job{
		ID: r.ID, RecruiterID: r.RecruiterID, Title: r.Title, Description: r.Description,
		RequiredSkills: r.RequiredSkills, NiceToHaveSkills: r.NiceToHaveSkills,
		Level: domain.Level(r.Level), Location: r.Location,
		SalaryMin: fromInt4(r.SalaryMin), SalaryMax: fromInt4(r.SalaryMax),
		Status: domain.Status(r.Status), Version: r.Version,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
		PublishedAt: r.PublishedAt, DeletedAt: r.DeletedAt,
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func toInt4(v *int) pgtype.Int4 {
	if v == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: int32(*v), Valid: true} //nolint:gosec // salary range is small
}

func fromInt4(v pgtype.Int4) *int {
	if !v.Valid {
		return nil
	}
	n := int(v.Int32)
	return &n
}

func toText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}
