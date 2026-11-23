package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

const (
	uniqueViolation       = "23505"
	idempotencyConstraint = "applications_idempotency_key"
	liveJobConstraint     = "applications_live_job_candidate"
)

// ApplicationRepo implements app.ApplicationRepo.
type ApplicationRepo struct{ pool *pgxpool.Pool }

// NewApplicationRepo returns an ApplicationRepo over pool.
func NewApplicationRepo(pool *pgxpool.Pool) *ApplicationRepo { return &ApplicationRepo{pool: pool} }

// Create implements app.ApplicationRepo.
func (r *ApplicationRepo) Create(ctx context.Context, a domain.Application) error {
	err := queries(ctx, r.pool).CreateApplication(ctx, sqlc.CreateApplicationParams{
		ID: a.ID, JobID: a.JobID, CandidateID: a.CandidateID, RecruiterID: a.RecruiterID, CvID: a.CVID,
		Status: string(a.Status), Reason: a.Reason, Score: a.Score, ScoreBreakdown: a.Breakdown,
		ModelVersion: a.ModelVersion, IdempotencyKey: a.IdempotencyKey, RequestHash: a.RequestHash,
		Version: a.Version, CreatedAt: a.CreatedAt, UpdatedAt: a.UpdatedAt,
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		switch pgErr.ConstraintName {
		case idempotencyConstraint:
			return domain.ErrIdempotencyKeyConflict
		case liveJobConstraint:
			return domain.ErrAlreadyApplied
		}
	}
	if err != nil {
		return fmt.Errorf("create application: %w", err)
	}
	return nil
}

// Get implements app.ApplicationRepo.
func (r *ApplicationRepo) Get(ctx context.Context, id uuid.UUID) (domain.Application, error) {
	return one(queries(ctx, r.pool).GetApplication(ctx, id))
}

// GetForUpdate implements app.ApplicationRepo.
func (r *ApplicationRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Application, error) {
	return one(queries(ctx, r.pool).GetApplicationForUpdate(ctx, id))
}

// GetByKey implements app.ApplicationRepo.
func (r *ApplicationRepo) GetByKey(ctx context.Context, candidateID uuid.UUID, key string) (domain.Application, error) {
	return one(queries(ctx, r.pool).GetApplicationByKey(ctx, sqlc.GetApplicationByKeyParams{
		CandidateID: candidateID, IdempotencyKey: key,
	}))
}

// Update implements app.ApplicationRepo.
func (r *ApplicationRepo) Update(ctx context.Context, a domain.Application) error {
	if err := queries(ctx, r.pool).UpdateApplication(ctx, sqlc.UpdateApplicationParams{
		ID: a.ID, Status: string(a.Status), Reason: a.Reason, Score: a.Score, ScoreBreakdown: a.Breakdown,
		ModelVersion: a.ModelVersion, Version: a.Version, UpdatedAt: a.UpdatedAt,
	}); err != nil {
		return fmt.Errorf("update application: %w", err)
	}
	return nil
}

// List implements app.ApplicationRepo.
func (r *ApplicationRepo) List(ctx context.Context, f app.ListFilter, after *app.Cursor, limit int) ([]domain.Application, error) {
	p := sqlc.ListApplicationsParams{
		CandidateID: f.CandidateID, RecruiterID: f.RecruiterID, JobID: f.JobID,
		RowLimit: int32(limit), //nolint:gosec // limit ≤ 101
	}
	if f.Status != nil {
		p.Status = pgtype.Text{String: string(*f.Status), Valid: true}
	}
	if after != nil {
		p.AfterAt, p.AfterID = &after.At, &after.ID
	}
	rows, err := queries(ctx, r.pool).ListApplications(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list applications: %w", err)
	}
	out := make([]domain.Application, len(rows))
	for i, row := range rows {
		out[i] = toDomain(row)
	}
	return out, nil
}

// AddHistory implements app.ApplicationRepo.
func (r *ApplicationRepo) AddHistory(ctx context.Context, appID uuid.UUID, h domain.HistoryEntry) error {
	p := sqlc.InsertHistoryParams{ApplicationID: appID, ToStatus: string(h.To), Actor: h.Actor, Reason: h.Reason, At: h.At}
	if h.From != nil {
		p.FromStatus = pgtype.Text{String: string(*h.From), Valid: true}
	}
	if err := queries(ctx, r.pool).InsertHistory(ctx, p); err != nil {
		return fmt.Errorf("insert history: %w", err)
	}
	return nil
}

// History implements app.ApplicationRepo.
func (r *ApplicationRepo) History(ctx context.Context, appID uuid.UUID) ([]domain.HistoryEntry, error) {
	rows, err := queries(ctx, r.pool).ListHistory(ctx, appID)
	if err != nil {
		return nil, fmt.Errorf("list history: %w", err)
	}
	out := make([]domain.HistoryEntry, len(rows))
	for i, row := range rows {
		out[i] = domain.HistoryEntry{To: domain.Status(row.ToStatus), Actor: row.Actor, Reason: row.Reason, At: row.At}
		if row.FromStatus.Valid {
			from := domain.Status(row.FromStatus.String)
			out[i].From = &from
		}
	}
	return out, nil
}

func one(row sqlc.Application, err error) (domain.Application, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Application{}, domain.ErrApplicationNotFound
	}
	if err != nil {
		return domain.Application{}, fmt.Errorf("get application: %w", err)
	}
	return toDomain(row), nil
}

func toDomain(r sqlc.Application) domain.Application {
	return domain.Application{
		ID: r.ID, JobID: r.JobID, CandidateID: r.CandidateID, RecruiterID: r.RecruiterID, CVID: r.CvID,
		Status: domain.Status(r.Status), Reason: r.Reason, Score: r.Score, Breakdown: r.ScoreBreakdown,
		ModelVersion: r.ModelVersion, IdempotencyKey: r.IdempotencyKey, RequestHash: r.RequestHash,
		Version: r.Version, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}
