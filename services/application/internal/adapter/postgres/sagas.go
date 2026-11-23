package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// SagaRepo implements app.SagaRepo.
type SagaRepo struct{ pool *pgxpool.Pool }

// NewSagaRepo returns a SagaRepo over pool.
func NewSagaRepo(pool *pgxpool.Pool) *SagaRepo { return &SagaRepo{pool: pool} }

// Create implements app.SagaRepo.
func (r *SagaRepo) Create(ctx context.Context, s domain.Saga) error {
	if err := queries(ctx, r.pool).CreateSaga(ctx, sqlc.CreateSagaParams{
		ID: s.ID, ApplicationID: s.ApplicationID, JobID: s.JobID, Step: string(s.Step), State: string(s.State),
		Attempts:  int32(s.Attempts), //nolint:gosec // small counter
		NextRunAt: s.NextRunAt, DeadlineAt: s.DeadlineAt,
		StepStartedAt: s.StepStartedAt, CancelReason: s.CancelReason, LastError: s.LastError,
		Version: s.Version, CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}); err != nil {
		return fmt.Errorf("create saga: %w", err)
	}
	return nil
}

// Claim implements app.SagaRepo. Called without a transaction in ctx it is
// one statement, so the lease is committed before any outbound call.
func (r *SagaRepo) Claim(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Saga, error) {
	rows, err := queries(ctx, r.pool).ClaimSagas(ctx, sqlc.ClaimSagasParams{
		LeaseUntil: now.Add(lease), Now: now, Batch: int32(limit), //nolint:gosec // small batch size
	})
	if err != nil {
		return nil, fmt.Errorf("claim sagas: %w", err)
	}
	out := make([]domain.Saga, len(rows))
	for i, row := range rows {
		out[i] = sagaToDomain(row)
	}
	return out, nil
}

// Save implements app.SagaRepo.
func (r *SagaRepo) Save(ctx context.Context, s domain.Saga) (bool, error) {
	n, err := queries(ctx, r.pool).SaveSaga(ctx, sqlc.SaveSagaParams{
		ID: s.ID, Version: s.Version, Step: string(s.Step), State: string(s.State),
		Attempts:  int32(s.Attempts), //nolint:gosec // small counter
		NextRunAt: s.NextRunAt, StepStartedAt: s.StepStartedAt,
		CancelReason: s.CancelReason, LastError: s.LastError, UpdatedAt: s.UpdatedAt,
	})
	if err != nil {
		return false, fmt.Errorf("save saga: %w", err)
	}
	return n == 1, nil
}

// GetByApplication implements app.SagaRepo.
func (r *SagaRepo) GetByApplication(ctx context.Context, appID uuid.UUID) (domain.Saga, error) {
	row, err := queries(ctx, r.pool).GetSagaByApplication(ctx, appID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Saga{}, domain.ErrApplicationNotFound
	}
	if err != nil {
		return domain.Saga{}, fmt.Errorf("get saga: %w", err)
	}
	return sagaToDomain(row), nil
}

// CancelRunningForJob implements app.SagaRepo.
func (r *SagaRepo) CancelRunningForJob(ctx context.Context, jobID uuid.UUID, reason string, now time.Time) (int, error) {
	n, err := queries(ctx, r.pool).CancelRunningSagasForJob(ctx, sqlc.CancelRunningSagasForJobParams{
		Reason: reason, Now: now, JobID: jobID,
	})
	if err != nil {
		return 0, fmt.Errorf("cancel sagas: %w", err)
	}
	return int(n), nil
}

func sagaToDomain(r sqlc.SagaInstance) domain.Saga {
	return domain.Saga{
		ID: r.ID, ApplicationID: r.ApplicationID, JobID: r.JobID, Step: domain.SagaStep(r.Step),
		State: domain.SagaState(r.State), Attempts: int(r.Attempts), NextRunAt: r.NextRunAt,
		DeadlineAt: r.DeadlineAt, StepStartedAt: r.StepStartedAt, CancelReason: r.CancelReason,
		LastError: r.LastError, Version: r.Version, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}
