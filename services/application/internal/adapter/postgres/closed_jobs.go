package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres/sqlc"
)

// ClosedJobRepo implements app.ClosedJobs.
type ClosedJobRepo struct{ pool *pgxpool.Pool }

// NewClosedJobRepo returns a ClosedJobRepo over pool.
func NewClosedJobRepo(pool *pgxpool.Pool) *ClosedJobRepo { return &ClosedJobRepo{pool: pool} }

// MarkClosed implements app.ClosedJobs.
func (r *ClosedJobRepo) MarkClosed(ctx context.Context, jobID uuid.UUID, at time.Time) error {
	if err := queries(ctx, r.pool).MarkJobClosed(ctx, sqlc.MarkJobClosedParams{JobID: jobID, ClosedAt: at}); err != nil {
		return fmt.Errorf("mark job closed: %w", err)
	}
	return nil
}

// IsClosed implements app.ClosedJobs.
func (r *ClosedJobRepo) IsClosed(ctx context.Context, jobID uuid.UUID) (bool, error) {
	closed, err := queries(ctx, r.pool).IsJobClosed(ctx, jobID)
	if err != nil {
		return false, fmt.Errorf("check closed job: %w", err)
	}
	return closed, nil
}
