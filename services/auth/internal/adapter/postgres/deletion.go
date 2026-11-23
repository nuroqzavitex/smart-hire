package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/idempotency"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// DeletionRepo implements app.DeletionRepo.
type DeletionRepo struct{ pool *pgxpool.Pool }

// NewDeletionRepo returns a DeletionRepo over pool.
func NewDeletionRepo(pool *pgxpool.Pool) *DeletionRepo { return &DeletionRepo{pool: pool} }

// Create inserts r; a second open request of the user is apperr.ErrConflict.
func (r *DeletionRepo) Create(ctx context.Context, d domain.DeletionRequest) error {
	err := queries(ctx, r.pool).CreateDeletionRequest(ctx, sqlc.CreateDeletionRequestParams{
		ID: d.ID, UserID: d.UserID, Role: string(d.Role), Status: string(d.Status),
		ExpectedServices: d.Expected, PurgedServices: nonNil(d.Purged),
		RequestedAt: d.RequestedAt, LastPublishedAt: d.LastPublishedAt, CompletedAt: d.CompletedAt,
	})
	if isUniqueViolation(err) {
		return apperr.New(apperr.ErrConflict, "account deletion already requested")
	}
	if err != nil {
		return fmt.Errorf("insert deletion request: %w", err)
	}
	return nil
}

// GetActiveByUser returns the user's request that is not COMPLETED.
func (r *DeletionRepo) GetActiveByUser(ctx context.Context, userID uuid.UUID) (domain.DeletionRequest, error) {
	row, err := queries(ctx, r.pool).GetOpenDeletionRequestByUser(ctx, userID)
	return toDeletion(row, err)
}

// Get returns a request by ID.
func (r *DeletionRepo) Get(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error) {
	row, err := queries(ctx, r.pool).GetDeletionRequest(ctx, id)
	return toDeletion(row, err)
}

// GetForUpdate is Get with a row lock.
func (r *DeletionRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.DeletionRequest, error) {
	row, err := queries(ctx, r.pool).GetDeletionRequestForUpdate(ctx, id)
	return toDeletion(row, err)
}

// Update writes the mutable fields.
func (r *DeletionRepo) Update(ctx context.Context, d domain.DeletionRequest) error {
	err := queries(ctx, r.pool).UpdateDeletionRequest(ctx, sqlc.UpdateDeletionRequestParams{
		ID: d.ID, Status: string(d.Status), PurgedServices: nonNil(d.Purged),
		LastPublishedAt: d.LastPublishedAt, CompletedAt: d.CompletedAt,
	})
	if err != nil {
		return fmt.Errorf("update deletion request: %w", err)
	}
	return nil
}

// ClaimSweep locks unfinished requests due for a republish or the STUCK flag.
func (r *DeletionRepo) ClaimSweep(ctx context.Context, republishBefore, stuckBefore time.Time, limit int) ([]domain.DeletionRequest, error) {
	rows, err := queries(ctx, r.pool).ClaimDeletionSweep(ctx, sqlc.ClaimDeletionSweepParams{
		RepublishBefore: republishBefore, StuckBefore: stuckBefore, Lim: int32(min(limit, 1000)), //nolint:gosec // bounded
	})
	if err != nil {
		return nil, fmt.Errorf("claim deletion requests: %w", err)
	}
	out := make([]domain.DeletionRequest, 0, len(rows))
	for _, row := range rows {
		d, _ := toDeletion(row, nil)
		out = append(out, d)
	}
	return out, nil
}

// CountByStatus counts requests per status.
func (r *DeletionRepo) CountByStatus(ctx context.Context) (map[domain.DeletionStatus]int64, error) {
	rows, err := queries(ctx, r.pool).CountDeletionRequestsByStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("count deletion requests: %w", err)
	}
	out := map[domain.DeletionStatus]int64{}
	for _, row := range rows {
		out[domain.DeletionStatus(row.Status)] = row.N
	}
	return out, nil
}

func toDeletion(row sqlc.DeletionRequest, err error) (domain.DeletionRequest, error) {
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.DeletionRequest{}, domain.ErrDeletionNotFound
		}
		return domain.DeletionRequest{}, fmt.Errorf("get deletion request: %w", err)
	}
	return domain.DeletionRequest{
		ID: row.ID, UserID: row.UserID, Role: authz.Role(row.Role), Status: domain.DeletionStatus(row.Status),
		Expected: nonNil(row.ExpectedServices), Purged: nonNil(row.PurgedServices),
		RequestedAt: row.RequestedAt, LastPublishedAt: row.LastPublishedAt, CompletedAt: row.CompletedAt,
	}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// Idempotency implements app.Idempotency over processed_events.
type Idempotency struct{ pool *pgxpool.Pool }

// NewIdempotency returns an Idempotency over pool.
func NewIdempotency(pool *pgxpool.Pool) *Idempotency { return &Idempotency{pool: pool} }

// Once records eventID in the tx carried by ctx.
func (i *Idempotency) Once(ctx context.Context, eventID string) (bool, error) {
	return idempotency.Once(ctx, dbtx(ctx, i.pool), eventID)
}
