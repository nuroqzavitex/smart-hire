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

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
)

const (
	reservationReserved = "RESERVED"
	reservationReleased = "RELEASED"
)

// QuotaRepo implements app.QuotaRepo. The reservation row is written first,
// so concurrent calls for one application serialize on its primary key and
// the daily counter is touched at most once per application.
type QuotaRepo struct{ pool *pgxpool.Pool }

// NewQuotaRepo returns a QuotaRepo over pool.
func NewQuotaRepo(pool *pgxpool.Pool) *QuotaRepo { return &QuotaRepo{pool: pool} }

// Reserve implements app.QuotaRepo; call it inside a transaction.
func (r *QuotaRepo) Reserve(ctx context.Context, appID, candidateID uuid.UUID, day time.Time, limit int) (app.QuotaResult, error) {
	q := queries(ctx, r.pool)
	d := pgtype.Date{Time: day, Valid: true}
	n, err := q.InsertReservation(ctx, sqlc.InsertReservationParams{
		ApplicationID: appID, CandidateID: &candidateID, Day: d, State: reservationReserved,
	})
	if err != nil {
		return 0, fmt.Errorf("insert reservation: %w", err)
	}
	if n == 0 {
		// Already reserved or released (possibly by a concurrent call, whose
		// transaction this read waits for).
		row, err := q.GetReservationForUpdate(ctx, appID)
		if err != nil {
			return 0, fmt.Errorf("read reservation: %w", err)
		}
		if row.State == reservationReleased {
			return app.QuotaAlreadyReleased, nil
		}
		return app.QuotaReserved, nil
	}
	if _, err := q.IncrementQuota(ctx, sqlc.IncrementQuotaParams{
		CandidateID: candidateID, Day: d, QuotaLimit: int32(limit), //nolint:gosec // limit is a small config value
	}); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("increment quota: %w", err)
		}
		// Used up: nothing is held, so a later retry may succeed.
		if err := q.DeleteReservation(ctx, appID); err != nil {
			return 0, fmt.Errorf("delete reservation: %w", err)
		}
		return app.QuotaExhausted, nil
	}
	return app.QuotaReserved, nil
}

// Release implements app.QuotaRepo; call it inside a transaction.
func (r *QuotaRepo) Release(ctx context.Context, appID uuid.UUID) error {
	q := queries(ctx, r.pool)
	n, err := q.InsertReservation(ctx, sqlc.InsertReservationParams{ApplicationID: appID, State: reservationReleased})
	if err != nil {
		return fmt.Errorf("insert tombstone: %w", err)
	}
	if n == 1 {
		return nil // nothing was reserved; the tombstone blocks a late reserve
	}
	row, err := q.GetReservationForUpdate(ctx, appID)
	if err != nil {
		return fmt.Errorf("read reservation: %w", err)
	}
	if row.State == reservationReleased {
		return nil
	}
	if err := q.MarkReservationReleased(ctx, appID); err != nil {
		return fmt.Errorf("release reservation: %w", err)
	}
	if row.CandidateID == nil {
		return nil
	}
	if err := q.DecrementQuota(ctx, sqlc.DecrementQuotaParams{CandidateID: *row.CandidateID, Day: row.Day}); err != nil {
		return fmt.Errorf("decrement quota: %w", err)
	}
	return nil
}

// DeleteCandidate removes the candidate's daily counters and reservations.
func (r *QuotaRepo) DeleteCandidate(ctx context.Context, candidateID uuid.UUID) error {
	q := queries(ctx, r.pool)
	if err := q.DeleteCandidateQuota(ctx, candidateID); err != nil {
		return fmt.Errorf("delete quota: %w", err)
	}
	if err := q.DeleteCandidateReservations(ctx, &candidateID); err != nil {
		return fmt.Errorf("delete reservations: %w", err)
	}
	return nil
}
