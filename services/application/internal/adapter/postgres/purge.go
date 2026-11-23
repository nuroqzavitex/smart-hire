package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/application/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
)

var _ app.UserPurger = (*PurgeRepo)(nil)

// PurgeRepo implements app.UserPurger.
type PurgeRepo struct{ pool *pgxpool.Pool }

// NewPurgeRepo returns a PurgeRepo over pool.
func NewPurgeRepo(pool *pgxpool.Pool) *PurgeRepo { return &PurgeRepo{pool: pool} }

// PurgeUser ends the candidate's sagas, cancels the live applications (with
// history), anonymises the user as a history actor, pseudonymises the
// candidate's applications and drops the user as recruiter. Run it in a tx.
func (r *PurgeRepo) PurgeUser(ctx context.Context, userID uuid.UUID, now time.Time) (app.PurgeStats, error) {
	q := queries(ctx, r.pool)
	var st app.PurgeStats
	if _, err := q.CompensateCandidateSagas(ctx, sqlc.CompensateCandidateSagasParams{Now: now, UserID: userID}); err != nil {
		return st, fmt.Errorf("compensate sagas: %w", err)
	}
	n, err := q.CancelCandidateApplications(ctx, sqlc.CancelCandidateApplicationsParams{Now: now, UserID: userID})
	if err != nil {
		return st, fmt.Errorf("cancel applications: %w", err)
	}
	st.Cancelled = int(n)
	if err := q.AnonymiseHistoryActor(ctx, userID.String()); err != nil {
		return st, fmt.Errorf("anonymise history: %w", err)
	}
	if n, err = q.PseudonymiseCandidateApplications(ctx, userID); err != nil {
		return st, fmt.Errorf("pseudonymise applications: %w", err)
	}
	st.Pseudonymised = int(n)
	if n, err = q.AnonymiseRecruiterApplications(ctx, userID); err != nil {
		return st, fmt.Errorf("anonymise recruiter: %w", err)
	}
	st.Recruiter = int(n)
	return st, nil
}
