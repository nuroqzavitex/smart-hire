package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// CandidateRepo implements app.CandidateRepo.
type CandidateRepo struct{ pool *pgxpool.Pool }

// NewCandidateRepo returns a CandidateRepo over pool.
func NewCandidateRepo(pool *pgxpool.Pool) *CandidateRepo { return &CandidateRepo{pool: pool} }

// Get returns domain.ErrProfileNotFound when userID has no profile.
func (r *CandidateRepo) Get(ctx context.Context, userID uuid.UUID) (domain.Candidate, error) {
	row, err := queries(ctx, r.pool).GetCandidate(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Candidate{}, domain.ErrProfileNotFound
	}
	if err != nil {
		return domain.Candidate{}, fmt.Errorf("get candidate: %w", err)
	}
	return toCandidate(row), nil
}

// Upsert inserts c or replaces the profile fields and updated_at of the
// existing row, keeping its created_at; it returns the stored row.
func (r *CandidateRepo) Upsert(ctx context.Context, c domain.Candidate) (domain.Candidate, error) {
	row, err := queries(ctx, r.pool).UpsertCandidate(ctx, sqlc.UpsertCandidateParams{
		UserID: c.UserID, FullName: c.FullName, Phone: toText(c.Phone), Location: toText(c.Location),
		CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	})
	if err != nil {
		return domain.Candidate{}, fmt.Errorf("upsert candidate: %w", err)
	}
	return toCandidate(row), nil
}

func toCandidate(r sqlc.Candidate) domain.Candidate {
	return domain.Candidate{
		UserID: r.UserID, FullName: r.FullName, Phone: fromText(r.Phone), Location: fromText(r.Location),
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func toText(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func fromText(t pgtype.Text) *string {
	if !t.Valid {
		return nil
	}
	return &t.String
}
