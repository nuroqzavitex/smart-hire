package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// RefreshTokenRepo implements app.RefreshTokenRepo.
type RefreshTokenRepo struct{ pool *pgxpool.Pool }

// NewRefreshTokenRepo returns a RefreshTokenRepo over pool.
func NewRefreshTokenRepo(pool *pgxpool.Pool) *RefreshTokenRepo { return &RefreshTokenRepo{pool: pool} }

// Create stores a hashed refresh token.
func (r *RefreshTokenRepo) Create(ctx context.Context, t domain.RefreshToken) error {
	err := queries(ctx, r.pool).CreateRefreshToken(ctx, sqlc.CreateRefreshTokenParams{
		ID: t.ID, UserID: t.UserID, FamilyID: t.FamilyID, TokenHash: t.Hash,
		ExpiresAt: t.ExpiresAt, CreatedAt: t.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// GetByHashForUpdate locks and returns the token row; it must run in a tx for
// the lock to outlive the call.
func (r *RefreshTokenRepo) GetByHashForUpdate(ctx context.Context, hash []byte) (domain.RefreshToken, error) {
	row, err := queries(ctx, r.pool).GetRefreshTokenByHashForUpdate(ctx, hash)
	if err != nil {
		return domain.RefreshToken{}, fmt.Errorf("get refresh token: %w", mapNotFound(err, "refresh token not found"))
	}
	return domain.RefreshToken{
		ID: row.ID, UserID: row.UserID, FamilyID: row.FamilyID, Hash: row.TokenHash,
		ExpiresAt: row.ExpiresAt, CreatedAt: row.CreatedAt, RevokedAt: row.RevokedAt, ReplacedBy: row.ReplacedBy,
	}, nil
}

// Revoke marks a token revoked, optionally recording its replacement.
func (r *RefreshTokenRepo) Revoke(ctx context.Context, id uuid.UUID, at time.Time, replacedBy *uuid.UUID) error {
	err := queries(ctx, r.pool).RevokeRefreshToken(ctx, sqlc.RevokeRefreshTokenParams{
		ID: id, RevokedAt: &at, ReplacedBy: replacedBy,
	})
	if err != nil {
		return fmt.Errorf("revoke refresh token: %w", err)
	}
	return nil
}

// RevokeFamily revokes every not-yet-revoked token of the family.
func (r *RefreshTokenRepo) RevokeFamily(ctx context.Context, familyID uuid.UUID, at time.Time) error {
	err := queries(ctx, r.pool).RevokeRefreshTokenFamily(ctx, sqlc.RevokeRefreshTokenFamilyParams{
		FamilyID: familyID, RevokedAt: &at,
	})
	if err != nil {
		return fmt.Errorf("revoke token family: %w", err)
	}
	return nil
}
