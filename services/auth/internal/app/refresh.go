package app

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// Refresh rotates a refresh token within its family. Presenting an already
// revoked token is treated as theft: the whole family is revoked and that
// revocation is committed (the closure returns nil) even though the call then
// fails with ErrInvalidRefreshToken. The transaction runs detached from ctx
// cancellation so a client disconnect cannot skip the family revocation.
func (s *Service) Refresh(ctx context.Context, raw string) (TokenPair, error) {
	var pair TokenPair
	var reuse bool
	var famID, userID uuid.UUID
	err := s.Tx.WithTx(context.WithoutCancel(ctx), func(ctx context.Context) error {
		t, err := s.Tokens.GetByHashForUpdate(ctx, hashRefresh(raw))
		if errors.Is(err, apperr.ErrNotFound) {
			return domain.ErrInvalidRefreshToken
		}
		if err != nil {
			return err
		}
		now := s.Now()
		if t.RevokedAt != nil {
			reuse, famID, userID = true, t.FamilyID, t.UserID
			return s.Tokens.RevokeFamily(ctx, t.FamilyID, now) // nil -> COMMIT
		}
		if !now.Before(t.ExpiresAt) {
			return domain.ErrInvalidRefreshToken
		}
		u, err := s.Users.GetByID(ctx, t.UserID)
		if err != nil {
			return err
		}
		if u.CanAuthenticate() != nil {
			return domain.ErrInvalidRefreshToken // not ACTIVE: 401, family untouched
		}
		var newID uuid.UUID
		pair, newID, err = s.issuePair(ctx, u, t.FamilyID) // assign outer vars
		if err != nil {
			return err
		}
		return s.Tokens.Revoke(ctx, t.ID, now, &newID)
	})
	if err != nil {
		return TokenPair{}, err
	}
	if reuse {
		s.Logger.WarnContext(ctx, "refresh token reuse detected", "event", "refresh_token_reuse",
			"user_id", userID, "family_id", famID)
		return TokenPair{}, domain.ErrInvalidRefreshToken
	}
	return pair, nil
}

// Logout revokes the presented refresh token. Unknown or already revoked
// tokens are not an error.
func (s *Service) Logout(ctx context.Context, raw string) error {
	return s.Tx.WithTx(ctx, func(ctx context.Context) error {
		t, err := s.Tokens.GetByHashForUpdate(ctx, hashRefresh(raw))
		if errors.Is(err, apperr.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if t.RevokedAt != nil {
			return nil
		}
		return s.Tokens.Revoke(ctx, t.ID, s.Now(), nil)
	})
}

// GetMe returns the user with the given ID.
func (s *Service) GetMe(ctx context.Context, userID uuid.UUID) (domain.User, error) {
	return s.Users.GetByID(ctx, userID)
}
