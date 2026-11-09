package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// reuseRevokeTimeout bounds the detached family revocation after reuse.
const reuseRevokeTimeout = 5 * time.Second

var errReuseDetected = errors.New("refresh token reuse")

// Refresh rotates a refresh token within its family. Rotation runs on the
// request ctx, so a cancelled client rolls it back and the old token stays
// valid. Presenting an already revoked token is treated as theft: the
// rotation transaction rolls back and the whole family is then revoked in a
// separate transaction on a detached ctx bounded by reuseRevokeTimeout, so a
// client disconnect cannot skip the revocation. The call still fails with
// ErrInvalidRefreshToken.
func (s *Service) Refresh(ctx context.Context, raw string) (TokenPair, error) {
	var pair TokenPair
	var famID, userID uuid.UUID
	err := s.Tx.WithTx(ctx, func(ctx context.Context) error {
		t, err := s.Tokens.GetByHashForUpdate(ctx, hashRefresh(raw))
		if errors.Is(err, apperr.ErrNotFound) {
			return domain.ErrInvalidRefreshToken
		}
		if err != nil {
			return err
		}
		now := s.Now()
		if t.RevokedAt != nil {
			famID, userID = t.FamilyID, t.UserID
			return errReuseDetected // rollback, nothing written yet
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
	if errors.Is(err, errReuseDetected) {
		s.Logger.WarnContext(ctx, "refresh token reuse detected", "event", "refresh_token_reuse",
			"user_id", userID, "family_id", famID)
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reuseRevokeTimeout)
		defer cancel()
		if err := s.Tx.WithTx(rctx, func(ctx context.Context) error {
			return s.Tokens.RevokeFamily(ctx, famID, s.Now())
		}); err != nil {
			s.Logger.ErrorContext(ctx, "refresh family revocation failed", "event", "refresh_token_reuse_revoke_failed",
				"user_id", userID, "family_id", famID, "err", err)
			return TokenPair{}, fmt.Errorf("revoke family: %w", err)
		}
		return TokenPair{}, domain.ErrInvalidRefreshToken
	}
	if err != nil {
		return TokenPair{}, err
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
