package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// Login authenticates by email and password and issues a new token family.
// Unknown emails and wrong passwords are indistinguishable (error and timing);
// account status is only revealed after the password has been proven.
func (s *Service) Login(ctx context.Context, email, password string) (TokenPair, error) {
	u, err := s.lookup(ctx, email)
	if err != nil {
		// Burn a Verify so unknown emails cost the same as wrong passwords.
		if _, verr := s.Hasher.Verify(password, s.dummyHash); verr != nil {
			s.Logger.WarnContext(ctx, "dummy password verify failed", "err", verr)
		}
		if errors.Is(err, apperr.ErrNotFound) || errors.Is(err, apperr.ErrValidation) {
			return TokenPair{}, domain.ErrInvalidCredentials
		}
		return TokenPair{}, err
	}
	ok, err := s.Hasher.Verify(password, u.PasswordHash)
	if err != nil {
		return TokenPair{}, fmt.Errorf("verify password: %w", err)
	}
	if !ok {
		return TokenPair{}, domain.ErrInvalidCredentials
	}
	if err := u.CanAuthenticate(); err != nil {
		return TokenPair{}, err
	}
	pair, _, err := s.issuePair(ctx, u, uuid.New())
	return pair, err
}

func (s *Service) lookup(ctx context.Context, email string) (domain.User, error) {
	email, err := domain.NewEmail(email)
	if err != nil {
		return domain.User{}, err
	}
	return s.Users.GetByEmail(ctx, email)
}
