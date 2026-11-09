package domain

import "github.com/nuroqzavitex/smarthire/pkg/apperr"

// Domain errors surfaced to callers. Credential errors are deliberately
// generic so they never reveal whether an email exists.
var (
	ErrInvalidCredentials     = apperr.New(apperr.ErrUnauthenticated, "invalid email or password")
	ErrInvalidRefreshToken    = apperr.New(apperr.ErrUnauthenticated, "invalid refresh token")
	ErrAccountLocked          = apperr.New(apperr.ErrForbidden, "account is locked")
	ErrAccountPendingDeletion = apperr.New(apperr.ErrForbidden, "account is pending deletion")
)
