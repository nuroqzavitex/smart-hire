package domain

import "github.com/nuroqzavitex/smarthire/pkg/apperr"

// Errors returned by the repositories and use cases.
var (
	ErrApplicationNotFound    = apperr.New(apperr.ErrNotFound, "application not found")
	ErrAlreadyApplied         = apperr.New(apperr.ErrConflict, "already applied to this job")
	ErrIdempotencyKeyConflict = apperr.New(apperr.ErrConflict, "idempotency key already used")
	ErrInvalidTransition      = apperr.New(apperr.ErrConflict, "invalid status transition")
)
