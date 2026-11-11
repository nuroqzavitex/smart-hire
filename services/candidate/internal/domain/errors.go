package domain

import (
	"errors"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// Domain errors surfaced to callers.
var (
	ErrInvalidTransition = apperr.New(apperr.ErrValidation, "invalid cv status transition")
	ErrCVNotFound        = apperr.New(apperr.ErrNotFound, "cv not found")
	ErrProfileNotFound   = apperr.New(apperr.ErrNotFound, "profile not found")
)

// ErrStaleResult reports a parse result whose attempt is no longer current
// (another attempt started, or the CV is not awaiting a result). It is used
// internally by the result consumer, which ignores such events; it is never
// returned to API callers.
var ErrStaleResult = errors.New("stale parse result")
