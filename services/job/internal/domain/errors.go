package domain

import "github.com/nuroqzavitex/smarthire/pkg/apperr"

// Domain errors surfaced to callers.
var (
	ErrInvalidTransition = apperr.New(apperr.ErrValidation, "invalid job status transition")
	ErrJobNotFound       = apperr.New(apperr.ErrNotFound, "job not found")
	ErrJobClosed         = apperr.New(apperr.ErrValidation, "closed job cannot be edited")
)
