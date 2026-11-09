// Package domain holds the auth service's entities and invariants. It has no
// dependency on infrastructure.
package domain

import (
	"net/mail"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

const (
	maxEmailLen      = 254
	minPasswordRunes = 8
	maxPasswordRunes = 128
	msgInvalidEmail  = "invalid email address"
	msgInvalidPass   = "password must be between 8 and 128 characters"
)

// UserStatus is the lifecycle state of a user account.
type UserStatus string

// User account states.
const (
	StatusActive          UserStatus = "ACTIVE"
	StatusLocked          UserStatus = "LOCKED"
	StatusDeletionPending UserStatus = "DELETION_PENDING"
)

// User is an account that can authenticate.
type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
	Role         authz.Role
	Status       UserStatus
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CanAuthenticate reports why the user may not sign in, or nil when allowed.
func (u User) CanAuthenticate() error {
	switch u.Status {
	case StatusActive:
		return nil
	case StatusLocked:
		return ErrAccountLocked
	case StatusDeletionPending:
		return ErrAccountPendingDeletion
	default:
		return apperr.New(apperr.ErrForbidden, "account is not active")
	}
}

// NewEmail trims and lowercases s and verifies it is a bare RFC 5322 address.
func NewEmail(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || len(s) > maxEmailLen {
		return "", apperr.New(apperr.ErrValidation, msgInvalidEmail)
	}
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return "", apperr.New(apperr.ErrValidation, msgInvalidEmail)
	}
	return s, nil
}

// ValidatePassword enforces the password length policy (in runes).
func ValidatePassword(p string) error {
	n := utf8.RuneCountInString(p)
	if n < minPasswordRunes || n > maxPasswordRunes {
		return apperr.New(apperr.ErrValidation, msgInvalidPass)
	}
	return nil
}
