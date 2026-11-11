// Package domain holds the candidate service's entities and invariants. It has
// no dependency on infrastructure.
package domain

import (
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

const (
	maxFullNameRunes = 200
	maxLocationRunes = 200
)

var phonePattern = regexp.MustCompile(`^[+0-9 ().-]{6,30}$`)

// Candidate is a candidate's profile; UserID is the JWT subject.
type Candidate struct {
	UserID    uuid.UUID
	FullName  string
	Phone     *string
	Location  *string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ProfileInput is the user-editable part of a profile.
type ProfileInput struct {
	FullName string
	Phone    *string
	Location *string
}

// NewProfile validates and normalizes in. Phone and Location that are empty
// after trimming become nil.
func NewProfile(userID uuid.UUID, in ProfileInput, now time.Time) (Candidate, error) {
	name := strings.TrimSpace(in.FullName)
	if n := utf8.RuneCountInString(name); n < 1 || n > maxFullNameRunes {
		return Candidate{}, apperr.New(apperr.ErrValidation, "full_name must be 1-200 characters")
	}
	phone := trimOptional(in.Phone)
	if phone != nil && !phonePattern.MatchString(*phone) {
		return Candidate{}, apperr.New(apperr.ErrValidation, "phone must be 6-30 characters of digits, spaces and + ( ) . -")
	}
	loc := trimOptional(in.Location)
	if loc != nil && utf8.RuneCountInString(*loc) > maxLocationRunes {
		return Candidate{}, apperr.New(apperr.ErrValidation, "location must be at most 200 characters")
	}
	return Candidate{
		UserID:    userID,
		FullName:  name,
		Phone:     phone,
		Location:  loc,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func trimOptional(s *string) *string {
	if s == nil {
		return nil
	}
	v := strings.TrimSpace(*s)
	if v == "" {
		return nil
	}
	return &v
}
