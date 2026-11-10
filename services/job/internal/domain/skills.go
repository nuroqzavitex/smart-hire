package domain

import (
	"strings"
	"unicode/utf8"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

const (
	maxSkills    = 30
	maxSkillRune = 50
)

// NormalizeSkills collapses whitespace, drops empty entries and case-insensitive
// duplicates (keeping the first spelling). It never returns a nil slice.
func NormalizeSkills(in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, s := range in {
		s = strings.Join(strings.Fields(s), " ")
		if s == "" {
			continue
		}
		if utf8.RuneCountInString(s) > maxSkillRune {
			return nil, apperr.New(apperr.ErrValidation, "skill must be at most 50 characters")
		}
		key := strings.ToLower(s)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, s)
	}
	if len(out) > maxSkills {
		return nil, apperr.New(apperr.ErrValidation, "at most 30 skills are allowed")
	}
	return out, nil
}
