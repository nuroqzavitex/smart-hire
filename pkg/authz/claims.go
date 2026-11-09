// Package authz verifies access tokens and provides claims/RBAC helpers.
package authz

import (
	"context"
	"strings"
	"time"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// Role is a user's authorization role.
type Role string

// Known roles.
const (
	RoleCandidate Role = "candidate"
	RoleRecruiter Role = "recruiter"
	RoleAdmin     Role = "admin"
)

func (r Role) valid() bool {
	switch r {
	case RoleCandidate, RoleRecruiter, RoleAdmin:
		return true
	}
	return false
}

// Claims are the verified identity claims of an access token.
type Claims struct {
	Subject   string
	Role      Role
	TokenID   string
	ExpiresAt time.Time
}

type ctxKey struct{}

// WithClaims returns a context carrying c.
func WithClaims(ctx context.Context, c Claims) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// ClaimsFrom returns the claims stored in ctx, if any.
func ClaimsFrom(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(Claims)
	return c, ok
}

// RequireRole fails with ErrUnauthenticated when ctx has no claims and with
// ErrForbidden when the role is not in roles. Empty roles only requires login.
func RequireRole(ctx context.Context, roles ...Role) error {
	c, ok := ClaimsFrom(ctx)
	if !ok {
		return apperr.New(apperr.ErrUnauthenticated, "authentication required")
	}
	if len(roles) == 0 {
		return nil
	}
	for _, r := range roles {
		if c.Role == r {
			return nil
		}
	}
	return apperr.New(apperr.ErrForbidden, "insufficient permissions")
}

// BearerToken extracts the token from an Authorization header value
// ("Bearer <t>", scheme case-insensitive).
func BearerToken(header string) (string, bool) {
	scheme, tok, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return "", false
	}
	return tok, true
}
