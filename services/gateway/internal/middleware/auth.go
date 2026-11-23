package middleware

import (
	"context"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
)

type authFailedKey struct{}

// OptionalAuth verifies a Bearer token when an Authorization header is present
// and stores the claims in the request context. A missing header passes
// through anonymously. A present but invalid header does NOT short-circuit:
// the request is marked auth-failed, carries no claims and continues, so the
// rate limiter counts it against the client IP. Chain order must be
// ClientIP -> OptionalAuth -> ratelimit -> RejectInvalidAuth, which then
// answers the marked requests with 401.
func OptionalAuth(v grpcx.TokenVerifier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := r.Header.Get("Authorization")
			if h == "" {
				next.ServeHTTP(w, r)
				return
			}
			failed := func() {
				ctx := context.WithValue(r.Context(), authFailedKey{}, true)
				next.ServeHTTP(w, r.WithContext(ctx))
			}
			raw, ok := authz.BearerToken(h)
			if !ok {
				failed()
				return
			}
			claims, err := v.Verify(raw)
			if err != nil {
				failed()
				return
			}
			ctx := authz.WithClaims(r.Context(), claims)
			ctx = logger.WithUserID(ctx, claims.Subject)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RejectInvalidAuth answers requests marked auth-failed by OptionalAuth with
// 401; everything else continues.
func RejectInvalidAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failed, _ := r.Context().Value(authFailedKey{}).(bool); failed {
			httperr.Write(w, r, status.Error(codes.Unauthenticated, "invalid or expired token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireAuth rejects requests that carry no verified claims.
func RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := authz.ClaimsFrom(r.Context()); !ok {
			httperr.Write(w, r, status.Error(codes.Unauthenticated, "authentication required"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// DenyChecker reports whether a user's tokens are denied (internal/denylist).
type DenyChecker interface {
	Contains(ctx context.Context, userID string) (bool, error)
}

// Denylist runs after OptionalAuth: a verified token of a user whose account
// is being deleted is marked auth-failed, so RejectInvalidAuth answers 401
// (spec §6.5, ADR 0014). A Redis error fails open with a warning: access
// tokens live 15 minutes and Auth already refuses refresh. A nil checker
// disables the check.
func Denylist(d DenyChecker, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if d == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, ok := authz.ClaimsFrom(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}
			denied, err := d.Contains(r.Context(), claims.Subject)
			if err != nil {
				log.WarnContext(r.Context(), "denylist check failed, allowing request", "err", err)
			}
			if denied {
				ctx := context.WithValue(r.Context(), authFailedKey{}, true)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
