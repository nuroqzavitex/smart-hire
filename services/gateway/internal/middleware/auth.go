package middleware

import (
	"context"
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
