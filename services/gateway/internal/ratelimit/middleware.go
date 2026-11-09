package ratelimit

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
)

// Allower is the subset of Limiter the middleware needs.
type Allower interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error)
}

// Rule is one rate-limit policy. Every rule whose Match returns true applies.
type Rule struct {
	Name   string
	Limit  int
	Window time.Duration
	Match  func(*http.Request) bool
	Key    func(*http.Request) string
}

// UserRule limits every request: per verified subject, or per IP when anonymous.
func UserRule() Rule {
	return Rule{
		Name: "user", Limit: 100, Window: time.Minute,
		Match: func(*http.Request) bool { return true },
		Key: func(r *http.Request) string {
			if c, ok := authz.ClaimsFrom(r.Context()); ok {
				return "gateway:rl:user:" + c.Subject
			}
			return "gateway:rl:ip:" + middleware.ClientIPFrom(r.Context())
		},
	}
}

// LoginRule limits POST /v1/auth/login per client IP.
func LoginRule() Rule {
	return Rule{
		Name: "login", Limit: 10, Window: time.Minute,
		Match: func(r *http.Request) bool {
			return r.Method == http.MethodPost && r.URL.Path == "/v1/auth/login"
		},
		Key: func(r *http.Request) string {
			return "gateway:rl:login:" + middleware.ClientIPFrom(r.Context())
		},
	}
}

// Middleware applies every matching rule. Response headers come from the
// tightest rule (lowest Remaining). A limiter error fails open.
func Middleware(l Allower, log *slog.Logger, rules ...Rule) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var tight *Result
		rules:
			for _, rule := range rules {
				if !rule.Match(r) {
					continue
				}
				res, err := l.Allow(r.Context(), rule.Key(r), rule.Limit, rule.Window)
				if err != nil {
					log.WarnContext(r.Context(), "rate limiter unavailable, failing open", "rule", rule.Name, "error", err)
					trace.SpanFromContext(r.Context()).SetAttributes(attribute.Bool("ratelimit.redis_error", true))
					// fail open once: do not pay the Redis timeout for each remaining rule
					tight = nil
					break rules
				}
				if !res.Allowed {
					secs := int(math.Ceil(res.RetryAfter.Seconds()))
					w.Header().Set("Retry-After", strconv.Itoa(max(secs, 1)))
					w.Header().Set("X-RateLimit-Limit", strconv.Itoa(res.Limit))
					w.Header().Set("X-RateLimit-Remaining", "0")
					httperr.Write(w, r, status.Error(codes.ResourceExhausted, "rate limit exceeded"))
					return
				}
				if tight == nil || res.Remaining < tight.Remaining {
					tight = &res
				}
			}
			if tight != nil {
				w.Header().Set("X-RateLimit-Limit", strconv.Itoa(tight.Limit))
				w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(tight.Remaining))
			}
			next.ServeHTTP(w, r)
		})
	}
}
