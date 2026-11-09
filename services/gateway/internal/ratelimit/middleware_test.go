package ratelimit_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
)

type fakeLimiter struct {
	keys []string
	fn   func(key string) (ratelimit.Result, error)
}

func (f *fakeLimiter) Allow(_ context.Context, key string, _ int, _ time.Duration) (ratelimit.Result, error) {
	f.keys = append(f.keys, key)
	return f.fn(key)
}

func ok(remaining int) func(string) (ratelimit.Result, error) {
	return func(string) (ratelimit.Result, error) {
		return ratelimit.Result{Allowed: true, Limit: 100, Remaining: remaining}, nil
	}
}

func run(t *testing.T, l ratelimit.Allower, method, path string, rules ...ratelimit.Rule) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	called := false
	h := ratelimit.Middleware(l, slog.New(slog.DiscardHandler), rules...)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(200)
	}))
	r := httptest.NewRequest(method, path, nil)
	r.RemoteAddr = "203.0.113.5:1"
	w := httptest.NewRecorder()
	middleware.ClientIP(nil)(h).ServeHTTP(w, r)
	return w, called
}

func TestMiddleware_Blocks429(t *testing.T) {
	l := &fakeLimiter{fn: func(string) (ratelimit.Result, error) {
		return ratelimit.Result{Allowed: false, Limit: 100, Remaining: 0, RetryAfter: 1500 * time.Millisecond}, nil
	}}
	w, called := run(t, l, "GET", "/v1/me", ratelimit.UserRule())
	assert.False(t, called)
	assert.Equal(t, 429, w.Code)
	assert.Equal(t, "2", w.Header().Get("Retry-After"))
	assert.Contains(t, w.Body.String(), `"RESOURCE_EXHAUSTED"`)
}

func TestMiddleware_AllowedSetsHeadersFromTightestRule(t *testing.T) {
	l := &fakeLimiter{fn: func(key string) (ratelimit.Result, error) {
		if strings.HasPrefix(key, "gateway:rl:login:") {
			return ratelimit.Result{Allowed: true, Limit: 10, Remaining: 3}, nil
		}
		return ratelimit.Result{Allowed: true, Limit: 100, Remaining: 90}, nil
	}}
	w, called := run(t, l, "POST", "/v1/auth/login", ratelimit.UserRule(), ratelimit.LoginRule())
	assert.True(t, called)
	assert.Equal(t, "10", w.Header().Get("X-RateLimit-Limit"))
	assert.Equal(t, "3", w.Header().Get("X-RateLimit-Remaining"))
	assert.Len(t, l.keys, 2)
}

func TestMiddleware_FailOpen(t *testing.T) {
	l := &fakeLimiter{fn: func(string) (ratelimit.Result, error) { return ratelimit.Result{}, errors.New("redis down") }}
	w, called := run(t, l, "GET", "/v1/me", ratelimit.UserRule())
	assert.True(t, called)
	assert.Equal(t, 200, w.Code)
}

func TestMiddleware_FailOpenCallsAllowOnce(t *testing.T) {
	l := &fakeLimiter{fn: func(string) (ratelimit.Result, error) { return ratelimit.Result{}, errors.New("redis down") }}
	w, called := run(t, l, "POST", "/v1/auth/login", ratelimit.UserRule(), ratelimit.LoginRule())
	assert.True(t, called)
	assert.Equal(t, 200, w.Code)
	assert.Len(t, l.keys, 1)
}

func TestChain_InvalidTokenCountsAgainstIPAndGets401(t *testing.T) {
	v, _ := newChainVerifier(t)
	l := &fakeLimiter{fn: ok(5)}
	called := false
	var h http.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	h = middleware.RejectInvalidAuth(h)
	h = ratelimit.Middleware(l, slog.New(slog.DiscardHandler), ratelimit.UserRule())(h)
	h = middleware.OptionalAuth(v)(h)
	h = middleware.ClientIP(nil)(h)

	r := httptest.NewRequest("GET", "/v1/me", nil)
	r.RemoteAddr = "203.0.113.5:1"
	r.Header.Set("Authorization", "Bearer garbage")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assert.False(t, called)
	assert.Equal(t, 401, w.Code)
	assert.Equal(t, "5", w.Header().Get("X-RateLimit-Remaining"))
	assert.Equal(t, []string{"gateway:rl:ip:203.0.113.5"}, l.keys)
}

func TestMiddleware_LoginRuleOnlyOnLoginPath(t *testing.T) {
	l := &fakeLimiter{fn: ok(5)}
	run(t, l, "GET", "/v1/me", ratelimit.UserRule(), ratelimit.LoginRule())
	require.NotEmpty(t, l.keys)
	for _, k := range l.keys {
		assert.True(t, strings.HasPrefix(k, "gateway:"), k)
		assert.False(t, strings.HasPrefix(k, "gateway:rl:login:"), k)
	}
	l.keys = nil
	run(t, l, "GET", "/v1/auth/login", ratelimit.UserRule(), ratelimit.LoginRule())
	for _, k := range l.keys {
		assert.False(t, strings.HasPrefix(k, "gateway:rl:login:"), "GET must not match login rule")
	}
	l.keys = nil
	run(t, l, "POST", "/v1/auth/login", ratelimit.UserRule(), ratelimit.LoginRule())
	assert.Contains(t, l.keys, "gateway:rl:login:203.0.113.5")
}

func TestUserRule_KeyBySubOrIP(t *testing.T) {
	l := &fakeLimiter{fn: ok(5)}
	h := ratelimit.Middleware(l, slog.New(slog.DiscardHandler), ratelimit.UserRule())(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "203.0.113.5:1"
	r = r.WithContext(authz.WithClaims(r.Context(), authz.Claims{Subject: "u-1"}))
	middleware.ClientIP(nil)(h).ServeHTTP(httptest.NewRecorder(), r)
	assert.Equal(t, []string{"gateway:rl:user:u-1"}, l.keys)

	l.keys = nil
	run(t, l, "GET", "/", ratelimit.UserRule())
	assert.Equal(t, []string{"gateway:rl:ip:203.0.113.5"}, l.keys)
}
