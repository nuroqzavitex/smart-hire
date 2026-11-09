//go:build integration

package integration_test

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/nuroqzavitex/smarthire/services/gateway/internal/middleware"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/ratelimit"
)

func startRedis(t *testing.T) (*redis.Client, *tcredis.RedisContainer) {
	t.Helper()
	ctr, err := tcredis.Run(t.Context(), "redis:7-alpine")
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	uri, err := ctr.ConnectionString(t.Context())
	require.NoError(t, err)
	opt, err := redis.ParseURL(uri)
	require.NoError(t, err)
	rdb := ratelimit.NewRedisClient(opt.Addr)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb, ctr
}

func TestLimiter_TokenBucket(t *testing.T) {
	rdb, _ := startRedis(t)
	l := ratelimit.New(rdb)
	ctx := t.Context()
	for i := range 10 {
		r, err := l.Allow(ctx, "k", 10, time.Minute)
		require.NoError(t, err)
		assert.True(t, r.Allowed, i)
	}
	r, err := l.Allow(ctx, "k", 10, time.Minute)
	require.NoError(t, err)
	assert.False(t, r.Allowed)
	assert.InDelta(t, float64(6*time.Second), float64(r.RetryAfter), float64(time.Second))
}

func TestLimiter_RedisDownFailsOpen(t *testing.T) {
	rdb, ctr := startRedis(t)
	l := ratelimit.New(rdb)
	require.NoError(t, testcontainers.TerminateContainer(ctr))

	called := false
	h := ratelimit.Middleware(l, slog.New(slog.DiscardHandler), ratelimit.UserRule())(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(200) }))
	r := httptest.NewRequest("GET", "/v1/me", nil)
	r.RemoteAddr = "203.0.113.5:1"
	w := httptest.NewRecorder()
	middleware.ClientIP(nil)(h).ServeHTTP(w, r)
	assert.True(t, called)
	assert.Equal(t, 200, w.Code)
}

// TestLimiter_HangingRedisFailsFast checks that the real client honours the
// limiter's ctx deadline when Redis accepts connections but never replies.
func TestLimiter_HangingRedisFailsFast(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		var conns []net.Conn // held open, never answered
		for {
			c, err := ln.Accept()
			if err != nil {
				for _, c := range conns {
					_ = c.Close()
				}
				return
			}
			conns = append(conns, c)
		}
	}()

	rdb := ratelimit.NewRedisClient(ln.Addr().String())
	t.Cleanup(func() { _ = rdb.Close() })
	start := time.Now()
	_, err = ratelimit.New(rdb).Allow(t.Context(), "k", 10, time.Minute)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 150*time.Millisecond)
}
