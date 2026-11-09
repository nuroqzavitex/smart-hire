// Package ratelimit implements a Redis token-bucket rate limiter and the
// gateway middleware built on it.
package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed script.lua
var bucketScript string

// redisTimeout bounds each limiter call so a dead Redis fails open quickly.
const redisTimeout = 100 * time.Millisecond

// Result is the outcome of one Allow call.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

// Limiter is a Redis-backed token bucket.
type Limiter struct {
	script *redis.Script
	rdb    redis.Scripter
}

// New returns a Limiter running its Lua script against rdb.
func New(rdb redis.Scripter) *Limiter {
	return &Limiter{script: redis.NewScript(bucketScript), rdb: rdb}
}

// Allow takes one token from the bucket at key (capacity limit, refilled
// evenly over window).
func (l *Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	raw, err := l.script.Run(ctx, l.rdb, []string{key}, limit, window.Milliseconds()).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit script: %w", err)
	}
	if len(raw) != 3 {
		return Result{}, fmt.Errorf("ratelimit script: unexpected reply length %d", len(raw))
	}
	var n [3]int64
	for i, v := range raw {
		x, ok := v.(int64)
		if !ok {
			return Result{}, fmt.Errorf("ratelimit script: unexpected reply type %T", v)
		}
		n[i] = x
	}
	return Result{
		Allowed:    n[0] == 1,
		Limit:      limit,
		Remaining:  int(n[1]),
		RetryAfter: time.Duration(n[2]) * time.Millisecond,
	}, nil
}
