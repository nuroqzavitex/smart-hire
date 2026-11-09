package ratelimit

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient returns a go-redis client tuned for the limiter. go-redis v9
// ignores ctx deadlines unless ContextTimeoutEnabled is set, so without it the
// limiter's 100ms bound would not hold against a hung Redis. Retries are
// disabled (-1; in go-redis 0 means "default 3") so a dead Redis fails open fast.
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  addr,
		ContextTimeoutEnabled: true,
		DialTimeout:           200 * time.Millisecond,
		ReadTimeout:           150 * time.Millisecond,
		WriteTimeout:          150 * time.Millisecond,
		PoolTimeout:           200 * time.Millisecond,
		MaxRetries:            -1,
	})
}
