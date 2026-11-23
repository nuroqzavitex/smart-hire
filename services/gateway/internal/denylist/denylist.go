// Package denylist keeps the users whose access tokens the gateway rejects
// while their account is being deleted (spec §6.5, ADR 0014). Access tokens
// are not revocable (ADR 0003), so the gateway stores the user ID in Redis
// for longer than the access-token TTL.
package denylist

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const prefix = "gateway:denylist:user:"

// Denylist stores denied user IDs in Redis.
type Denylist struct {
	rdb redis.Cmdable
	ttl time.Duration
}

// New returns a Denylist whose entries live for ttl.
func New(rdb redis.Cmdable, ttl time.Duration) *Denylist { return &Denylist{rdb: rdb, ttl: ttl} }

// Add denies every token of userID for the TTL.
func (d *Denylist) Add(ctx context.Context, userID string) error {
	if err := d.rdb.Set(ctx, prefix+userID, "1", d.ttl).Err(); err != nil {
		return fmt.Errorf("denylist add: %w", err)
	}
	return nil
}

// Contains reports whether userID is denied.
func (d *Denylist) Contains(ctx context.Context, userID string) (bool, error) {
	n, err := d.rdb.Exists(ctx, prefix+userID).Result()
	if err != nil {
		return false, fmt.Errorf("denylist check: %w", err)
	}
	return n > 0, nil
}
