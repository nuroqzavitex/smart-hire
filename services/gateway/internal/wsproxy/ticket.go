// Package wsproxy authenticates WebSocket connections with one-use tickets
// and proxies them to the notification service (ADR 0012). Browsers cannot
// send an Authorization header when opening a WebSocket and a JWT in the URL
// would end up in logs, so the client trades its JWT for a 30-second ticket
// first.
package wsproxy

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const ticketPrefix = "gateway:ws:ticket:"

// ErrInvalidTicket reports a missing, used or expired ticket.
var ErrInvalidTicket = errors.New("invalid websocket ticket")

// Tickets issues and redeems one-use tickets stored in Redis.
type Tickets struct {
	rdb redis.Cmdable
	ttl time.Duration
}

// NewTickets returns a ticket store whose tickets live for ttl.
func NewTickets(rdb redis.Cmdable, ttl time.Duration) *Tickets {
	return &Tickets{rdb: rdb, ttl: ttl}
}

// TTL is how long an issued ticket is valid.
func (t *Tickets) TTL() time.Duration { return t.ttl }

// Issue stores a fresh random ticket for userID.
func (t *Tickets) Issue(ctx context.Context, userID string) (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("ticket entropy: %w", err)
	}
	ticket := base64.RawURLEncoding.EncodeToString(buf)
	if err := t.rdb.Set(ctx, ticketPrefix+ticket, userID, t.ttl).Err(); err != nil {
		return "", fmt.Errorf("store ticket: %w", err)
	}
	return ticket, nil
}

// Redeem consumes a ticket atomically (GETDEL) and returns its user id.
func (t *Tickets) Redeem(ctx context.Context, ticket string) (string, error) {
	if ticket == "" {
		return "", ErrInvalidTicket
	}
	user, err := t.rdb.GetDel(ctx, ticketPrefix+ticket).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrInvalidTicket
	}
	if err != nil {
		return "", fmt.Errorf("redeem ticket: %w", err)
	}
	return user, nil
}
