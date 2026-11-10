package authz

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

const (
	retryAttemptTimeout = 5 * time.Second
	retryPause          = time.Second
)

// NewVerifierWithRetry calls NewVerifier until it succeeds, retrying while the
// auth service (JWKS endpoint) is still starting. Each attempt is bounded by
// 5s with a 1s pause in between. It gives up once maxWait has elapsed or ctx
// is cancelled and returns the last error.
func NewVerifierWithRetry(ctx context.Context, jwksURL, issuer, audience string,
	maxWait time.Duration, log *slog.Logger,
) (*Verifier, error) {
	deadline := time.Now().Add(maxWait)
	for {
		fctx, cancel := context.WithTimeout(ctx, retryAttemptTimeout)
		v, err := NewVerifier(fctx, jwksURL, issuer, audience)
		cancel()
		if err == nil {
			return v, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("jwks verifier: %w", err)
		}
		log.Warn("auth JWKS not available yet, retrying", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryPause):
		}
	}
}
