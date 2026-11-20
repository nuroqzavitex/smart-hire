package opensearch

import (
	"context"
	"time"

	"github.com/sony/gobreaker/v2"

	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

// Guarded bounds and circuit-breaks searches against the read model, so a
// down or slow OpenSearch fails fast and ListOpen falls back to Postgres.
type Guarded struct {
	inner   app.SearchBackend
	cb      *gobreaker.CircuitBreaker[app.SearchResult]
	timeout time.Duration
}

// NewGuarded opens the breaker after failures consecutive errors, lets one
// trial request through after open has elapsed, and gives each call timeout.
func NewGuarded(inner app.SearchBackend, failures uint32, open, timeout time.Duration) *Guarded {
	cb := gobreaker.NewCircuitBreaker[app.SearchResult](gobreaker.Settings{
		Name:        "job-search-opensearch",
		MaxRequests: 1,
		Timeout:     open,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
	})
	return &Guarded{inner: inner, cb: cb, timeout: timeout}
}

// Search implements app.SearchBackend.
func (g *Guarded) Search(ctx context.Context, q app.SearchQuery) (app.SearchResult, error) {
	return g.cb.Execute(func() (app.SearchResult, error) {
		ctx, cancel := context.WithTimeout(ctx, g.timeout)
		defer cancel()
		return g.inner.Search(ctx, q)
	})
}
