package opensearch

import (
	"context"
	"errors"
	"time"

	"github.com/sony/gobreaker/v2"

	pkgotel "github.com/nuroqzavitex/smarthire/pkg/otel"
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
		OnStateChange: func(name string, _, to gobreaker.State) {
			pkgotel.RecordBreakerState(context.Background(), name, pkgotel.BreakerStateFromString(to.String()))
		},
		Name:        "job-search-opensearch",
		MaxRequests: 1,
		Timeout:     open,
		ReadyToTrip: func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= failures },
		// A caller that cancelled or ran out of its own time says nothing
		// about OpenSearch; our own per-call timeout still counts.
		IsSuccessful: func(err error) bool {
			var gone *callerGone
			return err == nil || errors.As(err, &gone)
		},
	})
	pkgotel.RecordBreakerState(context.Background(), "job-search-opensearch", pkgotel.BreakerClosed)
	return &Guarded{inner: inner, cb: cb, timeout: timeout}
}

// callerGone marks an error caused by the caller's context ending.
type callerGone struct{ err error }

func (e *callerGone) Error() string { return e.err.Error() }
func (e *callerGone) Unwrap() error { return e.err }

// Search implements app.SearchBackend.
func (g *Guarded) Search(ctx context.Context, q app.SearchQuery) (app.SearchResult, error) {
	res, err := g.cb.Execute(func() (app.SearchResult, error) {
		cctx, cancel := context.WithTimeout(ctx, g.timeout)
		defer cancel()
		res, err := g.inner.Search(cctx, q)
		if err != nil && ctx.Err() != nil {
			return res, &callerGone{err: err}
		}
		return res, err
	})
	var gone *callerGone
	if errors.As(err, &gone) {
		return res, gone.err
	}
	return res, err
}
