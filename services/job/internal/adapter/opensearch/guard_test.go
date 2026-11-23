package opensearch_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/otel/oteltest"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/opensearch"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
)

type backendFunc func(ctx context.Context, q app.SearchQuery) (app.SearchResult, error)

func (f backendFunc) Search(ctx context.Context, q app.SearchQuery) (app.SearchResult, error) {
	return f(ctx, q)
}

func TestGuarded_OpensAfterFailures(t *testing.T) {
	var calls atomic.Int32
	fail := true
	inner := backendFunc(func(context.Context, app.SearchQuery) (app.SearchResult, error) {
		calls.Add(1)
		if fail {
			return app.SearchResult{}, errors.New("connection refused")
		}
		return app.SearchResult{}, nil
	})
	g := opensearch.NewGuarded(inner, 5, 50*time.Millisecond, time.Second)
	for i := 0; i < 5; i++ {
		_, err := g.Search(context.Background(), app.SearchQuery{})
		require.Error(t, err)
	}
	_, err := g.Search(context.Background(), app.SearchQuery{})
	require.Error(t, err)
	assert.Equal(t, int32(5), calls.Load(), "open breaker does not call opensearch")

	time.Sleep(80 * time.Millisecond)
	fail = false
	_, err = g.Search(context.Background(), app.SearchQuery{})
	require.NoError(t, err, "half-open trial reaches opensearch")
	assert.Equal(t, int32(6), calls.Load())
}

func TestGuarded_SuccessResetsFailures(t *testing.T) {
	n := 0
	inner := backendFunc(func(context.Context, app.SearchQuery) (app.SearchResult, error) {
		n++
		if n == 3 {
			return app.SearchResult{}, nil
		}
		return app.SearchResult{}, errors.New("boom")
	})
	g := opensearch.NewGuarded(inner, 3, time.Minute, time.Second)
	for i := 0; i < 5; i++ {
		_, _ = g.Search(context.Background(), app.SearchQuery{})
	}
	assert.Equal(t, 5, n, "failures were not consecutive, breaker stayed closed")
}

func TestGuarded_Timeout(t *testing.T) {
	inner := backendFunc(func(ctx context.Context, _ app.SearchQuery) (app.SearchResult, error) {
		<-ctx.Done()
		return app.SearchResult{}, ctx.Err()
	})
	g := opensearch.NewGuarded(inner, 5, time.Minute, 50*time.Millisecond)
	start := time.Now()
	_, err := g.Search(context.Background(), app.SearchQuery{})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)
}

func TestGuarded_CallerCancellationDoesNotTrip(t *testing.T) {
	var calls atomic.Int32
	inner := backendFunc(func(context.Context, app.SearchQuery) (app.SearchResult, error) {
		calls.Add(1)
		return app.SearchResult{}, context.Canceled
	})
	g := opensearch.NewGuarded(inner, 2, time.Minute, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 5; i++ {
		_, _ = g.Search(ctx, app.SearchQuery{})
	}
	assert.Equal(t, int32(5), calls.Load(), "client disconnects are not opensearch failures")
}

func TestGuarded_OwnTimeoutStillTrips(t *testing.T) {
	var calls atomic.Int32
	inner := backendFunc(func(ctx context.Context, _ app.SearchQuery) (app.SearchResult, error) {
		calls.Add(1)
		<-ctx.Done()
		return app.SearchResult{}, ctx.Err()
	})
	g := opensearch.NewGuarded(inner, 2, time.Minute, 20*time.Millisecond)
	for i := 0; i < 4; i++ {
		_, _ = g.Search(context.Background(), app.SearchQuery{})
	}
	assert.Equal(t, int32(2), calls.Load(), "a slow opensearch opens the breaker")
}

func TestGuarded_BreakerStateMetric(t *testing.T) {
	m := oteltest.Install(t)
	inner := backendFunc(func(context.Context, app.SearchQuery) (app.SearchResult, error) {
		return app.SearchResult{}, errors.New("connection refused")
	})
	g := opensearch.NewGuarded(inner, 2, time.Minute, time.Second)
	v, ok := m.Int64(t, "circuit_breaker.state", "breaker", "job-search-opensearch")
	require.True(t, ok, "closed state recorded at construction")
	assert.Zero(t, v)
	for range 2 {
		_, _ = g.Search(context.Background(), app.SearchQuery{})
	}
	v, _ = m.Int64(t, "circuit_breaker.state", "breaker", "job-search-opensearch")
	assert.Equal(t, int64(2), v)
}
