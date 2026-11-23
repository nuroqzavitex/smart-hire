//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

func quotaService(limit int) *app.Service {
	return app.NewService(app.Deps{Quota: postgres.NewQuotaRepo(pool), DailyQuota: limit, Tx: postgres.NewTxManager(pool)})
}

func usedToday(t *testing.T, cand uuid.UUID) int {
	t.Helper()
	var used int
	err := pool.QueryRow(context.Background(),
		`SELECT coalesce((SELECT used FROM daily_quota WHERE candidate_id = $1 AND day = (now() AT TIME ZONE 'UTC')::date), 0)`, cand).Scan(&used)
	require.NoError(t, err)
	return used
}

func TestQuota_ReserveIdempotent(t *testing.T) {
	s, ctx, cand, appID := quotaService(5), context.Background(), uuid.New(), uuid.New()
	require.NoError(t, s.ReserveQuota(ctx, appID, cand))
	require.NoError(t, s.ReserveQuota(ctx, appID, cand))
	assert.Equal(t, 1, usedToday(t, cand))
}

func TestQuota_LimitUnderConcurrency(t *testing.T) {
	s, ctx, cand := quotaService(3), context.Background(), uuid.New()
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := range errs {
		wg.Go(func() { errs[i] = s.ReserveQuota(ctx, uuid.New(), cand) })
	}
	wg.Wait()
	ok, exhausted := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			ok++
		case assert.ErrorIs(t, err, domain.ErrQuotaExhausted):
			exhausted++
		}
	}
	assert.Equal(t, 3, ok)
	assert.Equal(t, 7, exhausted)
	assert.Equal(t, 3, usedToday(t, cand))
}

func TestQuota_SameApplicationConcurrentCountsOnce(t *testing.T) {
	s, ctx, cand, appID := quotaService(5), context.Background(), uuid.New(), uuid.New()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { assert.NoError(t, s.ReserveQuota(ctx, appID, cand)) })
	}
	wg.Wait()
	assert.Equal(t, 1, usedToday(t, cand))
}

func TestQuota_ReleaseThenLateReserveAborted(t *testing.T) {
	s, ctx, cand, appID := quotaService(5), context.Background(), uuid.New(), uuid.New()
	require.NoError(t, s.ReleaseQuota(ctx, appID), "release of an unknown application leaves a tombstone")
	assert.ErrorIs(t, s.ReserveQuota(ctx, appID, cand), domain.ErrReservationReleased)
	assert.Equal(t, 0, usedToday(t, cand))
}

func TestQuota_ReleaseRestoresUsed(t *testing.T) {
	s, ctx, cand, appID := quotaService(1), context.Background(), uuid.New(), uuid.New()
	require.NoError(t, s.ReserveQuota(ctx, appID, cand))
	assert.ErrorIs(t, s.ReserveQuota(ctx, uuid.New(), cand), domain.ErrQuotaExhausted)
	require.NoError(t, s.ReleaseQuota(ctx, appID))
	require.NoError(t, s.ReleaseQuota(ctx, appID), "second release is a no-op")
	assert.Equal(t, 0, usedToday(t, cand))
	assert.ErrorIs(t, s.ReserveQuota(ctx, appID, cand), domain.ErrReservationReleased)
	require.NoError(t, s.ReserveQuota(ctx, uuid.New(), cand), "freed unit can be taken again")
}

func TestQuota_ExhaustedCanRetryLater(t *testing.T) {
	s, ctx, cand := quotaService(1), context.Background(), uuid.New()
	first, second := uuid.New(), uuid.New()
	require.NoError(t, s.ReserveQuota(ctx, first, cand))
	assert.ErrorIs(t, s.ReserveQuota(ctx, second, cand), domain.ErrQuotaExhausted)
	require.NoError(t, s.ReleaseQuota(ctx, first))
	require.NoError(t, s.ReserveQuota(ctx, second, cand), "an exhausted attempt holds nothing")
}
