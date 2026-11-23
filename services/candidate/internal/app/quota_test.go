package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

type fakeQuota struct {
	res      app.QuotaResult
	err      error
	appID    uuid.UUID
	cand     uuid.UUID
	day      time.Time
	limit    int
	released []uuid.UUID
	inTx     bool
}

func (f *fakeQuota) Reserve(ctx context.Context, appID, candidateID uuid.UUID, day time.Time, limit int) (app.QuotaResult, error) {
	f.appID, f.cand, f.day, f.limit = appID, candidateID, day, limit
	f.inTx = ctx.Value(txMarker{}) != nil
	return f.res, f.err
}

func (f *fakeQuota) Release(ctx context.Context, appID uuid.UUID) error {
	f.inTx = ctx.Value(txMarker{}) != nil
	f.released = append(f.released, appID)
	return f.err
}

type txMarker struct{}

type markTx struct{}

func (markTx) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(context.WithValue(ctx, txMarker{}, true))
}

func quotaSvc(q *fakeQuota, now time.Time) *app.Service {
	return app.NewService(app.Deps{Quota: q, DailyQuota: 7, Tx: markTx{}, Now: func() time.Time { return now }})
}

func TestReserveQuota_UsesUTCDayAndLimit(t *testing.T) {
	q := &fakeQuota{res: app.QuotaReserved}
	now := time.Date(2026, 11, 24, 1, 30, 0, 0, time.FixedZone("ICT", 7*3600))
	appID, cand := uuid.New(), uuid.New()
	require.NoError(t, quotaSvc(q, now).ReserveQuota(context.Background(), appID, cand))
	assert.Equal(t, appID, q.appID)
	assert.Equal(t, cand, q.cand)
	assert.Equal(t, time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC), q.day)
	assert.Equal(t, 7, q.limit)
	assert.True(t, q.inTx, "reserve runs in a transaction")
}

func TestReserveQuota_MapsResults(t *testing.T) {
	now := time.Now()
	err := quotaSvc(&fakeQuota{res: app.QuotaExhausted}, now).ReserveQuota(context.Background(), uuid.New(), uuid.New())
	assert.ErrorIs(t, err, domain.ErrQuotaExhausted)
	err = quotaSvc(&fakeQuota{res: app.QuotaAlreadyReleased}, now).ReserveQuota(context.Background(), uuid.New(), uuid.New())
	assert.ErrorIs(t, err, domain.ErrReservationReleased)
	err = quotaSvc(&fakeQuota{err: errBoom}, now).ReserveQuota(context.Background(), uuid.New(), uuid.New())
	assert.ErrorIs(t, err, errBoom)
}

func TestReleaseQuota_Delegates(t *testing.T) {
	q := &fakeQuota{}
	id := uuid.New()
	require.NoError(t, quotaSvc(q, time.Now()).ReleaseQuota(context.Background(), id))
	assert.Equal(t, []uuid.UUID{id}, q.released)
	assert.True(t, q.inTx)
}
