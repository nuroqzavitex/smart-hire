//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

func TestDeletion_EndToEnd(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := newService(t, postgres.NewOutboxWriter(pool))
	id, err := svc.Register(ctx, "del@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	_, err = svc.Login(ctx, "del@x.com", "password1")
	require.NoError(t, err)

	r, err := svc.DeleteMe(ctx, id, "password1")
	require.NoError(t, err)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM users WHERE id = $1 AND status = 'DELETION_PENDING'`, id))
	assert.Zero(t, count(t, `SELECT count(*) FROM refresh_tokens WHERE user_id = $1 AND revoked_at IS NULL`, id))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM outbox WHERE event_type = 'user.deleted' AND aggregate_id = $1`, id.String()))

	d := userdeletion.Deleted{UserID: id, Role: "candidate", RequestID: r.ID, RequestedAt: r.RequestedAt}
	for _, s := range userdeletion.Services {
		env, err := userdeletion.NewPurgedEvent(ctx, d, s, time.Now())
		require.NoError(t, err)
		require.NoError(t, svc.HandleDataPurged(ctx, env))
		require.NoError(t, svc.HandleDataPurged(ctx, env), "redelivery")
	}

	assert.Zero(t, count(t, `SELECT count(*) FROM users WHERE id = $1`, id), "user hard-deleted")
	assert.Zero(t, count(t, `SELECT count(*) FROM refresh_tokens WHERE user_id = $1`, id))
	assert.Zero(t, count(t, `SELECT count(*) FROM outbox WHERE event_type = 'user.registered' AND aggregate_id = $1`, id.String()),
		"the email left the outbox with the account")
	got, err := postgres.NewDeletionRepo(pool).Get(ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.DeletionCompleted, got.Status)
	assert.ElementsMatch(t, userdeletion.Services, got.Purged)
	require.NotNil(t, got.CompletedAt)

	// The email can register again.
	_, err = svc.Register(ctx, "del@x.com", "password1", authz.RoleCandidate)
	assert.NoError(t, err)
}

func TestDeletion_ConcurrentDeleteMeCreatesOneRequest(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	svc := newService(t, postgres.NewOutboxWriter(pool))
	id, err := svc.Register(ctx, "race@x.com", "password1", authz.RoleRecruiter)
	require.NoError(t, err)

	var wg sync.WaitGroup
	ids := make([]uuid.UUID, 8)
	for i := range ids {
		wg.Go(func() {
			r, err := svc.DeleteMe(ctx, id, "password1")
			assert.NoError(t, err)
			ids[i] = r.ID
		})
	}
	wg.Wait()
	for _, x := range ids {
		assert.Equal(t, ids[0], x)
	}
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM deletion_requests WHERE user_id = $1`, id))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM outbox WHERE event_type = 'user.deleted'`))
}

func TestDeletion_SweepRepublishesAndFlagsStuck(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	repo := postgres.NewDeletionRepo(pool)
	old := time.Now().Add(-25 * time.Hour).UTC().Truncate(time.Microsecond)
	r := domain.NewDeletionRequest(uuid.New(), authz.RoleCandidate, userdeletion.Services, old)
	require.NoError(t, repo.Create(ctx, r))
	fresh := domain.NewDeletionRequest(uuid.New(), authz.RoleCandidate, userdeletion.Services, time.Now())
	require.NoError(t, repo.Create(ctx, fresh))

	svc := newService(t, postgres.NewOutboxWriter(pool))
	rep, stuck, err := svc.SweepDeletions(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, rep)
	assert.Equal(t, 1, stuck)
	got, err := repo.Get(ctx, r.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.DeletionStuck, got.Status)
	assert.True(t, got.LastPublishedAt.After(old))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM outbox WHERE event_type = 'user.deleted'`))

	counts, err := repo.CountByStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[domain.DeletionStatus]int64{domain.DeletionStuck: 1, domain.DeletionPending: 1}, counts)
}
