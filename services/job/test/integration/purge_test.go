//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func TestPurge_DeletesAndAnonymisesRecruiterJobs(t *testing.T) {
	truncate(t)
	ctx := context.Background()
	rec := uuid.New()
	open := seed(t, seedOpts{title: "Go dev", recruiter: rec})
	draft := seedDraft(t, seedOpts{title: "Draft", recruiter: rec})
	other := seed(t, seedOpts{title: "Other"})

	svc := app.NewService(app.Deps{
		Jobs: postgres.NewJobRepo(pool), Outbox: postgres.NewOutboxWriter(pool),
		Tx: postgres.NewTxManager(pool), Idem: postgres.NewIdempotency(pool),
	})
	env, err := userdeletion.NewDeletedEvent(ctx, userdeletion.Deleted{
		UserID: rec, Role: "recruiter", RequestID: uuid.New(), RequestedAt: time.Now(),
	}, time.Now())
	require.NoError(t, err)
	require.NoError(t, svc.HandleUserDeleted(ctx, env))
	require.NoError(t, svc.HandleUserDeleted(ctx, env), "redelivery")

	repo := postgres.NewJobRepo(pool)
	for _, id := range []uuid.UUID{open.ID, draft.ID} {
		_, err := repo.Get(ctx, id)
		assert.ErrorIs(t, err, domain.ErrJobNotFound, "soft-deleted")
		var owner uuid.UUID
		var status string
		require.NoError(t, pool.QueryRow(ctx, `SELECT recruiter_id, status FROM jobs WHERE id = $1`, id).Scan(&owner, &status))
		assert.Equal(t, uuid.Nil, owner)
		assert.NotEqual(t, "open", status)
	}
	_, err = repo.Get(ctx, other.ID)
	assert.NoError(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE event_type = 'job.deleted'`).Scan(&n))
	assert.Equal(t, 2, n)
	var payload []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE event_type = $1`, userdeletion.TopicPurged).Scan(&payload))
	assert.NoError(t, eventschema.Validate(userdeletion.TopicPurged, payload))
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE event_type = 'job.deleted' LIMIT 1`).Scan(&payload))
	assert.NoError(t, eventschema.Validate("job.deleted", payload))
}
