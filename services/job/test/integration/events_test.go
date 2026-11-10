//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/job/internal/adapter/postgres"
	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

func recruiterCtx() context.Context {
	return authz.WithClaims(context.Background(), authz.Claims{Subject: uuid.NewString(), Role: authz.RoleRecruiter})
}

func newService(outbox app.OutboxWriter) *app.Service {
	return app.NewService(app.Deps{
		Jobs:   postgres.NewJobRepo(pool),
		Outbox: outbox,
		Tx:     postgres.NewTxManager(pool),
	})
}

var draft = domain.Draft{
	Title: "Senior Go Engineer", Description: "Xây dựng dịch vụ backend bằng Go và Kafka",
	RequiredSkills: []string{"Go"}, Level: domain.LevelSenior, Location: "Hà Nội",
}

func TestEvents_MatchSchemas(t *testing.T) {
	truncate(t)
	svc := newService(postgres.NewOutboxWriter(pool))
	ctx := recruiterCtx()

	j, err := svc.Create(ctx, draft)
	require.NoError(t, err)
	title := "Senior Go Engineer (platform)"
	_, err = svc.Update(ctx, j.ID, domain.Patch{Title: &title})
	require.NoError(t, err)
	_, err = svc.Publish(ctx, j.ID)
	require.NoError(t, err)
	_, err = svc.Close(ctx, j.ID)
	require.NoError(t, err)
	require.NoError(t, svc.Delete(ctx, j.ID))

	rows, err := pool.Query(context.Background(),
		`SELECT event_type, aggregate_id, payload FROM outbox ORDER BY created_at, id`)
	require.NoError(t, err)
	defer rows.Close()
	var types []string
	for rows.Next() {
		var typ, agg string
		var payload []byte
		require.NoError(t, rows.Scan(&typ, &agg, &payload))
		assert.Equal(t, j.ID.String(), agg)
		assert.NoError(t, eventschema.Validate(typ, payload), typ)
		types = append(types, typ)
	}
	require.NoError(t, rows.Err())
	// Publish is a state change of the job, so it emits job.updated.
	assert.Equal(t, []string{"job.created", "job.updated", "job.updated", "job.closed", "job.deleted"}, types)
}

// failingOutbox writes the event and then fails, simulating a late error.
type failingOutbox struct{ inner app.OutboxWriter }

func (f failingOutbox) Write(ctx context.Context, e event.Envelope, id string) error {
	if err := f.inner.Write(ctx, e, id); err != nil {
		return err
	}
	return errors.New("boom after write")
}

func TestEvents_RollbackLeavesNoOutboxRow(t *testing.T) {
	truncate(t)
	svc := newService(failingOutbox{inner: postgres.NewOutboxWriter(pool)})
	_, err := svc.Create(recruiterCtx(), draft)
	require.Error(t, err)

	var jobs, outbox int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&jobs))
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox`).Scan(&outbox))
	assert.Zero(t, jobs)
	assert.Zero(t, outbox)
}
