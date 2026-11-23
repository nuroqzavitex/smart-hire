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
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func (e *env) submitAs(t *testing.T, cand uuid.UUID, key string) uuid.UUID {
	t.Helper()
	a, err := e.svc.Submit(asCandidate(cand), app.SubmitInput{JobID: uuid.New(), CVID: uuid.New(), IdempotencyKey: key})
	require.NoError(t, err)
	return a.ID
}

func TestPurge_CandidateApplicationsArePseudonymised(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	cand := uuid.New()
	live := e.submitAs(t, cand, "k1")
	done := e.submitAs(t, cand, "k2")
	other := e.submit(t)
	_, err := pool.Exec(ctx, `UPDATE applications SET status = 'REJECTED' WHERE id = $1`, done)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE saga_instances SET state = 'COMPLETED' WHERE application_id = $1`, done)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO application_history (application_id, from_status, to_status, actor, reason, at)
		VALUES ($1, 'SCREENED', 'REJECTED', $2, '', now())`, done, cand.String())
	require.NoError(t, err)
	sagaBefore, err := e.sagas.GetByApplication(ctx, live)
	require.NoError(t, err)
	actorRows := count(t, `SELECT count(*) FROM application_history WHERE actor = $1`, cand.String())
	require.Positive(t, actorRows)

	env, err := userdeletion.NewDeletedEvent(ctx, userdeletion.Deleted{
		UserID: cand, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now(),
	}, time.Now())
	require.NoError(t, err)
	require.NoError(t, e.svc.HandleUserDeleted(ctx, env))
	require.NoError(t, e.svc.HandleUserDeleted(ctx, env), "redelivery")

	assert.Zero(t, count(t, `SELECT count(*) FROM applications WHERE candidate_id = $1`, cand), "no link to the person")
	a, err := e.apps.Get(ctx, live)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, a.Status)
	assert.Equal(t, domain.ReasonAccountDeleted, a.Reason)
	assert.Equal(t, uuid.Nil, a.CVID)
	assert.Nil(t, a.Breakdown)
	d, err := e.apps.Get(ctx, done)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusRejected, d.Status, "terminal applications keep their status")
	assert.NotEqual(t, a.CandidateID, d.CandidateID, "pseudonyms differ per application")

	s, err := e.sagas.GetByApplication(ctx, live)
	require.NoError(t, err)
	assert.Equal(t, domain.SagaCompensated, s.State)
	assert.Greater(t, s.Version, sagaBefore.Version, "a leased worker's write fails its version guard")
	_, err = e.svc.ProcessDue(ctx) // runs only the other candidate's saga
	require.NoError(t, err)
	a, err = e.apps.Get(ctx, live)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, a.Status, "the worker leaves the purged saga alone")

	assert.Zero(t, count(t, `SELECT count(*) FROM application_history WHERE actor = $1`, cand.String()))
	assert.Equal(t, actorRows, count(t, `SELECT count(*) FROM application_history WHERE actor = 'deleted-user'`))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM application_history WHERE application_id = $1 AND to_status = 'CANCELLED'`, live))

	o, err := e.apps.Get(ctx, other)
	require.NoError(t, err)
	assert.NotEqual(t, domain.StatusCancelled, o.Status, "other candidates untouched")

	var payload []byte
	require.NoError(t, pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE event_type = $1`, userdeletion.TopicPurged).Scan(&payload))
	assert.NoError(t, eventschema.Validate(userdeletion.TopicPurged, payload))
	assert.Zero(t, count(t, `SELECT count(*) FROM outbox WHERE event_type = 'application.status_changed'
		AND aggregate_id = $1 AND payload->'payload'->>'to' = 'CANCELLED'`, live.String()),
		"no status_changed for the purge cancellation")
}

func TestPurge_RecruiterIsAnonymised(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	id := e.submit(t)
	env, err := userdeletion.NewDeletedEvent(ctx, userdeletion.Deleted{
		UserID: e.recruiter, Role: "recruiter", RequestID: uuid.New(), RequestedAt: time.Now(),
	}, time.Now())
	require.NoError(t, err)
	require.NoError(t, e.svc.HandleUserDeleted(ctx, env))
	a, err := e.apps.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, a.RecruiterID)
	assert.Equal(t, domain.StatusPending, a.Status, "the candidate's application stays")
}
