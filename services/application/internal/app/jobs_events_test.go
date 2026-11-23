package app_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func jobEvent(t *testing.T, typ string, jobID uuid.UUID) []byte {
	t.Helper()
	env, err := event.New(context.Background(), "job-service", typ, 1, map[string]any{"job_id": jobID.String(), "version": 3}, t0)
	require.NoError(t, err)
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	return raw
}

func TestHandleJobEvent_CancelsRunningSagas(t *testing.T) {
	for _, typ := range []string{"job.closed", "job.deleted"} {
		t.Run(typ, func(t *testing.T) {
			e := newEnv(t)
			_, running := e.submit(t)
			_, done := e.submit(t)
			s := e.sagas.m[done.ID]
			s.State = domain.SagaCompleted
			e.sagas.m[done.ID] = s
			e.clock.Advance(time.Minute)
			wakes := e.wakes

			require.NoError(t, e.svc.HandleJobEvent(context.Background(), jobEvent(t, typ, e.job)))
			r := e.sagas.m[running.ID]
			assert.Equal(t, domain.SagaCompensating, r.State)
			assert.Equal(t, domain.StepReleaseQuota, r.Step)
			assert.Equal(t, domain.ReasonJobClosed, r.CancelReason)
			assert.Equal(t, t0.Add(time.Minute), r.NextRunAt)
			assert.Equal(t, domain.SagaCompleted, e.sagas.m[done.ID].State, "finished sagas are left alone")
			assert.Equal(t, wakes+1, e.wakes)

			e.step(t, 1)
			assert.Equal(t, domain.StatusCancelled, e.apps.m[running.ID].Status)
			assert.Equal(t, domain.ReasonJobClosed, e.apps.m[running.ID].Reason)
		})
	}
}

func TestHandleJobEvent_DuplicateIgnored(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	raw := jobEvent(t, "job.closed", e.job)
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), raw))
	version := e.sagas.m[a.ID].Version
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), raw))
	assert.Equal(t, version, e.sagas.m[a.ID].Version)
}

func TestHandleJobEvent_OtherJobsAndTypesIgnored(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t)
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), jobEvent(t, "job.closed", uuid.New())))
	require.NoError(t, e.svc.HandleJobEvent(context.Background(), jobEvent(t, "job.updated", e.job)))
	assert.Equal(t, domain.SagaRunning, e.sagas.m[a.ID].State)
}

func TestHandleJobEvent_Poison(t *testing.T) {
	e := newEnv(t)
	assert.ErrorIs(t, e.svc.HandleJobEvent(context.Background(), []byte("{")), app.ErrPoisonEvent)
	env, err := event.New(context.Background(), "job-service", "job.closed", 1, map[string]any{"job_id": "nope"}, t0)
	require.NoError(t, err)
	raw, err := json.Marshal(env)
	require.NoError(t, err)
	assert.ErrorIs(t, e.svc.HandleJobEvent(context.Background(), raw), app.ErrPoisonEvent)
}
