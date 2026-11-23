package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/application/internal/app"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

func TestWorker_WakeTriggersImmediateRun(t *testing.T) {
	e := newEnv(t)
	_, a := e.submit(t) // due now; the worker has not started yet
	claims := make(chan struct{}, 100)
	e.sagas.onClaim = func() { claims <- struct{}{} }

	w := app.NewWorker(e.svc, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Without waiting for the poll the worker drains everything due: the
	// reserve claim, the score claim, then an empty claim.
	for range 3 {
		select {
		case <-claims:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not drain the due saga")
		}
	}
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, domain.StatusScreened, e.apps.m[a.ID].Status)
}

func TestWorker_WakeInterruptsPoll(t *testing.T) {
	e := newEnv(t)
	claims := make(chan struct{}, 100)
	e.sagas.onClaim = func() { claims <- struct{}{} }
	reserved := make(chan struct{}, 1)
	e.quota.onReserve = func() { reserved <- struct{}{} }
	w := app.NewWorker(e.svc, time.Hour, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	<-claims // the first, empty loop ran; the worker now waits up to an hour

	e.submit(t) // the env's Wake hook only counts, so wake the worker by hand
	w.Wake()
	select {
	case <-reserved:
	case <-time.After(5 * time.Second):
		t.Fatal("wake did not interrupt the poll")
	}
	cancel()
	require.NoError(t, <-done)
	w.Wake() // never blocks, even with nobody listening
	w.Wake()
}
