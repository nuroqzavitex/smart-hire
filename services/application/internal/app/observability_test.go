package app_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// Final review: every saga step gets a span and an Info outcome log, and
// the events it writes carry that span's trace.
func TestSaga_StepSpanLogAndTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	e := newEnv(t)
	var logs bytes.Buffer
	e.svc.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	_, a := e.submit(t)
	events := len(e.ob.evs)
	e.step(t, 1)

	var step sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "saga RESERVE_QUOTA" {
			step = s
		}
	}
	require.NotNil(t, step, "span for the step")
	require.Greater(t, len(e.ob.evs), events)
	assert.Equal(t, step.SpanContext().TraceID().String(), e.ob.evs[len(e.ob.evs)-1].TraceID,
		"status_changed written by the step carries its trace")
	assert.Contains(t, logs.String(), `"msg":"saga step finished"`)
	assert.Contains(t, logs.String(), a.ID.String())
	assert.Contains(t, logs.String(), `"status":"SCREENING"`)
}

func TestSaga_StaleResultLoggedAtInfo(t *testing.T) {
	e := newEnv(t)
	var logs bytes.Buffer
	e.svc.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
	e.submit(t)
	e.quota.onReserve = func() {
		_, err := e.sagas.CancelRunningForJob(context.Background(), e.job, domain.ReasonJobClosed, e.clock.Now())
		require.NoError(t, err)
	}
	e.step(t, 1)
	assert.Contains(t, logs.String(), "result dropped")
}

// Final review: a saga whose lease ran out while earlier sagas of the batch
// were processed is skipped; another worker may own it by now.
func TestSaga_ExpiredLeaseInBatchSkipped(t *testing.T) {
	e := newEnv(t)
	_, first := e.submit(t)
	e.clock.Advance(time.Millisecond) // claim order follows next_run_at
	_, second := e.submit(t)
	e.quota.onReserve = func() { e.clock.Advance(lease + time.Second) } // a slow call
	e.step(t, 2)

	assert.Len(t, e.quota.reserved, 1, "only the first saga ran its step")
	assert.Equal(t, first.ID, e.quota.reserved[0])
	s := e.sagas.m[second.ID]
	assert.Equal(t, domain.StepReserveQuota, s.Step)
	assert.Equal(t, domain.StatusPending, e.apps.m[second.ID].Status)
}
