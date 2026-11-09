//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres"
)

func TestRegister_OutboxHasHeaders(t *testing.T) {
	truncate(t)
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "register")
	defer span.End()

	svc := newService(t, postgres.NewOutboxWriter(pool))
	_, err := svc.Register(ctx, "h@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)

	var traceparent string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT headers->>'traceparent' FROM outbox WHERE event_type='user.registered'`).Scan(&traceparent))
	assert.Contains(t, traceparent, span.SpanContext().TraceID().String())
}

func TestRegister_EventMatchesSchema(t *testing.T) {
	truncate(t)
	svc := newService(t, postgres.NewOutboxWriter(pool))
	_, err := svc.Register(context.Background(), "s@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)

	var payload []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE event_type='user.registered'`).Scan(&payload))
	require.NoError(t, eventschema.Validate("user.registered", payload))
}
