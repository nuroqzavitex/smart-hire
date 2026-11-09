package otel_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelapi "go.opentelemetry.io/otel"

	"github.com/nuroqzavitex/smarthire/pkg/otel"
)

func TestSetup_NoEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := otel.Setup(context.Background(), "x")
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))
	assert.Contains(t, otelapi.GetTextMapPropagator().Fields(), "traceparent")
}
