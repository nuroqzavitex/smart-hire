package otel_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelapi "go.opentelemetry.io/otel"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/nuroqzavitex/smarthire/pkg/otel"
)

func TestSetup_NoEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := otel.Setup(context.Background(), "x")
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))
	assert.Contains(t, otelapi.GetTextMapPropagator().Fields(), "traceparent")
}

// With an endpoint, Setup installs SDK meter and logger providers too (the
// exporters connect lazily, so an unreachable endpoint is fine here).
func TestSetup_InstallsMeterAndLoggerProviders(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	prevM, prevL := otelapi.GetMeterProvider(), otelapi.GetLoggerProvider()
	t.Cleanup(func() { otelapi.SetMeterProvider(prevM); otelapi.SetLoggerProvider(prevL) })
	shutdown, err := otel.Setup(context.Background(), "x")
	require.NoError(t, err)
	_, isSDKMeter := otelapi.GetMeterProvider().(*sdkmetric.MeterProvider)
	assert.True(t, isSDKMeter)
	_, isSDKLog := otelapi.GetLoggerProvider().(*sdklog.LoggerProvider)
	assert.True(t, isSDKLog)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(ctx) // flush errors are expected: nothing listens
}

func TestRecordBreakerState(t *testing.T) {
	r := sdkmetric.NewManualReader()
	prev := otelapi.GetMeterProvider()
	otelapi.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	t.Cleanup(func() { otelapi.SetMeterProvider(prev) })
	otel.RecordBreakerState(context.Background(), "x", otel.BreakerOpen)
	var rm metricdata.ResourceMetrics
	require.NoError(t, r.Collect(context.Background(), &rm))
	g := find(t, rm, "circuit_breaker.state").Data.(metricdata.Gauge[int64])
	require.Len(t, g.DataPoints, 1)
	assert.Equal(t, int64(2), g.DataPoints[0].Value)
	v, _ := g.DataPoints[0].Attributes.Value("breaker")
	assert.Equal(t, "x", v.AsString())
}

func find(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %s not found", name)
	return metricdata.Metrics{}
}
