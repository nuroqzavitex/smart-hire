// Package oteltest helps tests read metrics recorded through the global
// OTel meter provider.
package oteltest

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Metrics reads what the global meter provider recorded during a test.
type Metrics struct{ r *sdkmetric.ManualReader }

// Install points the global meter provider at a manual reader until the test
// ends. Instruments created afterwards (or looked up per call) report to it.
func Install(t *testing.T) *Metrics {
	t.Helper()
	r := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	return &Metrics{r: r}
}

// Int64 returns the value of the int64 sum or gauge data point of metric
// name whose attributes include every key/value pair in attrs.
func (m *Metrics) Int64(t *testing.T, name string, attrs ...string) (int64, bool) {
	t.Helper()
	for _, dp := range m.points(t, name) {
		if match(dp.Attributes, attrs) {
			return dp.Value, true
		}
	}
	return 0, false
}

// HistogramCount returns the sample count of the float64 histogram data point
// of metric name matching attrs.
func (m *Metrics) HistogramCount(t *testing.T, name string, attrs ...string) (uint64, bool) {
	t.Helper()
	for _, mt := range m.collect(t) {
		if mt.Name != name {
			continue
		}
		if h, ok := mt.Data.(metricdata.Histogram[float64]); ok {
			for _, dp := range h.DataPoints {
				if match(dp.Attributes, attrs) {
					return dp.Count, true
				}
			}
		}
	}
	return 0, false
}

func (m *Metrics) points(t *testing.T, name string) []metricdata.DataPoint[int64] {
	for _, mt := range m.collect(t) {
		if mt.Name != name {
			continue
		}
		switch d := mt.Data.(type) {
		case metricdata.Sum[int64]:
			return d.DataPoints
		case metricdata.Gauge[int64]:
			return d.DataPoints
		}
	}
	return nil
}

func (m *Metrics) collect(t *testing.T) []metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := m.r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var out []metricdata.Metrics
	for _, sm := range rm.ScopeMetrics {
		out = append(out, sm.Metrics...)
	}
	return out
}

func match(set attribute.Set, attrs []string) bool {
	for i := 0; i+1 < len(attrs); i += 2 {
		v, ok := set.Value(attribute.Key(attrs[i]))
		if !ok || v.String() != attrs[i+1] {
			return false
		}
	}
	return true
}
