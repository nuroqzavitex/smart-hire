package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Circuit breaker states as reported by circuit_breaker.state.
const (
	BreakerClosed   int64 = 0
	BreakerHalfOpen int64 = 1
	BreakerOpen     int64 = 2
)

const meterName = "github.com/nuroqzavitex/smarthire/pkg/otel"

// RecordBreakerState sets circuit_breaker.state{breaker} (0 closed,
// 1 half-open, 2 open). Services call it from their breaker's state-change
// hook and once at construction (closed).
func RecordBreakerState(ctx context.Context, breaker string, state int64) {
	// The instrument is looked up on every call (cheap, cached by the SDK)
	// so a provider installed after the breaker was built is honoured.
	g, err := Meter(meterName).Int64Gauge("circuit_breaker.state",
		metric.WithDescription("Circuit breaker state: 0 closed, 1 half-open, 2 open"))
	if err != nil {
		return
	}
	g.Record(ctx, state, metric.WithAttributes(attribute.String("breaker", breaker)))
}

// BreakerStateFromString maps a breaker library's state name ("closed",
// "half-open", "open", as gobreaker's State.String returns) to the gauge value.
func BreakerStateFromString(s string) int64 {
	switch s {
	case "open":
		return BreakerOpen
	case "half-open":
		return BreakerHalfOpen
	}
	return BreakerClosed
}
