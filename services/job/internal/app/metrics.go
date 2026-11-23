package app

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	pkgotel "github.com/nuroqzavitex/smarthire/pkg/otel"
)

const meterName = "github.com/nuroqzavitex/smarthire/services/job/internal/app"

// searchFallback counts searches served by the Postgres fallback
// (search.fallback, ADR 0009/0013).
func searchFallback(ctx context.Context) {
	c, err := pkgotel.Meter(meterName).Int64Counter("search.fallback",
		metric.WithDescription("Job searches served by the Postgres fallback"))
	if err == nil {
		c.Add(ctx, 1)
	}
}
