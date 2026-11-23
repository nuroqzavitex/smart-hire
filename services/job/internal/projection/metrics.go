package projection

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	pkgotel "github.com/nuroqzavitex/smarthire/pkg/otel"
)

const meterName = "github.com/nuroqzavitex/smarthire/services/job/internal/projection"

// recordLag records projection.lag: seconds from the event's occurred_at to
// the moment the read model accepted it (alert: p95 > 30s, ADR 0013).
func (p *Projector) recordLag(ctx context.Context, env event.Envelope) {
	h, err := pkgotel.Meter(meterName).Float64Histogram("projection.lag", metric.WithUnit("s"),
		metric.WithDescription("Seconds from a job event to its indexing in the read model"))
	if err != nil {
		return
	}
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	h.Record(ctx, max(0, now().Sub(env.OccurredAt).Seconds()))
}
