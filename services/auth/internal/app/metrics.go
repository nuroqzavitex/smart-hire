package app

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// DeletionCounter counts deletion requests per status.
type DeletionCounter interface {
	DeletionCounts(ctx context.Context) (map[domain.DeletionStatus]int64, error)
}

var deletionStatuses = []domain.DeletionStatus{domain.DeletionPending, domain.DeletionCompleted, domain.DeletionStuck}

// RegisterDeletionGauge registers account.deletions{status} (every status, 0
// when none; the AccountDeletionStuck alert reads STUCK). A failed count
// skips the observation.
func RegisterDeletionGauge(m metric.Meter, c DeletionCounter) (metric.Registration, error) {
	g, err := m.Int64ObservableGauge("account.deletions", metric.WithDescription("Account-deletion requests per status"))
	if err != nil {
		return nil, err
	}
	return m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		counts, err := c.DeletionCounts(ctx)
		if err != nil {
			return nil //nolint:nilerr // skip this observation
		}
		for _, st := range deletionStatuses {
			o.ObserveInt64(g, counts[st], metric.WithAttributes(attribute.String("status", string(st))))
		}
		return nil
	}, g)
}
