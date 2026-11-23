package app

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

// SagaCounter counts sagas per state.
type SagaCounter interface {
	CountByState(ctx context.Context) (map[domain.SagaState]int, error)
}

var sagaStates = []domain.SagaState{domain.SagaRunning, domain.SagaCompensating, domain.SagaCompleted, domain.SagaCompensated}

// RegisterSagaGauge registers saga.instances{state} (every state, 0 when
// none; ADR 0011/0013). A failed count skips the observation.
func RegisterSagaGauge(m metric.Meter, c SagaCounter) (metric.Registration, error) {
	g, err := m.Int64ObservableGauge("saga.instances", metric.WithDescription("Submit sagas per state"))
	if err != nil {
		return nil, err
	}
	return m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		counts, err := c.CountByState(ctx)
		if err != nil {
			return nil //nolint:nilerr // skip this observation
		}
		for _, st := range sagaStates {
			o.ObserveInt64(g, int64(counts[st]), metric.WithAttributes(attribute.String("state", string(st))))
		}
		return nil
	}, g)
}
