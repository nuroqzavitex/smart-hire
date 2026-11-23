package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	meterName       = "github.com/nuroqzavitex/smarthire/pkg/kafka"
	lagQueryTimeout = 5 * time.Second
)

// consumerMetrics holds the instruments of one consumer (ADR 0013).
type consumerMetrics struct {
	dlq metric.Int64Counter
	reg metric.Registration
}

// newConsumerMetrics creates dlq.messages{topic} and the observable gauge
// kafka.consumer.lag{group,topic} (sum over partitions, from the group's
// committed offsets; a failed lookup skips the observation).
func (c *Consumer) newConsumerMetrics() consumerMetrics {
	m := otel.GetMeterProvider().Meter(meterName)
	var cm consumerMetrics
	cm.dlq, _ = m.Int64Counter("dlq.messages", metric.WithDescription("Records written to a dead-letter topic"))
	lag, err := m.Int64ObservableGauge("kafka.consumer.lag",
		metric.WithDescription("Records not yet committed by the consumer group"))
	if err != nil {
		return cm
	}
	adm := kadm.NewClient(c.cl)
	group := c.cfg.Group
	cm.reg, _ = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, lagQueryTimeout)
		defer cancel()
		lags, err := adm.Lag(ctx, group)
		if err != nil {
			return nil //nolint:nilerr // skip this observation
		}
		gl, ok := lags[group]
		if !ok || gl.Error() != nil {
			return nil
		}
		for topic, sum := range gl.Lag.TotalByTopic() {
			o.ObserveInt64(lag, sum.Lag, metric.WithAttributes(
				attribute.String("group", group), attribute.String("topic", topic)))
		}
		return nil
	}, lag)
	return cm
}

func (cm consumerMetrics) close() {
	if cm.reg != nil {
		_ = cm.reg.Unregister()
	}
}
