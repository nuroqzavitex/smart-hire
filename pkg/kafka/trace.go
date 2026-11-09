package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// tracer is resolved lazily so a global provider set after init is honoured.
func tracer() trace.Tracer { return otel.Tracer("github.com/nuroqzavitex/smarthire/pkg/kafka") }

// headerCarrier adapts record headers to propagation.TextMapCarrier.
type headerCarrier struct{ h *[]kgo.RecordHeader }

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.h {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, val string) {
	for i, h := range *c.h {
		if h.Key == key {
			(*c.h)[i].Value = []byte(val)
			return
		}
	}
	*c.h = append(*c.h, kgo.RecordHeader{Key: key, Value: []byte(val)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.h))
	for _, h := range *c.h {
		keys = append(keys, h.Key)
	}
	return keys
}
