package outbox

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	tpTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	tp        = "00-" + tpTraceID + "-00f067aa0ba902b7-01"
)

func TestRecordFromRow_StripsTraceHeaders(t *testing.T) {
	old := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() { otel.SetTextMapPropagator(old) })

	ctx, rec := recordFromRow(context.Background(), row{AggregateID: "j1", EventType: "job.created",
		Payload: []byte(`{}`), Headers: map[string]string{"traceparent": tp, "tracestate": "a=b", "baggage": "k=v"}})
	assert.Equal(t, "job.created", rec.Topic)
	assert.Equal(t, []byte("j1"), rec.Key)
	assert.NotContains(t, rec.Headers, "traceparent")
	assert.NotContains(t, rec.Headers, "tracestate")
	assert.Equal(t, "k=v", rec.Headers["baggage"])
	assert.Equal(t, tpTraceID, trace.SpanContextFromContext(ctx).TraceID().String())
}
