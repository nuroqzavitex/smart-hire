// Package event defines the standard event envelope written to the outbox.
package event

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/trace"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

// Envelope is the event wrapper defined in spec §7.1.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	Source        string          `json:"source"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	CorrelationID string          `json:"correlation_id"`
	Payload       json.RawMessage `json:"payload"`
}

// New builds an Envelope with a fresh ULID, trace ID from ctx and the request
// ID as correlation ID.
func New(ctx context.Context, source, eventType string, version int, payload any, now time.Time) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	var traceID string
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		traceID = sc.TraceID().String()
	}
	return Envelope{
		EventID:       ulid.Make().String(),
		EventType:     eventType,
		EventVersion:  version,
		Source:        source,
		OccurredAt:    now.UTC(),
		TraceID:       traceID,
		CorrelationID: logger.RequestIDFrom(ctx),
		Payload:       raw,
	}, nil
}
