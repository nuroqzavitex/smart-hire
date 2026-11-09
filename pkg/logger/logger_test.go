package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

func TestLogger_AddsContextFields(t *testing.T) {
	var buf bytes.Buffer
	l := logger.New("auth", &buf, slog.LevelInfo)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	ctx = logger.WithUserID(logger.WithRequestID(ctx, "req-1"), "u-1")
	l.InfoContext(ctx, "hello")
	span.End()
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "auth", m["service"])
	assert.Equal(t, "req-1", m["request_id"])
	assert.Equal(t, "u-1", m["user_id"])
	assert.Equal(t, span.SpanContext().TraceID().String(), m["trace_id"])
	assert.Equal(t, span.SpanContext().SpanID().String(), m["span_id"])
}

func TestLogger_NoContextFields(t *testing.T) {
	var buf bytes.Buffer
	l := logger.New("auth", &buf, slog.LevelInfo)
	l.With("k", "v").Info("plain")
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "auth", m["service"])
	assert.Equal(t, "v", m["k"])
	assert.NotContains(t, m, "trace_id")
	assert.NotContains(t, m, "request_id")
	assert.Equal(t, "", logger.RequestIDFrom(context.Background()))
}
