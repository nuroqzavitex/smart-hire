package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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

// memExporter keeps exported OTel log records.
type memExporter struct {
	mu   sync.Mutex
	recs []sdklog.Record
}

func (m *memExporter) Export(_ context.Context, rs []sdklog.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range rs {
		m.recs = append(m.recs, r.Clone())
	}
	return nil
}
func (m *memExporter) Shutdown(context.Context) error   { return nil }
func (m *memExporter) ForceFlush(context.Context) error { return nil }

func (m *memExporter) records() []sdklog.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdklog.Record(nil), m.recs...)
}

// installLogs sets a global OTel LoggerProvider *after* the logger exists,
// as bootstrap does (logger first, otel.Setup second).
func installLogs(t *testing.T) *memExporter {
	t.Helper()
	exp := &memExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	prev := otelapi.GetLoggerProvider()
	otelapi.SetLoggerProvider(lp)
	t.Cleanup(func() { otelapi.SetLoggerProvider(prev) })
	return exp
}

func attr(r sdklog.Record, key string) (string, bool) {
	var val string
	var found bool
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			val, found = kv.Value.AsString(), true
			return false
		}
		return true
	})
	return val, found
}

func TestLogger_TeesToOTel(t *testing.T) {
	var buf bytes.Buffer
	l := logger.New("auth", &buf, slog.LevelInfo)
	exp := installLogs(t)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	l.InfoContext(logger.WithRequestID(ctx, "req-1"), "hello", "k", "v")
	span.End()

	assert.Contains(t, buf.String(), `"msg":"hello"`, "stdout JSON still written")
	recs := exp.records()
	require.Len(t, recs, 1)
	assert.Equal(t, "hello", recs[0].Body().AsString())
	assert.Equal(t, span.SpanContext().TraceID(), recs[0].TraceID(), "record carries the span's trace")
	got, ok := attr(recs[0], "request_id")
	assert.True(t, ok)
	assert.Equal(t, "req-1", got)
	got, _ = attr(recs[0], "service")
	assert.Equal(t, "auth", got)
	got, _ = attr(recs[0], "k")
	assert.Equal(t, "v", got)
}

func TestLogger_LevelAppliesToOTel(t *testing.T) {
	var buf bytes.Buffer
	l := logger.New("auth", &buf, slog.LevelInfo)
	exp := installLogs(t)
	l.Debug("noise")
	assert.Empty(t, exp.records())
	assert.Empty(t, buf.String())
}
