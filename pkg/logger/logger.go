// Package logger provides JSON slog loggers enriched with trace and request context.
package logger

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/trace"
)

type ctxKey int

const (
	requestIDKey ctxKey = iota
	userIDKey
)

// WithRequestID stores the request ID in ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFrom returns the request ID stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// WithUserID stores the user ID in ctx.
func WithUserID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, userIDKey, id)
}

type ctxHandler struct {
	slog.Handler
}

func (h ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	if id := RequestIDFrom(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	if id, _ := ctx.Value(userIDKey).(string); id != "" {
		r.AddAttrs(slog.String("user_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h ctxHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return ctxHandler{h.Handler.WithAttrs(attrs)}
}

func (h ctxHandler) WithGroup(name string) slog.Handler {
	return ctxHandler{h.Handler.WithGroup(name)}
}

// fanout sends every record to all handlers that accept its level.
type fanout []slog.Handler

func (f fanout) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range f {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (f fanout) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (f fanout) WithAttrs(attrs []slog.Attr) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithAttrs(attrs)
	}
	return out
}

func (f fanout) WithGroup(name string) slog.Handler {
	out := make(fanout, len(f))
	for i, h := range f {
		out[i] = h.WithGroup(name)
	}
	return out
}

// leveled drops records below min before they reach h.
type leveled struct {
	slog.Handler
	min slog.Level
}

func (l leveled) Enabled(ctx context.Context, lv slog.Level) bool {
	return lv >= l.min && l.Handler.Enabled(ctx, lv)
}

func (l leveled) WithAttrs(attrs []slog.Attr) slog.Handler {
	return leveled{l.Handler.WithAttrs(attrs), l.min}
}

func (l leveled) WithGroup(name string) slog.Handler {
	return leveled{l.Handler.WithGroup(name), l.min}
}

// New returns a JSON logger tagged with service that adds trace_id, span_id,
// request_id and user_id from the context passed to *Context methods. Every
// record is also sent to the global OTel LoggerProvider (exported to Loki
// through the collector, ADR 0013); a provider installed later by otel.Setup
// still receives them, and without one the bridge is a no-op.
func New(service string, w io.Writer, level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	bridge := leveled{otelslog.NewHandler(service), level}
	return slog.New(ctxHandler{fanout{base, bridge}}).With("service", service)
}
