// Package otel bootstraps OpenTelemetry for services: traces, metrics and
// logs all go over OTLP to the collector (ADR 0005, ADR 0013).
package otel

import (
	"context"
	"errors"
	"os"
	"time"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// metricInterval is how often metrics are pushed to the collector.
const metricInterval = 10 * time.Second

// Setup installs the TraceContext+Baggage propagator and, when
// OTEL_EXPORTER_OTLP_ENDPOINT is set, OTLP gRPC tracer, meter and logger
// providers. The returned shutdown flushes and stops them (no-op without an
// exporter).
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otelapi.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(service)))
	if err != nil {
		return nil, err
	}
	traceExp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	metricExp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	logExp, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(metricInterval))),
		sdkmetric.WithResource(res),
	)
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)), sdklog.WithResource(res))
	otelapi.SetTracerProvider(tp)
	otelapi.SetMeterProvider(mp)
	otelapi.SetLoggerProvider(lp)
	return func(ctx context.Context) error {
		// Logs last, so shutdown messages of the other two still go out.
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

// Meter returns a meter from the global provider (no-op until Setup runs
// with an endpoint).
func Meter(name string) metric.Meter { return otelapi.GetMeterProvider().Meter(name) }
