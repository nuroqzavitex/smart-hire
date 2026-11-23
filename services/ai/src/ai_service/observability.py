"""JSON logging and OpenTelemetry setup shared by ai-worker and ai-api."""

import json
import logging
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Attributes every LogRecord has; anything else came in through `extra=`.
_STANDARD = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: time, level, service, msg and the record's extras."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self._service,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and key not in doc:
                doc[key] = value
        if record.exc_info:
            # Type only: exception messages can carry CV text or DSNs.
            doc["error_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
        return json.dumps(doc, default=str, ensure_ascii=False)


def setup_logging(level: str, service: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def setup_otel(service: str) -> Callable[[], None]:
    """Installs tracer, meter and logger providers (ADR 0013).

    Telemetry is exported over OTLP gRPC only if OTEL_EXPORTER_OTLP_ENDPOINT is
    set; then metrics are pushed every 10s and log records also go to the
    collector (and on to Loki) through a LoggingHandler on the root logger.
    Call it after setup_logging, which replaces the root handlers.
    """
    resource = Resource.create({"service.name": service})
    provider = TracerProvider(resource=resource)
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        trace.set_tracer_provider(provider)
        return provider.shutdown
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(insecure=True)))
    trace.set_tracer_provider(provider)
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[
            PeriodicExportingMetricReader(OTLPMetricExporter(insecure=True), export_interval_millis=10_000)
        ],
    )
    metrics.set_meter_provider(meter_provider)
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(insecure=True)))
    set_logger_provider(logger_provider)
    logging.getLogger().addHandler(LoggingHandler(logger_provider=logger_provider))

    def shutdown() -> None:
        provider.shutdown()
        meter_provider.shutdown()
        logger_provider.shutdown()

    return shutdown


def meter(name: str = "ai_service") -> metrics.Meter:
    """Meter from the global provider (no-op until setup_otel runs with an endpoint)."""
    return metrics.get_meter(name)
