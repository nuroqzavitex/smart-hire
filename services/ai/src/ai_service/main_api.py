"""ai-api: health, readiness and Prometheus metrics endpoints."""

import logging
from collections.abc import Callable, Iterable

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import (
    CollectorRegistry,
    GCCollector,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from sqlalchemy import Engine, text

from ai_service.adapters.db import make_engine, outbox_pending
from ai_service.config import load_settings
from ai_service.observability import setup_logging, setup_otel

log = logging.getLogger("ai_service.api")


class OutboxPendingCollector(Collector):
    """Computes ai_outbox_pending at scrape time; a DB error just omits the metric."""

    def __init__(self, pending: Callable[[], int]) -> None:
        self._pending = pending

    def collect(self) -> Iterable[Metric]:
        try:
            value = self._pending()
        except Exception as exc:
            log.warning("outbox pending count failed", extra={"error_type": type(exc).__name__})
            return
        gauge = GaugeMetricFamily("ai_outbox_pending", "Outbox rows not yet published")
        gauge.add_metric([], value)
        yield gauge


def create_app(ready: Callable[[], None], pending: Callable[[], int] | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    GCCollector(registry=registry)
    if pending is not None:
        registry.register(OutboxPendingCollector(pending))

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> JSONResponse:
        try:
            ready()
        except Exception as exc:
            log.warning("not ready", extra={"error_type": type(exc).__name__})
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ok"})

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(registry), media_type="text/plain; version=0.0.4; charset=utf-8")

    return app


def _ready_check(engine: Engine) -> Callable[[], None]:
    def check() -> None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    return check


def main() -> None:
    settings = load_settings()
    setup_logging(settings.log_level, "ai-api")
    shutdown = setup_otel("ai-api")
    engine = make_engine(settings.db_dsn)
    app = create_app(_ready_check(engine), lambda: outbox_pending(engine))
    try:
        uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_config=None)  # noqa: S104
    finally:
        shutdown()
        engine.dispose()


if __name__ == "__main__":
    main()
