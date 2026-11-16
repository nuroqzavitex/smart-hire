"""ai-worker: consumes cv.uploaded, publishes cv.parsed / cv.parse_failed through the outbox relay.

The consumer/handler run single-threaded (the Anthropic adapter keeps per-call state).
"""

import logging
import os
import signal
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import cast

import minio
from opentelemetry import trace

from ai_service.adapters.db import PgResultStore, make_engine
from ai_service.adapters.kafka_consumer import (
    DlqWriteError,
    KafkaConsumerLike,
    RetryingConsumer,
    make_consumer,
)
from ai_service.adapters.kafka_producer import KafkaPublisher
from ai_service.adapters.llm import build_llm
from ai_service.adapters.outbox import OutboxRelay
from ai_service.adapters.pdf import PyMuPdfExtractor
from ai_service.adapters.storage import MinioObjectReader
from ai_service.config import Settings, load_settings
from ai_service.observability import setup_logging, setup_otel
from ai_service.services.cv_uploaded_handler import CvUploadedHandler

log = logging.getLogger("ai_service.worker")


def _current_trace_id() -> str:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else ""


def main() -> int:
    settings = load_settings()
    setup_logging(settings.log_level, "ai-worker")
    shutdown_otel = setup_otel("ai-worker")
    try:
        return _run(settings)
    finally:
        shutdown_otel()


def _run(settings: Settings) -> int:
    stop = threading.Event()

    def on_signal(_signum: int, _frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    engine = make_engine(settings.db_dsn)
    publisher: KafkaPublisher | None = None
    relay_thread: threading.Thread | None = None
    consumer = None
    relay_failed = threading.Event()
    code = 0
    try:
        storage = MinioObjectReader(
            minio.Minio(
                settings.s3_endpoint,
                access_key=settings.s3_access_key,
                secret_key=settings.s3_secret_key.get_secret_value(),
                secure=settings.s3_secure,
                region=settings.s3_region,
            ),
            settings.s3_bucket,
        )
        handler = CvUploadedHandler(
            storage,
            PyMuPdfExtractor(),
            build_llm(settings, os.environ, Path.home()),
            PgResultStore(engine),
            clock=lambda: datetime.now(UTC),
            trace_id=_current_trace_id,
        )
        publisher = KafkaPublisher(settings.kafka_brokers)
        relay = OutboxRelay(
            engine,
            publisher,
            interval_s=settings.outbox_interval_ms / 1000,
            stop=stop,
            log=log,
        )

        def run_relay() -> None:
            try:
                relay.run()
            except Exception as exc:
                log.error("outbox relay crashed", extra={"error_type": type(exc).__name__})
                relay_failed.set()
                stop.set()

        # The consumer is created before the (non-daemon) relay thread starts: if that raises,
        # nothing is left running that could keep the process alive.
        consumer = make_consumer(settings.kafka_brokers)
        relay_thread = threading.Thread(target=run_relay, name="outbox-relay", daemon=False)
        relay_thread.start()
        RetryingConsumer(cast(KafkaConsumerLike, consumer), publisher, handler.handle, stop, log=log).run()
    except DlqWriteError:
        log.error("dead-letter write failed, stopping")
        code = 1
    finally:
        stop.set()
        if relay_thread is not None:
            relay_thread.join(timeout=10)
        if consumer is not None:
            consumer.close()
        if publisher is not None:
            publisher.flush(5)
        engine.dispose()
    return 1 if relay_failed.is_set() else code


if __name__ == "__main__":
    raise SystemExit(main())
