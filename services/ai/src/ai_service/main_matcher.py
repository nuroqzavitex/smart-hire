"""ai-matcher: consumes job.* and cv.updated, keeps vectors and precomputed matches,
publishes match.completed through the outbox relay (ADR 0010)."""

import logging
import signal
import threading
from datetime import UTC, datetime
from types import FrameType
from typing import cast

from opentelemetry import trace
from qdrant_client import QdrantClient
from sqlalchemy.exc import OperationalError

from ai_service.adapters.db import make_engine
from ai_service.adapters.embedder import build_embedder
from ai_service.adapters.kafka_consumer import (
    DlqWriteError,
    KafkaConsumerLike,
    RetryingConsumer,
    make_consumer,
)
from ai_service.adapters.kafka_producer import KafkaPublisher
from ai_service.adapters.match_store import PgMatchStore
from ai_service.adapters.outbox import OutboxRelay
from ai_service.adapters.vectors import QdrantVectors, VectorStoreUnavailable
from ai_service.config import Settings, load_settings
from ai_service.observability import setup_logging, setup_otel
from ai_service.services.embedding import EmbeddingUnavailable
from ai_service.services.matcher import TOPICS, MatchHandler

log = logging.getLogger("ai_service.matcher")

GROUP_ID = "ai-matcher"
TRANSIENT = (EmbeddingUnavailable, VectorStoreUnavailable, OperationalError)
QDRANT_TIMEOUT_S = 10


def _current_trace_id() -> str:
    ctx = trace.get_current_span().get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else ""


def main() -> int:
    settings = load_settings()
    setup_logging(settings.log_level, "ai-matcher")
    shutdown_otel = setup_otel("ai-matcher")
    try:
        return _run(settings)
    finally:
        shutdown_otel()


def _ensure_collections(vectors: QdrantVectors, stop: threading.Event) -> bool:
    """Waits for Qdrant (it may start after us); False when stopped first."""
    delay = 0.5
    while not stop.is_set():
        try:
            vectors.ensure_collections()
            return True
        except VectorStoreUnavailable:
            log.warning("qdrant unavailable, retrying")
            stop.wait(delay)
            delay = min(delay * 2, 10)
    return False


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
        embedder = build_embedder(settings)
        vectors = QdrantVectors(
            QdrantClient(url=settings.qdrant_url, timeout=QDRANT_TIMEOUT_S), embedder.model_version
        )
        if not _ensure_collections(vectors, stop):
            return 0
        handler = MatchHandler(
            PgMatchStore(engine),
            vectors,
            embedder,
            clock=lambda: datetime.now(UTC),
            trace_id=_current_trace_id,
        )
        publisher = KafkaPublisher(settings.kafka_brokers)
        relay = OutboxRelay(
            engine, publisher, interval_s=settings.outbox_interval_ms / 1000, stop=stop, log=log
        )

        def run_relay() -> None:
            try:
                relay.run()
            except Exception as exc:
                log.error("outbox relay crashed", extra={"error_type": type(exc).__name__})
                relay_failed.set()
                stop.set()

        consumer = make_consumer(settings.kafka_brokers, group_id=GROUP_ID, topics=TOPICS)
        relay_thread = threading.Thread(target=run_relay, name="outbox-relay", daemon=False)
        relay_thread.start()
        log.info("matcher started", extra={"model_version": embedder.model_version})
        RetryingConsumer(
            cast(KafkaConsumerLike, consumer), publisher, handler.handle, stop, transient=TRANSIENT, log=log
        ).run()
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
