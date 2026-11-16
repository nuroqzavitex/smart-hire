"""cv.uploaded consumer: manual commit, in-process retry, DLQ (same semantics as Go pkg/kafka, ADR 0006).

Error text written to logs and `dlq-error` never includes CV content: schema/
validation errors are replaced by fixed messages and SQLAlchemy errors (which
echo statement parameters) are reduced to their class name.
"""

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

import confluent_kafka
from opentelemetry import propagate, trace
from opentelemetry.trace import Span, SpanKind
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from ai_service.adapters.kafka_producer import Publisher
from ai_service.domain.events import Envelope
from ai_service.services.cv_uploaded_handler import PermanentError

TOPIC = "cv.uploaded"
GROUP_ID = "ai-cv-parser"
DLQ_ERROR_MAX = 512
POLL_TIMEOUT_S = 1.0
_DLQ_PREFIX = "dlq-"


def backoff(attempt: int, base_s: float = 0.2, cap_s: float = 2.0) -> float:
    """Delay before retry number `attempt` (1-based): base * 2^(attempt-1), capped."""
    return float(min(base_s * 2 ** (attempt - 1), cap_s))


def dlq_headers(
    original: Sequence[tuple[str, bytes]],
    *,
    topic: str,
    partition: int,
    offset: int,
    error: str,
    attempts: int,
) -> list[tuple[str, bytes]]:
    kept = [(k, v) for k, v in original if not k.startswith(_DLQ_PREFIX)]
    return [
        *kept,
        ("dlq-error", error[:DLQ_ERROR_MAX].encode()),
        ("dlq-attempts", str(attempts).encode()),
        ("dlq-original-topic", topic.encode()),
        ("dlq-original-partition", str(partition).encode()),
        ("dlq-original-offset", str(offset).encode()),
    ]


class DlqWriteError(Exception):
    """The dead-letter write failed; the consumer must stop rather than skip the record."""


class MessageLike(Protocol):
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> Sequence[tuple[str, bytes | None]] | None: ...
    def error(self) -> Any: ...


class KafkaConsumerLike(Protocol):
    def poll(self, timeout: float) -> Any: ...
    def commit(self, *, message: Any, asynchronous: bool) -> Any: ...
    def close(self) -> None: ...


def _describe(exc: Exception) -> str:
    if isinstance(exc, SQLAlchemyError):
        return type(exc).__name__  # str(exc) would echo statement parameters
    return f"{type(exc).__name__}: {exc}"


class RetryingConsumer:
    def __init__(
        self,
        consumer: KafkaConsumerLike,
        dlq: Publisher,
        handler: Callable[[Envelope], None],
        stop: threading.Event,
        *,
        max_attempts: int = 3,
        base_s: float = 0.2,
        cap_s: float = 2.0,
        log: logging.Logger,
    ) -> None:
        self._consumer = consumer
        self._dlq = dlq
        self._handler = handler
        self._stop = stop
        self._max_attempts = max_attempts
        self._base_s = base_s
        self._cap_s = cap_s
        self._log = log

    def run(self) -> None:
        while not self._stop.is_set():
            msg = self._consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                continue
            if msg.error():
                self._log.warning("kafka consumer error", extra={"error_code": str(msg.error().name())})
                continue
            if not self.process(msg):
                return

    def process(self, msg: MessageLike) -> bool:
        """True: finished and committed. False: shutting down, left uncommitted."""
        raw_headers = [(k, v or b"") for k, v in (msg.headers() or [])]
        carrier = {k: v.decode("utf-8", "replace") for k, v in raw_headers}
        topic = msg.topic()
        tracer = trace.get_tracer("ai_service")
        with tracer.start_as_current_span(
            f"{topic} process",
            context=propagate.extract(carrier),
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": topic,
                "messaging.kafka.offset": msg.offset(),
            },
        ) as span:
            try:
                env = Envelope.model_validate_json(msg.value() or b"")
                if not env.event_id or not env.event_type:
                    raise ValueError("empty event_id or event_type")
            except (ValidationError, ValueError):
                self._log.warning("undecodable event", extra={"offset": msg.offset()})
                return self._dead_letter(msg, raw_headers, span, "undecodable event envelope", 0, "poison")
            return self._handle_with_retry(msg, raw_headers, span, env)

    def _handle_with_retry(
        self, msg: MessageLike, raw_headers: list[tuple[str, bytes]], span: Span, env: Envelope
    ) -> bool:
        attempts = 0
        while True:
            attempts += 1
            try:
                self._handler(env)
            except PermanentError as exc:
                self._log.warning("permanent error", extra={"event_id": env.event_id})
                return self._dead_letter(msg, raw_headers, span, _describe(exc), attempts, "permanent")
            except Exception as exc:
                if self._stop.is_set():
                    return False
                self._log.warning(
                    "handler failed",
                    extra={"event_id": env.event_id, "attempt": attempts, "error_type": type(exc).__name__},
                )
                if attempts >= self._max_attempts:
                    return self._dead_letter(msg, raw_headers, span, _describe(exc), attempts, "exhausted")
                if self._stop.wait(backoff(attempts, self._base_s, self._cap_s)):
                    return False
            else:
                self._commit(msg)
                return True

    def _dead_letter(
        self,
        msg: MessageLike,
        raw_headers: list[tuple[str, bytes]],
        span: Span,
        error: str,
        attempts: int,
        reason: str,
    ) -> bool:
        headers = dlq_headers(
            raw_headers,
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            error=error,
            attempts=attempts,
        )
        try:
            self._dlq.publish(
                msg.topic() + ".dlq",
                msg.key() or b"",
                msg.value() or b"",
                _as_mapping(headers),
            )
        except Exception as exc:
            if self._stop.is_set():
                return False
            self._log.error("dlq write failed", extra={"error_type": type(exc).__name__})
            raise DlqWriteError("failed to write to the dead-letter topic") from None
        span.set_attribute("smarthire.dlq", True)
        span.set_attribute("smarthire.dlq.reason", reason)
        self._commit(msg)
        return True

    def _commit(self, msg: MessageLike) -> None:
        self._consumer.commit(message=msg, asynchronous=False)


def _as_mapping(headers: Sequence[tuple[str, bytes]]) -> Mapping[str, str]:
    return {k: v.decode("utf-8", "replace") for k, v in headers}


def make_consumer(brokers: str) -> confluent_kafka.Consumer:
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 900000,
        }
    )
    consumer.subscribe([TOPIC])
    return consumer
