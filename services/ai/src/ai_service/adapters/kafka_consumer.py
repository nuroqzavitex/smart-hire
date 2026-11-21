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
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
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

# Values of the smarthire.dlq.reason span attribute (same as Go pkg/kafka).
DLQ_REASON_DECODE = "decode"
DLQ_REASON_PERMANENT = "permanent"
DLQ_REASON_EXHAUSTED = "exhausted"


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
    def assignment(self) -> Any: ...
    def pause(self, partitions: Any) -> Any: ...
    def resume(self, partitions: Any) -> Any: ...


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
        transient: tuple[type[Exception], ...] = (),
        transient_cap_s: float = 10.0,
        log: logging.Logger,
    ) -> None:
        self._consumer = consumer
        self._dlq = dlq
        self._handler = handler
        self._stop = stop
        self._max_attempts = max_attempts
        self._base_s = base_s
        self._cap_s = cap_s
        # Infrastructure errors (rate limit, store down) retry until stop without
        # counting attempts or dead-lettering: the partition waits (ADR 0010).
        self._transient = transient
        self._transient_cap_s = transient_cap_s
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
            except (ValidationError, ValueError) as exc:
                self._log.warning("undecodable event", extra={"offset": msg.offset()})
                return self._dead_letter(
                    msg, raw_headers, span, exc, "undecodable event envelope", 0, DLQ_REASON_DECODE
                )
            return self._handle_with_retry(msg, raw_headers, span, env)

    def _handle_with_retry(
        self, msg: MessageLike, raw_headers: list[tuple[str, bytes]], span: Span, env: Envelope
    ) -> bool:
        attempts = 0
        outages = 0
        paused: Any = None
        while True:
            attempts += 1
            try:
                self._handler(env)
            except Exception as exc:
                # Like Go: shutdown is checked first for every handler error.
                if self._stop.is_set():
                    return False
                if self._transient and isinstance(exc, self._transient):
                    attempts -= 1
                    outages += 1
                    self._log.warning(
                        "dependency unavailable, retrying",
                        extra={"event_id": env.event_id, "outage": outages, "error_type": type(exc).__name__},
                    )
                    if paused is None:
                        paused = self._pause()
                    if self._stop.wait(backoff(outages, self._base_s, self._transient_cap_s)):
                        return False
                    self._keep_alive()
                    continue
                if isinstance(exc, PermanentError):
                    self._log.warning("permanent error", extra={"event_id": env.event_id})
                    return self._dead_letter(
                        msg, raw_headers, span, exc, _describe(exc), attempts, DLQ_REASON_PERMANENT
                    )
                self._log.warning(
                    "handler failed",
                    extra={"event_id": env.event_id, "attempt": attempts, "error_type": type(exc).__name__},
                )
                if attempts >= self._max_attempts:
                    return self._dead_letter(
                        msg, raw_headers, span, exc, _describe(exc), attempts, DLQ_REASON_EXHAUSTED
                    )
                if self._stop.wait(backoff(attempts, self._base_s, self._cap_s)):
                    return False
            else:
                if paused is not None:
                    self._resume(paused)
                self._commit(msg)
                return True

    def _dead_letter(
        self,
        msg: MessageLike,
        raw_headers: list[tuple[str, bytes]],
        span: Span,
        cause: Exception,
        error: str,
        attempts: int,
        reason: str,
    ) -> bool:
        """Records the failure on the process span (like Go toDLQ) and writes the DLQ record."""
        _record(span, cause, error)
        span.set_status(Status(StatusCode.ERROR, f"dead-lettered: {reason}"))
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
            _record(span, exc, f"dlq write failed: {type(exc).__name__}")
            self._log.error("dlq write failed", extra={"error_type": type(exc).__name__})
            raise DlqWriteError("failed to write to the dead-letter topic") from None
        span.set_attribute("smarthire.dlq", True)
        span.set_attribute("smarthire.dlq.reason", reason)
        self._commit(msg)
        return True

    def _pause(self) -> Any:
        """Pauses the assignment so keep-alive polls fetch nothing (ADR 0010)."""
        partitions = self._consumer.assignment()
        self._consumer.pause(partitions)
        return partitions

    def _resume(self, partitions: Any) -> None:
        try:
            self._consumer.resume(partitions)
        except confluent_kafka.KafkaException:
            # Revoked meanwhile (rebalance): the new assignment is not paused.
            self._log.warning("resume after outage failed")

    def _keep_alive(self) -> None:
        """Polls between transient retries so a long outage never exceeds
        max.poll.interval.ms (the member would be evicted and its commit fail)."""
        msg = self._consumer.poll(0)
        if msg is not None and not msg.error():
            # Only a partition assigned after a rebalance is unpaused: rewind it so
            # the record is fetched again after the current one is handled.
            self._log.warning("record received while paused, rewinding", extra={"offset": msg.offset()})
            seek = getattr(self._consumer, "seek", None)
            if seek is not None:
                seek(confluent_kafka.TopicPartition(msg.topic(), msg.partition(), msg.offset()))

    def _commit(self, msg: MessageLike) -> None:
        try:
            self._consumer.commit(message=msg, asynchronous=False)
        except confluent_kafka.KafkaException:
            # Partition revoked while we worked (rebalance): its new owner gets the
            # record again and processed_events makes the redelivery a no-op.
            self._log.warning("commit failed after rebalance", extra={"offset": msg.offset()})


def _record(span: Span, exc: BaseException, message: str) -> None:
    """record_exception with the PII-safe message (the dlq-error text) in place of
    str(exc) and the traceback, both of which may echo payload values."""
    span.record_exception(
        exc,
        attributes={"exception.message": message, "exception.stacktrace": f"{type(exc).__name__}: {message}"},
    )


def _as_mapping(headers: Sequence[tuple[str, bytes]]) -> Mapping[str, str]:
    return {k: v.decode("utf-8", "replace") for k, v in headers}


def make_consumer(
    brokers: str, group_id: str = GROUP_ID, topics: Sequence[str] = (TOPIC,)
) -> confluent_kafka.Consumer:
    consumer = confluent_kafka.Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 900000,
        }
    )
    consumer.subscribe(list(topics))
    return consumer
