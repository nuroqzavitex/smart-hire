"""Kafka publisher (acks=all, idempotent) that records a PRODUCER span and propagates the trace."""

import threading
from collections.abc import Mapping
from typing import Protocol

import confluent_kafka
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.trace import SpanKind

FLUSH_TIMEOUT_S = 15


class PublishError(Exception):
    """A record could not be delivered. Never carries record content."""


class Publisher(Protocol):
    def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: Mapping[str, str],
        parent: Context | None = None,
    ) -> None: ...


class KafkaPublisher:
    def __init__(self, brokers: str) -> None:
        # One publish at a time: delivery callbacks run inside flush() of whichever thread
        # calls it, so concurrent publishers could miss each other's delivery errors.
        self._lock = threading.Lock()
        self._producer = confluent_kafka.Producer(
            {
                "bootstrap.servers": brokers,
                "acks": "all",
                "enable.idempotence": True,
                "delivery.timeout.ms": 10000,
            }
        )

    def publish(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: Mapping[str, str],
        parent: Context | None = None,
    ) -> None:
        tracer = trace.get_tracer("ai_service")
        with tracer.start_as_current_span(
            f"{topic} publish",
            context=parent,
            kind=SpanKind.PRODUCER,
            attributes={"messaging.system": "kafka", "messaging.destination.name": topic},
        ) as span:
            hdrs = dict(headers)
            if "traceparent" not in hdrs:
                propagate.inject(hdrs)
            errors: list[str] = []

            def on_delivery(err: confluent_kafka.KafkaError | None, _msg: object) -> None:
                if err is not None:
                    errors.append(err.name())

            try:
                with self._lock:
                    self._producer.produce(
                        topic,
                        value=value,
                        key=key,
                        headers=[(k, v.encode()) for k, v in hdrs.items()],
                        on_delivery=on_delivery,
                    )
                    remaining = self._producer.flush(FLUSH_TIMEOUT_S)
            except confluent_kafka.KafkaException as exc:
                span.record_exception(exc)
                raise PublishError("kafka produce failed") from None
            if errors:
                raise PublishError(f"kafka delivery failed: {errors[0]}")
            if remaining:
                raise PublishError("kafka flush timed out")

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))
