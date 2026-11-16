"""Transactional outbox writer and relay (same algorithm as Go pkg/outbox, ADR 0005)."""

import logging
import threading
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from opentelemetry import propagate
from opentelemetry.context import Context
from sqlalchemy import Connection, Engine, bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from ai_service.adapters.kafka_producer import Publisher
from ai_service.domain.events import Envelope, dump_envelope

_INSERT = text(
    "INSERT INTO outbox (id, aggregate_id, event_type, payload, headers, created_at) "
    "VALUES (:id, :aggregate_id, :event_type, CAST(:payload AS jsonb), :headers, :created_at)"
).bindparams(bindparam("headers", type_=JSONB))

_SELECT_PENDING = text(
    "SELECT id, aggregate_id, event_type, payload::text AS payload, headers FROM outbox "
    "WHERE published_at IS NULL ORDER BY created_at, id LIMIT :n FOR UPDATE SKIP LOCKED"
)
_MARK_PUBLISHED = text("UPDATE outbox SET published_at = now() WHERE id = ANY(CAST(:ids AS uuid[]))")

_TRACE_HEADERS = frozenset({"traceparent", "tracestate"})


def write(conn: Connection, env: Envelope, aggregate_id: str) -> None:
    """Stores env and the current W3C trace carrier on the business transaction `conn`."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    conn.execute(
        _INSERT,
        {
            "id": uuid4(),
            "aggregate_id": aggregate_id,
            "event_type": env.event_type,
            "payload": dump_envelope(env).decode("utf-8"),
            "headers": carrier,
            "created_at": env.occurred_at,
        },
    )


@dataclass
class OutboxRow:
    id: UUID
    aggregate_id: str
    event_type: str
    payload: bytes
    headers: dict[str, str]


def record_from_row(row: OutboxRow) -> tuple[Context, dict[str, str]]:
    """Parent context from the stored carrier, plus headers without traceparent/tracestate."""
    parent = propagate.extract(row.headers)
    headers = {k: v for k, v in row.headers.items() if k not in _TRACE_HEADERS}
    return parent, headers


class OutboxRelay:
    def __init__(
        self,
        engine: Engine,
        publisher: Publisher,
        *,
        batch_size: int = 100,
        interval_s: float = 0.5,
        stop: threading.Event,
        log: logging.Logger,
    ) -> None:
        self._engine = engine
        self._publisher = publisher
        self._batch_size = batch_size
        self._interval_s = interval_s
        self._stop = stop
        self._log = log

    def tick(self) -> int:
        """Publishes one batch in a single transaction; returns the number of events sent."""
        with self._engine.begin() as conn:
            rows = conn.execute(_SELECT_PENDING, {"n": self._batch_size}).all()
            sent: list[UUID] = []
            for r in rows:
                headers_raw: Any = r.headers
                row = OutboxRow(
                    r.id, r.aggregate_id, r.event_type, r.payload.encode("utf-8"), dict(headers_raw)
                )
                parent, headers = record_from_row(row)
                try:
                    self._publisher.publish(
                        row.event_type, row.aggregate_id.encode(), row.payload, headers, parent
                    )
                except Exception as exc:
                    self._log.warning(
                        "outbox publish failed",
                        extra={
                            "event_type": row.event_type,
                            "outbox_id": str(row.id),
                            "error_type": type(exc).__name__,
                        },
                    )
                    break
                sent.append(row.id)
            if sent:
                conn.execute(_MARK_PUBLISHED, {"ids": sent})
            return len(sent)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.tick() == self._batch_size:
                    continue
            except Exception as exc:
                self._log.warning("outbox relay tick failed", extra={"error_type": type(exc).__name__})
            self._stop.wait(self._interval_s)
