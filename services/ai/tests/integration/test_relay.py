import json
import logging
import threading
import time
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from conftest import Kafka
from opentelemetry.context import Context
from sqlalchemy import Engine, text
from ulid import ULID

from ai_service.adapters.db import outbox_pending
from ai_service.adapters.kafka_producer import KafkaPublisher
from ai_service.adapters.outbox import OutboxRelay

pytestmark = pytest.mark.integration

LOG = logging.getLogger("relay-test")


def _insert(engine: Engine, n: int, topic: str) -> list[str]:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    event_ids = []
    with engine.begin() as conn:
        for i in range(n):
            event_id = str(ULID())
            event_ids.append(event_id)
            conn.execute(
                text(
                    "INSERT INTO outbox (id, aggregate_id, event_type, payload, created_at) "
                    "VALUES (:id, :agg, :type, CAST(:payload AS jsonb), :ts)"
                ),
                {
                    "id": uuid4(),
                    "agg": f"agg-{i}",
                    "type": topic,
                    "payload": json.dumps({"event_id": event_id}),
                    "ts": base + timedelta(milliseconds=i),
                },
            )
    return event_ids


@pytest.fixture
def clean_outbox(engine: Engine) -> Iterator[None]:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM outbox"))
    yield
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM outbox"))


class FailingPublisher:
    def __init__(self, fail_on: int | None) -> None:
        self.fail_on = fail_on
        self.calls = 0

    def publish(
        self, topic: str, key: bytes, value: bytes, headers: Mapping[str, str], parent: Context | None = None
    ) -> None:
        self.calls += 1
        if self.calls == self.fail_on:
            raise RuntimeError("broker down")


def test_relay_stops_at_first_failure_and_marks_sent(engine: Engine, clean_outbox: None):
    _insert(engine, 5, "relay.test")
    publisher = FailingPublisher(fail_on=3)
    relay = OutboxRelay(engine, publisher, stop=threading.Event(), log=LOG)
    assert relay.tick() == 2
    assert outbox_pending(engine) == 3
    publisher.fail_on = None
    assert relay.tick() == 3
    assert outbox_pending(engine) == 0
    assert publisher.calls == 2 + 1 + 3  # the failed row is retried first


def test_concurrent_relays_no_duplicates(engine: Engine, kafka: Kafka, clean_outbox: None):
    event_ids = _insert(engine, 200, "relay.test")
    stop = threading.Event()
    relays = [
        OutboxRelay(
            engine, KafkaPublisher(kafka.bootstrap), batch_size=10, interval_s=0.05, stop=stop, log=LOG
        )
        for _ in range(2)
    ]
    threads = [threading.Thread(target=r.run) for r in relays]
    for t in threads:
        t.start()
    try:
        deadline = time.monotonic() + 90
        while outbox_pending(engine) and time.monotonic() < deadline:
            time.sleep(0.2)
    finally:
        stop.set()
        for t in threads:
            t.join(15)
    assert outbox_pending(engine) == 0

    wanted = set(event_ids)
    seen: list[str] = []

    def match(msg: Any) -> bool:
        event_id = json.loads(msg.value())["event_id"]
        if event_id in wanted:
            seen.append(event_id)
        return False

    kafka.read("relay.test", match, count=1, timeout=15)
    assert len(seen) == 200
    assert set(seen) == wanted
