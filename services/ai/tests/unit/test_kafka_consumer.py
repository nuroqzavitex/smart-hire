import json
import logging
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import pytest
from opentelemetry.context import Context

from ai_service.adapters.kafka_consumer import DlqWriteError, RetryingConsumer, backoff, dlq_headers
from ai_service.domain.events import Envelope
from ai_service.services.cv_uploaded_handler import PermanentError

TOPIC = "cv.uploaded"


def _envelope_bytes() -> bytes:
    env = Envelope(
        event_id="01JABCDEFGHJKMNPQRSTVWXYZ0",
        event_type=TOPIC,
        event_version=1,
        source="candidate-service",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        trace_id="t",
        correlation_id="c",
        payload={},
    )
    return json.dumps(env.model_dump(mode="json")).encode()


class FakeMessage:
    def __init__(self, value: bytes, headers: list[tuple[str, bytes]] | None = None) -> None:
        self._value = value
        self._headers = headers or []

    def topic(self) -> str:
        return TOPIC

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 7

    def key(self) -> bytes:
        return b"cv-1"

    def value(self) -> bytes:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self, stop: threading.Event) -> None:
        self.messages: list[FakeMessage] = []
        self.stop = stop
        self.commits: list[FakeMessage] = []

    def poll(self, timeout: float) -> FakeMessage | None:
        if self.messages:
            return self.messages.pop(0)
        self.stop.set()
        return None

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits.append(message)

    def close(self) -> None:
        pass


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.records: list[tuple[str, bytes, bytes, dict[str, str]]] = []

    def publish(
        self, topic: str, key: bytes, value: bytes, headers: Mapping[str, str], parent: Context | None = None
    ) -> None:
        if self.fail:
            raise RuntimeError("broker down")
        self.records.append((topic, key, value, dict(headers)))


class Setup:
    def __init__(
        self, handler: Callable[["Setup", Envelope], None], *, dlq: FakePublisher | None = None
    ) -> None:
        self.stop = threading.Event()
        self.consumer = FakeConsumer(self.stop)
        self.dlq = dlq or FakePublisher()
        self.calls = 0
        self.waits: list[float] = []

        def counted(env: Envelope) -> None:
            self.calls += 1
            handler(self, env)

        self.rc = RetryingConsumer(self.consumer, self.dlq, counted, self.stop, log=logging.getLogger("test"))
        real_wait = self.stop.wait

        def recording_wait(timeout: float | None = None) -> bool:
            self.waits.append(timeout or 0)
            return real_wait(0)  # never really sleep in unit tests

        self.stop.wait = recording_wait  # type: ignore[method-assign]


def _ok(s: Setup, env: Envelope) -> None:
    return None


def test_backoff():
    assert [backoff(n) for n in (1, 2, 4, 5, 40)] == [0.2, 0.4, 1.6, 2.0, 2.0]


def test_dlq_headers_replace_old():
    original = [("dlq-error", b"old"), ("traceparent", b"tp"), ("dlq-attempts", b"9")]
    out = dlq_headers(original, topic=TOPIC, partition=2, offset=7, error="é" * 600, attempts=3)
    assert [k for k, _ in out].count("dlq-error") == 1
    d = dict(out)
    assert d["traceparent"] == b"tp"
    assert len(d["dlq-error"].decode()) == 512
    assert d["dlq-attempts"] == b"3"
    assert d["dlq-original-topic"] == TOPIC.encode()
    assert d["dlq-original-partition"] == b"2"
    assert d["dlq-original-offset"] == b"7"


def test_transient_then_success_commits():
    def handler(s: Setup, env: Envelope) -> None:
        if s.calls < 3:
            raise RuntimeError("boom")

    s = Setup(handler)
    msg = FakeMessage(_envelope_bytes())
    assert s.rc.process(msg) is True
    assert s.calls == 3
    assert s.consumer.commits == [msg]
    assert s.dlq.records == []
    assert s.waits == [0.2, 0.4]


def test_permanent_goes_to_dlq_once():
    def handler(s: Setup, env: Envelope) -> None:
        raise PermanentError("bad")

    s = Setup(handler)
    msg = FakeMessage(_envelope_bytes(), [("traceparent", b"tp")])
    assert s.rc.process(msg) is True
    assert s.calls == 1
    assert len(s.dlq.records) == 1
    topic, key, value, headers = s.dlq.records[0]
    assert topic == "cv.uploaded.dlq"
    assert key == b"cv-1"
    assert value == msg.value()
    assert headers["dlq-attempts"] == "1"
    assert headers["traceparent"] == "tp"
    assert s.consumer.commits == [msg]


def test_exhausted_goes_to_dlq():
    def handler(s: Setup, env: Envelope) -> None:
        raise RuntimeError("boom")

    s = Setup(handler)
    assert s.rc.process(FakeMessage(_envelope_bytes())) is True
    assert s.calls == 3
    assert s.dlq.records[0][3]["dlq-attempts"] == "3"
    assert s.dlq.records[0][3]["dlq-error"] == "RuntimeError: boom"


@pytest.mark.parametrize("value", [b"not json", b"{}"])
def test_undecodable_goes_to_dlq(value: bytes):
    s = Setup(_ok)
    assert s.rc.process(FakeMessage(value)) is True
    assert s.calls == 0
    assert s.dlq.records[0][3]["dlq-attempts"] == "0"
    assert "not json" not in s.dlq.records[0][3]["dlq-error"]


def test_dlq_failure_raises():
    def handler(s: Setup, env: Envelope) -> None:
        raise PermanentError("bad")

    s = Setup(handler, dlq=FakePublisher(fail=True))
    with pytest.raises(DlqWriteError):
        s.rc.process(FakeMessage(_envelope_bytes()))
    assert s.consumer.commits == []


def test_shutdown_during_backoff_does_not_commit_or_dlq():
    def handler(s: Setup, env: Envelope) -> None:
        s.stop.set()
        raise RuntimeError("boom")

    s = Setup(handler)
    assert s.rc.process(FakeMessage(_envelope_bytes())) is False
    assert s.calls == 1
    assert s.consumer.commits == []
    assert s.dlq.records == []


def test_run_processes_until_stop():
    s = Setup(_ok)
    s.consumer.messages = [FakeMessage(_envelope_bytes()), FakeMessage(_envelope_bytes())]
    s.rc.run()
    assert s.calls == 2
    assert len(s.consumer.commits) == 2
