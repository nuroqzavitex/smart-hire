import json
import logging
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import pytest
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from ai_service.adapters import kafka_consumer
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
        self.events: list[tuple[str, object]] = []
        self.commit_error: Exception | None = None

    def poll(self, timeout: float) -> FakeMessage | None:
        self.events.append(("poll", timeout))
        if timeout == 0:
            return None  # keep-alive poll while paused
        if self.messages:
            return self.messages.pop(0)
        self.stop.set()
        return None

    def commit(self, *, message: FakeMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(message)

    def close(self) -> None:
        pass

    def assignment(self) -> list[str]:
        return ["tp-0"]

    def pause(self, partitions: list[str]) -> None:
        self.events.append(("pause", partitions))

    def resume(self, partitions: list[str]) -> None:
        self.events.append(("resume", partitions))


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
        self,
        handler: Callable[["Setup", Envelope], None],
        *,
        dlq: FakePublisher | None = None,
        transient: tuple[type[Exception], ...] = (),
    ) -> None:
        self.stop = threading.Event()
        self.consumer = FakeConsumer(self.stop)
        self.dlq = dlq or FakePublisher()
        self.calls = 0
        self.waits: list[float] = []

        def counted(env: Envelope) -> None:
            self.calls += 1
            handler(self, env)

        self.rc = RetryingConsumer(
            self.consumer, self.dlq, counted, self.stop, transient=transient, log=logging.getLogger("test")
        )
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


def test_shutdown_before_permanent_error_does_not_commit_or_dlq():
    # Like Go: shutdown is checked before classifying any handler error.
    def handler(s: Setup, env: Envelope) -> None:
        s.stop.set()
        raise PermanentError("bad")

    s = Setup(handler)
    assert s.rc.process(FakeMessage(_envelope_bytes())) is False
    assert s.consumer.commits == []
    assert s.dlq.records == []


@pytest.fixture
def spans(monkeypatch: pytest.MonkeyPatch) -> InMemorySpanExporter:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(kafka_consumer.trace, "get_tracer", provider.get_tracer)
    return exporter


def _raise_permanent(s: Setup, env: Envelope) -> None:
    raise PermanentError("cv.uploaded payload failed validation")


def _raise_runtime(s: Setup, env: Envelope) -> None:
    raise RuntimeError("boom")


PII_EMAIL = "nguyen.secret@example.com"


@pytest.mark.parametrize(
    ("value", "handler", "reason"),
    [
        (json.dumps({"event_id": 1, "email": PII_EMAIL}).encode(), _ok, "decode"),
        (_envelope_bytes(), _raise_permanent, "permanent"),
        (_envelope_bytes(), _raise_runtime, "exhausted"),
    ],
)
def test_dlq_span_records_error(
    spans: InMemorySpanExporter, value: bytes, handler: Callable[[Setup, Envelope], None], reason: str
):
    s = Setup(handler)
    assert s.rc.process(FakeMessage(value)) is True
    (span,) = spans.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert span.status.description == f"dead-lettered: {reason}"
    assert span.attributes is not None
    assert span.attributes["smarthire.dlq"] is True
    assert span.attributes["smarthire.dlq.reason"] == reason
    (event,) = [e for e in span.events if e.name == "exception"]
    assert event.attributes is not None
    assert event.attributes["exception.message"] == s.dlq.records[0][3]["dlq-error"]
    assert all(PII_EMAIL not in str(v) for v in event.attributes.values())


def test_dlq_write_failure_records_error_and_status(spans: InMemorySpanExporter):
    s = Setup(_raise_permanent, dlq=FakePublisher(fail=True))
    with pytest.raises(DlqWriteError):
        s.rc.process(FakeMessage(_envelope_bytes()))
    (span,) = spans.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    assert span.attributes is not None
    assert "smarthire.dlq" not in span.attributes
    messages = [e.attributes["exception.message"] for e in span.events if e.attributes]
    assert messages[:2] == [
        "PermanentError: cv.uploaded payload failed validation",
        "dlq write failed: RuntimeError",
    ]


class Unavailable(Exception):
    pass


def test_transient_error_retries_without_dlq():
    def handler(s: Setup, env: Envelope) -> None:
        if s.calls <= 8:
            raise Unavailable("rate limited")

    s = Setup(handler, transient=(Unavailable,))
    msg = FakeMessage(_envelope_bytes())
    assert s.rc.process(msg) is True
    assert s.calls == 9, "transient errors do not count towards max_attempts"
    assert s.dlq.records == []
    assert s.consumer.commits == [msg]
    assert s.waits[:3] == [0.2, 0.4, 0.8]
    assert max(s.waits) <= 10.0
    assert s.waits[-1] == 10.0


def test_transient_shutdown_leaves_uncommitted():
    def handler(s: Setup, env: Envelope) -> None:
        s.stop.set()
        raise Unavailable("down")

    s = Setup(handler, transient=(Unavailable,))
    assert s.rc.process(FakeMessage(_envelope_bytes())) is False
    assert s.consumer.commits == []
    assert s.dlq.records == []


def test_permanent_still_dlq_with_transient_configured():
    def handler(s: Setup, env: Envelope) -> None:
        raise PermanentError("bad")

    s = Setup(handler, transient=(Unavailable,))
    assert s.rc.process(FakeMessage(_envelope_bytes())) is True
    assert len(s.dlq.records) == 1


def test_make_consumer_topics_and_group(monkeypatch: pytest.MonkeyPatch):
    seen: dict[str, object] = {}

    class C:
        def __init__(self, conf: dict[str, object]) -> None:
            seen["conf"] = conf

        def subscribe(self, topics: list[str]) -> None:
            seen["topics"] = topics

    monkeypatch.setattr(kafka_consumer.confluent_kafka, "Consumer", C)
    kafka_consumer.make_consumer("b:9092", group_id="ai-matcher", topics=("job.created", "cv.updated"))
    assert seen["topics"] == ["job.created", "cv.updated"]
    assert seen["conf"]["group.id"] == "ai-matcher"  # type: ignore[index]
    kafka_consumer.make_consumer("b:9092")
    assert seen["topics"] == ["cv.uploaded"]


def test_transient_backoff_keeps_group_membership():
    # A long outage must not exceed max.poll.interval.ms: the consumer pauses its
    # partitions and keeps polling between retries, then resumes.
    def handler(s: Setup, env: Envelope) -> None:
        if s.calls <= 3:
            raise Unavailable("rate limited")

    s = Setup(handler, transient=(Unavailable,))
    s.consumer.messages = []
    msg = FakeMessage(_envelope_bytes())
    assert s.rc.process(msg) is True
    events = s.consumer.events
    assert events[0] == ("pause", ["tp-0"])
    assert events.count(("poll", 0)) == 3, "one keep-alive poll per backoff"
    assert events[-1] == ("resume", ["tp-0"])
    assert s.consumer.commits == [msg]


def test_commit_failure_after_rebalance_is_not_fatal():
    import confluent_kafka

    s = Setup(_ok)
    s.consumer.commit_error = confluent_kafka.KafkaException("UNKNOWN_MEMBER_ID")
    assert s.rc.process(FakeMessage(_envelope_bytes())) is True
