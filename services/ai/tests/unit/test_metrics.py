"""Business metrics of the AI service (ADR 0013), read through an in-memory reader."""

from typing import Any

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from ai_service.metrics import Instruments


def _instruments() -> tuple[Instruments, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    return Instruments(MeterProvider(metric_readers=[reader])), reader


def _points(reader: InMemoryMetricReader, name: str) -> list[Any]:
    data = reader.get_metrics_data()
    if data is None:
        return []
    return [
        p
        for rm in data.resource_metrics
        for sm in rm.scope_metrics
        for m in sm.metrics
        if m.name == name
        for p in m.data.data_points
    ]


def test_parse_duration_histogram() -> None:
    ins, reader = _instruments()
    ins.parse_duration(1.5, outcome="parsed")
    [p] = _points(reader, "ai.parse.duration")
    assert p.count == 1
    assert p.sum == 1.5
    assert p.attributes == {"outcome": "parsed"}


def test_match_score_histogram() -> None:
    ins, reader = _instruments()
    ins.match_score(87.5)
    ins.match_score(12.0)
    [p] = _points(reader, "ai.match.score")
    assert p.count == 2


def test_llm_tokens_counter() -> None:
    ins, reader = _instruments()
    ins.llm_tokens(input_tokens=120, output_tokens=30)
    got = {p.attributes["direction"]: p.value for p in _points(reader, "ai.llm.tokens")}
    assert got == {"input": 120, "output": 30}


def test_dlq_counter() -> None:
    ins, reader = _instruments()
    ins.dlq("cv.uploaded")
    [p] = _points(reader, "dlq.messages")
    assert p.value == 1
    assert p.attributes == {"topic": "cv.uploaded"}


def test_handler_records_parse_duration() -> None:
    from test_handler import CV_LINES, NOW, CountingLlm, FakeStorage, FakeStore, _env, _payload, pdf

    from ai_service.adapters.pdf import PyMuPdfExtractor
    from ai_service.services.cv_uploaded_handler import CvUploadedHandler

    ins, reader = _instruments()
    handler = CvUploadedHandler(
        FakeStorage(pdf(CV_LINES)),
        PyMuPdfExtractor(),
        CountingLlm(),
        FakeStore(),
        clock=lambda: NOW,
        trace_id=lambda: "t",
        instruments=ins,
    )
    handler.handle(_env(_payload()))
    [p] = _points(reader, "ai.parse.duration")
    assert p.count == 1
    assert p.attributes == {"outcome": "parsed"}


def test_consumer_counts_dlq_writes() -> None:
    import logging
    import threading

    from test_kafka_consumer import FakeConsumer, FakeMessage, FakePublisher, _envelope_bytes

    from ai_service.adapters.kafka_consumer import PermanentError, RetryingConsumer

    ins, reader = _instruments()
    stop = threading.Event()

    def handler(_: Any) -> None:
        raise PermanentError("bad")

    rc = RetryingConsumer(
        FakeConsumer(stop), FakePublisher(), handler, stop, log=logging.getLogger("t"), instruments=ins
    )
    assert rc.process(FakeMessage(_envelope_bytes())) is True
    [p] = _points(reader, "dlq.messages")
    assert p.value == 1
    assert p.attributes == {"topic": "cv.uploaded"}


def test_anthropic_records_token_usage() -> None:
    from test_anthropic_llm import MODEL, FakeClient, draft, response

    from ai_service.adapters.llm import AnthropicLlmParser

    ins, reader = _instruments()
    raw = response(parsed=draft())
    raw.http_response.json()["usage"] = {"input_tokens": 1200, "output_tokens": 340}
    AnthropicLlmParser(FakeClient(raw), MODEL, instruments=ins).parse("cv text")  # type: ignore[arg-type]
    got = {p.attributes["direction"]: p.value for p in _points(reader, "ai.llm.tokens")}
    assert got == {"input": 1200, "output": 340}


def test_parse_duration_uses_second_buckets() -> None:
    from ai_service.metrics import PARSE_BUCKETS

    ins, reader = _instruments()
    ins.parse_duration(3.0, outcome="parsed")
    [p] = _points(reader, "ai.parse.duration")
    assert list(p.explicit_bounds) == PARSE_BUCKETS
