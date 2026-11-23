import json
import logging

import pytest
from opentelemetry import trace
from opentelemetry.propagate import inject

from ai_service.observability import setup_logging, setup_otel


def test_log_line_is_json_with_extras(capsys: pytest.CaptureFixture[str]) -> None:
    setup_logging("INFO", "ai-test")
    logging.getLogger("t").info("x", extra={"cv_id": "abc"})
    line = capsys.readouterr().err.strip().splitlines()[-1]
    doc = json.loads(line)
    assert doc["msg"] == "x"
    assert doc["cv_id"] == "abc"
    assert doc["service"] == "ai-test"
    assert doc["level"] == "INFO"
    assert "time" in doc


def test_log_exception_has_type_only(capsys: pytest.CaptureFixture[str]) -> None:
    setup_logging("INFO", "ai-test")
    try:
        raise ValueError("secret cv text")
    except ValueError:
        logging.getLogger("t").exception("boom")
    err = capsys.readouterr().err
    assert json.loads(err.strip().splitlines()[-1])["error_type"] == "ValueError"
    assert "secret cv text" not in err


def test_setup_otel_without_endpoint_still_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    shutdown = setup_otel("ai-test")
    carrier: dict[str, str] = {}
    with trace.get_tracer("t").start_as_current_span("s"):
        inject(carrier)
    assert "traceparent" in carrier
    shutdown()


def test_setup_otel_installs_meter_and_logger_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry import metrics
    from opentelemetry._logs import get_logger_provider
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk.metrics import MeterProvider

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
    setup_logging("INFO", "ai-test")
    shutdown = setup_otel("ai-test")
    try:
        assert isinstance(metrics.get_meter_provider(), MeterProvider)
        assert isinstance(get_logger_provider(), LoggerProvider)
        assert any(isinstance(h, LoggingHandler) for h in logging.getLogger().handlers), "logs go to OTel too"
    finally:
        shutdown()
