import threading
from typing import Any

import pytest

from ai_service import main_worker


class _Engine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AI_DB_DSN", "postgres://u:p@h/d")
    monkeypatch.setenv("AI_S3_ACCESS_KEY", "k")
    monkeypatch.setenv("AI_S3_SECRET_KEY", "s")


def test_consumer_creation_failure_starts_no_relay_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _Engine()
    started: list[str] = []

    class FakePublisher:
        def __init__(self, *_a: Any) -> None: ...

        def flush(self, _t: float) -> None: ...

    class FakeRelay:
        def __init__(self, *_a: Any, **_k: Any) -> None: ...

        def run(self) -> None:
            started.append("relay")

    def boom(_brokers: str) -> None:
        raise RuntimeError("no brokers")

    monkeypatch.setattr(main_worker, "make_engine", lambda _dsn: engine)
    monkeypatch.setattr(main_worker, "KafkaPublisher", FakePublisher)
    monkeypatch.setattr(main_worker, "OutboxRelay", FakeRelay)
    monkeypatch.setattr(main_worker, "make_consumer", boom)
    monkeypatch.setattr(main_worker, "build_llm", lambda *_a: object())
    monkeypatch.setattr(main_worker.signal, "signal", lambda *_a: None)
    _env(monkeypatch)

    with pytest.raises(RuntimeError, match="no brokers"):
        main_worker._run(main_worker.load_settings())

    assert started == []
    assert not any(t.name == "outbox-relay" for t in threading.enumerate())
    assert engine.disposed


def test_main_shuts_down_otel_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(main_worker, "setup_logging", lambda *_a: None)
    monkeypatch.setattr(main_worker, "setup_otel", lambda _s: lambda: calls.append("shutdown"))

    def boom(_s: Any) -> int:
        raise RuntimeError("x")

    monkeypatch.setattr(main_worker, "_run", boom)
    _env(monkeypatch)
    with pytest.raises(RuntimeError):
        main_worker.main()
    assert calls == ["shutdown"]
