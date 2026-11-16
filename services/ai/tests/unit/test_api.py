from fastapi.testclient import TestClient

from ai_service.main_api import create_app


def test_healthz() -> None:
    resp = TestClient(create_app(lambda: None)).get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_ok_when_ready() -> None:
    assert TestClient(create_app(lambda: None)).get("/readyz").status_code == 200


def test_readyz_503_when_not_ready() -> None:
    def bad() -> None:
        raise RuntimeError("db down")

    resp = TestClient(create_app(bad)).get("/readyz")
    assert resp.status_code == 503
    assert "db down" not in resp.text


def test_metrics_exposes_outbox_gauge() -> None:
    body = TestClient(create_app(lambda: None, pending=lambda: 3)).get("/metrics").text
    assert "ai_outbox_pending 3.0" in body


def test_metrics_skips_gauge_when_db_fails() -> None:
    def boom() -> int:
        raise RuntimeError("db down")

    resp = TestClient(create_app(lambda: None, pending=boom)).get("/metrics")
    assert resp.status_code == 200
    assert "ai_outbox_pending" not in resp.text
