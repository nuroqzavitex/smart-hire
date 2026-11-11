import pytest
from pydantic import ValidationError

from ai_service.config import Settings, load_settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for key in list(os.environ):
        if key.startswith("AI_") or key in {"DB_DSN", "S3_ACCESS_KEY", "S3_SECRET_KEY"}:
            monkeypatch.delenv(key, raising=False)


def _required(monkeypatch: pytest.MonkeyPatch, dsn: str = "postgres://u:p@h/db") -> None:
    monkeypatch.setenv("AI_DB_DSN", dsn)
    monkeypatch.setenv("AI_S3_ACCESS_KEY", "ai-svc")
    monkeypatch.setenv("AI_S3_SECRET_KEY", "secret-value")


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _required(monkeypatch)
    s = load_settings()
    assert isinstance(s, Settings)
    assert s.db_dsn == "postgresql+psycopg://u:p@h/db"
    assert s.llm_provider == "fake"
    assert s.llm_model == "claude-opus-5-5"
    assert s.llm_timeout_s == 60
    assert s.kafka_brokers == "redpanda:9092"
    assert s.s3_endpoint == "minio:9000"
    assert s.s3_access_key == "ai-svc"
    assert s.s3_secret_key.get_secret_value() == "secret-value"
    assert "secret-value" not in repr(s)
    assert s.s3_bucket == "cvs"
    assert s.s3_secure is False
    assert s.http_port == 8100
    assert s.outbox_interval_ms == 500
    assert s.log_level == "INFO"


@pytest.mark.parametrize(
    ("dsn", "want"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql+psycopg://u:p@h:5432/db"),
        ("postgresql+psycopg://u:p@h/db", "postgresql+psycopg://u:p@h/db"),
    ],
)
def test_settings_normalizes_dsn_scheme(monkeypatch: pytest.MonkeyPatch, dsn: str, want: str) -> None:
    _required(monkeypatch, dsn)
    assert load_settings().db_dsn == want


def test_settings_rejects_other_dsn_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    _required(monkeypatch, "mysql://u:hunter2@h/db")
    with pytest.raises(ValidationError) as exc:
        load_settings()
    assert "hunter2" not in str(exc.value)


def test_settings_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    _required(monkeypatch)
    monkeypatch.setenv("AI_LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("AI_HTTP_PORT", "9100")
    monkeypatch.setenv("AI_S3_SECURE", "true")
    s = load_settings()
    assert s.llm_provider == "anthropic"
    assert s.http_port == 9100
    assert s.s3_secure is True


def test_settings_rejects_unknown_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    _required(monkeypatch)
    monkeypatch.setenv("AI_LLM_PROVIDER", "openai")
    with pytest.raises(ValidationError):
        load_settings()


def test_settings_ignores_unprefixed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_DSN", "postgres://u:p@h/db")
    monkeypatch.setenv("S3_ACCESS_KEY", "ai-svc")
    monkeypatch.setenv("S3_SECRET_KEY", "secret-value")
    with pytest.raises(ValidationError):
        load_settings()
