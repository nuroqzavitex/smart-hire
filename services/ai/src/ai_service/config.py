"""Service configuration, read only from AI_-prefixed environment variables."""

from typing import Literal

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DSN_SCHEMES = ("postgres://", "postgresql://", "postgresql+psycopg://")
_PSYCOPG_SCHEME = "postgresql+psycopg://"


def normalize_dsn(value: str) -> str:
    """Rewrites postgres:// and postgresql:// to the SQLAlchemy psycopg 3 dialect."""
    for scheme in _DSN_SCHEMES:
        if value.startswith(scheme):
            return _PSYCOPG_SCHEME + value[len(scheme) :]
    # The DSN may carry a password, so it is never echoed back.
    raise ValueError("db_dsn must start with postgres://, postgresql:// or postgresql+psycopg://")


class Settings(BaseSettings):
    # hide_input_in_errors: a bad DSN or secret must not end up in a startup error.
    model_config = SettingsConfigDict(env_prefix="AI_", extra="ignore", hide_input_in_errors=True)

    db_dsn: str
    kafka_brokers: str = "redpanda:9092"
    s3_endpoint: str = "minio:9000"
    s3_access_key: str
    s3_secret_key: SecretStr
    s3_bucket: str = "cvs"
    s3_secure: bool = False
    # Fixed region: without it minio-py asks the server for the bucket location,
    # which the least-privilege ai-svc policy (s3:GetObject only) forbids.
    s3_region: str = "us-east-1"
    llm_provider: Literal["fake", "anthropic"] = "fake"
    llm_model: str = "claude-opus-5-5"
    llm_timeout_s: float = 60
    outbox_interval_ms: int = 500
    http_port: int = 8100
    log_level: str = "INFO"

    @field_validator("db_dsn")
    @classmethod
    def _normalize_dsn(cls, value: str) -> str:
        return normalize_dsn(value)


def load_settings() -> Settings:
    return Settings()
