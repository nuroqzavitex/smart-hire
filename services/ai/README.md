# AI service

Python 3.12 service (managed with [uv](https://docs.astral.sh/uv/)) that consumes `cv.uploaded`,
extracts the CV text and turns it into a structured `ParsedCV`, then publishes `cv.parsed` or
`cv.parse_failed` through its outbox. Design: `docs/superpowers/specs/2026-11-11-m1b-candidate-ai-parse-design.md` §5.

## Layout

- `src/ai_service/config.py` — settings from `AI_`-prefixed env vars (pydantic-settings).
- `src/ai_service/domain/` — pure code: `ParsedCV` models + `sanitize`, skill aliases/normalization,
  event envelopes. Kept in lock-step with `contracts/events/*.json` by `tests/unit/test_contract_schema.py`.
- `tests/unit` — no Docker; `tests/integration` — testcontainers (marker `integration`).

## Commands (from the repo root, Git Bash on Windows)

```bash
make ai-sync      # uv sync --frozen
make ai-lint      # ruff check + ruff format --check + mypy
make ai-test      # unit tests
make ai-test-int  # integration tests (Docker)
make ai-cover     # unit + integration with coverage >= 70% (Docker)
```

## Configuration

| Variable | Default |
|---|---|
| `AI_DB_DSN` | required (`postgres://` is rewritten to `postgresql+psycopg://`) |
| `AI_KAFKA_BROKERS` | `redpanda:9092` |
| `AI_S3_ENDPOINT` | `minio:9000` |
| `AI_S3_ACCESS_KEY`, `AI_S3_SECRET_KEY` | required |
| `AI_S3_BUCKET` | `cvs` |
| `AI_S3_SECURE` | `false` |
| `AI_LLM_PROVIDER` | `fake` (`fake` or `anthropic`) |
| `AI_LLM_MODEL` | `claude-opus-5-5` |
| `AI_LLM_TIMEOUT_S` | `60` |
| `AI_OUTBOX_INTERVAL_MS` | `500` |
| `AI_HTTP_PORT` | `8100` |
| `AI_LOG_LEVEL` | `INFO` |
