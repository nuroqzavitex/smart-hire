# AI service

Python 3.12 service (managed with [uv](https://docs.astral.sh/uv/)) that consumes `cv.uploaded`,
extracts the CV text and turns it into a structured `ParsedCV`, then publishes `cv.parsed` or
`cv.parse_failed` through its outbox. Design: `docs/superpowers/specs/2026-11-11-m1b-candidate-ai-parse-design.md` §5.

## Processes

One image, two entry points (`[project.scripts]`):

- `ai-worker` (`ai_service.main_worker`) — consumer group `ai-cv-parser` on `cv.uploaded` (manual commit, 3 in-process
  retries with 200ms×2^n backoff capped at 2s, then `cv.uploaded.dlq`; a failed DLQ write stops the worker) plus the outbox
  relay thread (`FOR UPDATE SKIP LOCKED`, batch 100, every `AI_OUTBOX_INTERVAL_MS`) that publishes `cv.parsed` /
  `cv.parse_failed`. It runs single-threaded because `AnthropicLlmParser` keeps per-call state. Result row, `processed_events`
  and outbox row are written in one transaction; trace context is continued through Kafka.
- `ai-api` (`ai_service.main_api`) — FastAPI with `/healthz`, `/readyz`, `/metrics` on `AI_HTTP_PORT` (compose-internal,
  not published to the host). No business endpoints yet.

The LLM sits behind the `LlmParser` port: `FakeLlmParser` (rule based, deterministic, default) or `AnthropicLlmParser`
(`claude-opus-5-5`). See [ADR 0007](../../docs/adr/0007-ai-service-python-llm-port.md); MinIO access follows
[ADR 0008](../../docs/adr/0008-presigned-upload-valet-key.md).

## Layout

- `src/ai_service/config.py` — settings from `AI_`-prefixed env vars (pydantic-settings).
- `src/ai_service/domain/` — pure code: `ParsedCV` models + `sanitize`, skill aliases/normalization,
  event envelopes. Kept in lock-step with `contracts/events/*.json` by `tests/unit/test_contract_schema.py`.
- `src/ai_service/services/` — use cases and ports (`cv_parser`, `cv_uploaded_handler`, `ports`).
- `src/ai_service/adapters/` — PyMuPDF, LLM (fake + Anthropic), MinIO reader, Kafka consumer/producer, outbox + relay, DB.
- `migrations/` — alembic (`ai_db`: `cv_parse_results`, `processed_events`, `outbox`).
- `scripts/try_anthropic.py` — manual check of the real Anthropic adapter (see below).
- `tests/unit` — no Docker; `tests/integration` — testcontainers (marker `integration`, needs the local MinIO image).

## Commands (from the repo root, Git Bash on Windows)

```bash
make ai-sync      # uv sync --frozen
make ai-lint      # ruff check + ruff format --check + mypy
make ai-test      # unit tests
make ai-test-int  # integration tests (Docker; builds the local MinIO image first)
make ai-cover     # unit + integration with coverage >= 70% (Docker; builds the local MinIO image first)
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
| `AI_S3_REGION` | `us-east-1` (fixed so the MinIO client never calls `GetBucketLocation`, which the least-privilege `ai-svc` user may not make) |
| `AI_LLM_PROVIDER` | `fake` (`fake` or `anthropic`) |
| `AI_LLM_MODEL` | `claude-opus-5-5` |
| `AI_LLM_TIMEOUT_S` | `60` |
| `AI_OUTBOX_INTERVAL_MS` | `500` |
| `AI_HTTP_PORT` | `8100` |
| `AI_LOG_LEVEL` | `INFO` |

`ANTHROPIC_API_KEY` (unprefixed, read by the Anthropic SDK) is required when `AI_LLM_PROVIDER=anthropic`; without
credentials the worker exits at startup. `OTEL_EXPORTER_OTLP_ENDPOINT` configures tracing export.

## Trying the real Anthropic adapter

`scripts/try_anthropic.py` parses one PDF with the real `AnthropicLlmParser` and prints the outcome as JSON. It needs
credentials, calls the paid API, is never run by CI, and ignores `Settings` (it reads the key from the environment and
takes the model as an optional argument). It prints the parsed CV, which is PII: use only sample data.

```bash
cd services/ai
ANTHROPIC_API_KEY=... uv run python scripts/try_anthropic.py path/to/cv.pdf [model]
```
