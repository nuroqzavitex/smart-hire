# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

SmartHire is a recruiting platform (CV parsing, CV↔JD matching, application tracking) built as a **Backend-Go portfolio project**. Milestones **M0 (foundation + Auth + Gateway)**, **M1a (event backbone + Job service)**, **M1b (Candidate service + AI CV parsing)**, **M2a (CQRS job search on OpenSearch)**, **M2b (CV ↔ JD matching)**, **M3 (Application service + submit Saga)** and **M4a (Notification service + observability)** are implemented:

- `pkg/` — shared libraries: `apperr`, `authz` (JWT/JWKS verifier, `authztest`), `config`, `event` (envelope + `event/eventschema` JSON-schema validator for `contracts/events`), `grpcx`, `idempotency` (`processed_events` de-duplication), `kafka` (franz-go producer/consumer, retry, DLQ), `logger` (JSON to stdout + `otelslog` bridge), `outbox` (writer + relay + `outbox.pending` gauge), `otel` (OTLP traces/metrics/logs, `RecordBreakerState`, `oteltest`).
- `gen/go/` — generated protobuf/gRPC/grpc-gateway code (from `contracts/proto`).
- `services/auth` — Auth service; **the template service** later Go services copy.
- `services/gateway` — HTTP gateway (chi + grpc-gateway, rate limiting, cookie auth routes).
- `services/job` — Job service (JD CRUD, publish/close/soft-delete, outbox relay to Kafka); hexagonal like Auth. Since M2a public search (`ListJobs`) reads an OpenSearch read model through a circuit breaker (`internal/adapter/opensearch`, `sony/gobreaker`) and falls back to the M1a Postgres `tsvector` search (`degraded: true`). A second binary `cmd/projector` (compose `job-projector`) consumes `job.*` into OpenSearch (`internal/projection`: external versions, tombstones, read alias `jobs` / write alias `jobs_write`) and has `rebuild` / `reconcile` subcommands (ADR 0009). Since M2b `ListMatchedCandidates` (`GET /v1/jobs/{id}/candidates`, owner or admin) calls the AI gRPC service through a timeout + breaker (`internal/adapter/aiclient`), and the internal `BatchGetJobs` serves open jobs to Candidate.
- `services/candidate` — Candidate service (profile, CV upload through a presigned MinIO URL, consumer of `cv.parsed|cv.parse_failed`, view/edit/reparse of the parsed CV); hexagonal like Auth. Every parse-state change emits `cv.updated` (M2b); `ListRecommendedJobs` (`GET /v1/cvs/{id}/recommended-jobs`) asks AI `RecommendJobs` and fills in job details from Job `BatchGetJobs`, forwarding the caller's JWT. Since M3 it keeps a daily application quota (`daily_quota`, `quota_reservations`) behind the internal, JWT-less `ReserveQuota`/`ReleaseQuota` RPCs (idempotent per application, release leaves a tombstone).
- `services/application` — Application service (M3, ADR 0011): `POST /v1/applications` (header `Idempotency-Key`, stored in Postgres) checks the job/CV with the caller's JWT, writes the `PENDING` application + `saga_instances` + outbox and returns 202; an in-process worker claims due sagas with a lease (`next_run_at`, `FOR UPDATE SKIP LOCKED`), runs `RESERVE_QUOTA` → `SCORE` (AI `ScoreMatch`, timeout + grpc retry policy + breaker, fallback `NEEDS_MANUAL_REVIEW`) or the `RELEASE_QUOTA` compensation (10 min deadline on reserve, `job.closed|job.deleted` consumer), every write version-guarded. Recruiters move applications along the state machine (`internal/domain`); every change goes to `application_history` and `application.status_changed`.
- `services/notification` — Notification service (M4a, ADR 0012): consumes `user.registered`, `job.*`, `cv.parsed|parse_failed`, `application.*`, `match.completed` into replicas (`users` with email, `jobs` with version guard + tombstone), `notifications` and an `email_outbox` (one tx per event, deduped); an in-process sender mails over SMTP (Mailpit) with backoff, `FAILED` after 8; new notifications are pushed after commit to an in-memory WebSocket hub. `GET /v1/notifications`, `POST /v1/notifications/{id}:read`; the gateway issues one-use Redis tickets (`POST /v1/ws/ticket`) and reverse-proxies `GET /v1/ws?ticket=` to `notification:8005/ws` with `X-User-ID`.
- `services/ai` — AI service in **Python 3.12** (uv): `ai-worker` (consumes `cv.uploaded`, PyMuPDF text, `LlmParser` port with `AnthropicLlmParser`/`FakeLlmParser`, own outbox + relay publishing `cv.parsed|cv.parse_failed`), `ai-matcher` (M2b: consumes `job.*` + `cv.updated`, `Embedder` port with `FakeEmbedder`/`GeminiEmbedder`, vectors in Qdrant, precomputed `match_results`, publishes `match.completed`) and `ai-api` (health/metrics + compose-internal gRPC `smarthire.ai.v1.AIService` on :9100 without JWT, ADR 0010).
- `contracts/events` — JSON Schemas of the Kafka events (`user.registered`, `job.created|updated|closed|deleted`, `cv.uploaded`, `cv.parsed`, `cv.parse_failed`, `cv.updated`, `match.completed`, `application.submitted`, `application.status_changed`) plus shared fixtures in `testdata/` used by both the Go and Python tests.
- `tools/cmd/` — `keygen`, `openapimerge`, `smoke`, `obscheck`.
- `deploy/docker` — Compose stack (Postgres, Redis, Redpanda + Console + `topics-init`, OTel collector (contrib), Tempo, Prometheus (+ alert rules), Loki, Grafana (+ provisioned dashboard), Mailpit, MinIO + `minio-init`, OpenSearch, Qdrant, auth, job, job-projector, candidate, application, notification, ai-worker, ai-matcher, ai-api, gateway); `.github/workflows` — CI.

Design documents:

- `spec.md` — full target system spec (Vietnamese). Source of truth for event envelope (§7.1), topics (§7.2), data models (§8), REST error format (§13.1), NFRs (§14).
- `docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md` — approved design for milestone M0. Where it differs from `spec.md`, the M0 design wins (see its §8 "Lệch so với spec.md").
- `docs/superpowers/plans/2026-11-08-m0-foundation-auth-gateway.md` — task-by-task TDD implementation plan for M0 (Tasks 0–16), with exact interfaces, env vars, ports, and test names.
- `docs/superpowers/specs/2026-11-10-m1a-events-job-design.md` — approved design for M1a (incl. §10 rulings); `docs/superpowers/plans/2026-11-10-m1a-events-job.md` — its TDD plan (Tasks 1–15).
- `docs/superpowers/specs/2026-11-11-m1b-candidate-ai-parse-design.md` — approved design for M1b (incl. §10 rulings); `docs/superpowers/plans/2026-11-11-m1b-candidate-ai.md` — its TDD plan (Tasks 1–15).
- `docs/superpowers/specs/2026-11-20-m2a-cqrs-job-search-design.md` — approved design for M2a; `docs/superpowers/plans/2026-11-20-m2a-cqrs-job-search.md` — its plan (Tasks 1–8, executed inline).
- `docs/superpowers/specs/2026-11-20-m2b-matching-design.md` — approved design for M2b; `docs/superpowers/plans/2026-11-20-m2b-matching.md` — its plan (Tasks 1–9, executed inline).
- `docs/superpowers/specs/2026-11-23-m3-application-saga-design.md` — approved design for M3; `docs/superpowers/plans/2026-11-23-m3-application-saga.md` — its plan (Tasks 1–9, executed inline).
- `docs/superpowers/specs/2026-11-23-m4a-notification-observability-design.md` — approved design for M4a; `docs/superpowers/plans/2026-11-23-m4a-notification-observability.md` — its plan (Tasks 1–9, executed inline).
- `docs/adr/` — architecture decision records (0001–0004 from M0, 0005–0006 from M1a, 0007–0008 from M1b, 0009 from M2a, 0010 from M2b, 0011 from M3, 0012–0013 from M4a).

When extending M0 code, keep the names/signatures from the plan's **Interfaces** blocks. The user writes in Vietnamese; reply in Vietnamese.

## Scope strategy (decided with the user)

"Depth over breadth": keep all 7 services from `spec.md` but invest in backend patterns (Outbox, idempotent consumers, Saga + recovery, circuit breaker/retry, DLQ, OTel tracing across Kafka and Go↔Python, testcontainers, k6, chaos). Frontend is minimal and deferred to M5 (no admin portal, i18n, dark mode, Tiptap). Demo runs on Docker Compose; K8s/Helm is optional (M6). Showcase = GitHub repo + video, not a live deployment.

Milestones: M0 foundation+Auth+Gateway → M1a event backbone + Job (Kafka/Redpanda, outbox relay, job search on Postgres `tsvector`; done) → M1b Candidate + AI parse (MinIO presigned upload, Python AI worker, `cv.*` events; done) → M2a CQRS job search (OpenSearch read model + projector; `tsvector` is the fallback; done) → M2b matching (Gemini embeddings behind a port, Qdrant, scoring; done) → M3 Application Saga (orchestration) + resilience (done) → M4a Notification + full observability (done) → M4b account-deletion saga (choreography, `user.deleted`/`user.data_purged`) → M5 thin frontend + E2E + k6 → M6 Helm/K8s. `spec.md` §6.6 defines when a Saga is warranted vs. plain Outbox + idempotent consumer. Each milestone gets its own spec → plan → implementation cycle under `docs/superpowers/`.

## Architecture (target, per M0 design and spec.md)

- **Monorepo, Go workspace** (`go.work`) with separate modules under prefix `github.com/nuroqzavitex/smarthire`: `pkg`, `gen/go`, `services/<name>`, `tools`. The Python AI service lives at `services/ai` (M1b; not a Go module, so Go loops use `go list -m`): uv-managed (`uv.lock`), `src/ai_service` layout (`domain` → `services` → `adapters`, mypy strict for `domain`/`services`), alembic migrations, `pydantic-settings` config. Event schemas are validated at runtime on the Go side (`pkg/event/eventschema` in candidate); Python validates with pydantic models that tests (`jsonschema`, shared `contracts/events/testdata` fixtures) keep in lock-step with the JSON Schemas. It has its own outbox + relay with the same algorithm as `pkg/outbox` (ADR 0007). MinIO and `mc` are built from the official GitHub source (`make minio-image`, ADR 0008) because the upstream Docker Hub images are gone.
- **Contracts first**: `contracts/proto` (buf) generates `gen/go` (committed, never hand-edited) plus grpc-gateway handlers and Swagger. `contracts/openapi/gateway.yaml` = generated swagger merged with `gateway.handwritten.yaml` by `tools/cmd/openapimerge`.
- **Services never import each other's `internal/`**; sharing only via `pkg/` and `gen/go/`. Database-per-service at "level 1" (`spec.md` §4.2): one Postgres instance, each service has its own DB owned by its own least-privilege role (e.g. `auth_svc` → `auth_db`); shared Redis/Kafka/MinIO are partitioned by service prefix (Redis keys `gateway:...`).
- **Service layout (hexagonal)**: `internal/domain` (no framework/DB/proto imports) → `internal/app` (use cases; ports in `ports.go`) → `internal/adapter/{grpc,postgres,...}` → `internal/bootstrap` (manual DI, graceful shutdown). `services/auth` is the template other Go services copy.
- **Gateway**: chi router; cookie-handling auth routes (`/v1/auth/login|refresh|logout`), health, and later WS are hand-written; everything else under `/v1/*` falls through to the grpc-gateway mux. Middleware order: Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes (auth before ratelimit so limits key on a *verified* `sub`; an invalid bearer is rate-limited by IP, then rejected; the cookie routes `/v1/auth/login|refresh|logout` are exempt from RejectInvalidAuth).
- **Two-layer JWT verification**: Gateway verifies, then forwards the raw JWT as gRPC metadata `authorization`; each service re-verifies via cached JWKS in the `pkg/grpcx` auth interceptor. Public RPCs are allow-listed per service.
- **Domain errors**: return `pkg/apperr` sentinels (`ErrNotFound`, `ErrConflict`, `ErrValidation`, `ErrForbidden`, `ErrUnauthenticated`); the grpcx interceptor maps to gRPC codes, gateway `httperr` maps to HTTP + envelope `{"error":{code,message,details,request_id}}`. Unknown errors become `Internal "internal error"` (no detail leakage).
- **Events**: written to a per-service `outbox` table in the same transaction as business data (envelope from `pkg/event`, ULID `event_id`). Since M1a an in-process relay (`pkg/outbox.Relay`, `FOR UPDATE SKIP LOCKED`, at-least-once, per-key order within one replica only, consumers reconcile by `version`) publishes to Kafka: topic = `event_type`, key = `aggregate_id`, value = envelope, headers = the `outbox.headers jsonb` column holding the W3C trace carrier that the relay restores so traces continue through Kafka (ADR 0005). Producer is idempotent (franz-go default; not disabled) with `acks=all`; consumers (`pkg/kafka`) commit manually, retry 3x in-process (backoff 200ms×2^n, max 2s), then write to `<topic>.dlq` with `dlq-*` headers; shutdown never sends to the DLQ and handler errors must not contain PII (ADR 0006).
- **Account status**: `users.status` ∈ `ACTIVE|LOCKED|DELETION_PENDING`; login checks it only *after* the password matches (403 for inactive), refresh returns 401 for inactive users.
- **Refresh tokens**: opaque, sha256-hashed, rotated within a `family_id`; reuse of a revoked token revokes the whole family. Since M1a (R26, ADR 0004 amendment) the reuse branch returns `errReuseDetected` so the rotation tx **rolls back**; the reuse warning is logged, then the family is revoked in a **second** transaction on `context.WithTimeout(context.WithoutCancel(ctx), 5s)` so a client disconnect cannot skip it, and the call returns `Unauthenticated`. A failed revocation is logged at Error and returned (→ 500). A ctx already cancelled before the first tx skips detection (accepted).

## Commands

Prereqs: Go 1.27, make, Docker. On Windows run everything from Git Bash (the Makefile is POSIX).

```bash
make bootstrap                 # install pinned buf/golangci-lint/migrate, generate dev JWT key (deploy/docker/secrets, gitignored), create .env from .env.example
make proto-deps                # buf dep update (bump buf deps, rewrites buf.lock; commit it)
make proto                     # buf generate on the committed lock → gen/go + openapi, then merge openapi
make sqlc                      # regenerate sqlc code (runs sqlc via Docker; not installed by bootstrap)
make infra-up obs-up up        # postgres+redis+redpanda (+console, topics-init), otel-collector+tempo+grafana, auth+job+gateway
make migrate | make seed       # run migrations | create admin + sample users (seed runs the auth-seed compose service)
make smoke                     # end-to-end auth + job + CV + matching + application saga flow through the gateway (CV parsed by the fake LLM), plus events on Kafka (tools/cmd/smoke)
make obs-check                 # metrics in Prometheus, logs in Loki, 7 alert rules loaded (run after smoke)
make chaos-ai                  # stop ai-api, submit applications: all must end NEEDS_MANUAL_REVIEW and the AI breaker must open; restarts ai-api
make db-init                   # create application_db and notification_db (+ roles) on an existing Postgres volume (init scripts only run on a fresh one)
make minio-image               # build smarthire/minio:RELEASE.* from GitHub source (infra-up and the integration targets depend on it)
make opensearch-image          # build smarthire/opensearch:2.19.2-icu (official image + analysis-icu; infra-up and test-int depend on it)
make search-rebuild           # zero-downtime rebuild of the job read model from Postgres (job-projector rebuild)
make search-reconcile         # compare open jobs in Postgres vs OpenSearch, exit 2 on mismatch (also run at the end of make smoke)
make test                      # unit tests, no Docker needed
make test-int                  # integration tests (build tag `integration`, testcontainers; needs Docker)
make cover-auth                # auth domain+app coverage, must be ≥ 70%
make cover-job                 # job domain+app coverage, must be ≥ 70%
make cover-candidate           # candidate domain+app coverage, must be ≥ 70%
make cover-application         # application domain+app coverage, must be ≥ 70%
make cover-notification        # notification domain+app coverage, must be ≥ 70%
make ai-sync ai-lint ai-test   # Python AI service: uv sync --frozen | ruff + mypy | unit tests (no Docker)
make ai-test-int ai-cover      # AI integration tests (testcontainers, needs Docker) | unit+integration coverage ≥ 70%
make lint                      # buf lint + golangci-lint per module
make down
make down-v                    # down + remove volumes (wipes the shared compose project "smarthire", all worktrees)
```

Single test (run inside the module directory, e.g. `services/auth`):
```bash
go test ./internal/app -run TestRefresh_ReuseRevokesFamily -v
go test -tags integration ./test/integration -run TestRefresh_ConcurrentSameToken -v
```

`-race` needs cgo on Linux; on Windows run it in a container (from the repo root):
```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd):/src" -w /src golang:1.27 bash -c 'for m in pkg services/auth services/job services/candidate services/application services/notification services/gateway tools; do (cd $m && go test -race ./...) || exit 1; done'
```

Local URLs (published to the host on 127.0.0.1 only): gateway `:8080`, Redpanda Console `:8081`, Kafka dev listener `:19092` (use `127.0.0.1:19092`; in-network brokers are `redpanda:9092`), Grafana `:3000` (dashboard "SmartHire — overview"), Tempo `:3200`, Prometheus `:9090`, Loki `:3100`, Mailpit `:8025`, OTel collector `:4317`. MinIO S3 `:9000` and console `:9001` (login with `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` from `.env`). OpenSearch `:9200` (security plugin off, dev only); Qdrant `:6333` (dashboard at `/dashboard`); AI gRPC `:9100` is compose-internal; the job projector HTTP `:8012` is compose-internal. Job gRPC `:9002` / HTTP `:8002`, Candidate gRPC `:9003` / HTTP `:8003`, Application gRPC `:9004` / HTTP `:8004`, Notification gRPC `:9005` / HTTP+WS `:8005` and AI api `:8100` are likewise compose-internal. Auth gRPC `:9001` and HTTP (JWKS/health) `:8001` are internal to the compose network (`auth:9001`, `auth:8001`), not reachable from the host.

## Conventions

- Config only via env with service prefix (`AUTH_`, `GATEWAY_`, `JOB_`, `CANDIDATE_`, `APPLICATION_`, `NOTIFICATION_`), loaded with `github.com/caarlos0/env/v11` through `pkg/config` (unprefixed vars never satisfy a config); the Python AI service uses the `AI_` prefix via `pydantic-settings` (plus `ANTHROPIC_API_KEY` / `OTEL_*` unprefixed, as the SDKs read them).
- SQL via `pgx/v5` + `sqlc` (no ORM); migrations with `golang-migrate`, append-only once merged.
- Never log PII (email, password, tokens, CV content); `user_id` is fine.
- Conventional Commits with service scope, e.g. `feat(auth): ...`.
- Record significant architecture decisions as ADRs in `docs/adr/`, numbered in the order written (M0 owns 0001–0004, M1a owns 0005–0006, M1b owns 0007–0008, M2a owns 0009, M2b owns 0010, M3 owns 0011, M4a owns 0012–0013); `spec.md` §10 lists planned ADR topics without numbers.
