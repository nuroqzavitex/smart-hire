# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

SmartHire is a recruiting platform (CV parsing, CV↔JD matching, application tracking) built as a **Backend-Go portfolio project**. As of now the repo contains **only design documents, no code**:

- `spec.md` — full target system spec (Vietnamese). Source of truth for event envelope (§7.1), topics (§7.2), data models (§8), REST error format (§13.1), NFRs (§14).
- `docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md` — approved design for milestone M0. Where it differs from `spec.md`, the M0 design wins (see its §8 "Lệch so với spec.md").
- `docs/superpowers/plans/2026-11-08-m0-foundation-auth-gateway.md` — task-by-task TDD implementation plan for M0 (Tasks 0–16), with exact interfaces, env vars, ports, and test names.

Before implementing anything, read the relevant plan task and its **Interfaces** block; later tasks depend on the exact names/signatures defined there. The user writes in Vietnamese; reply in Vietnamese.

## Scope strategy (decided with the user)

"Depth over breadth": keep all 7 services from `spec.md` but invest in backend patterns (Outbox, idempotent consumers, Saga + recovery, circuit breaker/retry, DLQ, OTel tracing across Kafka and Go↔Python, testcontainers, k6, chaos). Frontend is minimal and deferred to M5 (no admin portal, i18n, dark mode, Tiptap). Demo runs on Docker Compose; K8s/Helm is optional (M6). Showcase = GitHub repo + video, not a live deployment.

Milestones: M0 foundation+Auth+Gateway → M1 Job+Candidate+AI parse (+Kafka, outbox relay; job search on Postgres `tsvector`) → M2 matching + CQRS job search (OpenSearch read model + projector; `tsvector` becomes the fallback) → M3 Application Saga (orchestration) + resilience → M4 Notification + full observability + account-deletion saga (choreography, `user.deleted`/`user.data_purged`) → M5 thin frontend + E2E + k6 → M6 Helm/K8s. `spec.md` §6.6 defines when a Saga is warranted vs. plain Outbox + idempotent consumer. Each milestone gets its own spec → plan → implementation cycle under `docs/superpowers/`.

## Architecture (target, per M0 design and spec.md)

- **Monorepo, Go workspace** (`go.work`) with separate modules under prefix `github.com/nuroqzavitex/smarthire`: `pkg`, `gen/go`, `services/<name>`, `tools`. Python AI service lives at `services/ai` (from M1).
- **Contracts first**: `contracts/proto` (buf) generates `gen/go` (committed, never hand-edited) plus grpc-gateway handlers and Swagger. `contracts/openapi/gateway.yaml` = generated swagger merged with `gateway.handwritten.yaml` by `tools/cmd/openapimerge`.
- **Services never import each other's `internal/`**; sharing only via `pkg/` and `gen/go/`. Database-per-service at "level 1" (`spec.md` §4.2): one Postgres instance, each service has its own DB owned by its own least-privilege role (e.g. `auth_svc` → `auth_db`); shared Redis/Kafka/MinIO are partitioned by service prefix (Redis keys `gateway:...`).
- **Service layout (hexagonal)**: `internal/domain` (no framework/DB/proto imports) → `internal/app` (use cases; ports in `ports.go`) → `internal/adapter/{grpc,postgres,...}` → `internal/bootstrap` (manual DI, graceful shutdown). `services/auth` is the template other Go services copy.
- **Gateway**: chi router; cookie-handling auth routes (`/v1/auth/login|refresh|logout`), health, and later WS are hand-written; everything else under `/v1/*` falls through to the grpc-gateway mux. Middleware order: Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes (auth before ratelimit so limits key on a *verified* `sub`; an invalid bearer is rate-limited by IP, then rejected; the cookie routes `/v1/auth/login|refresh|logout` are exempt from RejectInvalidAuth).
- **Two-layer JWT verification**: Gateway verifies, then forwards the raw JWT as gRPC metadata `authorization`; each service re-verifies via cached JWKS in the `pkg/grpcx` auth interceptor. Public RPCs are allow-listed per service.
- **Domain errors**: return `pkg/apperr` sentinels (`ErrNotFound`, `ErrConflict`, `ErrValidation`, `ErrForbidden`, `ErrUnauthenticated`); the grpcx interceptor maps to gRPC codes, gateway `httperr` maps to HTTP + envelope `{"error":{code,message,details,request_id}}`. Unknown errors become `Internal "internal error"` (no detail leakage).
- **Events**: written to a per-service `outbox` table in the same transaction as business data (envelope from `pkg/event`, ULID `event_id`). M0 only writes the outbox; the relay to Kafka arrives in M1.
- **Account status**: `users.status` ∈ `ACTIVE|LOCKED|DELETION_PENDING`; login checks it only *after* the password matches (403 for inactive), refresh returns 401 for inactive users.
- **Refresh tokens**: opaque, sha256-hashed, rotated within a `family_id`; reuse of a revoked token revokes the whole family. The family revocation must **commit** even though the call returns `Unauthenticated` (the closure returns nil, error is returned after the tx) — see plan Task 7.

## Commands (defined by the plan; available once the corresponding tasks are implemented)

Run from Git Bash on Windows (Makefile is POSIX). Prereqs: Go, make, Docker (Go and make were not installed as of plan writing — plan Task 0).

```bash
make bootstrap                 # install pinned buf/golangci-lint/migrate, generate dev JWT key (deploy/docker/secrets, gitignored), create .env from .env.example
make proto-deps                # buf dep update (bump buf deps, rewrites buf.lock; commit it)
make proto                     # buf generate on the committed lock → gen/go + openapi, then merge openapi
make sqlc                      # regenerate sqlc code (runs sqlc via Docker; not installed by bootstrap)
make infra-up obs-up up        # postgres+redis, otel-collector+tempo+grafana, auth+gateway
make migrate | make seed       # run migrations | create admin + sample users (seed runs the auth-seed compose service)
make smoke                     # end-to-end auth flow through the gateway (tools/cmd/smoke)
make test                      # unit tests, no Docker needed
make test-int                  # integration tests (build tag `integration`, testcontainers; needs Docker)
make cover-auth                # auth domain+app coverage, must be ≥ 70%
make lint                      # buf lint + golangci-lint per module
make down
```

Single test (run inside the module directory, e.g. `services/auth`):
```bash
go test ./internal/app -run TestRefresh_ReuseRevokesFamily -v
go test -tags integration ./test/integration -run TestRefresh_ConcurrentSameToken -v
```

Local URLs (published to the host on 127.0.0.1 only): gateway `:8080`, Grafana `:3000`, Tempo `:3200`, OTel collector `:4317`. Auth gRPC `:9001` and HTTP (JWKS/health) `:8001` are internal to the compose network (`auth:9001`, `auth:8001`), not reachable from the host.

## Conventions

- Config only via env with service prefix (`AUTH_`, `GATEWAY_`), loaded with `github.com/caarlos0/env/v11` through `pkg/config` (unprefixed vars never satisfy a config).
- SQL via `pgx/v5` + `sqlc` (no ORM); migrations with `golang-migrate`, append-only once merged.
- Never log PII (email, password, tokens, CV content); `user_id` is fine.
- Conventional Commits with service scope, e.g. `feat(auth): ...`.
- Record significant architecture decisions as ADRs in `docs/adr/`, numbered in the order written (M0 owns 0001–0004); `spec.md` §10 lists planned ADR topics without numbers.
