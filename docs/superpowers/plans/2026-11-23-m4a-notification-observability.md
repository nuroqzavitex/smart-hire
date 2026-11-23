# M4a Notification + Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Notification service (in-app + WebSocket + email outbox) behind gateway WS tickets, and metrics + logs for every service through the OTel collector into Prometheus and Loki with dashboards and alerts.

**Architecture:** Notification consumes 9 topics into local replicas, `notifications` and `email_outbox` in one tx per event, pushes to an in-memory WS hub after commit and sends mail with a SKIP-LOCKED sender loop. Gateway issues one-use Redis tickets and reverse-proxies `/v1/ws` to Notification with `X-User-ID`. `pkg/otel` adds Meter/Logger providers, `pkg/logger` tees to `otelslog`; shared instruments live in `pkg/outbox`, `pkg/kafka`, `pkg/otel`; the collector exports Prometheus `:8889` and OTLP logs to Loki.

**Tech Stack:** Go 1.27, OTel Go SDK (metric, log, `otelslog`), `github.com/coder/websocket`, `net/smtp`, Redis (go-redis v9), Prometheus, Loki 3, Grafana, Mailpit, Python `opentelemetry-sdk` (metrics, logs).

**Spec:** `docs/superpowers/specs/2026-11-23-m4a-notification-observability-design.md`

## Global Constraints

- Module `github.com/nuroqzavitex/smarthire/services/notification`, hexagonal like Application; gRPC `:9005`, HTTP `:8005`; env prefix `NOTIFICATION_`; DB `notification_db` / role `notification_svc`.
- Never log PII: no email address, no notification title/body in logs; no per-user metric labels.
- Dedupe every consumed event by `event_id` (`processed_events`) in the same tx as its writes.
- WS: ticket = 32 random bytes base64url, Redis key `gateway:ws:ticket:<t>` = user id, TTL 30s, `GETDEL`; Origin must be in `GATEWAY_CORS_ORIGINS` when present; never a JWT in a URL.
- Email: sender loop 1s, SMTP timeout 10s, backoff 5s·2ⁿ capped 10m, `FAILED` after 8 attempts; tests never reach a real mail server.
- Telemetry only via OTLP to the collector (`OTEL_EXPORTER_OTLP_ENDPOINT`); unset env = no-op providers.
- Metric names exactly as spec §7 table; breaker state gauge values 0 closed / 1 half-open / 2 open.
- Conventional Commits with scope; local commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A WS connection kept open longer than the gateway's `WriteTimeout` (15s) / `ReadTimeout` (10s) still receives messages — Task 7 test with a short-timeout server.
2. A ticket used twice, expired, or presented with a foreign `Origin` → 401/403 before upgrade, nothing proxied — Task 7.
3. A slow or never-reading WS client does not block the consumer (bounded queue, connection dropped) — Task 6 hub test.
4. Redelivered event (same `event_id`) creates no second notification or email — Task 5 unit + Task 6 integration.
5. SMTP down: notification still committed and listed; email retried with backoff then `FAILED` — Task 6 sender test.

---

### Task 1: Notification contract

**Files:** Create `contracts/proto/smarthire/notification/v1/notification.proto`; regenerate (`make proto`).

**Produces:** `NotificationService { ListNotifications(ListNotificationsRequest{bool unread_only; int32 page_size (0–100); string page_token (≤512)}) returns (ListNotificationsResponse{repeated Notification notifications; string next_page_token; int32 unread_count}) GET /v1/notifications; MarkNotificationRead(MarkNotificationReadRequest{string id (uuid)}) returns (Notification) POST /v1/notifications/{id}:read body "*" }`; `Notification{id, type, title, body, map<string,string> data, google.protobuf.Timestamp read_at, created_at}`.

- [ ] Write proto; `make proto`; `cd contracts/proto && buf lint` → clean; `go build ./...` in gateway OK.
- [ ] Commit `feat(contracts): add notification service`.

### Task 2: Telemetry pipeline (providers, logger tee, collector, Prometheus, Loki, Python)

**Files:** Modify `pkg/otel/otel.go` (+ `otel_test.go`), `pkg/logger/logger.go` (+ test), `deploy/docker/otel/collector.yaml`, `deploy/docker/otel/grafana-datasources.yaml`, `deploy/docker/docker-compose.obs.yml`; create `deploy/docker/prometheus/prometheus.yml`, `deploy/docker/loki/loki.yaml`; modify `services/ai/src/ai_service/observability.py` (+ unit test), `services/ai/pyproject.toml`/`uv.lock` if a package is missing.

**Produces:**
- `otel.Setup(ctx, service) (shutdown func(context.Context) error, err error)` now also installs global MeterProvider (OTLP gRPC, `PeriodicReader` 10s) and global LoggerProvider (OTLP gRPC, batch) when the env is set; shutdown flushes all three.
- `otel.Meter(name string) metric.Meter` = global meter (helper for packages).
- `logger.New(service, w, level)` writes JSON to `w` **and** to `otelslog.NewHandler(service)` (global LoggerProvider, so providers set later still receive logs); level applies to both; `trace_id/span_id/request_id/user_id/service` attrs on both.
- Collector pipelines: traces → tempo; metrics (otlp) → `prometheus` exporter `0.0.0.0:8889` (`resource_to_telemetry_conversion: enabled`); logs → `otlphttp/loki` `http://loki:3100/otlp`.
- Compose obs: `prometheus` (`prom/prometheus:v3.4.1`, `127.0.0.1:9090`, scrapes `otel-collector:8889` every 10s, `--web.enable-lifecycle`, rules dir mounted from Task 8), `loki` (`grafana/loki:3.5.1`, `127.0.0.1:3100`, single-binary filesystem config, `allow_structured_metadata: true`). Grafana datasources: Prometheus (uid `prometheus`), Loki (uid `loki`, derived field `trace_id` → Tempo), Tempo `tracesToLogsV2` → Loki.
- Python `setup_otel(service)` also sets MeterProvider (`PeriodicExportingMetricReader` + `OTLPMetricExporter`) and LoggerProvider (`BatchLogRecordProcessor` + `OTLPLogExporter`), attaching `LoggingHandler` to the root logger, when the endpoint env is set; `meter()` helper.

- [ ] Tests first: `TestSetup_NoEndpointIsNoop` (env unset → shutdown nil, no panic), `TestLogger_TeesToOTel` (set a global `sdklog.LoggerProvider` with an in-memory exporter after `logger.New`; `InfoContext` with a span ctx → exporter got one record with body "hello" and attribute `trace_id`; JSON writer also got the line), `TestLogger_LevelAppliesToOTel` (debug dropped at info). Python: `test_setup_otel_installs_meter_and_logger_providers` (env set to `http://127.0.0.1:1` → `metrics.get_meter_provider()` is SDK `MeterProvider`; shutdown callable). Run → FAIL.
- [ ] Implement; run `cd pkg && go test ./otel ./logger` and `make ai-test` → PASS; `make lint ai-lint`.
- [ ] Commit `feat(obs): export metrics and logs through the otel collector to prometheus and loki`.

### Task 3: Shared instruments (outbox, kafka, breaker)

**Files:** Modify `pkg/outbox/relay.go` (+ test), `pkg/kafka/consumer.go`, `pkg/kafka/dlq.go` (+ tests); create `pkg/otel/breaker.go` (+ test); wire breakers in `services/job/internal/adapter/{opensearch,aiclient}`, `services/candidate/internal/adapter/aiclient`, `services/application/internal/adapter/aiclient`.

**Produces:**
- `func outbox.RegisterPendingGauge(m metric.Meter, q Querier) (metric.Registration, error)`: observable int64 gauge `outbox.pending` counting `SELECT count(*) FROM outbox WHERE published_at IS NULL` (2s timeout; error → skip observation). Called from each service bootstrap that runs a relay (auth, job, candidate, application, notification has none).
- `pkg/kafka`: counter `dlq.messages{topic}` incremented on every DLQ write; observable gauge `kafka.consumer.lag{group,topic}` summed over partitions via `kadm.NewClient(cl).Lag(ctx, group)` (5s timeout) registered in `NewConsumer`, unregistered in `Close`.
- `otel.RecordBreakerState(ctx, breaker string, state int64)` (pkg stays free of gobreaker) sets gauge `circuit_breaker.state{breaker}` (Int64Gauge) with consts `BreakerClosed=0, BreakerHalfOpen=1, BreakerOpen=2`; each service's `OnStateChange` calls it and the initial closed state is recorded at construction.

- [ ] Tests first (all with `sdk/metric.NewManualReader`): `TestPendingGauge_CountsUnpublished` (outbox integration test package: 3 rows, 1 published → value 2), `TestDLQ_CountsByTopic` (kafka integration: poison record → `dlq.messages{topic="t"}`=1), `TestConsumerLag_Observed` (kafka integration: 5 records produced, none consumed by group → lag 5), `TestRecordBreakerState` (unit: open → 2 for breaker "x"). Per-service: extend each existing breaker test with a ManualReader asserting state 2 after opening. Run → FAIL.
- [ ] Implement + wire; `make test` and `go test -tags integration` for `pkg/outbox`, `pkg/kafka` → PASS.
- [ ] Commit `feat(obs): add outbox, kafka and circuit breaker metrics`.

### Task 4: Service metrics (application, job, ai)

**Files:** application `internal/app/metrics.go`/bootstrap (saga gauge via repo `CountByState(ctx) (map[domain.SagaState]int, error)` + sqlc query); job `ListJobs` fallback counter + projector lag histogram; ai `ai.parse.duration`, `ai.match.score`, `ai.llm.tokens{direction}`, Python consumer `dlq.messages{topic}`.

**Produces:** gauge `saga.instances{state}` (all four states, 0 when absent); counter `search.fallback`; histogram `projection.lag` (seconds, `now − envelope.occurred_at` on each indexed event); Python histogram `ai.parse.duration` (s, per parse attempt), histogram `ai.match.score` (per scored pair), counter `ai.llm.tokens{direction=input|output}` (Anthropic usage only), counter `dlq.messages{topic}`.

- [ ] Tests first with ManualReader / Python `InMemoryMetricReader`: `TestSagaGauge_ReportsEveryState`, `TestListJobs_FallbackCounted`, `TestProjector_RecordsLag`, `test_parse_records_duration`, `test_match_records_scores`, `test_dlq_counter`. Run → FAIL.
- [ ] Implement; unit tests + `make ai-test` → PASS.
- [ ] Commit `feat(obs): add saga, search, projection and ai metrics`.

### Task 5: Notification domain + use cases

**Files:** Create `services/notification/{go.mod,internal/domain/*,internal/app/*,internal/config/config.go}`; `go.work`, Makefile (`sqlc`, `cover-notification`).

**Produces (domain):** `Type` consts `TypeWelcome="welcome"`, `TypeCVParsed="cv_parsed"`, `TypeCVParseFailed="cv_parse_failed"`, `TypeApplicationReceived="application_received"`, `TypeApplicationStatus="application_status"`, `TypeCandidatesMatched="candidates_matched"`; `Notification{ID, UserID uuid.UUID; Type Type; Title, Body string; Data map[string]string; ReadAt *time.Time; CreatedAt time.Time}`; `Email{ID, NotificationID uuid.UUID; To, Subject, HTML, Text string; Status EmailStatus; Attempts int; NextAttemptAt time.Time; LastError string}` with `EmailPending|EmailSent|EmailFailed`; `User{ID uuid.UUID; Email, Role string}`; `Job{ID, RecruiterID uuid.UUID; Title string; Version int64}`; `ErrNotificationNotFound` (apperr NotFound).
**Produces (app):** ports `Users{Upsert, Get}`, `Jobs{Upsert(ctx, Job) (applied bool, err) /* version guard */, Get, Delete}`, `Notifications{Create, List(ctx, userID, unreadOnly, after *Cursor, limit), CountUnread, MarkRead(ctx, id, userID, at) (Notification, error)}`, `Emails{Enqueue, ClaimDue(ctx, now, limit), MarkSent, MarkRetry(ctx, id, next time.Time, lastErr string), MarkFailed}`, `Mailer{Send(ctx, to, subject, html, text string) error}`, `Pusher{Push(userID uuid.UUID, n domain.Notification)}`, Tx/Idempotency as other services. `Service.HandleEvent(ctx, raw []byte) error` (routes by `event_type`, poison → `ErrPoisonEvent`), `List(ctx, ListInput{UnreadOnly bool; PageSize int; PageToken string}) ([]Notification, next string, unread int, error)`, `MarkRead(ctx, id) (Notification, error)`, `SendDue(ctx) (int, error)`; templates in `internal/app/templates/*.tmpl` via `embed`.

Rules (spec §4): welcome+email for `user.registered` (also upsert user); cv events → candidate, no email; `application.submitted` → recruiter, email; `application.status_changed` only `to ∈ {INTERVIEW,OFFERED,HIRED,REJECTED}` and `actor != "system"` → candidate, email; `match.completed` → job's recruiter when replica exists and `len(top) > 0`, no email; `job.created|updated` upsert replica (version guard), `job.deleted` delete. Missing user email → no email row. Push happens after the tx commits.

- [ ] Tests first: table `TestHandleEvent_Recipients` (each event → expected user, type, email yes/no), `TestHandleEvent_DuplicateIgnored`, `TestHandleEvent_StatusChangeSkipsSystemAndInternalSteps`, `TestHandleEvent_MatchWithoutJobReplicaSkipped`, `TestHandleEvent_JobReplicaVersionGuard`, `TestHandleEvent_NoEmailWithoutAddress`, `TestHandleEvent_PushAfterCommit` (pusher called only when tx succeeds), `TestHandleEvent_Poison`, `TestList_PagesAndUnreadCount`, `TestMarkRead_OwnerOnly` (other user → NotFound; second call keeps first `read_at`), `TestSendDue_BackoffThenFailed` (Mailer errors: next = now+5s, +10s, … cap 10m; 8th failure → FAILED; success → SENT), templates render Vietnamese text without raw `{{`. Run → FAIL.
- [ ] Implement; `make cover-notification` ≥ 70%.
- [ ] Commit `feat(notification): turn events into notifications and queued emails`.

### Task 6: Notification adapters, hub, bootstrap

**Files:** Create `migrations/000001_init.*`, `internal/adapter/postgres/*` (+ sqlc), `internal/adapter/mailer/smtp.go`, `internal/adapter/ws/{hub.go,handler.go}`, `internal/adapter/grpc/*`, `internal/adapter/consumer/handler.go`, `internal/adapter/httpx`, `internal/bootstrap`, `cmd/server`, `Dockerfile`, `README.md`, `test/integration/*`.

**Produces:** `ws.NewHub(queue int) *Hub` (`Push` non-blocking: full queue → close that connection), `ws.Handler(hub, log) http.Handler` serving `GET /ws` (reads `X-User-ID`, 400 if missing/invalid; ping 30s; JSON `{"type":"notification.created","notification":{...}}` using the REST field names); `mailer.NewSMTP(addr, from string, timeout time.Duration)`; consumer `Group="notification"`, `Topics` = the 9 topics; config `NOTIFICATION_SMTP_ADDR` (default `mailpit:1025`), `NOTIFICATION_MAIL_FROM` (default `SmartHire <no-reply@smarthire.local>`), `NOTIFICATION_EMAIL_POLL` 1s, plus the usual DB/JWKS/Kafka/ports; metrics `notifications.created{type}`, `emails.sent`, `emails.failed`.

- [ ] Tests first: `TestHub_DeliversToAllConnectionsOfUser`, `TestHub_SlowClientDropped` (queue 1, never-reading client → closed, Push returns immediately), `TestWSHandler_RequiresUserHeader`, `TestWSHandler_StreamsPushedNotification` (httptest server + `websocket.Dial`), `TestSMTP_SendsToFakeServer` (in-test SMTP listener records DATA), grpc handler mapping tests; integration (testcontainers Postgres): `TestRepo_EmailClaimSkipLocked`, `TestConsumer_EndToEnd` (HandleEvent for `user.registered` + `application.status_changed` → 2 notifications, 2 emails; redelivery → still 2), `TestMigrations_UpDown`. Run → FAIL.
- [ ] Implement; `go test ./... && go test -count=1 -tags integration ./test/integration` → PASS; lint.
- [ ] Commit `feat(notification): serve notifications over grpc and websocket and send queued email`.

### Task 7: Gateway WS ticket + proxy + notification routes

**Files:** Create `services/gateway/internal/wsproxy/{ticket.go,proxy.go,proxy_test.go}`; modify router, config (`NotificationGRPCAddr` default `notification:9005`, `NotificationWSURL` default `http://notification:8005/ws`), main.

**Produces:** `wsproxy.NewTickets(rdb redis.Cmdable, ttl time.Duration) *Tickets` with `Issue(ctx, userID string) (string, error)` and `Redeem(ctx, ticket string) (userID string, err error)` (`GETDEL`; missing → `ErrInvalidTicket`); `wsproxy.Handler(tickets, target *url.URL, origins []string, log) http.Handler` for `GET /v1/ws` (Origin check → 403; redeem → 401; Redis error → 503; clears read/write deadlines via `http.NewResponseController`; `httputil.ReverseProxy` with `X-User-ID`, query stripped); `POST /v1/ws/ticket` handler (requires verified claims, else 401) returning `{"ticket","expires_in":30}`; grpc-gateway registers NotificationService.

- [ ] Tests first (miniredis or a fake `redis.Cmdable`): `TestTicket_OneUse`, `TestTicket_Expires`, `TestTicketRoute_RequiresAuth`, `TestWSProxy_ProxiesWithUserHeader` (fake upstream WS echoes `X-User-ID`), `TestWSProxy_RejectsReusedTicket`, `TestWSProxy_RejectsForeignOrigin`, `TestWSProxy_OutlivesServerTimeouts` (gateway `http.Server{ReadTimeout:200ms, WriteTimeout:200ms}`, upstream pushes after 600ms → client receives), `TestRouter_NotificationRoutes`. Run → FAIL.
- [ ] Implement; gateway tests + lint → PASS.
- [ ] Commit `feat(gateway): issue websocket tickets and proxy notifications`.

### Task 8: Deploy, dashboards, alerts, CI

**Files:** `deploy/docker/init/postgres/06-create-notification-db.sql` (idempotent), compose infra (`mailpit` `axllent/mailpit:v1.27`, env password), compose app (`notification-migrate`, `notification`, gateway env), compose obs (prometheus rules mount, grafana dashboards provisioning), `deploy/docker/prometheus/alerts.yml`, `deploy/docker/grafana/dashboards/{dashboards.yaml,smarthire-overview.json}`, `.env.example` (`NOTIFICATION_DB_PASSWORD`), Makefile (`db-init` also runs 06, `migrate`, `up` waits for `/v1/notifications` = 401, `cover-notification`, `obs-check`), `.github/workflows/ci-go.yml`.

Alert rules (exact): `KafkaConsumerLagHigh: max(kafka_consumer_lag) by (group) > 1000 for 5m`; `DLQMessages: increase(dlq_messages_total[5m]) > 0`; `GRPCErrorRateHigh: sum(rate(rpc_server_duration_milliseconds_count{rpc_grpc_status_code!="0"}[5m])) / sum(rate(rpc_server_duration_milliseconds_count[5m])) > 0.02 for 5m`; `CircuitBreakerOpen: max(circuit_breaker_state) by (breaker) == 2 for 2m`; `OutboxBacklog: max(outbox_pending) by (service_name) > 500 for 5m`; `ProjectionLagHigh: histogram_quantile(0.95, sum(rate(projection_lag_seconds_bucket[5m])) by (le)) > 30`; `EmailFailed: increase(emails_failed_total[15m]) > 0`. (Builder adjusts label names to what the collector actually emits, verified with `obs-check`.)

- [ ] `make db-init infra-up obs-up up` → notification up (401); `curl localhost:9090/api/v1/rules` lists the 7 rules; Grafana `/api/search?query=SmartHire` finds the dashboard.
- [ ] Commit `feat(deploy): wire notification, mailpit, prometheus, loki, dashboards and alerts`.

### Task 9: Smoke, obs-check, docs

**Files:** `tools/cmd/smoke/notifications.go`, `tools/cmd/obscheck/main.go` (Prometheus + Loki queries), main wiring, ADR 0012/0013, `docs/adr/README.md`, README, CLAUDE.md, `services/notification/README.md`.

- [ ] Smoke steps: after register → `GET /v1/notifications` has `welcome` (poll 30s) and Mailpit `GET :8025/api/v1/search?query=to:<email>` finds one message; candidate opens WS via ticket (`coder/websocket` client, no Origin); recruiter moves the M3 smoke application to INTERVIEW → candidate receives `notification.created` with type `application_status` within 15s; reused ticket → 401; mark read → `read_at` set, `unread_count` drops. `obs-check`: Prometheus non-empty for `saga_instances`, `outbox_pending`, `rpc_server_duration_milliseconds_count`, `circuit_breaker_state`; Loki `{service_name="application"}` returns ≥1 line in last 15m; 7 alert rules loaded.
- [ ] `make smoke`, `make obs-check`, `make chaos-ai` → PASS (chaos also shows `circuit_breaker_state{breaker="application-ai-score"}` = 2 in Prometheus).
- [ ] Docs + ADRs; `make lint test`.
- [ ] Commit `feat(smoke): cover notifications and websocket and check telemetry` and `docs: add ADR 0012 and 0013 and update docs for M4a`.
