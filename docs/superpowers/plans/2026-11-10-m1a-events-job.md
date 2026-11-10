# M1a — Event backbone + Job Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Đưa event backbone (Redpanda + `pkg/kafka` + `pkg/outbox` relay + `pkg/idempotency` + DLQ) vào hệ thống, bật relay cho Auth, và xây Job service (CRUD JD, state machine, tìm kiếm `tsvector` bỏ dấu, event `job.*`) đi qua Gateway, kiểm chứng end-to-end bằng smoke.

**Architecture:** Mỗi service phát event ghi envelope vào bảng `outbox` (kèm carrier W3C trong cột `headers`) trong cùng transaction nghiệp vụ; `outbox.Relay` chạy như goroutine trong `cmd/server`, poll `FOR UPDATE SKIP LOCKED` và produce qua `kafka.Producer` (franz-go). `kafka.Consumer` dùng consumer group, retry trong process rồi chuyển `<topic>.dlq`. Job service sao chép template hexagonal của Auth (`domain → app → adapter → bootstrap`), Gateway mount `JobService` lên cùng grpc-gateway mux.

**Tech Stack:** Go 1.27 (workspace), franz-go (`kgo`, `kadm`), Redpanda + Redpanda Console, pgx/v5 + sqlc, golang-migrate, Postgres `unaccent` + `tsvector`, gRPC + grpc-gateway v2 + protovalidate, caarlos0/env/v11, OpenTelemetry, santhosh-tekuri/jsonschema/v6, testify, testcontainers-go (modules postgres, redpanda), Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-10-m1a-events-job-design.md` (rulings D1–D9 ở §10 là ràng buộc). Kế thừa M0: `docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md`, `spec.md` §7.1 (envelope), §7.2 (topic), §6.6.

## Global Constraints

- Module prefix `github.com/nuroqzavitex/smarthire`; module mới `.../services/job` thêm vào `go.work`. Directive `go 1.27` trong mọi `go.mod`. `go.mod` của service có `replace` cho `pkg` và `gen/go` (như Auth); Dockerfile build với `GOWORK=off`.
- Không import chéo `internal/` giữa các service; chia sẻ chỉ qua `pkg/`, `gen/go/`. `gen/` được commit, không sửa tay; sinh bằng `make proto`.
- Config chỉ qua env có prefix service, đọc bằng `github.com/caarlos0/env/v11` qua `pkg/config.Load(prefix, &cfg)`; tag `env:"NAME"`, `envDefault:"..."`, `env:"NAME,required"`, `envSeparator:","`. Biến không prefix không bao giờ thoả config.
- Ports: Job gRPC `9002`, Job HTTP nội bộ `8002` (health). Redpanda listener nội bộ `redpanda:9092`; listener dev `127.0.0.1:19092` (D9); Redpanda Console `127.0.0.1:8081`. Mọi cổng publish ra host bind `127.0.0.1`.
- Topic M1a: `user.registered`, `job.created`, `job.updated`, `job.closed`, `job.deleted` và `<topic>.dlq` cho mỗi topic; 3 partition, replication 1; tắt auto-create topic.
- Kafka: idempotent producer, `acks=all`; `MaxAttempts=3`; backoff `200ms × 2^n`, tối đa `2s`; header DLQ `dlq-error` (cắt 512 ký tự), `dlq-attempts`, `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset`.
- Outbox: `RelayOptions` mặc định `BatchSize=100`, `Interval=500ms`. Topic = `event_type`, key = `aggregate_id`, value = `payload` (envelope JSON đầy đủ, như Auth M0 đang lưu), headers = `headers`.
- Event: envelope `pkg/event` (spec.md §7.1), `event_version=1`, `source` = `auth-service` / `job-service`, key = id aggregate (`user_id` / `job_id`).
- Postgres: mỗi service một role làm owner DB riêng. Job dùng role `job_svc` sở hữu `job_db`, mật khẩu `JOB_DB_PASSWORD`. Không service nào kết nối bằng `postgres`.
- Key Redis có tiền tố service (`gateway:`); M1a không thêm key Redis mới.
- Job: `title` 5–200 rune, `description` 20–20 000 rune, tối đa 30 skill, `location` 1–100 rune, `salary_min ≤ salary_max`, cả hai ≥ 0; `Level` ∈ `intern|junior|middle|senior|lead`; `Status` ∈ `draft|open|closed`; `page_size` mặc định 20, tối đa 100.
- Chuyển trạng thái sai → `apperr.ErrValidation` (→ `InvalidArgument`, 400) (D6). Không phải owner → `apperr.ErrNotFound` (D8).
- `PublicMethods` của mọi service gồm `grpc_health_v1.Health_Check_FullMethodName`. Job: thêm `GetJob`, `ListJobs`.
- Gateway: chain middleware giữ nguyên Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes; route grpc-gateway nằm sau `MaxBytesHandler` và `stripGRPCTimeout`.
- SQL: `pgx/v5` + `sqlc` (chạy qua Docker bằng `make sqlc`, `MSYS_NO_PATHCONV=1`, mount `$(CURDIR)`); `pkg/outbox` và `pkg/idempotency` dùng SQL thô (pkg không dùng sqlc). Migration append-only: Auth thêm `000002`, không sửa `000001`.
- Integration test dùng build tag `integration` + testcontainers; `make test` không cần Docker. `-race` chạy trên Linux (CI hoặc container `golang:1.27`), máy Windows không có cgo.
- Không log/không đưa vào `dlq-error` PII (email, password, token, nội dung CV). Log `user_id`, `job_id` được phép.
- ADR tiếp tục đánh số: 0005 (outbox relay + headers), 0006 (Kafka client + retry/DLQ).
- Commit theo Conventional Commits có scope (`feat(job): ...`), message kết thúc bằng dòng `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Makefile viết cho POSIX shell (Git Bash trên Windows, Ubuntu trên CI).

## Quyết định khi lập plan

Các điểm spec để ngỏ, chốt tại đây (executor không tự đổi):

1. **R26:** Lookup + xoay vòng chạy trong một transaction trên ctx của request. Nhánh dùng lại trả sentinel nội bộ `errReuseDetected` (rollback, không ghi gì), rồi thu hồi family trong **transaction thứ hai** trên `context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)`. `RevokeFamily` chỉ đụng token chưa revoke nên việc tách hai transaction là an toàn (token xoay vòng chen vào giữa cũng bị thu hồi). Ctx đã huỷ **trước** khi Refresh bắt đầu thì không phát hiện được reuse (không chạm DB) — chấp nhận; test M0 `TestRefresh_ReuseRevokesFamilyWithCancelledContext` được thay bằng biến thể huỷ *giữa chừng*.
2. **Trace qua Kafka:** `Producer.Produce` luôn mở span `"<topic> publish"` (kind Producer) và inject context của span đó, trừ khi `Record.Headers` đã có `traceparent`. Relay extract carrier lưu trong `headers` thành ctx cha, **bỏ** `traceparent`/`tracestate` khỏi header gửi đi, để producer span là con của request gốc và consumer span `"<topic> process"` là con của producer span.
3. **Relay produce tuần tự, đồng bộ** từng bản ghi (giữ thứ tự theo `created_at, id`); `kgo.RecordDeliveryTimeout(10s)` để broker chết không treo tick.
4. **Consumer** một goroutine, xử lý record theo thứ tự fetch, `CommitRecords` đồng bộ sau mỗi record thành công hoặc sau khi vào DLQ. Ctx bị huỷ giữa lúc backoff → trả về, không commit (message sẽ được giao lại).
5. **Skill filter không phân biệt hoa thường:** skill lưu ở dạng chuẩn hoá giữ hoa/thường gốc (ví dụ `Go`). Thêm hàm `lower_text_array(text[]) IMMUTABLE` và index GIN biểu thức `jobs_skills_gin ON jobs USING GIN (lower_text_array(required_skills))` thay cho GIN trên cột thô; app hạ chữ thường tham số `skills`. `array_to_string` là `STABLE` nên generated column dùng wrapper `text_array_join(text[]) IMMUTABLE`.
6. **Keyset:** thứ tự tie-break là `id DESC` (spec ghi `id`, không nêu chiều) để so sánh bằng row-value `(rank, published_at, id) < ($r, $p, $id)`. Cursor `{r?, p, id}`; ListMyJobs dùng cùng struct với `p = created_at`. Token có `r` mà request không có `q` (hoặc ngược lại) → `InvalidArgument`.
7. **`UpdateJob` không có field nào** → no-op: trả JD hiện tại, không tăng version, không ghi event. Không có cách xoá lương về NULL trong M1a.
8. **`DeleteJob` khi `open`** → `ErrValidation` (phải close trước), đúng bảng chuyển trạng thái.
9. **CreateJob trả 201** bằng forward-response option kiểm `runtime.RPCMethod(ctx) == jobv1.JobService_CreateJob_FullMethodName` (`CreateJob` trả `Job`, trùng kiểu với `GetJob` nên không phân biệt được bằng kiểu message).
10. **JSON Schema** mô tả **toàn bộ envelope** (field envelope bắt buộc, `event_type` là `const`, `payload` có `additionalProperties: false`). Helper `pkg/event/eventschema` tìm thư mục `contracts/events` bằng cách đi ngược lên từ cwd; chỉ dùng trong test.
11. **Verifier có retry** chuyển vào `pkg/authz.NewVerifierWithRetry` (Job cần chờ JWKS của Auth như Gateway); Gateway chuyển sang dùng hàm này.
12. **Role `job_svc`** tạo trong file init mới `02-create-job-db.sql`. Init script chỉ chạy trên volume mới: volume Postgres cũ phải xoá (`docker compose ... down -v`), ghi trong README.
13. **`make up`** chờ thêm `GET /v1/jobs` trả 200 (Gateway → Job đã sẵn sàng) trước khi báo xong, để smoke không đụng Job đang khởi động.
14. **Image pin:** `docker.redpanda.com/redpandadata/redpanda:v24.2.7`, `docker.redpanda.com/redpandadata/console:v2.8.0` (dùng chung cho compose và testcontainers). Nếu tag không pull được thì nâng lên patch gần nhất cùng minor và ghi lại.
15. **So sánh location bỏ dấu + không phân biệt hoa thường** (`lower(unaccent_immutable(...))` hai vế): image `postgres:16-alpine` chạy locale C nên `lower()` chỉ hạ chữ ASCII; bỏ dấu trước giúp so sánh đúng với tiếng Việt. Skill filter hạ chữ thường ở Go (`strings.ToLower`) và ở DB (`lower`) — chỉ nhất quán với skill ASCII; chấp nhận cho M1a (bảng alias skill để M2).

## Review Focus

1. **Client ngắt kết nối giữa lúc xoay vòng refresh token** — mong đợi rollback, RT cũ vẫn dùng được; còn nếu đã phát hiện dùng lại thì family vẫn bị thu hồi dù client đi mất. Test: `TestRefresh_CancelledMidRotationKeepsOldToken` (Task 1, integration), `TestRefresh_ReuseRevokesFamilyWhenCancelledMidway` (Task 1).
2. **`page_token` bị sửa tay, hết hạn ngữ nghĩa, hoặc lấy từ truy vấn khác (có `q` ↔ không `q`)** — mong đợi `400 InvalidArgument`, không phải 500 hay trang sai. Test: `TestDecodeCursor_Rejects` (Task 9), `TestListJobs_CursorFromOtherQueryRejected` (Task 9).
3. **Chuỗi `q` có ký tự đặc biệt của tsquery** (`c++ & | ! "`, dấu ngoặc lệch, chỉ toàn dấu câu) — mong đợi 200 với kết quả rỗng hoặc hợp lý, không lỗi SQL. Test: `TestSearch_HostileQueryDoesNotError` (Task 10).
4. **Tiếng Việt có `đ/Đ` và chữ hoa có dấu** (`"ĐÀ NẴNG"`, `"Kỹ sư Đường ống"` tìm bằng `"duong ong"`) — mong đợi khớp như chữ không dấu thường. Test: `TestSearch_UnaccentHandlesDStrokeAndCase` (Task 10).
5. **Broker chết khi service đang tắt** — relay đang treo produce không được chặn graceful shutdown, bản ghi chưa gửi vẫn `published_at IS NULL`. Test: `TestRelay_StopsPromptlyWhenBrokerDown` (Task 4, integration).

---

## File Structure

```
go.work                                              # + ./services/job
Makefile                                             # sqlc/test-int/cover-job/infra-up/up/migrate
.env.example                                         # + JOB_DB_PASSWORD
.github/workflows/ci-go.yml                          # job vào filter/matrix/docker, pkg integration
contracts/proto/smarthire/job/v1/job.proto
contracts/events/{user.registered,job.created,job.updated,job.closed,job.deleted}.v1.json
gen/go/smarthire/job/v1/*.go                         # sinh
contracts/openapi/gen/gateway.swagger.json, contracts/openapi/gateway.yaml   # sinh
pkg/internal/testinfra/testinfra.go                  # Postgres + Redpanda testcontainers, tạo topic (chỉ test)
pkg/kafka/record.go, producer.go, consumer.go, dlq.go, trace.go, errors.go
pkg/kafka/*_test.go                                  # unit + integration
pkg/outbox/writer.go, relay.go, *_test.go
pkg/idempotency/idempotency.go, idempotency_integration_test.go
pkg/event/eventschema/eventschema.go, eventschema_test.go
pkg/authz/retry.go, retry_test.go                    # NewVerifierWithRetry
services/auth/
  migrations/000002_outbox_headers.{up,down}.sql
  internal/app/refresh.go, refresh_test.go, fakes_test.go
  internal/adapter/postgres/outbox.go, tx.go          # uỷ quyền pkg/outbox.Writer
  internal/adapter/postgres/queries/outbox.sql        # xoá (sqlc sinh lại)
  internal/config/config.go, internal/bootstrap/bootstrap.go
  test/integration/postgres_test.go, outbox_test.go
services/job/
  go.mod, Dockerfile, README.md, sqlc.yaml
  migrations/000001_init.{up,down}.sql
  internal/config/config.go
  internal/domain/job.go, skills.go, errors.go, domain_test.go
  internal/app/ports.go, service.go, commands.go, queries.go, cursor.go, events.go, *_test.go
  internal/adapter/postgres/queries/jobs.sql, sqlc/ (sinh), tx.go, jobs.go, outbox.go
  internal/adapter/grpc/handler.go, mapping.go, handler_test.go
  internal/adapter/httpx/handler.go
  internal/bootstrap/bootstrap.go
  cmd/server/main.go
  test/integration/main_test.go, repo_test.go, search_test.go, events_test.go
services/gateway/
  internal/config/config.go, internal/router/router.go, router_test.go, ready.go
  cmd/gateway/main.go
  test/integration/ratelimit_test.go                  # ngưỡng 150ms
tools/cmd/smoke/main.go, jobs.go, kafka.go
deploy/docker/docker-compose.infra.yml, docker-compose.yml
deploy/docker/init/postgres/02-create-job-db.sql
deploy/docker/init/redpanda/create-topics.sh
docs/adr/0005-outbox-relay-headers.md, 0006-kafka-client-retry-dlq.md, docs/adr/README.md
README.md, CLAUDE.md, services/job/README.md
```

---

### Task 1: Auth Refresh R26 + ngưỡng hanging Redis

**Files:**
- Modify: `services/auth/internal/app/refresh.go`, `services/auth/internal/app/refresh_test.go`, `services/auth/internal/app/fakes_test.go`
- Modify: `services/auth/test/integration/postgres_test.go`
- Modify: `services/gateway/test/integration/ratelimit_test.go:93`

**Interfaces:**
- Consumes: `app.TxManager`, `app.RefreshTokenRepo` (không đổi).
- Produces: `const reuseRevokeTimeout = 5 * time.Second` (không export) trong `refresh.go`; chữ ký `(*Service).Refresh(ctx context.Context, raw string) (TokenPair, error)` không đổi.

- [ ] **Step 1: Sửa fake và viết test unit**

`fakes_test.go`:
- `fakeTx.WithTx`: sau khi `fn(ctx)` trả nil, nếu `ctx.Err() != nil` thì đặt `*lastCommitted = false` và trả `ctx.Err()` (mô phỏng `tx.Commit` thất bại trên ctx đã huỷ).
- `fakeTokens` thêm `afterGet func()` (gọi sau `GetByHashForUpdate` nếu khác nil) và `revokeFamilyCtx context.Context` (ctx lần gọi `RevokeFamily` gần nhất).

`refresh_test.go`: xoá `TestRefresh_ReuseRevokesFamilyWithCancelledContext`, thêm:

```go
func TestRefresh_ReuseRevokesFamilyWhenCancelledMidway(t *testing.T) {
	r := newRefreshEnv(t)
	p1 := r.login(t)
	p2, err := r.svc.Refresh(context.Background(), p1.RefreshToken)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r.tokens.afterGet = cancel // client đi mất ngay sau lookup
	_, err = r.svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	assert.True(t, *r.committed, "family revocation must commit on a detached ctx")
	nw, _ := r.tokens.byHash(p2.RefreshToken)
	assert.NotNil(t, nw.RevokedAt)
}

func TestRefresh_ReuseRevocationHasBoundedDetachedCtx(t *testing.T) {
	// như trên; sau Refresh:
	rc := r.tokens.revokeFamilyCtx
	require.NotNil(t, rc)
	assert.NoError(t, rc.Err())
	dl, ok := rc.Deadline()
	require.True(t, ok)
	assert.LessOrEqual(t, time.Until(dl), 5*time.Second)
}

func TestRefresh_CancelledRotationRollsBack(t *testing.T) {
	r := newRefreshEnv(t)
	p1 := r.login(t)
	ctx, cancel := context.WithCancel(context.Background())
	r.tokens.afterGet = cancel
	_, err := r.svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, *r.committed)
}
```

- [ ] **Step 2: Run** `cd services/auth && go test ./internal/app -run TestRefresh -v` — Expected: FAIL (`TestRefresh_ReuseRevocationHasBoundedDetachedCtx` không có deadline; `TestRefresh_CancelledRotationRollsBack` committed=true vì tx đang detached).

- [ ] **Step 3: Implement trong `refresh.go`**

```go
var errReuseDetected = errors.New("refresh token reuse")

func (s *Service) Refresh(ctx context.Context, raw string) (TokenPair, error) {
	var pair TokenPair
	var famID, userID uuid.UUID
	err := s.Tx.WithTx(ctx, func(ctx context.Context) error { // ctx của request
		t, err := s.Tokens.GetByHashForUpdate(ctx, hashRefresh(raw))
		// ... ErrNotFound → domain.ErrInvalidRefreshToken
		if t.RevokedAt != nil {
			famID, userID = t.FamilyID, t.UserID
			return errReuseDetected // rollback, chưa ghi gì
		}
		// ... expiry, user status, issuePair, Revoke (giữ nguyên logic M0)
	})
	if errors.Is(err, errReuseDetected) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reuseRevokeTimeout)
		defer cancel()
		if err := s.Tx.WithTx(rctx, func(ctx context.Context) error {
			return s.Tokens.RevokeFamily(ctx, famID, s.Now())
		}); err != nil {
			return TokenPair{}, fmt.Errorf("revoke family: %w", err)
		}
		s.Logger.WarnContext(ctx, "refresh token reuse detected", "event", "refresh_token_reuse",
			"user_id", userID, "family_id", famID)
		return TokenPair{}, domain.ErrInvalidRefreshToken
	}
	if err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}
```

Cập nhật doc comment: rotation theo ctx của request; chỉ việc thu hồi family chạy detached có giới hạn 5s.

- [ ] **Step 4: Run** `go test ./internal/app -v` — Expected: PASS toàn bộ (gồm `TestRefresh_ReuseRevokesFamily`, `TestRefresh_RevokedAndExpiredIsReuse`).

- [ ] **Step 5: Integration test**

Trong `postgres_test.go` thêm helper `newServiceWithTokens(t, tokens app.RefreshTokenRepo) *app.Service` và wrapper `cancelAfterCreate{inner app.RefreshTokenRepo; cancel func()}` (gọi `cancel()` sau `Create` thành công).

```go
func TestRefresh_CancelledMidRotationKeepsOldToken(t *testing.T) {
	truncate(t)
	cctx, cancel := context.WithCancel(context.Background())
	ctx := context.WithValue(cctx, cancelMarker{}, true)
	svc := newServiceWithTokens(t, cancelAfterCreate{inner: postgres.NewRefreshTokenRepo(pool), cancel: cancel})
	p1 := login(t, svc) // ctx nền không có marker → wrapper không cancel
	_, err := svc.Refresh(ctx, p1.RefreshToken)
	require.Error(t, err)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens`))           // RT mới bị rollback
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`))
	_, err = newService(t, postgres.NewOutboxWriter(pool)).Refresh(context.Background(), p1.RefreshToken)
	assert.NoError(t, err) // RT cũ vẫn hợp lệ
}
```

(Wrapper chỉ gọi `cancel` khi `ctx.Value(cancelMarker{}) != nil`.)

- [ ] **Step 6: Siết ngưỡng hanging Redis**

`ratelimit_test.go`: `assert.Less(t, time.Since(start), 150*time.Millisecond)` (limiter có `redisTimeout = 100ms`).

- [ ] **Step 7: Run**

`cd services/auth && go test -count=1 -tags integration ./test/integration -run 'TestRefresh' -v` và `cd services/gateway && go test -count=1 -tags integration ./test/integration -run TestLimiter_HangingRedisFailsFast -v` — Expected: PASS. Nếu ngưỡng 150ms flaky do `DialTimeout`, không nới ngưỡng: kiểm tra `ContextTimeoutEnabled` và `redisTimeout`.

- [ ] **Step 8: Commit**

```bash
git add services/auth/internal/app services/auth/test/integration services/gateway/test/integration/ratelimit_test.go
git commit -m "fix(auth): bind refresh rotation to request ctx and bound reuse revocation"
```

---

### Task 2: Hạ tầng Redpanda + topics + role `job_svc`

**Files:**
- Modify: `deploy/docker/docker-compose.infra.yml`, `.env.example`, `Makefile` (`infra-up`)
- Create: `deploy/docker/init/redpanda/create-topics.sh`, `deploy/docker/init/postgres/02-create-job-db.sql`

**Interfaces:**
- Produces: service compose `redpanda` (`redpanda:9092`, host `127.0.0.1:19092`), `redpanda-console` (`127.0.0.1:8081`), `topics-init` (one-shot, idempotent); DB `job_db` owner `job_svc`; biến `.env` `JOB_DB_PASSWORD`.

- [ ] **Step 1: Thêm service vào `docker-compose.infra.yml`**

```yaml
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    command:
      - redpanda
      - start
      - --mode=dev-container
      - --smp=1
      - --memory=1G
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda:9092,external://127.0.0.1:19092
    ports:
      - "127.0.0.1:19092:19092"   # D9: chỉ cho dev/smoke
    volumes: [redpanda-data:/var/lib/redpanda/data]
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true'"]
      interval: 2s
      timeout: 3s
      retries: 30
    restart: unless-stopped

  topics-init:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.7
    entrypoint: ["/bin/bash", "/init/create-topics.sh"]
    volumes: [./init/redpanda:/init:ro]
    depends_on: { redpanda: { condition: service_healthy } }
    restart: "no"

  redpanda-console:
    image: docker.redpanda.com/redpandadata/console:v2.8.0
    environment: { KAFKA_BROKERS: redpanda:9092 }
    ports: ["127.0.0.1:8081:8080"]
    depends_on: { redpanda: { condition: service_healthy } }
    restart: unless-stopped
```

Postgres thêm env `JOB_DB_PASSWORD: ${JOB_DB_PASSWORD:?set JOB_DB_PASSWORD in .env}`. Thêm volume `redpanda-data`.

- [ ] **Step 2: `create-topics.sh`** (bash, `set -euo pipefail`, `B=redpanda:9092`):
1. `rpk -X brokers=$B cluster config set auto_create_topics_enabled false`
2. Với mỗi `t` trong `user.registered job.created job.updated job.closed job.deleted` và `$t.dlq`: `rpk -X brokers=$B topic create "$t" -p 3 -r 1 || rpk -X brokers=$B topic describe "$t" >/dev/null` (chạy lại không lỗi khi topic đã tồn tại; lỗi khác vẫn fail vì describe cũng fail).
3. In `topics ready`.

- [ ] **Step 3: `02-create-job-db.sql`** — giống `01-create-dbs.sql`: `\getenv job_password JOB_DB_PASSWORD` + guard `\quit 1`, `CREATE ROLE job_svc LOGIN PASSWORD :'job_password'; CREATE DATABASE job_db OWNER job_svc; REVOKE ALL ON DATABASE job_db FROM PUBLIC;`. `.env.example` thêm `JOB_DB_PASSWORD=job-dev-only` (cùng ghi chú tránh ký tự URL).

- [ ] **Step 4: Makefile** — `infra-up`: `$(COMPOSE) up -d --wait postgres redis redpanda redpanda-console` rồi `$(COMPOSE) run --rm topics-init`.

- [ ] **Step 5: Kiểm tra trên volume sạch**

Run: `make down && MSYS_NO_PATHCONV=1 docker compose -p smarthire --project-directory deploy/docker --env-file .env -f deploy/docker/docker-compose.infra.yml down -v && make infra-up && make infra-up` (lần hai kiểm tính idempotent), rồi
`docker compose -p smarthire ... exec redpanda rpk topic list` và
`docker compose -p smarthire ... exec postgres psql -U postgres -tAc "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname='job_db'"` và
`docker compose -p smarthire ... exec redpanda rpk cluster config get auto_create_topics_enabled`.
Expected: 10 topic, mỗi topic `PARTITIONS 3`; `job_svc`; `false`; `curl -fsS http://127.0.0.1:8081 >/dev/null` thành công.

- [ ] **Step 6: Commit** `feat(deploy): add redpanda, console, topic init and job_svc role`

---

### Task 3: `pkg/kafka`

**Files:**
- Create: `pkg/kafka/{record.go,producer.go,consumer.go,dlq.go,trace.go,errors.go}`, `pkg/kafka/kafka_test.go` (unit), `pkg/kafka/kafka_integration_test.go`, `pkg/internal/testinfra/testinfra.go`
- Modify: `pkg/go.mod` (franz-go `kgo`, `kadm`; testcontainers + module `redpanda`, `postgres`)

**Interfaces:**
- Consumes: `event.Envelope`.
- Produces:
  - `type Record struct{ Topic string; Key, Value []byte; Headers map[string]string }`
  - `func NewProducer(brokers []string, opts ...kgo.Opt) (*Producer, error)` — `kgo.SeedBrokers`, `kgo.RequiredAcks(kgo.AllISRAcks())` (idempotent mặc định của franz-go giữ nguyên), `kgo.RecordDeliveryTimeout(10*time.Second)`; `opts` áp sau.
  - `func (p *Producer) Produce(ctx context.Context, r Record) error` — span `"<topic> publish"` (kind Producer, attr `messaging.system=kafka`, `messaging.destination.name`), inject `traceparent`/`tracestate` nếu `r.Headers` chưa có `traceparent`; `ProduceSync(...).FirstErr()`.
  - `func (p *Producer) Close()`
  - `type Handler func(ctx context.Context, env event.Envelope, rec *kgo.Record) error`
  - `type ConsumerConfig struct{ Brokers []string; Group string; Topics []string; Handler Handler; MaxAttempts int; BaseBackoff, MaxBackoff time.Duration; Logger *slog.Logger }` — zero value → `3`, `200ms`, `2s`, `slog.Default()`
  - `func NewConsumer(cfg ConsumerConfig) (*Consumer, error)`; `func (c *Consumer) Run(ctx context.Context) error` — chặn tới khi ctx huỷ (trả nil) hoặc ghi DLQ lỗi (trả lỗi); `func (c *Consumer) Close()`
  - `func Permanent(err error) error`; `func IsPermanent(err error) bool`
  - `func DLQTopic(topic string) string` — `topic + ".dlq"`
  - `func backoff(attempt int, base, max time.Duration) time.Duration` (không export) — `min(base<<(attempt-1), max)`, `attempt` bắt đầu từ 1
  - Test helper `testinfra`: `func Redpanda(ctx context.Context) (broker string, terminate func(), err error)` (image pin như compose; bảo đảm `auto_create_topics_enabled=false` — kiểm bằng `kadm` DescribeClusterConfigs, nếu module bật thì tắt qua `AlterClusterConfigs`), `func CreateTopics(ctx context.Context, broker string, topics ...string) error` (3 partition, rf 1, qua `kadm`), `func Postgres(ctx context.Context) (dsn string, terminate func(), err error)` (`postgres:16-alpine`, chờ log ready 2 lần như Auth).

- [ ] **Step 1: Viết test unit** (`kafka_test.go`, không Docker)

```go
func TestBackoff(t *testing.T) {
	b, m := 200*time.Millisecond, 2*time.Second
	assert.Equal(t, 200*time.Millisecond, backoff(1, b, m))
	assert.Equal(t, 400*time.Millisecond, backoff(2, b, m))
	assert.Equal(t, 1600*time.Millisecond, backoff(4, b, m))
	assert.Equal(t, 2*time.Second, backoff(5, b, m))
	assert.Equal(t, 2*time.Second, backoff(40, b, m)) // không tràn số
}
func TestPermanent(t *testing.T) {
	e := Permanent(errors.New("bad"))
	assert.True(t, IsPermanent(fmt.Errorf("wrap: %w", e)))
	assert.False(t, IsPermanent(errors.New("x")))
	assert.Equal(t, "bad", e.Error())
}
func TestDLQHeaders_TruncatesError(t *testing.T) {
	h := dlqHeaders(&kgo.Record{Topic: "job.created", Partition: 2, Offset: 7}, errors.New(strings.Repeat("é", 600)), 3)
	assert.Equal(t, 512, utf8.RuneCountInString(h["dlq-error"]))
	assert.Equal(t, "3", h["dlq-attempts"]); assert.Equal(t, "job.created", h["dlq-original-topic"])
	assert.Equal(t, "2", h["dlq-original-partition"]); assert.Equal(t, "7", h["dlq-original-offset"])
}
```

`dlqHeaders(rec *kgo.Record, err error, attempts int) map[string]string` (không export, `dlq.go`) — cắt theo rune.

- [ ] **Step 2: Viết test integration** (`//go:build integration`; `TestMain` gọi `testinfra.Redpanda`; mỗi test tạo topic tên ngẫu nhiên `t-<ulid>` + `.dlq` và group ngẫu nhiên; consumer dùng `BaseBackoff: 10*time.Millisecond`)

```go
func TestRoundTrip(t *testing.T)               { /* Produce envelope → handler nhận đúng env.EventID, rec.Key */ }
func TestTracePropagates(t *testing.T) {
	// ctx có span từ sdktrace.NewTracerProvider (đặt làm global + propagator TraceContext)
	// handler: trace.SpanContextFromContext(ctx).TraceID() == traceID của span gốc
}
func TestTransientErrorRetriedThenSucceeds(t *testing.T) { /* handler lỗi 2 lần rồi nil → gọi đúng 3 lần, DLQ rỗng, offset đã commit */ }
func TestPermanentErrorGoesToDLQ(t *testing.T) {
	// handler trả Permanent(errors.New("nope")) → gọi 1 lần;
	// đọc <topic>.dlq: key/value giữ nguyên, header dlq-error="nope", dlq-attempts="1",
	// dlq-original-topic, -partition, -offset khớp record gốc
}
func TestExhaustedRetriesGoToDLQ(t *testing.T) { /* lỗi thường mãi → gọi 3 lần, dlq-attempts="3" */ }
func TestUndecodableGoesToDLQ(t *testing.T)    { /* value "not json" và "{}" (thiếu event_id) → DLQ, handler không được gọi */ }
func TestOrderPerKey(t *testing.T)             { /* 50 record cùng key, payload seq 0..49 → handler thấy tăng dần */ }
func TestCommittedOffsetsSurviveRestart(t *testing.T) { /* consumer 1 xử lý 5 record, Close; consumer 2 cùng group không nhận lại */ }
func TestDLQWriteFailureStopsRun(t *testing.T) { /* topic không có .dlq (auto-create tắt) + Permanent → Run trả lỗi, offset không commit */ }
```

- [ ] **Step 3: Run** `cd pkg && go test ./kafka/...` (FAIL: chưa có package) và `go test -tags integration ./kafka/...` — Expected: FAIL.

- [ ] **Step 4: Implement**
- `trace.go`: `headerCarrier` (adapter `propagation.TextMapCarrier` trên `[]kgo.RecordHeader`); dùng `otel.GetTextMapPropagator()` và tracer `otel.Tracer("github.com/nuroqzavitex/smarthire/pkg/kafka")`.
- `consumer.go`: client `kgo.ConsumerGroup`, `kgo.ConsumeTopics`, `kgo.DisableAutoCommit()`, `kgo.RecordDeliveryTimeout(10*time.Second)` (cho produce DLQ), `kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())`. Vòng lặp: `PollFetches(ctx)` → với từng record theo thứ tự: extract ctx, span `"<topic> process"` (kind Consumer); `json.Unmarshal` envelope, thiếu `EventID` hoặc `EventType` coi là decode lỗi → DLQ (attempts 0); ngược lại thử tối đa `MaxAttempts`, `IsPermanent` → DLQ ngay; giữa các lần `select` trên `ctx.Done()` và `time.After(backoff(...))` (ctx huỷ → `return nil` không commit). Sau thành công/DLQ: `CommitRecords(ctx, rec)`. DLQ produce dùng chính client (`ProduceSync`), key/value giữ nguyên, header gốc giữ nguyên + header `dlq-*`; lỗi → `return fmt.Errorf("dlq %s: %w", ...)`.
- Log lỗi handler kèm `topic`, `partition`, `offset`, `event_id`, `attempt` (không log value).

- [ ] **Step 5: Run** cả hai lệnh ở Step 3 — Expected: PASS.

- [ ] **Step 6: Commit** `feat(pkg): add kafka producer, consumer with retry and DLQ`

---

### Task 4: `pkg/outbox` (Writer + Relay)

**Files:**
- Create: `pkg/outbox/{writer.go,relay.go,outbox_test.go,outbox_integration_test.go}`

**Interfaces:**
- Consumes: `kafka.Record`, `event.Envelope`, `testinfra.Postgres`, `testinfra.Redpanda`, `testinfra.CreateTopics`.
- Produces:
  - `type Querier interface{ Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) }` (`pgx.Tx`, `*pgxpool.Pool` đều thoả)
  - `type Writer struct{}`; `func (Writer) Write(ctx context.Context, q Querier, env event.Envelope, aggregateID string) error` — `INSERT INTO outbox (id, aggregate_id, event_type, payload, headers, created_at) VALUES ($1..$6)`; `id=uuid.New()`, `payload=json.Marshal(env)`, `headers` = JSON của `propagation.MapCarrier` inject từ ctx (`{}` khi không có span), `created_at=env.OccurredAt`
  - `type Publisher interface{ Produce(ctx context.Context, r kafka.Record) error }` (`*kafka.Producer` thoả)
  - `type RelayOptions struct{ BatchSize int; Interval time.Duration; Logger *slog.Logger }` — zero → `100`, `500ms`, `slog.Default()`
  - `func NewRelay(pool *pgxpool.Pool, pub Publisher, opts RelayOptions) *Relay`
  - `func (r *Relay) Run(ctx context.Context) error` — trả nil khi ctx huỷ; lỗi tick chỉ log (`WarnContext`, kèm số bản ghi, không kèm payload)
  - `func (r *Relay) tick(ctx context.Context) (selected int, err error)` (không export, test được trong package)
  - `const SchemaSQL` (export, để test và tài liệu dùng chung): DDL bảng `outbox` chuẩn §3.3 + index `outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL`

- [ ] **Step 1: Test unit** (`outbox_test.go`)

```go
func TestRecordFromRow_StripsTraceHeaders(t *testing.T) {
	ctx, rec := recordFromRow(context.Background(), row{AggregateID: "j1", EventType: "job.created",
		Payload: []byte(`{}`), Headers: map[string]string{"traceparent": tp, "baggage": "k=v"}})
	assert.Equal(t, "job.created", rec.Topic); assert.Equal(t, []byte("j1"), rec.Key)
	assert.NotContains(t, rec.Headers, "traceparent"); assert.Equal(t, "k=v", rec.Headers["baggage"])
	assert.Equal(t, tpTraceID, trace.SpanContextFromContext(ctx).TraceID().String())
}
```

`type row struct{ ID uuid.UUID; AggregateID, EventType string; Payload []byte; Headers map[string]string }`; `func recordFromRow(ctx context.Context, r row) (context.Context, kafka.Record)` — extract carrier vào ctx, bỏ `traceparent`/`tracestate` (quyết định #2).

- [ ] **Step 2: Test integration** (`TestMain` khởi Postgres + Redpanda, tạo bảng bằng `SchemaSQL`, mỗi test `TRUNCATE outbox` và dùng topic ngẫu nhiên làm `event_type`)

```go
func TestWriter_RolledBackTxNotPublished(t *testing.T) { /* Write trong tx rồi Rollback; chạy tick → 0 record trên topic */ }
func TestWriter_StoresTraceHeaders(t *testing.T)       { /* ctx có span → headers->>'traceparent' chứa traceID */ }
func TestRelay_PublishesAndMarks(t *testing.T) {
	// 5 bản ghi → tick → topic có 5 record, key = aggregate_id, value = payload;
	// SELECT count(*) WHERE published_at IS NULL = 0
}
func TestRelay_ConcurrentRelaysNoDuplicates(t *testing.T) {
	// 300 bản ghi; 2 Relay chạy Run song song tới khi hết pending;
	// đọc toàn bộ topic, đếm theo event_id: mỗi id đúng 1 lần, tổng 300
}
func TestRelay_MidBatchFailureRetriesRest(t *testing.T) {
	// Publisher giả bọc Producer thật, lỗi ở record thứ 3 của lần đầu
	// → sau tick 1: 2 bản ghi published, 3 còn pending; tick 2: hết pending; topic có đúng 5 record
}
func TestRelay_FullBatchContinuesImmediately(t *testing.T) { /* BatchSize 10, Interval 1h, 25 bản ghi → hết pending trong < 5s */ }
func TestRelay_PreservesTraceAcrossKafka(t *testing.T) {
	// Write với ctx có span S; relay publish; đọc record: header traceparent có traceID của S
	// và span ID khác S (là producer span con)
}
func TestRelay_StopsPromptlyWhenBrokerDown(t *testing.T) {
	// Producer trỏ tới 127.0.0.1:1 (không có broker); 1 bản ghi; Run trong goroutine;
	// cancel sau 200ms → Run trả nil trong < 2s; bản ghi vẫn published_at IS NULL
}
```

- [ ] **Step 3: Run** `cd pkg && go test ./outbox/... && go test -tags integration ./outbox/...` — Expected: FAIL.

- [ ] **Step 4: Implement `tick`** (thuật toán spec chưa định đủ chi tiết):

```go
func (r *Relay) tick(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil { return 0, fmt.Errorf("begin: %w", err) }
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }() // no-op sau Commit
	rows := query(tx, `SELECT id, aggregate_id, event_type, payload, headers FROM outbox
		WHERE published_at IS NULL ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.opts.BatchSize)
	var sent []uuid.UUID
	var pubErr error
	for _, rw := range rows {
		pctx, rec := recordFromRow(ctx, rw)
		if pubErr = r.pub.Produce(pctx, rec); pubErr != nil { break }
		sent = append(sent, rw.ID)
	}
	if len(sent) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, sent); err != nil {
			return len(rows), errors.Join(pubErr, err)
		}
		if err := tx.Commit(ctx); err != nil { return len(rows), errors.Join(pubErr, err) }
	}
	return len(rows), pubErr
}
```

`Run`: lặp `n, err := r.tick(ctx)`; ctx huỷ → `return nil`; `err == nil && n == BatchSize` → tick tiếp ngay; ngược lại `select` `ctx.Done()` / `time.After(Interval)`.

- [ ] **Step 5: Run** lại — Expected: PASS.

- [ ] **Step 6: Commit** `feat(pkg): add transactional outbox writer and relay`

---

### Task 5: `pkg/idempotency`

**Files:**
- Create: `pkg/idempotency/idempotency.go`, `pkg/idempotency/idempotency_integration_test.go`

**Interfaces:**
- Produces:
  - `type Querier interface{ Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) }`
  - `func Once(ctx context.Context, q Querier, eventID string) (first bool, err error)` — `INSERT INTO processed_events (event_id, processed_at) VALUES ($1, now()) ON CONFLICT (event_id) DO NOTHING`; `first = tag.RowsAffected() == 1`; `eventID == ""` → lỗi
  - `const SchemaSQL` — `CREATE TABLE processed_events (event_id text PRIMARY KEY, processed_at timestamptz NOT NULL DEFAULT now())`

- [ ] **Step 1: Test** (`TestMain` dùng `testinfra.Postgres`, tạo bảng bằng `SchemaSQL`)

```go
func TestOnce_FirstThenDuplicate(t *testing.T) {
	first, err := idempotency.Once(ctx, pool, "e1"); require.NoError(t, err); assert.True(t, first)
	first, err = idempotency.Once(ctx, pool, "e1"); require.NoError(t, err); assert.False(t, first)
}
func TestOnce_RollbackForgets(t *testing.T) {
	tx, _ := pool.Begin(ctx)
	first, _ := idempotency.Once(ctx, tx, "e2"); assert.True(t, first)
	require.NoError(t, tx.Rollback(ctx))
	first, _ = idempotency.Once(ctx, pool, "e2"); assert.True(t, first)
}
func TestOnce_EmptyID(t *testing.T) { _, err := idempotency.Once(ctx, pool, ""); assert.Error(t, err) }
```

- [ ] **Step 2: Run** `cd pkg && go test -tags integration ./idempotency/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(pkg): add idempotency helper for event consumers`

---

### Task 6: Auth — migration `000002`, Writer uỷ quyền, relay

**Files:**
- Create: `services/auth/migrations/000002_outbox_headers.{up,down}.sql`, `services/auth/test/integration/outbox_test.go`
- Modify: `services/auth/internal/adapter/postgres/{outbox.go,tx.go}`, `services/auth/internal/config/{config.go,config_test.go}`, `services/auth/internal/bootstrap/bootstrap.go`, `services/auth/go.mod`
- Delete: `services/auth/internal/adapter/postgres/queries/outbox.sql` (sqlc sinh lại, `sqlc/outbox.sql.go` biến mất, `models.go` có `Headers []byte`)

**Interfaces:**
- Consumes: `outbox.Writer`, `outbox.NewRelay`, `outbox.RelayOptions`, `kafka.NewProducer`.
- Produces:
  - `func dbtx(ctx context.Context, pool *pgxpool.Pool) outbox.Querier` (không export, `tx.go`) — tx trong ctx nếu có, ngược lại pool
  - `postgres.NewOutboxWriter(pool)` giữ nguyên chữ ký; `Write` gọi `outbox.Writer{}.Write(ctx, dbtx(ctx, w.pool), e, aggregateID)`
  - Config thêm `KafkaBrokers []string \`env:"KAFKA_BROKERS" envDefault:"redpanda:9092" envSeparator:","\``, `OutboxInterval time.Duration \`env:"OUTBOX_INTERVAL" envDefault:"500ms"\`` (vào `Config`, không vào `Core` — seed không chạy relay)

- [ ] **Step 1: Migration** — up: `ALTER TABLE outbox ADD COLUMN headers jsonb NOT NULL DEFAULT '{}';` down: `ALTER TABLE outbox DROP COLUMN headers;`

- [ ] **Step 2: Test**

`config_test.go`: `TestLoad_KafkaDefaults` — chỉ đặt biến bắt buộc → `KafkaBrokers == []string{"redpanda:9092"}`, `OutboxInterval == 500*time.Millisecond`; `AUTH_KAFKA_BROKERS=a:1,b:2` → 2 phần tử.

`outbox_test.go` (integration):
```go
func TestRegister_OutboxHasHeaders(t *testing.T) {
	// ctx có span (sdktrace + propagator TraceContext); Register
	// → SELECT headers->>'traceparent' FROM outbox chứa traceID của span
}
func TestRegister_EventMatchesSchema(t *testing.T) {
	// SELECT payload FROM outbox → eventschema.Validate("user.registered", payload) == nil
	// (eventschema có ở Task 7; nếu Task 7 chưa xong, tạm t.Skip và bật lại ở Task 7 Step 6)
}
```
`TestMigrations_UpDown` sẵn có tự phủ `000002`.

- [ ] **Step 3: Run** `cd services/auth && go test ./internal/config/... && go test -count=1 -tags integration ./test/integration -run 'Outbox|Migrations' -v` — Expected: FAIL.

- [ ] **Step 4: Implement**
- Xoá `queries/outbox.sql`, chạy `make sqlc` (Makefile ở bước này vẫn chỉ auth).
- `outbox.go` uỷ quyền như Interfaces; `go.mod` không cần thêm gì ngoài `pkg` (đã có replace) — chạy `go mod tidy`.
- `bootstrap.Run`: sau khi tạo pool, `producer, err := kafka.NewProducer(cfg.KafkaBrokers)` (defer `Close`); `relay := outbox.NewRelay(pool, producer, outbox.RelayOptions{Interval: cfg.OutboxInterval, Logger: log})`; chạy `go func() { relayDone <- relay.Run(runCtx) }()` cùng `runCtx` của gRPC; khi shutdown: `cancel()` rồi chờ `relayDone` **trước** khi `pool.Close()` (defer pool đã đăng ký trước nên thứ tự đúng nếu chờ relay ngay sau `cancel()`).

- [ ] **Step 5: Run** lại Step 3 và `go test ./...` — Expected: PASS.

- [ ] **Step 6: Commit** `feat(auth): publish outbox events to kafka via shared relay`

---

### Task 7: Contracts — job proto, JSON Schema event, codegen

**Files:**
- Create: `contracts/proto/smarthire/job/v1/job.proto`, `contracts/events/{user.registered,job.created,job.updated,job.closed,job.deleted}.v1.json`, `pkg/event/eventschema/{eventschema.go,eventschema_test.go}`
- Generated: `gen/go/smarthire/job/v1/{job.pb.go,job_grpc.pb.go,job.pb.gw.go}`, `contracts/openapi/gen/gateway.swagger.json`, `contracts/openapi/gateway.yaml`
- Modify: `pkg/go.mod` (`github.com/santhosh-tekuri/jsonschema/v6`)

**Interfaces:**
- Produces: package `jobv1` (`github.com/nuroqzavitex/smarthire/gen/go/smarthire/job/v1`): `JobServiceServer`, `JobServiceClient`, `RegisterJobServiceServer`, `RegisterJobServiceHandlerClient`, hằng `JobService_<Method>_FullMethodName`, message bên dưới.
- Produces (`eventschema`): `func Dir() (string, error)` (đi lên từ cwd tới thư mục chứa `contracts/events`); `func Validate(eventType string, doc []byte) error` (nạp `<Dir>/<eventType>.v1.json`, draft 2020-12, cache compiler).

- [ ] **Step 1: Viết `job.proto`**

```protobuf
syntax = "proto3";
package smarthire.job.v1;

import "buf/validate/validate.proto";
import "google/api/annotations.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

service JobService {
  rpc CreateJob(CreateJobRequest) returns (Job) { option (google.api.http) = { post: "/v1/jobs" body: "*" }; }
  rpc UpdateJob(UpdateJobRequest) returns (Job) { option (google.api.http) = { patch: "/v1/jobs/{id}" body: "*" }; }
  rpc PublishJob(PublishJobRequest) returns (Job) { option (google.api.http) = { post: "/v1/jobs/{id}:publish" body: "*" }; }
  rpc CloseJob(CloseJobRequest) returns (Job) { option (google.api.http) = { post: "/v1/jobs/{id}:close" body: "*" }; }
  rpc DeleteJob(DeleteJobRequest) returns (google.protobuf.Empty) { option (google.api.http) = { delete: "/v1/jobs/{id}" }; }
  rpc GetJob(GetJobRequest) returns (Job) { option (google.api.http) = { get: "/v1/jobs/{id}" }; }
  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse) { option (google.api.http) = { get: "/v1/jobs" }; }
  rpc ListMyJobs(ListMyJobsRequest) returns (ListJobsResponse) { option (google.api.http) = { get: "/v1/me/jobs" }; }
}

enum JobLevel { JOB_LEVEL_UNSPECIFIED = 0; JOB_LEVEL_INTERN = 1; JOB_LEVEL_JUNIOR = 2; JOB_LEVEL_MIDDLE = 3; JOB_LEVEL_SENIOR = 4; JOB_LEVEL_LEAD = 5; }
enum JobStatus { JOB_STATUS_UNSPECIFIED = 0; JOB_STATUS_DRAFT = 1; JOB_STATUS_OPEN = 2; JOB_STATUS_CLOSED = 3; }

message StringList { repeated string values = 1 [(buf.validate.field).repeated = {max_items: 30, items: {string: {max_len: 50}}}]; }

message Job {
  string id = 1; string recruiter_id = 2; string title = 3; string description = 4;
  repeated string required_skills = 5; repeated string nice_to_have_skills = 6;
  JobLevel level = 7; string location = 8; optional int32 salary_min = 9; optional int32 salary_max = 10;
  JobStatus status = 11; int64 version = 12;
  google.protobuf.Timestamp created_at = 13; google.protobuf.Timestamp updated_at = 14; google.protobuf.Timestamp published_at = 15;
}

message CreateJobRequest {
  string title = 1 [(buf.validate.field).string = {min_len: 5, max_len: 200}];
  string description = 2 [(buf.validate.field).string = {min_len: 20, max_len: 20000}];
  repeated string required_skills = 3 [(buf.validate.field).repeated = {max_items: 30, items: {string: {max_len: 50}}}];
  repeated string nice_to_have_skills = 4 [(buf.validate.field).repeated = {max_items: 30, items: {string: {max_len: 50}}}];
  JobLevel level = 5 [(buf.validate.field).enum = {defined_only: true, not_in: [0]}];
  string location = 6 [(buf.validate.field).string = {min_len: 1, max_len: 100}];
  optional int32 salary_min = 7 [(buf.validate.field).int32.gte = 0];
  optional int32 salary_max = 8 [(buf.validate.field).int32.gte = 0];
}
message UpdateJobRequest {
  string id = 1 [(buf.validate.field).string.uuid = true];
  optional string title = 2 [(buf.validate.field).string = {min_len: 5, max_len: 200}];
  optional string description = 3 [(buf.validate.field).string = {min_len: 20, max_len: 20000}];
  StringList required_skills = 4; StringList nice_to_have_skills = 5;
  optional JobLevel level = 6 [(buf.validate.field).enum = {defined_only: true, not_in: [0]}];
  optional string location = 7 [(buf.validate.field).string = {min_len: 1, max_len: 100}];
  optional int32 salary_min = 8 [(buf.validate.field).int32.gte = 0];
  optional int32 salary_max = 9 [(buf.validate.field).int32.gte = 0];
}
message PublishJobRequest { string id = 1 [(buf.validate.field).string.uuid = true]; }
message CloseJobRequest   { string id = 1 [(buf.validate.field).string.uuid = true]; }
message DeleteJobRequest  { string id = 1 [(buf.validate.field).string.uuid = true]; }
message GetJobRequest     { string id = 1 [(buf.validate.field).string.uuid = true]; }
message ListJobsRequest {
  string q = 1 [(buf.validate.field).string.max_len = 200];
  repeated string skills = 2 [(buf.validate.field).repeated = {max_items: 30, items: {string: {max_len: 50}}}];
  string location = 3 [(buf.validate.field).string.max_len = 100];
  JobLevel level = 4 [(buf.validate.field).enum.defined_only = true];   // 0 = không lọc
  optional int32 salary_min = 5 [(buf.validate.field).int32.gte = 0];
  int32 page_size = 6 [(buf.validate.field).int32 = {gte: 0, lte: 100}]; // 0 = mặc định 20
  string page_token = 7 [(buf.validate.field).string.max_len = 512];
}
message ListMyJobsRequest {
  JobStatus status = 1 [(buf.validate.field).enum.defined_only = true]; // 0 = mọi trạng thái
  int32 page_size = 2 [(buf.validate.field).int32 = {gte: 0, lte: 100}];
  string page_token = 3 [(buf.validate.field).string.max_len = 512];
}
message ListJobsResponse { repeated Job jobs = 1; string next_page_token = 2; }
```

- [ ] **Step 2: Sinh code** — `make proto && (cd contracts/proto && buf lint) && go build ./gen/go/...` — Expected: không lỗi, có `gen/go/smarthire/job/v1/job.pb.gw.go`, swagger có path `/v1/jobs/{id}:publish` và `/v1/me/jobs`.

- [ ] **Step 3: JSON Schema** — mỗi file draft 2020-12, `$id` `https://smarthire.local/events/<type>.v1.json`, mô tả envelope: required `event_id` (string, pattern ULID `^[0-9A-HJKMNP-TV-Z]{26}$`), `event_type` (`const`), `event_version` (`const 1`), `source` (`const "auth-service"` / `"job-service"`), `occurred_at` (`format: date-time`), `trace_id`, `correlation_id` (string), `payload`. Payload (`additionalProperties: false`, mọi field required):
- `user.registered`: `user_id` (uuid), `email` (string), `role` (`enum: candidate, recruiter, admin`)
- `job.created`, `job.updated`: `job_id`, `recruiter_id` (uuid), `title`, `description`, `location` (string), `required_skills`, `nice_to_have_skills` (array string), `level` (`enum` 5 giá trị), `salary_min`, `salary_max` (`["integer","null"]`, min 0), `status` (`enum: draft, open, closed`), `version` (integer ≥ 1)
- `job.closed`, `job.deleted`: `job_id` (uuid), `version` (integer ≥ 1)

Bật `AssertFormat` khi compile để `uuid`/`date-time` được kiểm.

- [ ] **Step 4: Test `eventschema`**

```go
func TestValidate_Samples(t *testing.T) {
	for _, typ := range []string{"user.registered", "job.created", "job.updated", "job.closed", "job.deleted"} {
		require.NoError(t, eventschema.Validate(typ, validSample(t, typ)), typ) // sample dựng bằng event.New
	}
}
func TestValidate_Rejects(t *testing.T) {
	assert.Error(t, eventschema.Validate("job.closed", sampleWith(t, "job.closed", `{"job_id":"x","version":1}`)))      // uuid sai
	assert.Error(t, eventschema.Validate("job.closed", sampleWith(t, "job.closed", `{"job_id":"<uuid>","version":1,"extra":1}`)))
	assert.Error(t, eventschema.Validate("job.created", sampleWithType(t, "job.updated")))                            // event_type sai
	assert.Error(t, eventschema.Validate("nope", []byte(`{}`)))                                                       // không có schema
}
```

- [ ] **Step 5: Run** `cd pkg && go test ./event/...` — FAIL trước khi implement, PASS sau.
- [ ] **Step 6:** Bỏ `t.Skip` trong `TestRegister_EventMatchesSchema` (Task 6) nếu đã đặt; chạy `cd services/auth && go test -count=1 -tags integration ./test/integration -run EventMatchesSchema` — Expected: PASS.
- [ ] **Step 7: Commit** `feat(contracts): add job proto, event JSON schemas and codegen`

---

### Task 8: Job domain

**Files:**
- Create: `services/job/go.mod` (replace `pkg`, `gen/go` như Auth), `services/job/internal/domain/{job.go,skills.go,errors.go,domain_test.go}`
- Modify: `go.work` (thêm `./services/job`)

**Interfaces:**
- Produces (`domain`, không import proto/DB):
  - `type Status string`; `StatusDraft = "draft"`, `StatusOpen = "open"`, `StatusClosed = "closed"`; `func ParseStatus(s string) (Status, error)`
  - `type Level string`; `LevelIntern = "intern"`, `LevelJunior = "junior"`, `LevelMiddle = "middle"`, `LevelSenior = "senior"`, `LevelLead = "lead"`; `func ParseLevel(s string) (Level, error)`
  - `type Job struct{ ID, RecruiterID uuid.UUID; Title, Description string; RequiredSkills, NiceToHaveSkills []string; Level Level; Location string; SalaryMin, SalaryMax *int; Status Status; Version int64; CreatedAt, UpdatedAt time.Time; PublishedAt, DeletedAt *time.Time }`
  - `type Draft struct{ Title, Description string; RequiredSkills, NiceToHaveSkills []string; Level Level; Location string; SalaryMin, SalaryMax *int }`
  - `type Patch struct{ Title, Description *string; RequiredSkills, NiceToHaveSkills *[]string; Level *Level; Location *string; SalaryMin, SalaryMax *int }`; `func (p Patch) Empty() bool`
  - `func NewJob(id, recruiterID uuid.UUID, d Draft, now time.Time) (Job, error)` — chuẩn hoá skill, validate, `Status=draft`, `Version=1`, `CreatedAt=UpdatedAt=now`
  - `func (j *Job) Apply(p Patch, now time.Time) error` — `closed` → `ErrInvalidTransition`; trộn rồi validate toàn bộ; thành công thì `Version++`, `UpdatedAt=now`; lỗi thì `j` không đổi
  - `func (j *Job) Publish(now time.Time) error` — chỉ `draft`; `Status=open`, `PublishedAt=&now`
  - `func (j *Job) Close(now time.Time) error` — chỉ `open`
  - `func (j *Job) Delete(now time.Time) error` — chỉ `draft|closed`; `DeletedAt=&now`
  - Mọi phương thức thành công tăng `Version` đúng 1 và đặt `UpdatedAt=now`
  - `func (j Job) OwnedBy(subject string) bool` — so `RecruiterID.String()`
  - `func NormalizeSkills(in []string) ([]string, error)` — `strings.Fields` + join 1 space, bỏ rỗng, bỏ trùng theo `strings.ToLower` (giữ bản đầu tiên), >30 hoặc skill >50 rune → `ErrValidation`; trả slice không nil
  - `var ErrInvalidTransition = apperr.New(apperr.ErrValidation, "invalid job status transition")`, `ErrJobNotFound = apperr.New(apperr.ErrNotFound, "job not found")`, `ErrJobClosed = apperr.New(apperr.ErrValidation, "closed job cannot be edited")`

- [ ] **Step 1: Viết test** (`domain_test.go`)

```go
func TestNormalizeSkills(t *testing.T) {
	got, err := domain.NormalizeSkills([]string{"  Go ", "go", "Postgre   SQL", "", "GO", "postgre sql"})
	require.NoError(t, err)
	assert.Equal(t, []string{"Go", "Postgre SQL"}, got)
	_, err = domain.NormalizeSkills(make31Distinct()); assert.ErrorIs(t, err, apperr.ErrValidation)
	got, _ = domain.NormalizeSkills(nil); assert.NotNil(t, got); assert.Empty(t, got)
}
func TestNewJob_Validation(t *testing.T) {
	cases := map[string]func(*domain.Draft){
		"title short": func(d *domain.Draft) { d.Title = "Abcd" },
		"title long":  func(d *domain.Draft) { d.Title = strings.Repeat("á", 201) },
		"desc short":  func(d *domain.Draft) { d.Description = strings.Repeat("x", 19) },
		"desc long":   func(d *domain.Draft) { d.Description = strings.Repeat("x", 20001) },
		"no location": func(d *domain.Draft) { d.Location = " " },
		"salary neg":  func(d *domain.Draft) { d.SalaryMin = ptr(-1) },
		"salary inv":  func(d *domain.Draft) { d.SalaryMin, d.SalaryMax = ptr(10), ptr(5) },
		"bad level":   func(d *domain.Draft) { d.Level = "guru" },
	}
	for name, mod := range cases {
		d := validDraft(); mod(&d)
		_, err := domain.NewJob(uuid.New(), uuid.New(), d, now)
		assert.ErrorIs(t, err, apperr.ErrValidation, name)
	}
	j, err := domain.NewJob(uuid.New(), uuid.New(), validDraft(), now)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDraft, j.Status); assert.EqualValues(t, 1, j.Version)
	d := validDraft(); d.Title = strings.Repeat("á", 200); _, err = domain.NewJob(uuid.New(), uuid.New(), d, now)
	assert.NoError(t, err) // biên trên tính theo rune
}
func TestTransitions(t *testing.T) {
	// bảng: (from, action) → ok?
	// draft: Publish ok, Close lỗi, Delete ok, Apply ok
	// open:  Publish lỗi, Close ok, Delete lỗi, Apply ok
	// closed: Publish lỗi, Close lỗi, Delete ok, Apply → ErrJobClosed
	// mỗi lần ok: Version tăng đúng 1, UpdatedAt = now; lỗi: Job không đổi (assert.Equal trước/sau)
}
func TestPublish_SetsPublishedAt(t *testing.T) { /* PublishedAt == now */ }
func TestApply_ValidatesMerged(t *testing.T)   { /* Patch SalaryMax=1 khi SalaryMin=10 sẵn có → ErrValidation, Version giữ nguyên */ }
func TestPatchEmpty(t *testing.T)              { /* Patch{}.Empty()==true; Patch{Title:&s}.Empty()==false */ }
func TestOwnedBy(t *testing.T)                 { /* đúng recruiter → true; uuid khác / "" → false */ }
```

- [ ] **Step 2: Run** `cd services/job && go test ./internal/domain/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces; đếm độ dài bằng `utf8.RuneCountInString` sau `strings.TrimSpace` (title, location được trim khi lưu; description giữ nguyên nhưng kiểm độ dài sau trim).
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(job): add job domain with state machine and validation`

---

### Task 9: Job app — use case

**Files:**
- Create: `services/job/internal/app/{ports.go,service.go,commands.go,queries.go,cursor.go,events.go}`, `services/job/internal/app/{fakes_test.go,commands_test.go,queries_test.go,cursor_test.go}`

**Interfaces:**
- Consumes: domain (Task 8), `authz.ClaimsFrom`, `authz.RequireRole`, `event.New`.
- Produces (`ports.go`):
  ```go
  type TxManager interface{ WithTx(ctx context.Context, fn func(ctx context.Context) error) error }
  type OutboxWriter interface{ Write(ctx context.Context, e event.Envelope, aggregateID string) error }
  type Cursor struct{ Rank *float32 `json:"r,omitempty"`; At time.Time `json:"p"`; ID uuid.UUID `json:"id"` }
  type SearchFilter struct{ Query string; Skills []string; Location string; Level *domain.Level; SalaryMin *int }
  type SearchHit struct{ Job domain.Job; Rank float32 }
  type JobRepo interface {
  	Create(ctx context.Context, j domain.Job) error
  	Get(ctx context.Context, id uuid.UUID) (domain.Job, error)          // bỏ qua deleted; không có → domain.ErrJobNotFound
  	GetForUpdate(ctx context.Context, id uuid.UUID) (domain.Job, error) // như Get + FOR UPDATE
  	Update(ctx context.Context, j domain.Job) error                     // ghi mọi field thay đổi được
  	Search(ctx context.Context, f SearchFilter, after *Cursor, limit int) ([]SearchHit, error) // chỉ open, chưa xoá; Rank=0 khi Query rỗng
  	ListByRecruiter(ctx context.Context, recruiterID uuid.UUID, status *domain.Status, after *Cursor, limit int) ([]domain.Job, error)
  }
  ```
- Produces (`service.go`, `commands.go`, `queries.go`):
  ```go
  type Deps struct{ Jobs JobRepo; Outbox OutboxWriter; Tx TxManager; Now func() time.Time; NewID func() uuid.UUID; Logger *slog.Logger }
  func NewService(d Deps) *Service
  type PageRequest struct{ Size int; Token string }   // Size 0 → 20; > 100 → ErrValidation
  type Page struct{ Jobs []domain.Job; NextToken string }
  func (s *Service) Create(ctx context.Context, d domain.Draft) (domain.Job, error)        // RequireRole(recruiter); job.created
  func (s *Service) Update(ctx context.Context, id uuid.UUID, p domain.Patch) (domain.Job, error) // owner; job.updated; Patch rỗng → no-op
  func (s *Service) Publish(ctx context.Context, id uuid.UUID) (domain.Job, error)         // owner; job.updated
  func (s *Service) Close(ctx context.Context, id uuid.UUID) (domain.Job, error)           // owner; job.closed
  func (s *Service) Delete(ctx context.Context, id uuid.UUID) error                        // owner; job.deleted
  func (s *Service) Get(ctx context.Context, id uuid.UUID) (domain.Job, error)             // public nếu open; owner thấy mọi trạng thái
  func (s *Service) ListOpen(ctx context.Context, f SearchFilter, pr PageRequest) (Page, error)
  func (s *Service) ListMine(ctx context.Context, status *domain.Status, pr PageRequest) (Page, error) // RequireRole(recruiter)
  ```
- Produces (`cursor.go`): `func EncodeCursor(c Cursor) string` (base64url RawURLEncoding của JSON); `func DecodeCursor(s string) (Cursor, error)` — lỗi base64/JSON/`ID == uuid.Nil`/`At` zero → `apperr.New(apperr.ErrValidation, "invalid page token")`
- Produces (`events.go`): `const source = "job-service"`; `type jobPayload struct{ JobID, RecruiterID string; Title, Description string; RequiredSkills, NiceToHaveSkills []string; Level, Location string; SalaryMin, SalaryMax *int; Status string; Version int64 }` (tag JSON snake_case như schema); `type jobRefPayload struct{ JobID string; Version int64 }`.

Quy tắc chung của command có owner: trong `WithTx`: `GetForUpdate` → `!j.OwnedBy(claims.Subject)` → `domain.ErrJobNotFound` → gọi phương thức domain → `Jobs.Update` → `Outbox.Write(event.New(ctx, source, <type>, 1, payload, now), j.ID.String())`. Không có claims → `ErrUnauthenticated` (qua `authz.RequireRole(ctx, authz.RoleRecruiter)`; role khác recruiter → `ErrForbidden`).

Phân trang: lấy `limit = size+1`; nếu nhận đủ `size+1` thì cắt và `NextToken = EncodeCursor(phần tử cuối trang)` (`At = PublishedAt` cho ListOpen, `CreatedAt` cho ListMine; `Rank` chỉ khi có `Query`). `SearchFilter` trước khi gọi repo: `Query = strings.TrimSpace`, `Skills` hạ chữ thường + bỏ rỗng.

- [ ] **Step 1: Viết test** (fakes in-memory: `fakeJobs` lưu map, `Search` lọc open + sort đơn giản; `fakeTx` như Auth; `fakeOutbox` ghi lại envelope; ctx recruiter dựng bằng `authz.WithClaims`)

```go
func TestCreate_RecruiterOnly(t *testing.T) {
	_, err := svc.Create(context.Background(), validDraft()); assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
	_, err = svc.Create(asCandidate(), validDraft()); assert.ErrorIs(t, err, apperr.ErrForbidden)
	j, err := svc.Create(asRecruiter(r1), validDraft()); require.NoError(t, err)
	assert.Equal(t, r1, j.RecruiterID)
	ev := outbox.last(); assert.Equal(t, "job.created", ev.EventType); assert.Equal(t, "job-service", ev.Source)
	assert.Equal(t, j.ID.String(), outbox.lastKey())
	assert.JSONEq(t, `{"job_id":"`+j.ID.String()+`", ..., "status":"draft","version":1}`, string(ev.Payload))
}
func TestCommands_EventTypesAndVersion(t *testing.T) {
	// Create(v1) → Update(v2, job.updated) → Publish(v3, job.updated status open) → Close(v4, job.closed {job_id,version:4}) → Delete(v5, job.deleted)
}
func TestCommands_NonOwnerGetsNotFound(t *testing.T) {
	// recruiter r2 gọi Update/Publish/Close/Delete JD của r1 → ErrNotFound; outbox không thêm event; version không đổi
}
func TestUpdate_EmptyPatchIsNoop(t *testing.T) { /* version giữ nguyên, không event */ }
func TestDelete_OpenRejected(t *testing.T)     { /* ErrValidation */ }
func TestCommand_RollsBackOnOutboxError(t *testing.T) { /* fakeOutbox trả lỗi → fakeJobs không đổi (fakeTx snapshot/restore) */ }
func TestGet_Visibility(t *testing.T) {
	// draft: ẩn danh → NotFound; r2 → NotFound; owner → OK
	// open: ẩn danh → OK; closed: owner OK, ẩn danh NotFound; deleted: owner NotFound
}
func TestListOpen_PagesAndToken(t *testing.T) {
	// 45 JD open; Size 20 → 20, 20, 5; trang cuối NextToken == ""; không trùng id giữa các trang
}
func TestListOpen_DefaultAndMaxSize(t *testing.T) { /* Size 0 → 20; Size 101 → ErrValidation */ }
func TestListJobs_CursorFromOtherQueryRejected(t *testing.T) {
	// token lấy từ trang có Query="go" dùng cho Query="" → ErrValidation; và ngược lại
}
func TestListMine_FiltersByOwnerAndStatus(t *testing.T) { /* chỉ JD của mình; status=draft chỉ trả draft */ }
func TestDecodeCursor_Rejects(t *testing.T) {
	for _, s := range []string{"!!!", base64("not json"), base64(`{"p":"2026-01-01T00:00:00Z"}`), base64(`{"id":"`+uuid.NewString()+`"}`)} {
		_, err := app.DecodeCursor(s); assert.ErrorIs(t, err, apperr.ErrValidation, s)
	}
	c := app.Cursor{Rank: ptr32(0.5), At: now, ID: uuid.New()}
	got, err := app.DecodeCursor(app.EncodeCursor(c)); require.NoError(t, err); assert.Equal(t, c.ID, got.ID)
}
```

- [ ] **Step 2: Run** `cd services/job && go test ./internal/app/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Run** `go test ./internal/... && go test -coverprofile=c.out ./internal/domain/... ./internal/app/... && go tool cover -func=c.out | tail -1` — Expected: PASS, total ≥ 70%.
- [ ] **Step 5: Commit** `feat(job): add job use cases with ownership, events and cursor paging`

---

### Task 10: Job postgres adapter + migration + integration test

**Files:**
- Create: `services/job/migrations/000001_init.{up,down}.sql`, `services/job/sqlc.yaml`, `services/job/internal/adapter/postgres/queries/jobs.sql`, `services/job/internal/adapter/postgres/{tx.go,jobs.go,outbox.go}`, `services/job/internal/adapter/postgres/sqlc/` (sinh), `services/job/test/integration/{main_test.go,repo_test.go,search_test.go,events_test.go}`
- Modify: `Makefile` (`sqlc` lặp auth + job)

**Interfaces:**
- Consumes: `app.JobRepo`, `app.TxManager`, `app.OutboxWriter`, `app.Cursor`, `app.SearchFilter`, `app.SearchHit`, `outbox.Writer`, `eventschema.Validate`.
- Produces: `postgres.NewPool(ctx, dsn) (*pgxpool.Pool, error)`, `postgres.NewTxManager(pool) *TxManager`, `postgres.NewJobRepo(pool) *JobRepo`, `postgres.NewOutboxWriter(pool) *OutboxWriter` (copy pattern `tx.go` của Auth gồm `queries(ctx, pool)` và `dbtx(ctx, pool) outbox.Querier`).

- [ ] **Step 1: Migration `000001_init.up.sql`**

```sql
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE FUNCTION unaccent_immutable(text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT public.unaccent('public.unaccent'::regdictionary, $1) $$;

CREATE FUNCTION text_array_join(text[]) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT array_to_string($1, ' ') $$;

CREATE FUNCTION lower_text_array(text[]) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT
AS $$ SELECT coalesce(array_agg(lower(x)), '{}') FROM unnest($1) AS x $$;

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    recruiter_id uuid NOT NULL,
    title text NOT NULL,
    description text NOT NULL,
    required_skills text[] NOT NULL DEFAULT '{}',
    nice_to_have_skills text[] NOT NULL DEFAULT '{}',
    level text NOT NULL CHECK (level IN ('intern','junior','middle','senior','lead')),
    location text NOT NULL,
    salary_min int, salary_max int,
    status text NOT NULL CHECK (status IN ('draft','open','closed')),
    version bigint NOT NULL DEFAULT 1,
    published_at timestamptz, deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    search_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', unaccent_immutable(title)), 'A') ||
        setweight(to_tsvector('simple', unaccent_immutable(text_array_join(required_skills))), 'B') ||
        setweight(to_tsvector('simple', unaccent_immutable(description)), 'C')) STORED
);
CREATE INDEX jobs_search_gin ON jobs USING GIN (search_vector) WHERE status = 'open' AND deleted_at IS NULL;
CREATE INDEX jobs_open_published ON jobs (published_at DESC, id DESC) WHERE status = 'open' AND deleted_at IS NULL;
CREATE INDEX jobs_recruiter ON jobs (recruiter_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX jobs_skills_gin ON jobs USING GIN (lower_text_array(required_skills));
-- outbox: đúng outbox.SchemaSQL (pkg/outbox); processed_events: đúng idempotency.SchemaSQL
```

Down: drop theo thứ tự ngược (`processed_events`, `outbox`, `jobs`, 3 function, `EXTENSION unaccent`).

- [ ] **Step 2: Query keyset** (`queries/jobs.sql`; ngoài ra `InsertJob`, `GetJob`, `GetJobForUpdate` (`... AND deleted_at IS NULL FOR UPDATE`), `UpdateJob` — không `SELECT *`, liệt kê cột trừ `search_vector`)

```sql
-- name: SearchOpenJobs :many
WITH q AS (SELECT websearch_to_tsquery('simple', unaccent_immutable(sqlc.arg(q)::text)) AS tsq)
SELECT j.id, j.recruiter_id, j.title, j.description, j.required_skills, j.nice_to_have_skills, j.level,
       j.location, j.salary_min, j.salary_max, j.status, j.version, j.published_at, j.deleted_at,
       j.created_at, j.updated_at, ts_rank(j.search_vector, q.tsq)::real AS rank
FROM jobs j, q
WHERE j.status = 'open' AND j.deleted_at IS NULL AND j.search_vector @@ q.tsq
  AND (sqlc.narg(skills)::text[] IS NULL OR lower_text_array(j.required_skills) && sqlc.narg(skills)::text[])
  AND (sqlc.narg(location)::text IS NULL
       OR lower(unaccent_immutable(j.location)) = lower(unaccent_immutable(sqlc.narg(location)::text)))
  AND (sqlc.narg(level)::text IS NULL OR j.level = sqlc.narg(level)::text)
  AND (sqlc.narg(salary_min)::int IS NULL OR j.salary_max IS NULL OR j.salary_max >= sqlc.narg(salary_min)::int)
  AND (sqlc.narg(after_id)::uuid IS NULL OR
       (ts_rank(j.search_vector, q.tsq)::real, j.published_at, j.id)
         < (sqlc.narg(after_rank)::real, sqlc.narg(after_at)::timestamptz, sqlc.narg(after_id)::uuid))
ORDER BY rank DESC, j.published_at DESC, j.id DESC
LIMIT sqlc.arg(lim);

-- name: ListOpenJobs :many   -- không có q: cùng filter, không CTE/rank
--   keyset: (j.published_at, j.id) < (after_at, after_id); ORDER BY j.published_at DESC, j.id DESC

-- name: ListJobsByRecruiter :many
--   WHERE recruiter_id = $1 AND deleted_at IS NULL AND (sqlc.narg(status)::text IS NULL OR status = ...)
--   keyset (created_at, id) < (after_at, after_id); ORDER BY created_at DESC, id DESC
```

`JobRepo.Search` chọn `SearchOpenJobs` khi `f.Query != ""`, ngược lại `ListOpenJobs`. `sqlc.yaml` copy Auth (override uuid/timestamptz); nếu sqlc không biết kiểu `tsvector` thì thêm override `db_type: tsvector → string` (cột không được select nên chỉ ảnh hưởng `models.go`). `salary_*` sqlc sinh `*int32` → chuyển sang `*int` ở adapter.

- [ ] **Step 3: Makefile `sqlc`**: `for s in auth job; do MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR)/services/$$s:/src" -w /src sqlc/sqlc:1.29.0 generate || exit 1; done`. Run `make sqlc` — Expected: sinh `services/job/internal/adapter/postgres/sqlc/`.

- [ ] **Step 4: Viết test integration** (`main_test.go` copy Auth: `tcpostgres` với `job_db`/`job_svc`, migrate up; helper `seedOpen(t, title, skills, location, level, salaryMin, salaryMax, publishedAt)` đi qua `JobRepo.Create` + `Update`)

```go
func TestMigrations_UpDown(t *testing.T)            { /* Down rồi Up không lỗi */ }
func TestRepo_GetIgnoresDeleted(t *testing.T)       { /* Delete (deleted_at) → Get trả ErrNotFound */ }
func TestSearch_UnaccentMatches(t *testing.T) {
	seedOpen(t, "Lập trình viên Go", ...)
	hits := search(t, app.SearchFilter{Query: "lap trinh"}); require.Len(t, hits, 1)
	hits = search(t, app.SearchFilter{Query: "Lập Trình"}); require.Len(t, hits, 1)
}
func TestSearch_UnaccentHandlesDStrokeAndCase(t *testing.T) {
	seedOpen(t, "Kỹ sư Đường ống ĐÀ NẴNG", ...)
	assert.Len(t, search(t, app.SearchFilter{Query: "duong ong"}), 1)
	assert.Len(t, search(t, app.SearchFilter{Query: "da nang"}), 1)
}
func TestSearch_HostileQueryDoesNotError(t *testing.T) {
	for _, q := range []string{`c++ & | !`, `"unbalanced`, `((`, `!!!`, `-`, `or`} {
		_, err := repo.Search(ctx, app.SearchFilter{Query: q}, nil, 21); assert.NoError(t, err, q)
	}
}
func TestSearch_TitleRanksAboveDescription(t *testing.T) { /* "golang" trong title xếp trước JD chỉ có "golang" trong description */ }
func TestSearch_Filters(t *testing.T) {
	// skills: ["go"] khớp JD có "Go"; location "HA NOI" và "hà nội" khớp "Hà Nội", "Hải Phòng" không khớp
	// level senior; salary_min 2000: lấy JD salary_max NULL và salary_max ≥ 2000, loại salary_max 1500
}
func TestSearch_OnlyOpenNotDeleted(t *testing.T) { /* draft, closed, open-đã-xoá không xuất hiện (cả có q lẫn không q) */ }
func TestSearch_CursorStableAcrossPages(t *testing.T) {
	// 25 JD open, 5 cặp trùng published_at; có q và không q:
	// đi trang 10 qua cursor (lấy từ hit cuối) → 25 id khác nhau, đúng thứ tự như truy vấn một lần LIMIT 100
}
func TestListByRecruiter_StatusAndPaging(t *testing.T) { /* chỉ JD của recruiter, lọc status, phân trang created_at */ }
```

`events_test.go`:
```go
func TestEvents_MatchSchemas(t *testing.T) {
	// app.Service thật trên adapter postgres; Create → Update → Publish → Close → Delete
	// SELECT event_type, aggregate_id, payload FROM outbox ORDER BY created_at, id:
	// 5 dòng, loại đúng thứ tự, aggregate_id = job_id, eventschema.Validate(event_type, payload) == nil
}
func TestEvents_RollbackLeavesNoOutboxRow(t *testing.T) { /* Outbox wrapper lỗi sau khi ghi → 0 job, 0 outbox */ }
```

- [ ] **Step 5: Run** `cd services/job && go test -count=1 -tags integration ./test/integration/... -v` — Expected: FAIL (adapter chưa có).
- [ ] **Step 6: Implement** `tx.go` (copy Auth + `dbtx`), `jobs.go` (map sqlc ↔ domain; `ErrNoRows` → `domain.ErrJobNotFound`), `outbox.go` (uỷ quyền `outbox.Writer`, như Auth Task 6).
- [ ] **Step 7: Run** lại — Expected: PASS.
- [ ] **Step 8: Commit** `feat(job): add postgres adapter with unaccent full-text search`

---

### Task 11: Job gRPC/HTTP adapter + bootstrap + Dockerfile

**Files:**
- Create: `services/job/internal/adapter/grpc/{handler.go,mapping.go,handler_test.go}`, `services/job/internal/adapter/httpx/handler.go`, `services/job/internal/config/{config.go,config_test.go}`, `services/job/internal/bootstrap/bootstrap.go`, `services/job/cmd/server/main.go`, `services/job/Dockerfile`, `services/job/README.md`
- Create: `pkg/authz/retry.go`, `pkg/authz/retry_test.go`

**Interfaces:**
- Consumes: `app.Service` (Task 9), adapter postgres (Task 10), `grpcx.NewServer`, `grpcx.Serve`, `outbox.NewRelay`, `kafka.NewProducer`, `jobv1`.
- Produces:
  - `func authz.NewVerifierWithRetry(ctx context.Context, jwksURL, issuer, audience string, maxWait time.Duration, log *slog.Logger) (*Verifier, error)` — thử `NewVerifier` với timeout 5s mỗi lần, nghỉ 1s giữa các lần, bỏ cuộc sau `maxWait` hoặc khi ctx huỷ (logic chuyển từ `services/gateway/cmd/gateway/main.go:newVerifier`)
  - `jobgrpc.PublicMethods = []string{jobv1.JobService_GetJob_FullMethodName, jobv1.JobService_ListJobs_FullMethodName}`
  - `type UseCases interface{ Create; Update; Publish; Close; Delete; Get; ListOpen; ListMine }` (chữ ký y như `*app.Service`, Task 9) và `func jobgrpc.NewHandler(uc UseCases) *Handler` (implements `jobv1.JobServiceServer`)
  - `mapping.go`: `toProto(domain.Job) *jobv1.Job`, `levelFromProto(jobv1.JobLevel) (domain.Level, bool)`, `levelToProto`, `statusToProto`, `statusFromProto`, `patchFromProto(*jobv1.UpdateJobRequest) domain.Patch`, `filterFromProto(*jobv1.ListJobsRequest) app.SearchFilter` — `id` parse lỗi → `apperr.ErrValidation`
  - `func httpx.NewHandler(readyCheck func(ctx context.Context) error) http.Handler` — `/healthz`, `/readyz` (copy Auth, bỏ JWKS)
  - `config.Config` (prefix `JOB`): `GRPCPort int envDefault:"9002"`, `HTTPPort int envDefault:"8002"`, `DBDSN string env:"DB_DSN,required"`, `JWKSURL envDefault:"http://auth:8001/.well-known/jwks.json"`, `JWTIssuer envDefault:"smarthire-auth"`, `JWTAudience envDefault:"smarthire-api"`, `KafkaBrokers []string envDefault:"redpanda:9092" envSeparator:","`, `OutboxInterval time.Duration envDefault:"500ms"`, `LogLevel envDefault:"info"`; `func Load() (Config, error)`
  - `func bootstrap.Run(ctx context.Context, cfg config.Config) error`

- [ ] **Step 1: Test `authz` retry**

```go
func TestNewVerifierWithRetry_SucceedsWhenAvailable(t *testing.T) { /* authztest issuer → OK ngay */ }
func TestNewVerifierWithRetry_GivesUpAfterMaxWait(t *testing.T) {
	start := time.Now()
	_, err := authz.NewVerifierWithRetry(t.Context(), "http://127.0.0.1:1/jwks", authztest.Issuer, authztest.Audience, 1500*time.Millisecond, slog.New(slog.DiscardHandler))
	require.Error(t, err); assert.Less(t, time.Since(start), 4*time.Second)
}
```

- [ ] **Step 2: Test handler** (`handler_test.go`: bufconn + `grpcx.NewServer` với `authztest` verifier và `PublicMethods` = health + `jobgrpc.PublicMethods`; `UseCases` giả ghi lại tham số)

```go
func TestHandler_PublicMethodsNoToken(t *testing.T)    { /* ListJobs, GetJob không token → OK; CreateJob không token → Unauthenticated */ }
func TestHandler_CreateMapsFields(t *testing.T)        { /* level JOB_LEVEL_SENIOR → domain.LevelSenior, salary optional → *int; response status JOB_STATUS_DRAFT, version 1 */ }
func TestHandler_UpdateOnlyPresentFields(t *testing.T) { /* chỉ title + required_skills → Patch có Title, RequiredSkills; các field khác nil */ }
func TestHandler_ListJobsMapsFilter(t *testing.T)      { /* level 0 → Level nil; salary_min unset → nil; page_size 0 truyền nguyên (app áp mặc định) */ }
func TestHandler_ValidationErrors(t *testing.T)        { /* title "abc" → InvalidArgument (protovalidate); id không phải uuid → InvalidArgument */ }
func TestHandler_ErrorMapping(t *testing.T)            { /* uc trả domain.ErrJobNotFound → NotFound; ErrInvalidTransition → InvalidArgument */ }
```

`config_test.go`: `TestLoad_Defaults` (chỉ `JOB_DB_DSN` → port 9002/8002, brokers `[redpanda:9092]`, interval 500ms), `TestLoad_RequiresDSN`.

- [ ] **Step 3: Run** `cd pkg && go test ./authz/...` và `cd services/job && go test ./internal/...` — Expected: FAIL.

- [ ] **Step 4: Implement**
- `bootstrap.Run` theo khuôn Auth: logger + otel → pool → producer → service (`Now: time.Now`, `NewID: uuid.New`) → HTTP health (`pool.Ping`) → verifier (`NewVerifierWithRetry`, `maxWait 30s`) → gRPC (`PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, jobgrpc.PublicMethods...)`) → relay goroutine trên `runCtx` → chờ ctx/lỗi → `hs.Shutdown()`, `cancel()`, chờ gRPC + relay, `httpSrv.Shutdown`.
- `cmd/server/main.go` copy Auth (prefix lỗi `job:`).
- `Dockerfile` copy Auth: `COPY services/job/`, chỉ build `/out/server`, copy `migrations`, `EXPOSE 9002 8002`.
- `README.md` của service: mục đích, port, env, RPC ↔ REST, event phát ra.

- [ ] **Step 5: Run** lại Step 3 + `docker build -f services/job/Dockerfile .` — Expected: PASS, image build được.
- [ ] **Step 6: Commit** `feat(job): add grpc handler, bootstrap with outbox relay and dockerfile`

---

### Task 12: Gateway — route job

**Files:**
- Modify: `services/gateway/internal/config/{config.go,config_test.go}`, `services/gateway/internal/router/{router.go,router_test.go}`, `services/gateway/cmd/gateway/main.go`
- Create: `services/gateway/internal/router/ready.go`

**Interfaces:**
- Consumes: `jobv1.RegisterJobServiceHandlerClient`, `jobv1.NewJobServiceClient`, `authz.NewVerifierWithRetry`.
- Produces:
  - Config `JobGRPCAddr string \`env:"JOB_GRPC_ADDR" envDefault:"job:9002"\``
  - `router.Deps` thêm `JobConn *grpc.ClientConn`
  - `func router.ConnsReady(conns ...*grpc.ClientConn) func(context.Context) error` — lỗi nếu conn nào `nil` hoặc `GetState() == connectivity.Shutdown`
  - Forward option: `*authv1.RegisterResponse` **hoặc** `runtime.RPCMethod(ctx) == jobv1.JobService_CreateJob_FullMethodName` → `201`

- [ ] **Step 1: Test** (`router_test.go`: `setup` thêm server job giả dựng bằng `grpcx.NewServer(grpcx.ServerConfig{Verifier: v, PublicMethods: []string{jobv1.JobService_GetJob_FullMethodName, jobv1.JobService_ListJobs_FullMethodName}})` trên bufconn thứ hai, `fakeJob` đếm lời gọi và lưu request)

```go
func TestRouter_ListJobsAnonymous200(t *testing.T) {
	rec := e.do("GET", "/v1/jobs?q=go&skills=go&skills=sql&page_size=5", "", nil)
	assert.Equal(t, 200, rec.Code); assert.Equal(t, []string{"go", "sql"}, e.job.listReq.Skills)
}
func TestRouter_GetJobAnonymous200(t *testing.T)    { /* GET /v1/jobs/<uuid> → 200 */ }
func TestRouter_CreateJobWithoutToken401(t *testing.T) { /* POST /v1/jobs không Authorization → 401 envelope UNAUTHENTICATED; fake không được gọi */ }
func TestRouter_CreateJobRecruiter201(t *testing.T) { /* Bearer recruiter → 201, body có "status":"JOB_STATUS_DRAFT" */ }
func TestRouter_PublishCustomVerb(t *testing.T)     { /* POST /v1/jobs/<uuid>:publish → PublishJob được gọi với id đúng */ }
func TestRouter_MyJobsRoutesToJobService(t *testing.T) { /* GET /v1/me/jobs với token → ListMyJobs gọi 1 lần, auth GetMe 0 lần */ }
func TestRouter_GetJobNotCreated201(t *testing.T)   { /* GET /v1/jobs/<uuid> trả 200, không phải 201 (forward option chỉ theo method) */ }
func TestConnsReady_ShutdownConnNotReady(t *testing.T) { /* conn đã Close → lỗi; conn mở → nil */ }
```

`config_test.go`: default `JobGRPCAddr == "job:9002"`.

- [ ] **Step 2: Run** `cd services/gateway && go test ./internal/...` — Expected: FAIL.
- [ ] **Step 3: Implement** — `router.New` đăng ký `jobv1.RegisterJobServiceHandlerClient(ctx, mux, jobv1.NewJobServiceClient(d.JobConn))` trên cùng mux, không thêm route chi mới (`/v1/me/jobs` rơi vào `r.Handle("/v1/*", api)`; route `/v1/me` của Auth là khớp chính xác nên không chặn). `main.go`: dial `cfg.JobGRPCAddr`, `Ready: router.ConnsReady(authConn, jobConn)`, thay `newVerifier` bằng `authz.NewVerifierWithRetry(ctx, ..., 30*time.Second, log)` (xoá hàm cũ).
- [ ] **Step 4: Run** `go test ./...` và `go test -count=1 -tags integration ./...` — Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): route job service through grpc-gateway`

---

### Task 13: Compose job + smoke + Makefile

**Files:**
- Modify: `deploy/docker/docker-compose.yml`, `Makefile`, `tools/cmd/smoke/main.go`, `tools/go.mod`
- Create: `tools/cmd/smoke/jobs.go`, `tools/cmd/smoke/kafka.go`

**Interfaces:**
- Consumes: mọi thứ ở trên; tài khoản seed `recruiter@smarthire.local` / `Password123!`.
- Produces:
  - Compose: `job-migrate` (`migrate/migrate:v4.18.3`, `-path=/migrations`, DSN `postgres://job_svc:${JOB_DB_PASSWORD}@postgres:5432/job_db?sslmode=disable`), `job` (image `smarthire/job:dev`, env `JOB_DB_DSN`, `JOB_JWKS_URL: http://auth:8001/.well-known/jwks.json`, `JOB_KAFKA_BROKERS: redpanda:9092`, `JOB_LOG_LEVEL: info`, `OTEL_EXPORTER_OTLP_ENDPOINT`; depends `job-migrate` completed, `topics-init` completed, `auth` started). `auth` thêm `AUTH_KAFKA_BROKERS: redpanda:9092` và depends `topics-init: service_completed_successfully`. `gateway` thêm `GATEWAY_JOB_GRPC_ADDR: job:9002`, depends `job: service_started`.
  - Smoke flag mới `-kafka` (mặc định `127.0.0.1:19092`), `-recruiter-email` (mặc định `recruiter@smarthire.local`), `-recruiter-password` (mặc định `Password123!`).
  - `func (s *smoke) jobSteps() []namedStep` (`jobs.go`); `func waitForKey(ctx context.Context, broker, topic, key string) error` (`kafka.go`; client franz-go không group, `ConsumeResetOffset(AtStart)`, quét tới khi gặp key, timeout 10s).

- [ ] **Step 1: Viết smoke** (đây là test của task) — sau 7 bước Auth, thêm theo thứ tự, với `tag = "smk" + lowercase(ulid)`, title `"Lập trình viên Go " + tag`, description ≥ 20 ký tự, `required_skills ["Go","PostgreSQL"]`, `level "JOB_LEVEL_SENIOR"`, `location "Hà Nội"`:
1. `recruiter login` → 200, lấy access token.
2. `create job` → `POST /v1/jobs` 201, `status == "JOB_STATUS_DRAFT"`, lưu `id`.
3. `job.created on kafka` → `waitForKey(ctx, kafka, "job.created", id)` trong 10s.
4. `draft hidden` → `GET /v1/jobs?q=lap+trinh+<tag>` 200, không chứa `id`.
5. `publish job` → `POST /v1/jobs/{id}:publish` 200, `JOB_STATUS_OPEN`.
6. `search finds job` → cùng truy vấn không dấu, chứa `id`.
7. `close job then hidden` → `POST /v1/jobs/{id}:close` 200; tìm lại không chứa `id`.

- [ ] **Step 2: Run** `make smoke` với stack cũ (chưa có job) — Expected: 7 bước Auth PASS, `FAIL create job: ...` (404/503).

- [ ] **Step 3: Compose + Makefile**
- Compose như Interfaces.
- Makefile: `up` sau vòng chờ `/healthz` thêm vòng chờ `curl -fsS -o /dev/null $(GATEWAY_URL)/v1/jobs` (60 lần × 1s; lỗi thì in `logs --tail=50 job gateway`); `migrate`: `$(COMPOSE) run --rm auth-migrate && $(COMPOSE) run --rm job-migrate`; `smoke`: thêm `-kafka $(KAFKA_ADDR)` với `KAFKA_ADDR ?= 127.0.0.1:19092`; `test-int`: lặp `pkg services/*/`; thêm `cover-job` (như `cover-auth`, thư mục `services/job`, ngưỡng 70%); `.PHONY` cập nhật.

- [ ] **Step 4: Kiểm tra end-to-end trên volume sạch**

Run: `make down` + `down -v` (như Task 2 Step 5), rồi `make bootstrap && make infra-up obs-up up && make seed && make smoke`
Expected: 7 + 7 dòng `PASS`, exit 0. Mở `http://localhost:8081` thấy message trong `user.registered` và `job.created`/`job.updated`/`job.closed`. Grafana → Tempo, service `gateway`, trace `POST /v1/jobs`: span gateway → job → pgx; tìm trace theo trace ID đó thấy span `job.created publish` (service `job`) cùng trace.

- [ ] **Step 5: Commit** `feat(deploy): run job service in compose and extend smoke with job flow`

---

### Task 14: CI

**Files:**
- Modify: `.github/workflows/ci-go.yml`

**Interfaces:**
- Consumes: Makefile `cover-job`.
- Produces: job `module` chạy cho `services/job`; integration test chạy cho `pkg` và mọi `services/*`; docker build `auth, gateway, job`.

- [ ] **Step 1: Sửa workflow**
- `paths-filter` thêm `job: ['services/job/**']`; `shared` thêm `contracts/events/**` (schema được test ở `pkg` và các service); `env` + bước matrix thêm `JOB` → `services/job`. `pkg/**` đã nằm trong `shared` nên kích hoạt mọi module (giữ nguyên).
- `Integration tests`: `if: startsWith(matrix.module, 'services/') || matrix.module == 'pkg'`.
- `Coverage gate`: thêm bước `if: matrix.module == 'services/job'` chạy `make cover-job`.
- `docker.strategy.matrix.service: [auth, gateway, job]`.

- [ ] **Step 2: Kiểm tra cục bộ** — `docker run --rm -v "$(pwd):/repo" -w /repo rhysd/actionlint:1.7.7` (Git Bash cần `MSYS_NO_PATHCONV=1`) — Expected: không lỗi. Chạy race như CI: `MSYS_NO_PATHCONV=1 docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$(pwd):/src" -w /src golang:1.27 bash -c 'for m in pkg services/auth services/gateway services/job tools; do (cd $m && go test -race ./...) || exit 1; done'` — Expected: PASS.

- [ ] **Step 3: Commit** `ci: add job service and pkg integration tests to pipeline`

---

### Task 15: Tài liệu — ADR 0005/0006, README, CLAUDE.md

**Files:**
- Create: `docs/adr/0005-outbox-relay-headers.md`, `docs/adr/0006-kafka-client-retry-dlq.md`
- Modify: `docs/adr/README.md`, `README.md`, `CLAUDE.md`

- [ ] **Step 1: Viết ADR** (cùng khuôn ADR 0001–0004: Bối cảnh / Quyết định / Hệ quả / Phương án đã cân nhắc)
- 0005: relay goroutine trong `cmd/server` (D3), poll `FOR UPDATE SKIP LOCKED`, at-least-once, thứ tự theo key trong một replica; cột `headers jsonb` lưu carrier W3C (D4) và cách relay nối span (quyết định #2); khi nào tách `cmd/relay`; CDC (Debezium) là phương án bị loại.
- 0006: Redpanda cho local (D1), franz-go (D2), idempotent producer `acks=all`; retry trong process 3 lần backoff 200ms×2^n ≤ 2s rồi `<topic>.dlq` với header `dlq-*` (D5), `kafka.Permanent`, DLQ lỗi thì dừng consumer; phương án topic retry trễ để sau; `pkg/idempotency` cho consumer có side effect DB.

- [ ] **Step 2: README** — sơ đồ: tô xanh thêm Job Service, `job_db`, Kafka; đoạn "Ở M0, Auth chỉ ghi..." thay bằng mô tả relay; Quick start ghi smoke có luồng recruiter và volume cũ cần `down -v` để tạo `job_svc`; bảng URL thêm Redpanda Console `http://localhost:8081`, Kafka dev `127.0.0.1:19092`, Job gRPC/HTTP `9002/8002` (chỉ nội bộ); Roadmap tách dòng M1 thành `M1a` ✅ và `M1b` (Candidate + AI parse) chưa bắt đầu; mục Tài liệu thêm `services/job`, ADR 0005/0006, design + plan M1a. `docs/adr/README.md` thêm 2 dòng.

- [ ] **Step 3: CLAUDE.md** — Project state: M1a đã có (`pkg/kafka`, `pkg/outbox`, `pkg/idempotency`, `pkg/event/eventschema`, `services/job`, `contracts/events`), thêm design/plan M1a vào danh sách tài liệu; Architecture/Events: relay đã chạy, headers W3C, DLQ; Commands thêm `make cover-job`, lệnh race thêm `services/job`, URL thêm Console `:8081` và Kafka `:19092`; Conventions: config prefix `JOB_`, ADR M1a dùng 0005–0006.

- [ ] **Step 4: Kiểm tra** — làm đúng Quick start trên clone mới: `git clone . /tmp/sh && cd /tmp/sh && make bootstrap && make infra-up obs-up up && make seed && make smoke && make lint test test-int cover-auth cover-job` — Expected: PASS hết; sau đó `make down`.

- [ ] **Step 5: Commit** `docs: add ADR 0005-0006 and update README and CLAUDE.md for M1a`
