# M0 — Nền móng + Auth + Gateway — Design

| Mục | Giá trị |
|---|---|
| Ngày | 2026-11-08 |
| Trạng thái | Đã duyệt |
| Spec gốc | [`spec.md`](../../../spec.md) (v0.3.0) |
| Milestone | M0 / M0–M6 |

---

## 1. Bối cảnh & mục tiêu dự án

**Mục đích:** portfolio xin việc vị trí **Backend Go**. Một người làm, 3–6 tháng, không deadline cứng. Người xem chủ yếu đọc repo GitHub và xem video demo; không cần bản live 24/7.

**Hướng tiếp cận đã chọn — "Làm sâu hơn làm rộng":**
- Giữ đủ 7 service theo `spec.md`; dồn công vào pattern backend: Outbox, idempotent consumer, Saga + recovery, circuit breaker/retry, DLQ, OTel trace xuyên Kafka/Go↔Python, testcontainers, k6, chaos test.
- Frontend tối giản (M5), đủ để quay video demo: không admin portal, i18n, dark mode, Tiptap, Lighthouse.
- AI service Python gọi LLM API; eval mức cơ bản.
- Demo bằng Docker Compose; K8s/Helm là tuỳ chọn (M6).

**Thành công của toàn dự án:** `make up` chạy được; README có sơ đồ + ADR; có test; trace Go↔Python trên Grafana; video 3–5 phút chạy luồng CV → matching → nộp đơn kèm cảnh kill AI service để thấy hệ thống vẫn chạy.

**Roadmap milestone:**

| # | Milestone | Demo được |
|---|---|---|
| **M0** | Nền móng + Auth + Gateway (tài liệu này) | register/login qua Gateway, có trace |
| M1 | Job + Candidate + AI parse, `pkg/kafka`, `pkg/outbox`, DLQ | upload PDF → JSON parsed, event trên Kafka UI |
| M2 | Matching: embedding, Qdrant, `FindCandidates`/`RecommendJobs`, cache Redis; **CQRS tìm kiếm JD** (OpenSearch + projector, Postgres `tsvector` thành fallback) | đăng JD → ứng viên xếp hạng; search qua read model |
| M3 | Application + Saga + resilience | kill AI → `NEEDS_MANUAL_REVIEW`, CB mở |
| M4 | Notification + Observability (Prometheus, Loki, dashboard, alerts, WS); **saga xoá tài khoản** (choreography, `deletion_requests`, `DELETE /v1/me`, denylist token ở Gateway) | realtime, dashboard, xoá tài khoản end-to-end |
| M5 | Frontend tối giản + E2E + k6 + README/video | video demo hoàn chỉnh |
| M6 | (tuỳ chọn) Helm/K8s | deploy lên kind |

---

## 2. Phạm vi M0

**Mục tiêu:** dựng bộ khung chuẩn cho mọi service Go về sau và chứng minh nó bằng một luồng thật: `register → login → GET /v1/me → refresh → logout` qua Gateway, xem được trace trên Grafana.

### 2.1 Trong phạm vi
- `git init`; monorepo theo `spec.md` §10 nhưng chỉ tạo phần M0 cần: `go.work`, `contracts/proto/smarthire/auth/v1`, cấu hình `buf`, `gen/go` (commit), `pkg/`, `services/auth`, `services/gateway`, `deploy/docker`, `tools/cmd/smoke`, `docs/adr`.
- Auth service đầy đủ: đăng ký, đăng nhập, refresh có rotate và phát hiện dùng lại, logout, JWKS, RBAC.
- Gateway: chi + grpc-gateway, verify JWT qua JWKS, rate limit Redis, CORS, request ID, cookie refresh.
- Compose: Postgres, Redis, OTel Collector, Tempo, Grafana.
- Makefile, CI GitHub Actions, 4 ADR, README.

### 2.2 Ngoài phạm vi (milestone sau)
| Hạng mục | Milestone | Ghi chú |
|---|---|---|
| Kafka, outbox relay, `pkg/kafka`, `pkg/outbox`, `pkg/idempotency` | M1 | M0 vẫn **ghi** `user.registered` vào bảng `outbox` trong cùng transaction; chưa có relay |
| Prometheus, Loki, alerts | M4 | M0 chỉ có traces |
| WS ticket, WS proxy | M4 | |
| Frontend, sinh type từ OpenAPI | M5 | |
| Release/push image, Helm | M6 | |

### 2.3 Công cụ
Go bản stable mới nhất (workspace `go.work`), `pgx/v5` + `sqlc`, `golang-migrate`, `caarlos0/env/v11` (tag `envDefault`/`required` có sẵn), `log/slog`, `buf`, `grpc-gateway` v2, `chi` v5, `golang-jwt/jwt/v5`, `alexedwards/argon2id`, `redis/go-redis/v9`, OTel Go SDK (`otelgrpc`, `otelhttp`), `testify`, `testcontainers-go`, `golangci-lint`.

---

## 3. Thư viện dùng chung `pkg/`

Một Go module duy nhất `github.com/nuroqzavitex/smarthire/pkg`. Mỗi package chỉ làm một việc, không phụ thuộc service nào.

| Package | Trách nhiệm | API chính |
|---|---|---|
| `config` | Đọc env theo prefix (`AUTH_`, `GATEWAY_`), validate trường bắt buộc, lỗi rõ ràng khi khởi động | `config.Load(prefix string, dst any) error` |
| `logger` | `slog` JSON; tự gắn `trace_id`, `span_id`, `request_id`, `user_id` từ context; không log PII | `logger.New(service string, w io.Writer, level slog.Level) *slog.Logger`, `logger.From(ctx) *slog.Logger` |
| `otel` | TracerProvider OTLP gRPC → Collector; propagator W3C `traceparent` | `otel.Setup(ctx, service) (shutdown func(context.Context) error, err error)` |
| `apperr` (thư mục `pkg/apperr`, tránh trùng tên stdlib `errors`) | Sentinel: `ErrNotFound`, `ErrConflict`, `ErrValidation`, `ErrForbidden`, `ErrUnauthenticated`; map sang gRPC status theo `spec.md` §11 | `apperr.ToGRPC(err) error` |
| `grpcx` | Server builder với interceptor: recovery → otel → logging → auth → protovalidate; health service chuẩn; graceful shutdown; client dial có timeout mặc định | `grpcx.NewServer(opts...)`, `grpcx.Dial(target, opts...)` |
| `authz` | Verify JWT RS256 qua JWKS (cache, refresh khi gặp `kid` lạ); struct `Claims{Sub, Role, ...}`; `RequireRole`; helper context | `authz.NewVerifier(ctx, jwksURL, issuer, audience)`, `authz.ClaimsFrom(ctx)`, `authz.WithClaims(ctx, c)` |

**Verify token hai lớp (ADR 0003):** Gateway verify JWT rồi chuyển **nguyên JWT** xuống service qua metadata `authorization`. Interceptor `auth` của `grpcx` verify lại bằng JWKS đã cache. Lý do: `spec.md` §14.4 yêu cầu RBAC + ownership check tại service; tin header `x-user-id` từ Gateway thì ai gọi thẳng vào service là giả mạo được user. RPC public (vd. `Register`, `Login`) được khai báo trong allowlist của interceptor.

**Test:** mỗi package có unit test; `authz` test bằng khoá RSA sinh trong lúc test + JWKS server giả bằng `httptest`.

---

## 4. Auth service

### 4.1 Cấu trúc
Theo template hexagonal (`spec.md` §10–11): `internal/domain` → `internal/app` (use cases + ports) → `internal/adapter/{grpc,postgres}` → `internal/bootstrap`. Đây là **bản mẫu** để các service sau sao chép.

Ports trong `app/ports.go`: `UserRepo`, `RefreshTokenRepo`, `OutboxWriter`, `TxManager`, `PasswordHasher`, `TokenIssuer`, `Clock`.

### 4.2 Proto `smarthire/auth/v1`
```protobuf
service AuthService {
  rpc Register(RegisterRequest) returns (RegisterResponse); // email, password, role ∈ {candidate, recruiter}
  rpc Login(LoginRequest)       returns (TokenPair);        // access_token, expires_in, refresh_token
  rpc Refresh(RefreshRequest)   returns (TokenPair);
  rpc Logout(LogoutRequest)     returns (google.protobuf.Empty);
  rpc GetMe(GetMeRequest)       returns (User);             // từ claims trong context
}
```
- Annotation `google.api.http` cho `Register` (`POST /v1/auth/register`) và `GetMe` (`GET /v1/me`).
- Validate bằng `protovalidate`: email hợp lệ, password ≥ 8 ký tự, role trong tập cho phép.
- Admin **không** đăng ký được qua API; tạo bằng `make seed`.

### 4.3 Endpoint HTTP nội bộ (port `8001`)
`/.well-known/jwks.json`, `/healthz`, `/readyz` (kiểm tra DB). gRPC ở port `9001`.

### 4.4 Data model (`auth_db`)
```sql
users(id UUID PK, email CITEXT UNIQUE, password_hash TEXT, role TEXT,
      status TEXT DEFAULT 'ACTIVE',   -- ACTIVE | LOCKED | DELETION_PENDING (spec.md §8.1)
      created_at, updated_at)
refresh_tokens(id UUID PK, user_id UUID FK, family_id UUID, token_hash BYTEA UNIQUE,
               expires_at, revoked_at NULL, replaced_by UUID NULL, created_at)
outbox(id UUID PK, aggregate_id, event_type, payload JSONB, created_at, published_at NULL)
```
`deletion_requests`, `processed_events` (spec.md §8.1) để M4. So với `spec.md` §8.1: thêm `family_id`, `replaced_by`, `created_at` vào `refresh_tokens`; `token_hash` là `sha256`.

### 4.5 Access token
JWT RS256, TTL 15 phút. Claims: `sub`, `role`, `iss`, `aud`, `exp`, `iat`, `jti`; header `kid`. Key nạp từ file PEM (`AUTH_JWT_PRIVATE_KEY_PATH`). JWKS hỗ trợ nhiều key (key đang ký + key cũ) để xoay key không làm hỏng token đang lưu hành.

### 4.6 Refresh token (ADR 0004)
- 32 byte ngẫu nhiên, base64url, opaque. DB chỉ lưu `sha256`. TTL 7 ngày.
- Login tạo `family_id` mới.
- Refresh, trong một transaction:
  1. `SELECT … FOR UPDATE` theo `token_hash`. Không thấy → `Unauthenticated`.
  2. `revoked_at` khác NULL → **token bị dùng lại**: thu hồi toàn bộ token cùng `family_id`, log cảnh báo (`event=refresh_token_reuse`, `user_id`, `family_id`), trả `Unauthenticated`. Kiểm tra **trước** hạn dùng: token đã xoay vòng bị phát lại sau khi hết hạn vẫn là dấu hiệu bị đánh cắp.
  3. Hết hạn → `Unauthenticated`.
  4. Nạp user; user không `ACTIVE` → `Unauthenticated` (không thu hồi family).
  5. Hợp lệ → set `revoked_at`, tạo token mới cùng family, ghi `replaced_by`, trả cặp token mới.
- Logout: thu hồi token hiện tại (idempotent — token không tồn tại vẫn trả OK).

### 4.7 Mật khẩu & đăng ký
- argon2id, tham số theo khuyến nghị OWASP, cấu hình được qua env.
- Login chống dò user: email không tồn tại vẫn chạy hash giả; mọi lỗi đều là "sai email hoặc mật khẩu" (`Unauthenticated`).
- Trạng thái tài khoản: kiểm tra **sau** khi mật khẩu đúng (không lộ thông tin cho người dò). `LOCKED` → `PermissionDenied` "account is locked"; `DELETION_PENDING` → `PermissionDenied` "account is pending deletion".
- Register trùng email → `AlreadyExists` (đánh đổi UX, ghi trong ADR 0004 phần "không làm").
- Register tạo user + ghi `user.registered` (envelope `spec.md` §7.1, payload `user_id, email, role`) vào `outbox` **trong cùng transaction**.

### 4.8 Test
- Unit: domain + use case với port được mock; mục tiêu coverage `domain` + `app` ≥ 70%.
- Integration (testcontainers Postgres): migration, repo, register ghi outbox atomic, **refresh đồng thời** — hai request cùng một token chạy song song → đúng một thành công, request kia kích hoạt phát hiện dùng lại, cả family bị thu hồi.

---

## 5. Gateway

### 5.1 Middleware (chi)
`Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes`
- `OptionalAuth` đặt **trước** `ratelimit` để khoá rate limit theo `sub` đã verify (khoá theo `sub` chưa verify thì kẻ tấn công đổi `sub` để lách).
- Bearer không hợp lệ không bị từ chối ngay: `OptionalAuth` chỉ đánh dấu, request vẫn bị rate limit theo IP, rồi `RejectInvalidAuth` trả 401. Ba route cookie `/v1/auth/login|refresh|logout` được miễn `RejectInvalidAuth` (SPA gửi access token hết hạn kèm lời gọi refresh vẫn phục hồi được phiên).
- `auth` tuỳ chọn theo route: gắn claims nếu có Bearer hợp lệ; route bắt buộc dùng `RequireAuth` / `RequireRole`.
- CORS: chỉ origin Web App từ config (`GATEWAY_CORS_ORIGINS`), `credentials: true`.

### 5.2 Route
| Route | Cách triển khai | Ghi chú |
|---|---|---|
| `POST /v1/auth/register` | grpc-gateway | public |
| `GET /v1/me` | grpc-gateway | bắt buộc đăng nhập |
| `POST /v1/auth/login` | chi viết tay | gọi `Login`; body chỉ `access_token`, `expires_in`; set cookie `sh_rt` (`HttpOnly; Secure; SameSite=Strict; Path=/v1/auth`, Max-Age 7 ngày) và cookie cờ `sh_session` (không chứa token) |
| `POST /v1/auth/refresh` | chi viết tay | đọc `sh_rt`; kiểm tra `Origin` thuộc allowlist (chống CSRF); rotate; set lại cookie |
| `POST /v1/auth/logout` | chi viết tay | gọi `Logout`; xoá cả hai cookie |
| `/healthz`, `/readyz` | chi | `readyz` chỉ kiểm tra kết nối gRPC tới Auth; không kiểm tra Redis vì rate limiter fail-open |

- Route viết tay đăng ký trước; grpc-gateway mount làm fallback cho `/v1/*` (ADR 0002).
- Chuyển xuống service: `authorization`, `x-request-id`; `traceparent` do `otelgrpc`.
- Timeout mặc định mọi lời gọi gRPC: 3 giây.
- Cờ `Secure` của cookie cấu hình được (`GATEWAY_COOKIE_SECURE`, mặc định `true`; dev local qua HTTP đặt `false`).

### 5.3 Lỗi
Custom error handler của grpc-gateway, dùng chung cho route viết tay. Map gRPC → HTTP: `InvalidArgument→400`, `Unauthenticated→401`, `PermissionDenied→403`, `NotFound→404`, `AlreadyExists→409`, `ResourceExhausted→429`, `DeadlineExceeded→504`, `Unavailable→503`, còn lại `500`. Envelope:
```json
{ "error": { "code": "INVALID_ARGUMENT", "message": "...", "details": [], "request_id": "..." } }
```

### 5.4 Rate limit
- Token bucket trên Redis bằng Lua script (atomic).
- 100 req/phút/user (khoá theo `sub`, hoặc IP nếu chưa đăng nhập); 10 req/phút/IP cho `/v1/auth/login`.
- Key Redis có tiền tố service (spec.md §4.2): `gateway:rl:user:<sub>`, `gateway:rl:ip:<ip>`, `gateway:rl:login:<ip>`.
- Bị chặn → `429` + `Retry-After`; mọi response có `X-RateLimit-Limit`, `X-RateLimit-Remaining`.
- **Redis lỗi → fail-open**: cho request qua, log warning, tăng counter `ratelimit_redis_errors_total` (span attribute ở M0, metric Prometheus ở M4). Ghi trong ADR 0002.
- IP lấy từ `RemoteAddr`; chỉ tin `X-Forwarded-For` khi cấu hình `GATEWAY_TRUSTED_PROXIES`.

### 5.5 OpenAPI
`protoc-gen-openapiv2` sinh cho route có annotation; route viết tay mô tả trong `contracts/openapi/gateway.handwritten.yaml`; `make openapi` gộp thành `contracts/openapi/gateway.yaml`. Ở M0 chỉ để làm tài liệu.

### 5.6 Test
- Handler test: `httptest` + AuthService giả qua `bufconn` — cookie, kiểm tra `Origin`, map lỗi, envelope.
- Unit test từng middleware.
- Rate limit với Redis thật (testcontainers), gồm trường hợp Redis dừng → fail-open.

---

## 6. Infra, tooling, CI

### 6.1 Docker Compose (`deploy/docker/`)
- `docker-compose.infra.yml`: Postgres 16, Redis 7. Script init tạo role `auth_svc` (mật khẩu từ env) làm **owner** của `auth_db` — quyền tối thiểu theo spec.md §4.2 mức 1; Auth và `auth-migrate` kết nối bằng `auth_svc`, không dùng `postgres` (`citext` là trusted extension nên owner tự cài được).
- `docker-compose.obs.yml`: OTel Collector, Tempo, Grafana (provision datasource Tempo).
- `docker-compose.yml`: `auth-migrate` (container chạy `golang-migrate` một lần), `auth` (phụ thuộc migrate hoàn tất), `gateway`.
- Dockerfile Go multi-stage → `gcr.io/distroless/static:nonroot`.
- Khoá JWT dev: `make bootstrap` sinh RSA vào `deploy/docker/secrets/` (gitignored), mount vào container `auth`.

### 6.2 Makefile
| Lệnh | Việc làm |
|---|---|
| `bootstrap` | cài buf, golangci-lint, migrate (pin phiên bản); sinh khoá JWT dev; tạo `.env` từ `.env.example` |
| `proto-deps` | `buf dep update` (chủ động nâng deps, ghi lại `buf.lock`) |
| `proto` | buf generate (lock đã commit) → `gen/go` + OpenAPI; gọi `openapi` |
| `openapi` | gộp OpenAPI sinh + viết tay |
| `sqlc` | sinh code sqlc (chạy sqlc qua Docker, không cài bởi `bootstrap`) |
| `infra-up` / `obs-up` / `up` / `down` | bật/tắt nhóm container |
| `migrate` | chạy migration |
| `seed` | tạo admin + user mẫu |
| `test` / `test-int` | unit / integration |
| `smoke` | `go run ./tools/cmd/smoke` trên stack đang chạy |
| `lint` | golangci-lint + buf lint |

### 6.3 Smoke test (`tools/cmd/smoke/`)
Chạy qua Gateway thật, fail nếu bất kỳ bước nào sai:
1. register (candidate) → 201
2. login → 200, có cookie `sh_rt`
3. `GET /v1/me` → đúng email/role
4. refresh → token mới (RT1 → RT2)
5. refresh lại bằng RT1 → 401 (phát hiện dùng lại)
6. refresh bằng RT2 → 401 (family đã bị thu hồi)
7. login lại → 200; logout → cookie bị xoá; refresh → 401

### 6.4 CI (GitHub Actions)
- `ci-proto.yml`: `buf lint`, `buf breaking` so với `main`, `make proto && git diff --exit-code` để chắc `gen/` đã cập nhật.
- `ci-go.yml`: path filter; golangci-lint; `make test`; `make test-int`; docker build (không push).

### 6.5 Tài liệu
- `README.md`: giới thiệu, sơ đồ kiến trúc tổng (mermaid), quick start, bảng roadmap M0–M6 kèm trạng thái.
- `docs/adr/` (đánh số theo thứ tự viết; spec.md §10 chỉ liệt kê chủ đề, chưa gán số):
  - 0001 — Monorepo + Go workspace
  - 0002 — Gateway hybrid chi + grpc-gateway (gồm fail-open rate limit)
  - 0003 — Verify JWT ở cả Gateway và service
  - 0004 — Refresh token rotation + phát hiện dùng lại
- README riêng cho `services/auth`, `services/gateway`.

---

## 7. Definition of Done
1. Clone mới → `make bootstrap && make infra-up obs-up up && make smoke` pass.
2. Một request `GET /v1/me` hiện thành một trace trên Grafana Tempo: Gateway → Auth → Postgres.
3. CI xanh: lint, unit, integration.
4. Coverage `domain` + `app` của Auth ≥ 70%.
5. Có 4 ADR, README gốc và README từng service.

## 8. Lệch so với `spec.md`
| Mục | `spec.md` | M0 |
|---|---|---|
| Roadmap | 8 tuần, FE song song từ tuần 1 | M0–M6, FE tối giản ở M5 |
| `refresh_tokens` | `id, user_id, token_hash, expires_at, revoked_at` | thêm `family_id`, `replaced_by`, `created_at` |
| Q1 Gateway | mặc định grpc-gateway | hybrid chi + grpc-gateway |
| Outbox `user.registered` | publish qua relay | M0 chỉ ghi bảng; relay ở M1 |
