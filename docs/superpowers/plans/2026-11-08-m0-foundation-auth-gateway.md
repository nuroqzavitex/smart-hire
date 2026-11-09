# M0 — Nền móng + Auth + Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dựng monorepo Go với thư viện dùng chung `pkg/`, Auth service (JWT RS256 + refresh token rotation có phát hiện dùng lại) và API Gateway (chi + grpc-gateway, rate limit Redis, cookie refresh), chạy end-to-end bằng Docker Compose với trace trên Grafana Tempo.

**Architecture:** Go workspace gồm 5 module (`pkg`, `gen/go`, `services/auth`, `services/gateway`, `tools`). Proto trong `contracts/proto` là nguồn sự thật; `buf generate` sinh Go + grpc-gateway + OpenAPI vào `gen/` (commit). Auth theo hexagonal (`domain → app → adapter → bootstrap`); Gateway là router chi, route cookie viết tay, còn lại fallback sang grpc-gateway mux.

**Tech Stack:** Go (workspace), gRPC, grpc-gateway v2, buf, protovalidate, chi v5, pgx/v5 + sqlc, golang-migrate, envconfig, slog, OpenTelemetry, golang-jwt/v5 + keyfunc/v3, alexedwards/argon2id, go-redis/v9, testify, testcontainers-go, Docker Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md` (và `spec.md` gốc cho envelope event, error format).

## Global Constraints

- Module prefix: `github.com/nuroqzavitex/smarthire` → modules `.../pkg`, `.../gen/go`, `.../services/auth`, `.../services/gateway`, `.../tools`. Tất cả liệt kê trong `go.work`.
- Go ≥ 1.25; directive `go` trong mọi `go.mod` = phiên bản cài ở Task 0.
- Không import chéo `internal/` giữa các service; chỉ chia sẻ qua `pkg/`, `gen/go/`.
- `gen/` được commit; không sửa tay.
- Config chỉ qua env, prefix theo service (`AUTH_`, `GATEWAY_`). Thư viện: `kelseyhightower/envconfig` (spec.md §9.1 cho phép; thay cho koanf vì có sẵn tag `default`/`required`).
- Package lỗi dùng chung đặt ở `pkg/apperr` (không đặt tên `errors` để tránh trùng stdlib).
- Ports: Auth gRPC `9001`, Auth HTTP nội bộ `8001`, Gateway HTTP `8080`, Grafana `3000`, OTel Collector `4317`.
- Access token: RS256, TTL 15 phút, `iss=smarthire-auth`, `aud=smarthire-api`. Refresh token: 32 byte ngẫu nhiên base64url (RawURLEncoding), DB lưu `sha256`, TTL 7 ngày.
- Cookie: `sh_rt` (`HttpOnly; SameSite=Strict; Path=/v1/auth; Max-Age=604800`; `Secure` theo `GATEWAY_COOKIE_SECURE`, mặc định `true`), `sh_session=1` (`HttpOnly; SameSite=Strict; Path=/`; cùng Max-Age, cùng cờ Secure).
- Rate limit: 100 req/phút/user (theo `sub`, chưa đăng nhập theo IP); 10 req/phút/IP cho `POST /v1/auth/login`. Redis lỗi → fail-open.
- Key Redis luôn có tiền tố service (spec.md §4.2): Gateway dùng `gateway:`.
- Postgres: mỗi service một role riêng làm owner DB của mình (spec.md §4.2 mức 1). Auth dùng role `auth_svc` sở hữu `auth_db`; không service nào kết nối bằng `postgres`.
- `users.status` ∈ `ACTIVE | LOCKED | DELETION_PENDING` (spec.md §8.1). Bảng `deletion_requests`, `processed_events` của Auth thuộc M4, không tạo ở M0.
- ADR đánh số theo thứ tự viết; M0 dùng 0001–0004 (spec.md §10 chỉ liệt kê chủ đề).
- Timeout mặc định mọi lời gọi gRPC client: 3 giây.
- Error envelope HTTP: `{"error":{"code":"<GRPC_CODE_NAME>","message":"...","details":[],"request_id":"..."}}`.
- Event envelope theo spec.md §7.1: `event_id` (ULID), `event_type`, `event_version`, `source`, `occurred_at` (RFC3339 UTC), `trace_id`, `correlation_id`, `payload`.
- Integration test dùng build tag `integration`; `make test` không cần Docker, `make test-int` cần Docker.
- Không log PII (email, password, token). Log `user_id` được phép.
- Commit theo Conventional Commits, scope theo service (`feat(auth): ...`), kết thúc bằng dòng `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Makefile viết cho POSIX shell (chạy từ Git Bash trên Windows và Ubuntu trên CI).

## Review Focus

1. **Revoke family khi phát hiện dùng lại bị rollback** — use case trả lỗi trong transaction thì việc thu hồi family cũng bị huỷ; người dùng mong token bị đánh cắp phải chết thật. Test: `TestRefresh_ReuseRevokesFamilyPersisted` (Task 8, integration) kiểm tra DB sau lỗi.
2. **Mật khẩu cực dài (vd. 1 MB)** — argon2id trên input lớn gây tốn CPU; mong đợi bị từ chối `InvalidArgument`. Giới hạn 128 rune. Test: `TestValidatePassword` (Task 6) + `max_len` trong proto (Task 1).
3. **Email khác hoa/thường hoặc có khoảng trắng** (`" Foo@X.com "` vs `foo@x.com`) — mong đợi coi là cùng một user (trùng → 409, login được). Test: `TestNewEmail_Normalizes` (Task 6), `TestUserRepo_DuplicateEmailCaseInsensitive` (Task 8).
4. **Client giả `X-Forwarded-For` để lách rate limit login** — mong đợi bị bỏ qua khi không phải trusted proxy. Test: `TestClientIP_IgnoresXFFFromUntrusted` (Task 11).
5. **Gọi thẳng vào Auth gRPC không qua Gateway với token giả / thiếu token cho `GetMe`** — mong đợi `Unauthenticated`. Test: `TestAuthInterceptor_RejectsMissingTokenOnPrivate` (Task 5).

---

## File Structure

```
go.work
Makefile
.golangci.yml
contracts/proto/buf.yaml, buf.gen.yaml, buf.lock
contracts/proto/smarthire/auth/v1/auth.proto
contracts/openapi/gateway.handwritten.yaml        # swagger 2.0, route viết tay
contracts/openapi/gen/gateway.swagger.json        # sinh
contracts/openapi/gateway.yaml                    # gộp
gen/go/go.mod, gen/go/smarthire/auth/v1/*.go      # sinh
pkg/go.mod
pkg/apperr/apperr.go            # kind + ToGRPC
pkg/config/config.go            # envconfig wrapper
pkg/logger/logger.go            # slog JSON + context handler
pkg/otel/otel.go                # tracer provider + propagator
pkg/event/envelope.go           # envelope chuẩn spec §7.1
pkg/authz/claims.go, verifier.go
pkg/grpcx/server.go, interceptors.go, dial.go
services/auth/
  go.mod, Dockerfile, README.md, sqlc.yaml
  migrations/000001_init.{up,down}.sql
  internal/config/config.go
  internal/domain/user.go, refresh_token.go, errors.go
  internal/app/ports.go, service.go, register.go, login.go, refresh.go, tokens.go
  internal/adapter/crypto/hasher.go, issuer.go, jwks.go
  internal/adapter/postgres/queries/*.sql, sqlc/ (sinh), tx.go, users.go, tokens.go, outbox.go
  internal/adapter/grpc/handler.go
  internal/adapter/httpx/handler.go          # jwks + healthz/readyz
  internal/bootstrap/bootstrap.go
  cmd/server/main.go, cmd/seed/main.go
  test/integration/*_test.go
services/gateway/
  go.mod, Dockerfile, README.md
  internal/config/config.go
  internal/httperr/httperr.go
  internal/middleware/requestid.go, recovery.go, logging.go, clientip.go, auth.go
  internal/ratelimit/limiter.go, script.lua, middleware.go
  internal/authhttp/handlers.go, cookies.go
  internal/router/router.go
  cmd/gateway/main.go
tools/
  go.mod
  cmd/keygen/main.go
  cmd/openapimerge/main.go, merge.go
  cmd/smoke/main.go
deploy/docker/docker-compose.infra.yml, docker-compose.obs.yml, docker-compose.yml
deploy/docker/init/postgres/01-create-dbs.sql
deploy/docker/otel/collector.yaml, tempo.yaml, grafana-datasources.yaml
.github/workflows/ci-proto.yml, ci-go.yml
docs/adr/0001-monorepo.md … 0004-refresh-token-rotation.md
README.md
```

---

### Task 0: Cài công cụ (thao tác của người dùng)

Máy hiện thiếu Go và make (Docker, Compose v5, git, openssl đã có).

- [ ] **Step 1: Cài Go và make**

Run (PowerShell): `winget install GoLang.Go ; winget install ezwinports.make`
Mở lại terminal.

- [ ] **Step 2: Xác minh**

Run (Git Bash): `go version && make --version | head -1 && docker info --format '{{.ServerVersion}}'`
Expected: in ra 3 phiên bản, không lỗi. Ghi lại phiên bản Go để đặt directive `go` trong các `go.mod`.

---

### Task 1: Monorepo skeleton + proto + codegen

**Files:**
- Create: `go.work`, `Makefile`, `.golangci.yml`, `contracts/proto/buf.yaml`, `contracts/proto/buf.gen.yaml`, `contracts/proto/smarthire/auth/v1/auth.proto`, `gen/go/go.mod`
- Generated: `gen/go/smarthire/auth/v1/{auth.pb.go,auth_grpc.pb.go,auth.pb.gw.go}`, `contracts/openapi/gen/gateway.swagger.json`

**Interfaces:**
- Produces: package `authv1` (`github.com/nuroqzavitex/smarthire/gen/go/smarthire/auth/v1`) với `AuthServiceServer`, `AuthServiceClient`, `RegisterAuthServiceHandlerClient`, các message dưới đây. Full method names `/smarthire.auth.v1.AuthService/<Method>`.

- [ ] **Step 1: Viết `auth.proto`**

```protobuf
syntax = "proto3";
package smarthire.auth.v1;

import "buf/validate/validate.proto";
import "google/api/annotations.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

service AuthService {
  rpc Register(RegisterRequest) returns (RegisterResponse) {
    option (google.api.http) = { post: "/v1/auth/register" body: "*" };
  }
  rpc Login(LoginRequest) returns (TokenPair);
  rpc Refresh(RefreshRequest) returns (TokenPair);
  rpc Logout(LogoutRequest) returns (google.protobuf.Empty);
  rpc GetMe(GetMeRequest) returns (User) {
    option (google.api.http) = { get: "/v1/me" };
  }
}

enum Role { ROLE_UNSPECIFIED = 0; ROLE_CANDIDATE = 1; ROLE_RECRUITER = 2; ROLE_ADMIN = 3; }

message RegisterRequest {
  string email = 1 [(buf.validate.field).string = {email: true, max_len: 254}];
  string password = 2 [(buf.validate.field).string = {min_len: 8, max_len: 128}];
  Role role = 3 [(buf.validate.field).enum = {in: [1, 2]}];
}
message RegisterResponse { string user_id = 1; }
message LoginRequest {
  string email = 1 [(buf.validate.field).string = {min_len: 1, max_len: 254}];
  string password = 2 [(buf.validate.field).string = {min_len: 1, max_len: 128}];
}
message RefreshRequest { string refresh_token = 1 [(buf.validate.field).string.min_len = 1]; }
message LogoutRequest  { string refresh_token = 1; }
message TokenPair { string access_token = 1; int64 expires_in = 2; string refresh_token = 3; }
message GetMeRequest {}
message User { string id = 1; string email = 2; Role role = 3; google.protobuf.Timestamp created_at = 4; }
```

- [ ] **Step 2: Cấu hình buf**

`buf.yaml` (v2): module path `.`, deps `buf.build/googleapis/googleapis`, `buf.build/bufbuild/protovalidate`; lint `STANDARD`; breaking `FILE`.
`buf.gen.yaml` (v2): managed mode `go_package_prefix: github.com/nuroqzavitex/smarthire/gen/go` (except googleapis, protovalidate); remote plugins `buf.build/protocolbuffers/go`, `buf.build/grpc/go`, `buf.build/grpc-ecosystem/gateway` → `out: ../../gen/go`, `opt: paths=source_relative`; `buf.build/grpc-ecosystem/openapiv2` → `out: ../openapi/gen`, `opt: allow_merge=true,merge_file_name=gateway`.

- [ ] **Step 3: go.work, gen/go/go.mod, Makefile, .golangci.yml**

`go.work` liệt kê 5 module (tạo `go.mod` rỗng tối thiểu cho module chưa có code ở task sau thì thêm vào `go.work` ở task đó — ở task này chỉ `./gen/go`).
Makefile targets lúc này: `bootstrap` (go install `github.com/bufbuild/buf/cmd/buf@latest`, `github.com/sqlc-dev/sqlc/cmd/sqlc@latest`, `github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`, `github.com/golang-migrate/migrate/v4/cmd/migrate@latest` với tag `postgres`), `proto` (`cd contracts/proto && buf dep update && buf generate` rồi `cd gen/go && go mod tidy`), `lint` (`buf lint` + `golangci-lint run` lặp qua các module trong `go.work`). Các target khác thêm ở task sở hữu chúng.
`.golangci.yml` (v2): bật `errcheck, govet, staticcheck, unused, ineffassign, gosec, revive, gofmt, goimports`; exclude `gen/`.

- [ ] **Step 4: Sinh code và kiểm tra**

Run: `make bootstrap && make proto && (cd contracts/proto && buf lint) && go build ./gen/go/...`
Expected: không lỗi; tồn tại `gen/go/smarthire/auth/v1/auth.pb.gw.go` và `contracts/openapi/gen/gateway.swagger.json`.

- [ ] **Step 5: Commit**

```bash
git add go.work Makefile .golangci.yml contracts gen
git commit -m "chore: scaffold monorepo, auth proto and codegen"
```

---

### Task 2: `pkg/apperr` + `pkg/config`

**Files:**
- Create: `pkg/go.mod`, `pkg/apperr/apperr.go`, `pkg/apperr/apperr_test.go`, `pkg/config/config.go`, `pkg/config/config_test.go`
- Modify: `go.work` (thêm `./pkg`)

**Interfaces:**
- Produces:
  - `var apperr.ErrNotFound, ErrConflict, ErrValidation, ErrForbidden, ErrUnauthenticated error` (sentinel)
  - `func apperr.New(kind error, msg string) error` — lỗi có `Error()==msg`, `errors.Is(err, kind)==true`
  - `func apperr.ToGRPC(err error) error`
  - `func config.Load(prefix string, dst any) error`

- [ ] **Step 1: Viết test**

```go
func TestToGRPC(t *testing.T) {
	cases := []struct{ in error; code codes.Code; msg string }{
		{apperr.New(apperr.ErrNotFound, "user not found"), codes.NotFound, "user not found"},
		{apperr.New(apperr.ErrConflict, "email taken"), codes.AlreadyExists, "email taken"},
		{apperr.New(apperr.ErrValidation, "bad"), codes.InvalidArgument, "bad"},
		{apperr.New(apperr.ErrForbidden, "no"), codes.PermissionDenied, "no"},
		{apperr.New(apperr.ErrUnauthenticated, "who"), codes.Unauthenticated, "who"},
		{fmt.Errorf("wrap: %w", apperr.New(apperr.ErrNotFound, "x")), codes.NotFound, "x"},
		{context.DeadlineExceeded, codes.DeadlineExceeded, "deadline exceeded"},
		{status.Error(codes.Unavailable, "down"), codes.Unavailable, "down"},
		{errors.New("pq: secret detail"), codes.Internal, "internal error"}, // không lộ chi tiết
	}
	for _, c := range cases {
		st, _ := status.FromError(apperr.ToGRPC(c.in))
		assert.Equal(t, c.code, st.Code()); assert.Equal(t, c.msg, st.Message())
	}
	assert.Nil(t, apperr.ToGRPC(nil))
}

func TestLoad(t *testing.T) {
	type C struct {
		Port int    `envconfig:"GRPC_PORT" default:"9001"`
		DSN  string `envconfig:"DB_DSN" required:"true"`
	}
	t.Setenv("X_DB_DSN", "postgres://x")
	var c C
	require.NoError(t, config.Load("X", &c))
	assert.Equal(t, 9001, c.Port); assert.Equal(t, "postgres://x", c.DSN)
}

func TestLoad_MissingRequired(t *testing.T) {
	type C struct{ DSN string `envconfig:"DB_DSN" required:"true"` }
	var c C
	err := config.Load("Y", &c)
	require.Error(t, err); assert.Contains(t, err.Error(), "Y_DB_DSN")
}
```

- [ ] **Step 2: Run** `cd pkg && go test ./apperr/... ./config/...` — Expected: FAIL (package chưa có).

- [ ] **Step 3: Implement**

`apperr`: struct không export `{kind error; msg string}` với `Unwrap() error { return kind }`. `ToGRPC`: nil→nil; nếu đã là gRPC status (`status.FromError` ok và không phải `codes.Unknown`) → trả nguyên; `errors.Is` theo thứ tự sentinel; `context.DeadlineExceeded`→`DeadlineExceeded`, `context.Canceled`→`Canceled`; còn lại `codes.Internal, "internal error"`.
`config.Load`: `envconfig.Process(prefix, dst)`; lỗi wrap `fmt.Errorf("config: %w", err)` (envconfig đã nêu tên biến).

- [ ] **Step 4: Run** lại — Expected: PASS.

- [ ] **Step 5: Commit** `feat(pkg): add apperr and config packages`

---

### Task 3: `pkg/logger` + `pkg/otel` + `pkg/event`

**Files:**
- Create: `pkg/logger/logger.go`, `pkg/logger/logger_test.go`, `pkg/otel/otel.go`, `pkg/otel/otel_test.go`, `pkg/event/envelope.go`, `pkg/event/envelope_test.go`

**Interfaces:**
- Produces:
  - `func logger.New(service string, w io.Writer, level slog.Level) *slog.Logger` — JSON; mỗi record có `service`; khi gọi `*Context(ctx, ...)` tự thêm `trace_id`, `span_id` (từ span hợp lệ), `request_id`, `user_id` (nếu có trong ctx)
  - `func logger.WithRequestID(ctx, id string) context.Context`, `logger.RequestIDFrom(ctx) string`, `logger.WithUserID(ctx, id string) context.Context`
  - `func otel.Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error)` — luôn set propagator `TraceContext+Baggage`; nếu env `OTEL_EXPORTER_OTLP_ENDPOINT` rỗng → không tạo exporter, trả shutdown no-op; ngược lại OTLP gRPC exporter + batch span processor, resource `service.name`
  - `type event.Envelope struct{ EventID, EventType string; EventVersion int; Source string; OccurredAt time.Time; TraceID, CorrelationID string; Payload json.RawMessage }` (JSON tag snake_case như spec §7.1)
  - `func event.New(ctx context.Context, source, eventType string, version int, payload any, now time.Time) (Envelope, error)` — `EventID` = ULID, `TraceID` từ span ctx, `CorrelationID` = `logger.RequestIDFrom(ctx)`, `OccurredAt` = `now.UTC()`

- [ ] **Step 1: Viết test**

```go
func TestLogger_AddsContextFields(t *testing.T) {
	var buf bytes.Buffer
	l := logger.New("auth", &buf, slog.LevelInfo)
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	ctx = logger.WithUserID(logger.WithRequestID(ctx, "req-1"), "u-1")
	l.InfoContext(ctx, "hello"); span.End()
	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Equal(t, "auth", m["service"]); assert.Equal(t, "req-1", m["request_id"])
	assert.Equal(t, "u-1", m["user_id"]); assert.Equal(t, span.SpanContext().TraceID().String(), m["trace_id"])
}

func TestSetup_NoEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := otel.Setup(context.Background(), "x")
	require.NoError(t, err); require.NoError(t, shutdown(context.Background()))
	assert.Contains(t, otelapi.GetTextMapPropagator().Fields(), "traceparent")
}

func TestEnvelope_New(t *testing.T) {
	now := time.Date(2026, 11, 8, 16, 0, 0, 0, time.FixedZone("ICT", 7*3600))
	ctx := logger.WithRequestID(context.Background(), "req-9")
	e, err := event.New(ctx, "auth-service", "user.registered", 1, map[string]string{"user_id": "u"}, now)
	require.NoError(t, err)
	b, _ := json.Marshal(e)
	var m map[string]any; _ = json.Unmarshal(b, &m)
	assert.Len(t, m["event_id"], 26)
	assert.Equal(t, "user.registered", m["event_type"]); assert.EqualValues(t, 1, m["event_version"])
	assert.Equal(t, "2026-11-08T09:00:00Z", m["occurred_at"]); assert.Equal(t, "req-9", m["correlation_id"])
	assert.Equal(t, map[string]any{"user_id": "u"}, m["payload"])
}
```

- [ ] **Step 2: Run** `cd pkg && go test ./logger/... ./otel/... ./event/...` — Expected: FAIL.
- [ ] **Step 3: Implement** — logger: `slog.Handler` bọc `slog.NewJSONHandler`, override `Handle` để `AddAttrs` từ ctx; ctx keys là kiểu không export. event: `github.com/oklog/ulid/v2` (`ulid.Make()`).
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(pkg): add logger, otel bootstrap and event envelope`

---

### Task 4: `pkg/authz`

**Files:**
- Create: `pkg/authz/claims.go`, `pkg/authz/verifier.go`, `pkg/authz/authz_test.go`, `pkg/authz/authztest/authztest.go`

**Interfaces:**
- Produces:
  - `type Role string`; `const RoleCandidate Role = "candidate"; RoleRecruiter = "recruiter"; RoleAdmin = "admin"`
  - `type Claims struct{ Subject string; Role Role; TokenID string; ExpiresAt time.Time }`
  - `func WithClaims(ctx, Claims) context.Context`; `func ClaimsFrom(ctx) (Claims, bool)`
  - `func RequireRole(ctx context.Context, roles ...Role) error` — không có claims → `apperr.ErrUnauthenticated`; role không thuộc `roles` → `apperr.ErrForbidden`; `roles` rỗng = chỉ cần đăng nhập
  - `func BearerToken(header string) (string, bool)` — chấp nhận `Bearer <t>` không phân biệt hoa/thường ở scheme
  - `type Verifier struct`; `func NewVerifier(ctx context.Context, jwksURL, issuer, audience string) (*Verifier, error)`; `func (v *Verifier) Verify(raw string) (Claims, error)` — mọi lỗi → `apperr.New(apperr.ErrUnauthenticated, "invalid token")`
  - Test helper `authztest.NewIssuer(t) *Issuer` với `(*Issuer).JWKSURL() string` (httptest server), `(*Issuer).Sign(t, sub string, role Role, ttl time.Duration, mods ...func(jwt.MapClaims)) string`, hằng `authztest.Issuer = "smarthire-auth"`, `authztest.Audience = "smarthire-api"`

- [ ] **Step 1: Viết test**

```go
func TestVerify(t *testing.T) {
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(context.Background(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)

	c, err := v.Verify(is.Sign(t, "u1", authz.RoleRecruiter, time.Minute))
	require.NoError(t, err)
	assert.Equal(t, "u1", c.Subject); assert.Equal(t, authz.RoleRecruiter, c.Role)

	for name, tok := range map[string]string{
		"expired":   is.Sign(t, "u1", authz.RoleCandidate, -time.Minute),
		"wrong aud": is.Sign(t, "u1", authz.RoleCandidate, time.Minute, func(m jwt.MapClaims) { m["aud"] = "x" }),
		"wrong iss": is.Sign(t, "u1", authz.RoleCandidate, time.Minute, func(m jwt.MapClaims) { m["iss"] = "x" }),
		"other key": authztest.NewIssuer(t).Sign(t, "u1", authz.RoleCandidate, time.Minute),
		"garbage":   "abc.def.ghi",
	} {
		_, err := v.Verify(tok)
		assert.ErrorIs(t, err, apperr.ErrUnauthenticated, name)
	}
}

func TestVerify_RejectsHS256(t *testing.T) { /* token ký HS256 với kid hợp lệ → ErrUnauthenticated */ }

func TestRequireRole(t *testing.T) {
	assert.ErrorIs(t, authz.RequireRole(context.Background()), apperr.ErrUnauthenticated)
	ctx := authz.WithClaims(context.Background(), authz.Claims{Subject: "u", Role: authz.RoleCandidate})
	assert.NoError(t, authz.RequireRole(ctx))
	assert.ErrorIs(t, authz.RequireRole(ctx, authz.RoleRecruiter), apperr.ErrForbidden)
}

func TestBearerToken(t *testing.T) {
	tok, ok := authz.BearerToken("bearer abc"); assert.True(t, ok); assert.Equal(t, "abc", tok)
	_, ok = authz.BearerToken("Basic abc"); assert.False(t, ok)
	_, ok = authz.BearerToken("Bearer "); assert.False(t, ok)
}
```

- [ ] **Step 2: Run** `cd pkg && go test ./authz/...` — Expected: FAIL.
- [ ] **Step 3: Implement** — `github.com/MicahParks/keyfunc/v3` (`keyfunc.NewDefaultCtx`, tự refresh khi gặp `kid` lạ); `jwt.Parse` với `jwt.WithValidMethods([]string{"RS256"})`, `jwt.WithIssuer`, `jwt.WithAudience`, `jwt.WithExpirationRequired()`, `jwt.WithLeeway(30*time.Second)`. Claim tuỳ biến `role`. `authztest` sinh RSA 2048 + phục vụ JWKS `{"keys":[{kty,kid,use:"sig",alg:"RS256",n,e}]}`.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(pkg): add authz JWT verifier and RBAC helpers`

---

### Task 5: `pkg/grpcx`

**Files:**
- Create: `pkg/grpcx/server.go`, `pkg/grpcx/interceptors.go`, `pkg/grpcx/dial.go`, `pkg/grpcx/grpcx_test.go`

**Interfaces:**
- Consumes: `apperr.ToGRPC`, `authz.Verifier`, `authz.WithClaims`, `authz.BearerToken`, `logger.WithRequestID`, `logger.WithUserID`
- Produces:
  - `type TokenVerifier interface{ Verify(raw string) (authz.Claims, error) }` (`*authz.Verifier` thoả mãn)
  - `type ServerConfig struct{ Logger *slog.Logger; Verifier TokenVerifier; PublicMethods []string }`
  - `func NewServer(cfg ServerConfig) (*grpc.Server, *health.Server)` — stats handler `otelgrpc.NewServerHandler()`; unary chain theo thứ tự: recovery → requestID (đọc metadata `x-request-id`) → logging → errorMapping (`apperr.ToGRPC` trên lỗi handler) → auth → protovalidate (`go-grpc-middleware/v2/interceptors/protovalidate`); đăng ký `grpc_health_v1`
  - `func Serve(ctx context.Context, srv *grpc.Server, lis net.Listener) error` — chặn tới khi ctx huỷ, rồi `GracefulStop`, ép `Stop` sau 30s
  - `func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)` — `grpc.NewClient`, insecure creds, `otelgrpc.NewClientHandler()`, unary interceptor đặt timeout 3s nếu ctx chưa có deadline
  - Auth interceptor: method trong `PublicMethods` → nếu có token hợp lệ thì gắn claims, không có thì cho qua; method khác → thiếu/sai token trả `codes.Unauthenticated`. Gắn `logger.WithUserID(ctx, claims.Subject)`.

- [ ] **Step 1: Viết test** — dùng `bufconn` + health service làm "private" method và một server test nhỏ (đăng ký `authv1.AuthServiceServer` giả từ `gen/go`, `Register` public, `GetMe` private, trả về `Subject` trong `User.Id`):

```go
func TestAuthInterceptor_RejectsMissingTokenOnPrivate(t *testing.T) {
	c := newTestClient(t) // bufconn, PublicMethods: ["/smarthire.auth.v1.AuthService/Register"]
	_, err := c.GetMe(context.Background(), &authv1.GetMeRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	_, err = c.GetMe(withBearer("forged"), &authv1.GetMeRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
func TestAuthInterceptor_SetsClaims(t *testing.T) {
	u, err := c.GetMe(withBearer(is.Sign(t, "u42", authz.RoleCandidate, time.Minute)), &authv1.GetMeRequest{})
	require.NoError(t, err); assert.Equal(t, "u42", u.Id)
}
func TestPublicMethod_NoToken_OK(t *testing.T)            { /* Register hợp lệ không token → OK */ }
func TestValidation_InvalidArgument(t *testing.T)         { /* Register email "x" → codes.InvalidArgument */ }
func TestErrorMapping(t *testing.T)                       { /* handler trả apperr.New(ErrConflict,"dup") → AlreadyExists "dup" */ }
func TestRecovery(t *testing.T)                           { /* handler panic → codes.Internal, server vẫn sống */ }
func TestDial_DefaultTimeout(t *testing.T)                { /* handler sleep 5s → client nhận DeadlineExceeded sau ~3s */ }
func TestServe_GracefulOnCancel(t *testing.T)             { /* cancel ctx → Serve trả nil trong < 1s khi không có request */ }
```

- [ ] **Step 2: Run** `cd pkg && go test ./grpcx/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces. Test đặt ở package `grpcx_test`; `gen/go` chỉ được import trong test (resolve qua `go.work`), code non-test của `pkg/` không import `gen/go`.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(pkg): add grpcx server, interceptors and dial helper`

---

### Task 6: Auth domain + register/login use cases

**Files:**
- Create: `services/auth/go.mod`, `internal/domain/{user.go,refresh_token.go,errors.go,domain_test.go}`, `internal/app/{ports.go,service.go,register.go,login.go,tokens.go,app_test.go,fakes_test.go}`
- Modify: `go.work` (thêm `./services/auth`)

**Interfaces:**
- Produces (`domain`):
  - `func NewEmail(s string) (string, error)` — `strings.TrimSpace` + `strings.ToLower`, kiểm tra `net/mail.ParseAddress` và địa chỉ parse ra == input, ≤254; lỗi `apperr.ErrValidation`
  - `func ValidatePassword(p string) error` — 8 ≤ `utf8.RuneCountInString` ≤ 128; lỗi `apperr.ErrValidation`
  - `type UserStatus string`; `const StatusActive UserStatus = "ACTIVE"; StatusLocked = "LOCKED"; StatusDeletionPending = "DELETION_PENDING"`
  - `type User struct{ ID uuid.UUID; Email, PasswordHash string; Role authz.Role; Status UserStatus; CreatedAt, UpdatedAt time.Time }` (user mới luôn `StatusActive`)
  - `func (u User) CanAuthenticate() error` — `ACTIVE` → nil; `LOCKED` → `ErrAccountLocked`; `DELETION_PENDING` → `ErrAccountPendingDeletion`
  - `type RefreshToken struct{ ID, UserID, FamilyID uuid.UUID; Hash []byte; ExpiresAt, CreatedAt time.Time; RevokedAt *time.Time; ReplacedBy *uuid.UUID }`
  - `var ErrInvalidCredentials = apperr.New(apperr.ErrUnauthenticated, "invalid email or password")`, `ErrInvalidRefreshToken = apperr.New(apperr.ErrUnauthenticated, "invalid refresh token")`, `ErrAccountLocked = apperr.New(apperr.ErrForbidden, "account is locked")`, `ErrAccountPendingDeletion = apperr.New(apperr.ErrForbidden, "account is pending deletion")`
- Produces (`app`, file `ports.go`):
  ```go
  type TxManager interface{ WithTx(ctx context.Context, fn func(ctx context.Context) error) error }
  type UserRepo interface {
  	Create(ctx context.Context, u domain.User) error            // trùng email → apperr.ErrConflict
  	GetByEmail(ctx context.Context, email string) (domain.User, error) // không có → apperr.ErrNotFound
  	GetByID(ctx context.Context, id uuid.UUID) (domain.User, error)
  }
  type RefreshTokenRepo interface {
  	Create(ctx context.Context, t domain.RefreshToken) error
  	GetByHashForUpdate(ctx context.Context, hash []byte) (domain.RefreshToken, error) // ErrNotFound
  	Revoke(ctx context.Context, id uuid.UUID, at time.Time, replacedBy *uuid.UUID) error
  	RevokeFamily(ctx context.Context, familyID uuid.UUID, at time.Time) error // chỉ token chưa revoke
  }
  type OutboxWriter interface{ Write(ctx context.Context, e event.Envelope, aggregateID string) error }
  type PasswordHasher interface{ Hash(pw string) (string, error); Verify(pw, hash string) (bool, error) }
  type TokenIssuer interface{ IssueAccess(u domain.User, now time.Time) (token string, ttl time.Duration, err error) }
  ```
- Produces (`app`, `service.go`):
  ```go
  type Deps struct{ Users UserRepo; Tokens RefreshTokenRepo; Outbox OutboxWriter; Tx TxManager
  	Hasher PasswordHasher; Issuer TokenIssuer; Now func() time.Time; RefreshTTL time.Duration; Logger *slog.Logger }
  func NewService(d Deps) (*Service, error) // tính sẵn dummyHash = Hasher.Hash("dummy-password-for-timing")
  type TokenPair struct{ AccessToken string; ExpiresIn time.Duration; RefreshToken string }
  func (s *Service) Register(ctx context.Context, email, password string, role authz.Role) (uuid.UUID, error) // role ∉ {candidate, recruiter} → ErrValidation
  func (s *Service) CreateUser(ctx context.Context, email, password string, role authz.Role) (uuid.UUID, error) // cho phép admin; dùng bởi seed
  func (s *Service) Login(ctx context.Context, email, password string) (TokenPair, error)
  ```
  - `tokens.go`: `func newRefreshSecret() (raw string, hash []byte, err error)`, `func hashRefresh(raw string) []byte` (sha256)
  - `CreateUser` trong một `WithTx`: `Users.Create` + `Outbox.Write(event.New(ctx, "auth-service", "user.registered", 1, {user_id, email, role}, now), userID)`
  - `Login`: email không chuẩn hoá được hoặc không tồn tại → vẫn `Hasher.Verify(password, dummyHash)` rồi trả `ErrInvalidCredentials`; mật khẩu sai → `ErrInvalidCredentials`; mật khẩu đúng nhưng `u.CanAuthenticate()` lỗi → trả lỗi đó (chỉ kiểm tra **sau** mật khẩu để không lộ trạng thái cho người dò); đúng → `issuePair(ctx, user, uuid.New() /*family*/)` (helper tạo access + refresh, `Tokens.Create`)

- [ ] **Step 1: Viết test** (fakes in-memory trong `fakes_test.go`; `fakeTx.WithTx` chỉ gọi `fn(ctx)`; fake hasher `"h:"+pw`)

```go
func TestNewEmail_Normalizes(t *testing.T) {
	e, err := domain.NewEmail("  Foo@X.com "); require.NoError(t, err); assert.Equal(t, "foo@x.com", e)
	for _, bad := range []string{"", "x", "a@", "Foo <foo@x.com>", strings.Repeat("a", 250) + "@x.com"} {
		_, err := domain.NewEmail(bad); assert.ErrorIs(t, err, apperr.ErrValidation, bad)
	}
}
func TestValidatePassword(t *testing.T) {
	assert.ErrorIs(t, domain.ValidatePassword("1234567"), apperr.ErrValidation)
	assert.NoError(t, domain.ValidatePassword("12345678"))
	assert.NoError(t, domain.ValidatePassword(strings.Repeat("é", 128)))
	assert.ErrorIs(t, domain.ValidatePassword(strings.Repeat("a", 129)), apperr.ErrValidation)
}
func TestRegister_WritesUserAndOutbox(t *testing.T) {
	id, err := svc.Register(ctx, "A@x.com", "password1", authz.RoleCandidate)
	require.NoError(t, err)
	u := users.byEmail["a@x.com"]; assert.Equal(t, id, u.ID); assert.Equal(t, "h:password1", u.PasswordHash)
	require.Len(t, outbox.events, 1); assert.Equal(t, "user.registered", outbox.events[0].EventType)
	assert.JSONEq(t, fmt.Sprintf(`{"user_id":%q,"email":"a@x.com","role":"candidate"}`, id), string(outbox.events[0].Payload))
}
func TestRegister_RejectsAdminRole(t *testing.T) { /* RoleAdmin → ErrValidation; CreateUser với RoleAdmin → OK */ }
func TestRegister_Duplicate(t *testing.T)       { /* lần 2 cùng email khác hoa thường → ErrConflict, outbox vẫn 1 event */ }
func TestLogin_OK(t *testing.T) {
	p, err := svc.Login(ctx, "a@x.com", "password1")
	require.NoError(t, err)
	assert.Equal(t, 15*time.Minute, p.ExpiresIn); assert.Len(t, p.RefreshToken, 43) // 32 byte RawURL
	stored := tokens.all()[0]; assert.Equal(t, sha256Sum(p.RefreshToken), stored.Hash)
	assert.Equal(t, now.Add(7*24*time.Hour), stored.ExpiresAt)
}
func TestLogin_InactiveAccount(t *testing.T) {
	users.setStatus("a@x.com", domain.StatusLocked)
	_, err := svc.Login(ctx, "a@x.com", "wrong-pass"); assert.ErrorIs(t, err, apperr.ErrUnauthenticated) // sai mật khẩu: không lộ trạng thái
	_, err = svc.Login(ctx, "a@x.com", "password1"); assert.ErrorIs(t, err, domain.ErrAccountLocked)
	users.setStatus("a@x.com", domain.StatusDeletionPending)
	_, err = svc.Login(ctx, "a@x.com", "password1"); assert.ErrorIs(t, err, domain.ErrAccountPendingDeletion)
	assert.Empty(t, tokens.all()) // không phát token
}
func TestLogin_UnknownEmailAndWrongPasswordSameError(t *testing.T) {
	_, e1 := svc.Login(ctx, "nobody@x.com", "password1")
	_, e2 := svc.Login(ctx, "a@x.com", "wrong-pass")
	assert.Equal(t, e1.Error(), e2.Error()); assert.ErrorIs(t, e1, apperr.ErrUnauthenticated)
	assert.Equal(t, 1, hasher.verifyCallsFor("nobody")) // vẫn chạy verify với dummy hash
}
```

- [ ] **Step 2: Run** `cd services/auth && go test ./internal/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo Interfaces.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(auth): add domain model and register/login use cases`

---

### Task 7: Refresh/Logout/GetMe use cases

**Files:**
- Create: `services/auth/internal/app/refresh.go`, `services/auth/internal/app/refresh_test.go`

**Interfaces:**
- Consumes: ports Task 6
- Produces:
  ```go
  func (s *Service) Refresh(ctx context.Context, raw string) (TokenPair, error)
  func (s *Service) Logout(ctx context.Context, raw string) error   // không tìm thấy / đã revoke → nil
  func (s *Service) GetMe(ctx context.Context, userID uuid.UUID) (domain.User, error)
  ```
- **Thuật toán Refresh** (quyết định quan trọng — revoke family phải được commit dù trả lỗi):
  ```go
  var pair TokenPair; var reuse bool; var famID, userID uuid.UUID
  err := s.tx.WithTx(ctx, func(ctx context.Context) error {
  	t, err := s.tokens.GetByHashForUpdate(ctx, hashRefresh(raw))
  	if errors.Is(err, apperr.ErrNotFound) { return domain.ErrInvalidRefreshToken }
  	if err != nil { return err }
  	now := s.now()
  	if t.RevokedAt != nil { reuse, famID, userID = true, t.FamilyID, t.UserID
  		return s.tokens.RevokeFamily(ctx, t.FamilyID, now) }          // trả nil → COMMIT
  	if !now.Before(t.ExpiresAt) { return domain.ErrInvalidRefreshToken }
  	u, err := s.users.GetByID(ctx, t.UserID); if err != nil { return err }
  	if u.CanAuthenticate() != nil { return domain.ErrInvalidRefreshToken }  // không ACTIVE → 401, không revoke family
  	var newID uuid.UUID
  	pair, newID, err = s.issuePair(ctx, u, t.FamilyID)                // gán biến ngoài, KHÔNG dùng :=
  	if err != nil { return err }
  	return s.tokens.Revoke(ctx, t.ID, now, &newID)
  })
  if err != nil { return TokenPair{}, err }
  if reuse { s.log.WarnContext(ctx, "refresh token reuse detected", "event", "refresh_token_reuse",
  	"user_id", userID, "family_id", famID); return TokenPair{}, domain.ErrInvalidRefreshToken }
  return pair, nil
  ```
  (Điều chỉnh `issuePair` từ Task 6 trả thêm ID token mới: `issuePair(ctx, u, familyID) (TokenPair, uuid.UUID, error)`.)

- [ ] **Step 1: Viết test**

```go
func TestRefresh_Rotates(t *testing.T) {
	p1 := login(t); p2, err := svc.Refresh(ctx, p1.RefreshToken)
	require.NoError(t, err); assert.NotEqual(t, p1.RefreshToken, p2.RefreshToken)
	old := tokens.byHash(p1.RefreshToken); assert.NotNil(t, old.RevokedAt)
	nw := tokens.byHash(p2.RefreshToken); assert.Equal(t, old.FamilyID, nw.FamilyID); assert.Equal(t, nw.ID, *old.ReplacedBy)
}
func TestRefresh_ReuseRevokesFamily(t *testing.T) {
	p1 := login(t); p2, _ := svc.Refresh(ctx, p1.RefreshToken)
	_, err := svc.Refresh(ctx, p1.RefreshToken)
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated); assert.True(t, tx.lastCommitted) // fn trả nil → commit
	assert.NotNil(t, tokens.byHash(p2.RefreshToken).RevokedAt)
	_, err = svc.Refresh(ctx, p2.RefreshToken); assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}
func TestRefresh_Expired(t *testing.T)  { /* clock += 7*24h → ErrUnauthenticated; token không bị revoke family */ }
func TestRefresh_Unknown(t *testing.T)  { /* "nope" → ErrUnauthenticated */ }
func TestRefresh_InactiveUser(t *testing.T) { /* login, rồi setStatus LOCKED → Refresh trả ErrUnauthenticated; token không bị revoke family */ }
func TestLogout(t *testing.T)           { /* revoke; refresh sau đó → ErrUnauthenticated; Logout("nope") → nil; Logout 2 lần → nil */ }
func TestGetMe_NotFound(t *testing.T)   { /* uuid.New() → ErrNotFound */ }
```
`fakeTx` ghi `lastCommitted = (fn trả nil)` sau mỗi lần `WithTx`.

- [ ] **Step 2: Run** `cd services/auth && go test ./internal/app/...` — Expected: FAIL.
- [ ] **Step 3: Implement** theo thuật toán trên.
- [ ] **Step 4: Run** + coverage: `go test -cover ./internal/domain/... ./internal/app/...` — Expected: PASS, coverage ≥ 70% mỗi package.
- [ ] **Step 5: Commit** `feat(auth): add refresh rotation with reuse detection, logout, get-me`

---

### Task 8: Auth Postgres adapter + migrations

**Files:**
- Create: `services/auth/migrations/000001_init.up.sql`, `000001_init.down.sql`, `services/auth/sqlc.yaml`, `internal/adapter/postgres/queries/{users,refresh_tokens,outbox}.sql`, `internal/adapter/postgres/{tx.go,users.go,tokens.go,outbox.go}`, `services/auth/test/integration/{main_test.go,postgres_test.go}`
- Generated: `internal/adapter/postgres/sqlc/*`
- Modify: `Makefile` (thêm `sqlc`, `test`, `test-int`, `cover-auth`)

**Interfaces:**
- Consumes: ports Task 6/7
- Produces:
  - `func postgres.NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error)` (gắn `otelpgx` tracer)
  - `func postgres.NewTxManager(pool *pgxpool.Pool) *TxManager` (implements `app.TxManager`; tx lưu trong ctx; repo dùng tx nếu có, không thì pool)
  - `func postgres.NewUserRepo(pool) *UserRepo`, `NewRefreshTokenRepo(pool) *RefreshTokenRepo`, `NewOutboxWriter(pool) *OutboxWriter`
- Migration up:
  ```sql
  CREATE EXTENSION IF NOT EXISTS citext;
  CREATE TABLE users (id uuid PRIMARY KEY, email citext NOT NULL UNIQUE, password_hash text NOT NULL,
    role text NOT NULL CHECK (role IN ('candidate','recruiter','admin')),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','LOCKED','DELETION_PENDING')),
    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
  CREATE TABLE refresh_tokens (id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    family_id uuid NOT NULL, token_hash bytea NOT NULL UNIQUE, expires_at timestamptz NOT NULL,
    revoked_at timestamptz, replaced_by uuid, created_at timestamptz NOT NULL DEFAULT now());
  CREATE INDEX refresh_tokens_family_idx ON refresh_tokens(family_id) WHERE revoked_at IS NULL;
  CREATE TABLE outbox (id uuid PRIMARY KEY, aggregate_id text NOT NULL, event_type text NOT NULL,
    payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz);
  CREATE INDEX outbox_unpublished_idx ON outbox(created_at) WHERE published_at IS NULL;
  ```
  `outbox.id` = `uuid.New()` (khác `event_id` ULID nằm trong payload); `payload` = toàn bộ envelope JSON.
- Mapping lỗi: `pgx.ErrNoRows` → `apperr.ErrNotFound`; pg code `23505` → `apperr.New(apperr.ErrConflict, "email already registered")`.

- [ ] **Step 1: Viết integration test** (`//go:build integration`; `main_test.go` khởi động `postgres:16-alpine` bằng testcontainers, chạy migration qua `golang-migrate` library, truncate giữa các test)

```go
func TestUserRepo_DuplicateEmailCaseInsensitive(t *testing.T) {
	require.NoError(t, users.Create(ctx, user("a@x.com")))
	assert.ErrorIs(t, users.Create(ctx, user("A@X.com")), apperr.ErrConflict)
	u, err := users.GetByEmail(ctx, "A@x.COM"); require.NoError(t, err); assert.Equal(t, "a@x.com", u.Email)
}
func TestRegister_AtomicWithOutbox(t *testing.T) {
	// outbox giả lỗi sau khi user được insert → không có user nào trong DB
}
func TestRefresh_ReuseRevokesFamilyPersisted(t *testing.T) {
	p1 := login(t); p2, _ := svc.Refresh(ctx, p1.RefreshToken)
	_, err := svc.Refresh(ctx, p1.RefreshToken); require.ErrorIs(t, err, apperr.ErrUnauthenticated)
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`).Scan(&n))
	assert.Equal(t, 0, n); _ = p2
}
func TestRefresh_ConcurrentSameToken(t *testing.T) {
	p1 := login(t); var wg sync.WaitGroup; errs := make([]error, 2)
	for i := range 2 { wg.Add(1); go func() { defer wg.Done(); _, errs[i] = svc.Refresh(ctx, p1.RefreshToken) }() }
	wg.Wait()
	ok := 0; for _, e := range errs { if e == nil { ok++ } }
	assert.Equal(t, 1, ok)
	var active int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM refresh_tokens WHERE revoked_at IS NULL`).Scan(&active)
	assert.Equal(t, 0, active) // request thứ hai phát hiện dùng lại → cả family bị thu hồi
}
func TestMigrations_UpDown(t *testing.T) { /* up → down → up không lỗi */ }
```
`svc` ở đây là `app.Service` thật nối với repo Postgres + hasher/issuer giả nhanh.

- [ ] **Step 2: Run** `make test-int` — Expected: FAIL (chưa có adapter).
- [ ] **Step 3: Implement** — viết queries sqlc (`-- name: GetRefreshTokenByHashForUpdate :one ... FOR UPDATE`), `make sqlc`, rồi repo. Makefile: `test` = `go test ./...` trong mỗi module; `test-int` = `go test -tags integration ./...` trong `services/*`; `cover-auth` = `go test -coverprofile=coverage.out ./internal/domain/... ./internal/app/...` + kiểm tra tổng ≥ 70% bằng `go tool cover -func | awk`.
- [ ] **Step 4: Run** `make test-int` — Expected: PASS.
- [ ] **Step 5: Commit** `feat(auth): add postgres adapter, migrations and integration tests`

---

### Task 9: Auth crypto adapters + gRPC/HTTP adapters + bootstrap + seed

**Files:**
- Create: `internal/adapter/crypto/{hasher.go,issuer.go,jwks.go,crypto_test.go}`, `internal/adapter/grpc/{handler.go,handler_test.go}`, `internal/adapter/httpx/{handler.go,handler_test.go}`, `internal/config/config.go`, `internal/bootstrap/bootstrap.go`, `cmd/server/main.go`, `cmd/seed/main.go`, `services/auth/Dockerfile`

**Interfaces:**
- Consumes: `app.Service` (Task 6–7), postgres adapter (Task 8), `grpcx.NewServer/Serve`, `authz`, `otel.Setup`, `logger.New`, `config.Load`
- Produces:
  - `crypto.NewHasher(p argon2id.Params) *Hasher` (`alexedwards/argon2id`; mặc định `Memory: 19456, Iterations: 2, Parallelism: 1, SaltLength: 16, KeyLength: 32` — OWASP)
  - `crypto.LoadKeys(currentPath string, extraPaths ...string) (*KeySet, error)`; `kid` = base64url(sha256(DER public key))[:16]
  - `crypto.NewIssuer(keys *KeySet, issuer, audience string, ttl time.Duration) *Issuer` (implements `app.TokenIssuer`; claims `sub, role, iss, aud, exp, iat, jti`, header `kid`)
  - `crypto.JWKSHandler(keys *KeySet) http.Handler` — `Cache-Control: public, max-age=300`
  - `grpc.NewHandler(svc *app.Service) authv1.AuthServiceServer`; `PublicMethods = []string{".../Register", ".../Login", ".../Refresh", ".../Logout"}`
  - `httpx.NewHandler(keys, readyCheck func(ctx) error) http.Handler` — `/.well-known/jwks.json`, `/healthz` (200), `/readyz` (200/503)
  - Config (prefix `AUTH`): `GRPC_PORT=9001`, `HTTP_PORT=8001`, `DB_DSN` (required), `JWT_PRIVATE_KEY_PATH` (required), `JWT_EXTRA_PUBLIC_KEY_PATHS` (list), `JWT_ISSUER=smarthire-auth`, `JWT_AUDIENCE=smarthire-api`, `ACCESS_TTL=15m`, `REFRESH_TTL=168h`, `JWKS_URL=http://localhost:8001/.well-known/jwks.json` (để server tự verify token cho `GetMe`), `ARGON2_MEMORY_KIB=19456`, `ARGON2_ITERATIONS=2`, `ARGON2_PARALLELISM=1`, `LOG_LEVEL=info`
  - Seed (env `SEED_ADMIN_EMAIL`, `SEED_ADMIN_PASSWORD` required; tạo thêm `candidate@smarthire.local` / `recruiter@smarthire.local` mật khẩu `Password123!`): gọi `svc.CreateUser`; `ErrConflict` → bỏ qua (idempotent)

- [ ] **Step 1: Viết test**

```go
func TestIssuer_TokenVerifiesAgainstJWKS(t *testing.T) {
	keys := genKeySet(t); iss := crypto.NewIssuer(keys, "smarthire-auth", "smarthire-api", 15*time.Minute)
	srv := httptest.NewServer(crypto.JWKSHandler(keys))
	tok, ttl, err := iss.IssueAccess(domain.User{ID: uid, Role: authz.RoleRecruiter}, time.Now())
	require.NoError(t, err); assert.Equal(t, 15*time.Minute, ttl)
	v, _ := authz.NewVerifier(ctx, srv.URL, "smarthire-auth", "smarthire-api")
	c, err := v.Verify(tok); require.NoError(t, err); assert.Equal(t, uid.String(), c.Subject)
}
func TestJWKS_IncludesExtraKeys(t *testing.T) { /* LoadKeys(cur, old) → JWKS có 2 key, kid khác nhau */ }
func TestHasher_RoundTrip(t *testing.T)    { /* Hash → Verify true; sai mật khẩu → false; hash bắt đầu "$argon2id$" */ }
func TestGRPCHandler_GetMe_UsesClaims(t *testing.T) { /* ctx có claims sub=uid → trả User với role enum đúng */ }
func TestGRPCHandler_Login_MapsTokenPair(t *testing.T) { /* expires_in = 900 */ }
func TestReadyz_503WhenDBDown(t *testing.T) { /* readyCheck trả lỗi → 503 */ }
```

- [ ] **Step 2: Run** `cd services/auth && go test ./internal/...` — Expected: FAIL.
- [ ] **Step 3: Implement** adapters; `bootstrap.Run(ctx, cfg) error` wiring: logger → otel → pool → repos → service → gRPC server (`grpcx.NewServer` với verifier tới chính JWKS của mình, khởi tạo **sau** khi HTTP server đã listen) + HTTP server; `signal.NotifyContext(SIGINT, SIGTERM)`; shutdown 30s. Dockerfile multi-stage build từ root repo (context `.`, `go build -trimpath -ldflags="-s -w"` cả `server` và `seed`) → `gcr.io/distroless/static:nonroot`.
- [ ] **Step 4: Run** `go test ./internal/... && go build ./cmd/... && docker build -f services/auth/Dockerfile .` — Expected: PASS, image build thành công.
- [ ] **Step 5: Commit** `feat(auth): add crypto, grpc and http adapters, bootstrap, seed`

---

### Task 10: Gateway nền: config, error envelope, middleware cơ bản

**Files:**
- Create: `services/gateway/go.mod`, `internal/config/config.go`, `internal/httperr/{httperr.go,httperr_test.go}`, `internal/middleware/{requestid.go,recovery.go,logging.go,middleware_test.go}`
- Modify: `go.work`

**Interfaces:**
- Produces:
  - Config (prefix `GATEWAY`): `HTTP_PORT=8080`, `AUTH_GRPC_ADDR=auth:9001`, `AUTH_JWKS_URL=http://auth:8001/.well-known/jwks.json`, `JWT_ISSUER=smarthire-auth`, `JWT_AUDIENCE=smarthire-api`, `REDIS_ADDR=redis:6379`, `CORS_ORIGINS=http://localhost:3001` (list), `COOKIE_SECURE=true`, `TRUSTED_PROXIES` (list CIDR, mặc định rỗng), `RATE_LIMIT_USER_PER_MIN=100`, `RATE_LIMIT_LOGIN_PER_MIN=10`, `LOG_LEVEL=info`
  - `func httperr.Write(w http.ResponseWriter, r *http.Request, err error)` — lấy gRPC status (`status.Convert`; lỗi không phải status → Internal "internal error"), map HTTP theo spec §5.3, ghi envelope; `request_id` từ `logger.RequestIDFrom(r.Context())`
  - `func httperr.GatewayErrorHandler(ctx context.Context, mux *runtime.ServeMux, m runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error)` — gọi `Write`
  - `func middleware.RequestID(next http.Handler) http.Handler` — nhận `X-Request-ID` nếu khớp `^[A-Za-z0-9._-]{1,64}$`, không thì sinh ULID; set response header + `logger.WithRequestID`
  - `func middleware.Recovery(log *slog.Logger) func(http.Handler) http.Handler` — panic → `httperr.Write(Internal)`
  - `func middleware.Logging(log *slog.Logger) func(http.Handler) http.Handler` — log method, path (không query), status, duration_ms

- [ ] **Step 1: Viết test**

```go
func TestWrite_MapsCodes(t *testing.T) {
	for code, http := range map[codes.Code]int{codes.InvalidArgument: 400, codes.Unauthenticated: 401,
		codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.ResourceExhausted: 429,
		codes.DeadlineExceeded: 504, codes.Unavailable: 503, codes.Internal: 500, codes.DataLoss: 500} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil).WithContext(logger.WithRequestID(ctx, "rid"))
		httperr.Write(rec, r, status.Error(code, "m"))
		assert.Equal(t, http, rec.Code)
		assert.JSONEq(t, fmt.Sprintf(`{"error":{"code":%q,"message":"m","details":[],"request_id":"rid"}}`,
			rpccode.Code(code).String()), rec.Body.String())
	}
}
```
(`rpccode` = `google.golang.org/genproto/googleapis/rpc/code`; `.String()` cho dạng `INVALID_ARGUMENT`, `ALREADY_EXISTS`. Implementation dùng cùng cách.)
```go
func TestRequestID_GeneratesAndAccepts(t *testing.T) { /* không header → 26 ký tự; "abc-1" → giữ; "bad id!" → sinh mới */ }
func TestRecovery_Returns500Envelope(t *testing.T) { /* handler panic → 500, body có "INTERNAL" */ }
```

- [ ] **Step 2: Run** `cd services/gateway && go test ./internal/...` — Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): add config, error envelope and base middleware`

---

### Task 11: Gateway client IP + auth middleware + rate limiter

**Files:**
- Create: `internal/middleware/{clientip.go,auth.go,clientip_test.go,auth_test.go}`, `internal/ratelimit/{limiter.go,script.lua,middleware.go,limiter_test.go,middleware_test.go}`, `services/gateway/test/integration/ratelimit_test.go`

**Interfaces:**
- Consumes: `authz.Verifier`, `httperr.Write`, `logger.WithUserID`
- Produces:
  - `func middleware.ClientIP(trusted []netip.Prefix) func(http.Handler) http.Handler` + `func middleware.ClientIPFrom(ctx) string` — IP = `RemoteAddr`; nếu `RemoteAddr` thuộc `trusted` thì lấy IP **phải nhất** trong `X-Forwarded-For` không thuộc `trusted`
  - `func middleware.OptionalAuth(v grpcx.TokenVerifier) func(http.Handler) http.Handler` — có Bearer hợp lệ → `authz.WithClaims`; Bearer sai → 401 envelope; không có → đi tiếp
  - `func middleware.RequireAuth(next http.Handler) http.Handler` — không có claims → 401
  - `type ratelimit.Result struct{ Allowed bool; Limit, Remaining int; RetryAfter time.Duration }`
  - `func ratelimit.New(rdb redis.Scripter) *Limiter`; `func (l *Limiter) Allow(ctx, key string, limit int, window time.Duration) (Result, error)`
  - `type ratelimit.Rule struct{ Name string; Limit int; Window time.Duration; Match func(*http.Request) bool; Key func(*http.Request) string }`
  - `func ratelimit.Middleware(l *Limiter, log *slog.Logger, rules ...Rule) func(http.Handler) http.Handler` — áp mọi rule khớp; header từ rule chặt nhất (Remaining nhỏ nhất); bị chặn → 429 envelope `RESOURCE_EXHAUSTED` + `Retry-After` (giây, làm tròn lên); `Allow` lỗi → **cho qua**, log warn, `span.SetAttributes(attribute.Bool("ratelimit.redis_error", true))`
  - Rule chuẩn: `user` (mọi request; key `gateway:rl:user:<sub>` hoặc `gateway:rl:ip:<ip>`, 100/phút), `login` (`POST /v1/auth/login`; key `gateway:rl:login:<ip>`, 10/phút)
- **Thứ tự middleware (điều chỉnh so với spec §5.1):** `OptionalAuth` đặt **trước** `ratelimit` để khoá theo `sub` đã verify (khoá theo `sub` chưa verify thì kẻ tấn công đổi `sub` ngẫu nhiên để lách). Chuỗi cuối: `Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit`.
- Token bucket Lua (`script.lua`, `KEYS[1]`=key, `ARGV`= capacity, window_ms):
  ```lua
  local cap = tonumber(ARGV[1]); local window = tonumber(ARGV[2])
  local t = redis.call('TIME'); local now = t[1]*1000 + math.floor(t[2]/1000)
  local rate = cap / window                                  -- token/ms
  local s = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
  local tokens = tonumber(s[1]) or cap; local ts = tonumber(s[2]) or now
  tokens = math.min(cap, tokens + (now - ts) * rate)
  local allowed = 0; local retry = 0
  if tokens >= 1 then tokens = tokens - 1; allowed = 1 else retry = math.ceil((1 - tokens) / rate) end
  redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
  redis.call('PEXPIRE', KEYS[1], window)
  return {allowed, math.floor(tokens), retry}
  ```

- [ ] **Step 1: Viết test**

```go
func TestClientIP_IgnoresXFFFromUntrusted(t *testing.T) {
	r := httptest.NewRequest("POST", "/", nil); r.RemoteAddr = "203.0.113.5:1234"; r.Header.Set("X-Forwarded-For", "1.2.3.4")
	assert.Equal(t, "203.0.113.5", ipVia(middleware.ClientIP(nil), r))
}
func TestClientIP_UsesXFFFromTrusted(t *testing.T) {
	r.RemoteAddr = "10.0.0.2:1"; r.Header.Set("X-Forwarded-For", "9.9.9.9, 1.2.3.4, 10.0.0.3")
	assert.Equal(t, "1.2.3.4", ipVia(middleware.ClientIP([]netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}), r))
}
func TestOptionalAuth(t *testing.T) { /* không header → next, không claims; token hợp lệ → claims; "Bearer x" → 401 */ }
func TestMiddleware_Blocks429(t *testing.T) {
	// fake Limiter (interface nhỏ trong package) trả Allowed=false, RetryAfter=1500ms → 429, Retry-After "2", code RESOURCE_EXHAUSTED
}
func TestMiddleware_FailOpen(t *testing.T) { /* fake trả error → 200, handler được gọi */ }
func TestMiddleware_LoginRuleOnlyOnLoginPath(t *testing.T) { /* GET /v1/me không đụng key gateway:rl:login:*; mọi key fake nhận được đều bắt đầu "gateway:" */ }
```
Integration (`//go:build integration`, testcontainers `redis:7-alpine`):
```go
func TestLimiter_TokenBucket(t *testing.T) {
	for i := range 10 { r, err := l.Allow(ctx, "k", 10, time.Minute); require.NoError(t, err); assert.True(t, r.Allowed, i) }
	r, _ := l.Allow(ctx, "k", 10, time.Minute); assert.False(t, r.Allowed); assert.InDelta(t, 6*time.Second, r.RetryAfter, float64(time.Second))
}
func TestLimiter_RedisDownFailsOpen(t *testing.T) { /* stop container → middleware vẫn trả 200 */ }
```

- [ ] **Step 2: Run** `cd services/gateway && go test ./internal/... && go test -tags integration ./test/...` — Expected: FAIL.
- [ ] **Step 3: Implement** (`//go:embed script.lua`, `redis.NewScript`). `Middleware` nhận interface `allower interface{ Allow(...) (Result, error) }` để test được bằng fake.
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): add client ip, auth middleware and redis rate limiter`

---

### Task 12: Gateway auth handlers (cookie) + router + main

**Files:**
- Create: `internal/authhttp/{cookies.go,handlers.go,handlers_test.go}`, `internal/router/{router.go,router_test.go}`, `cmd/gateway/main.go`, `services/gateway/Dockerfile`

**Interfaces:**
- Consumes: `authv1.AuthServiceClient`, middleware & ratelimit (Task 10–11), `httperr`
- Produces:
  - `func authhttp.New(client authv1.AuthServiceClient, cfg authhttp.Config) *Handlers`; `type Config struct{ CookieSecure bool; AllowedOrigins []string }`
  - `(*Handlers).Login`, `.Refresh`, `.Logout` (`http.HandlerFunc`)
    - Login: body JSON `{email,password}` (giới hạn 4 KiB, sai JSON → 400 `INVALID_ARGUMENT`) → `client.Login` → 200 `{"access_token":"...","expires_in":900}` + set `sh_rt`, `sh_session`
    - Refresh: `Origin` thiếu hoặc không thuộc `AllowedOrigins` → 403 `PERMISSION_DENIED`; thiếu cookie → 401; `client.Refresh` → 200 như Login + set lại cookie; gRPC `Unauthenticated` → 401 **và xoá cookie**
    - Logout: `Origin` kiểm tra như Refresh; gọi `client.Logout` nếu có cookie; luôn xoá cả hai cookie (`Max-Age=-1`); 204
  - `type router.Deps struct{ Log *slog.Logger; AuthConn *grpc.ClientConn; Verifier grpcx.TokenVerifier; Limiter *ratelimit.Limiter; Cfg config.Config; Ready func(context.Context) error }`
  - `func router.New(ctx context.Context, d Deps) (http.Handler, error)`:
    - chi routes: `POST /v1/auth/login|refresh|logout`, `GET /healthz`, `GET /readyz`
    - `GET /v1/me` → `RequireAuth` rồi grpc-gateway mux
    - `/v1/*` còn lại → grpc-gateway mux (`authv1.RegisterAuthServiceHandlerClient`)
    - mux options: `runtime.WithErrorHandler(httperr.GatewayErrorHandler)`, `runtime.WithIncomingHeaderMatcher` (chỉ forward `Authorization`→`authorization`, `X-Request-Id`→`x-request-id`), `runtime.WithMetadata` thêm `x-request-id` từ ctx, `runtime.WithForwardResponseOption` set `201` khi message là `*authv1.RegisterResponse`, marshaler `UseProtoNames: true, EmitUnpopulated: true`
    - CORS: `github.com/go-chi/cors` với `AllowedOrigins=cfg.CORSOrigins`, `AllowCredentials: true`, headers `Authorization, Content-Type, X-Request-ID`, expose `X-Request-ID, X-RateLimit-Limit, X-RateLimit-Remaining, Retry-After`
  - `cmd/gateway/main.go`: config → logger → otel → `grpcx.Dial(AUTH_GRPC_ADDR)` → verifier (retry tối đa 30s khi Auth chưa lên) → redis → router → `http.Server` (ReadHeaderTimeout 5s) + graceful shutdown 30s

- [ ] **Step 1: Viết test** (AuthService giả qua `bufconn`, ghi lại metadata nhận được)

```go
func TestLogin_SetsCookiesAndHidesRefreshToken(t *testing.T) {
	rec := do(t, "POST", "/v1/auth/login", `{"email":"a@x.com","password":"password1"}`, nil)
	assert.Equal(t, 200, rec.Code)
	assert.JSONEq(t, `{"access_token":"AT","expires_in":900}`, rec.Body.String())
	rt := cookie(rec, "sh_rt"); assert.Equal(t, "RT", rt.Value); assert.True(t, rt.HttpOnly)
	assert.Equal(t, http.SameSiteStrictMode, rt.SameSite); assert.Equal(t, "/v1/auth", rt.Path); assert.Equal(t, 604800, rt.MaxAge)
	assert.Equal(t, "1", cookie(rec, "sh_session").Value)
}
func TestRefresh_RejectsBadOrigin(t *testing.T) {
	rec := do(t, "POST", "/v1/auth/refresh", "", hdr("Origin", "https://evil.example", "Cookie", "sh_rt=RT"))
	assert.Equal(t, 403, rec.Code); assert.Equal(t, 0, fake.refreshCalls)
}
func TestRefresh_Unauthenticated_ClearsCookies(t *testing.T) { /* fake trả Unauthenticated → 401, sh_rt MaxAge<0 */ }
func TestLogout_AlwaysClears(t *testing.T)   { /* không cookie → 204, vẫn có Set-Cookie xoá */ }
func TestRouter_RegisterReturns201(t *testing.T) { /* POST /v1/auth/register qua grpc-gateway → 201 {"user_id":"..."} */ }
func TestRouter_MeRequiresAuth(t *testing.T) { /* không token → 401 envelope, fake GetMe không được gọi */ }
func TestRouter_ForwardsAuthorizationAndRequestID(t *testing.T) {
	// token hợp lệ + X-Request-ID: r-1 → fake GetMe thấy md["authorization"]=="Bearer <tok>", md["x-request-id"]=="r-1"
}
func TestRouter_GRPCErrorUsesEnvelope(t *testing.T) { /* fake Register trả AlreadyExists → 409 {"error":{"code":"ALREADY_EXISTS",...}} */ }
func TestRouter_CORSPreflight(t *testing.T) { /* OPTIONS với Origin cho phép → Access-Control-Allow-Credentials: true */ }
```

- [ ] **Step 2: Run** `cd services/gateway && go test ./internal/...` — Expected: FAIL.
- [ ] **Step 3: Implement** + Dockerfile (giống Auth).
- [ ] **Step 4: Run** `go test ./internal/... && go build ./cmd/... && docker build -f services/gateway/Dockerfile .` — Expected: PASS.
- [ ] **Step 5: Commit** `feat(gateway): add cookie auth handlers, router and main`

---

### Task 13: Docker Compose + tools (keygen, smoke) + Makefile vận hành

**Files:**
- Create: `tools/go.mod`, `tools/cmd/keygen/main.go`, `tools/cmd/smoke/main.go`, `deploy/docker/{docker-compose.infra.yml,docker-compose.obs.yml,docker-compose.yml}`, `deploy/docker/init/postgres/01-create-dbs.sql`, `deploy/docker/otel/{collector.yaml,tempo.yaml,grafana-datasources.yaml}`, `.env.example`
- Modify: `go.work`, `Makefile`

**Interfaces:**
- Produces:
  - `go run ./tools/cmd/keygen -out deploy/docker/secrets/jwt.pem` — RSA 2048 PKCS#8 PEM; file đã tồn tại → giữ nguyên, in thông báo (không ghi đè)
  - `go run ./tools/cmd/smoke -base http://localhost:8080 -origin http://localhost:3001` — exit 0 nếu 7 bước trong spec §6.3 đúng; in `PASS <bước>` / `FAIL <bước>: <lý do>`; email ngẫu nhiên `smoke-<ulid>@smarthire.local`; tự gửi header `Cookie` (không dùng cookiejar, vì cần phát lại RT1)
  - Compose project name `smarthire`, network `smarthire`. Services: `postgres` (16-alpine, healthcheck `pg_isready`; init script `01-create-dbs.sql` chạy bằng superuser: `CREATE ROLE auth_svc LOGIN PASSWORD :'auth_password'` (mật khẩu từ env `AUTH_DB_PASSWORD` qua `.env`, mặc định dev trong `.env.example`), `CREATE DATABASE auth_db OWNER auth_svc`, `REVOKE ALL ON DATABASE auth_db FROM PUBLIC`; dùng `psql -v` trong file `.sh` nếu cần biến), `auth`/`auth-migrate` dùng DSN `postgres://auth_svc:${AUTH_DB_PASSWORD}@postgres:5432/auth_db?sslmode=disable`, `redis` (7-alpine), `otel-collector` (nhận OTLP gRPC 4317 → export Tempo), `tempo`, `grafana` (3000, anonymous admin, datasource Tempo provisioned), `auth-migrate` (image `migrate/migrate`, mount `services/auth/migrations`, `restart: "no"`), `auth` (`depends_on: auth-migrate: condition: service_completed_successfully`, mount `./secrets/jwt.pem:/secrets/jwt.pem:ro`, `AUTH_JWKS_URL=http://localhost:8001/...`, `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317`, healthcheck không có shell trong distroless → dùng `depends_on` + retry trong gateway), `gateway` (8080, `GATEWAY_COOKIE_SECURE=false`, `GATEWAY_CORS_ORIGINS=http://localhost:3001`)
  - Makefile thêm: `bootstrap` gọi keygen; `infra-up`, `obs-up`, `up` (`docker compose -f ... up -d --build`), `down`, `migrate` (chạy lại `auth-migrate`), `seed` (`docker compose run --rm auth /seed`), `smoke`, `logs`

- [ ] **Step 1: Viết smoke test tool** (đây là test của task này)
- [ ] **Step 2: Run** `make smoke` khi stack chưa chạy — Expected: FAIL `register: connection refused`.
- [ ] **Step 3: Viết compose/config, keygen, Makefile**
- [ ] **Step 4: Run**
  `make bootstrap && make infra-up obs-up up && make seed && make smoke`
  Expected: 7 dòng `PASS`, exit 0. Kiểm tra quyền tối thiểu: `docker compose exec postgres psql -U auth_svc -d postgres -c 'CREATE DATABASE x'` → lỗi `permission denied`. Sau đó: `curl -s -H "Authorization: Bearer <token từ login>" localhost:8080/v1/me`, mở `http://localhost:3000` → Explore → Tempo → tìm service `gateway`: một trace có span gateway HTTP → gRPC client → auth gRPC server → pgx query.
- [ ] **Step 5: Commit** `feat(deploy): add docker compose stack, keygen and smoke test`

---

### Task 14: OpenAPI gộp

**Files:**
- Create: `tools/cmd/openapimerge/{main.go,merge.go,merge_test.go}`, `contracts/openapi/gateway.handwritten.yaml`
- Generated: `contracts/openapi/gateway.yaml`
- Modify: `Makefile` (`openapi`, và `proto` gọi `openapi`)

**Interfaces:**
- Produces: `func merge(generated, handwritten map[string]any) (map[string]any, error)` — cả hai Swagger 2.0; gộp `paths` và `definitions`; trùng cặp (path, method) hoặc trùng tên definition khác nội dung → lỗi nêu rõ tên; giữ `info` của bản sinh, đặt `info.title = "SmartHire Gateway API"`. CLI: `openapimerge -gen contracts/openapi/gen/gateway.swagger.json -hand contracts/openapi/gateway.handwritten.yaml -out contracts/openapi/gateway.yaml`.
- `gateway.handwritten.yaml` mô tả `POST /v1/auth/login` (body `{email,password}`, 200 `{access_token, expires_in}` + header `Set-Cookie`), `/v1/auth/refresh` (cookie `sh_rt`, header `Origin`), `/v1/auth/logout` (204), và definition `ErrorEnvelope`.

- [ ] **Step 1: Viết test** `TestMerge_CombinesPaths`, `TestMerge_DuplicatePathMethodFails` (cùng `post /v1/auth/register` → lỗi chứa `"/v1/auth/register"`), `TestMerge_DefinitionConflictFails`.
- [ ] **Step 2: Run** `cd tools && go test ./cmd/openapimerge/...` — Expected: FAIL.
- [ ] **Step 3: Implement** (`sigs.k8s.io/yaml` để đọc/ghi YAML).
- [ ] **Step 4: Run** test + `make proto` — Expected: PASS; `gateway.yaml` có 5 path.
- [ ] **Step 5: Commit** `feat(contracts): merge generated and handwritten openapi`

---

### Task 15: CI GitHub Actions

**Files:**
- Create: `.github/workflows/ci-proto.yml`, `.github/workflows/ci-go.yml`

**Interfaces:**
- `ci-proto.yml` (on PR + push main, paths `contracts/proto/**`, `gen/**`): `bufbuild/buf-action` lint; breaking `--against '.git#branch=main,subdir=contracts/proto'` (bỏ qua khi `main` chưa có thư mục proto); `make proto && git diff --exit-code gen contracts/openapi`.
- `ci-go.yml`: job `changes` (`dorny/paths-filter`: `pkg`, `auth`, `gateway`, `tools`; thay đổi ở `pkg/**` hoặc `gen/**` kích hoạt mọi module); job `module` matrix theo module bị đổi: `actions/setup-go` (`go-version-file: go.work`), `golangci-lint` action, `go test ./...`, `go test -tags integration ./...` (auth, gateway), `make cover-auth` (auth); job `docker` build image auth + gateway (không push).

- [ ] **Step 1: Viết workflow**
- [ ] **Step 2: Kiểm tra cú pháp** Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:latest` — Expected: không lỗi.
- [ ] **Step 3: Chạy toàn bộ cục bộ** Run: `make lint test test-int cover-auth` — Expected: PASS, coverage ≥ 70%.
- [ ] **Step 4: Commit** `ci: add proto and go workflows`

---

### Task 16: Tài liệu: README + 4 ADR

**Files:**
- Create: `README.md`, `docs/adr/0001-monorepo.md`, `docs/adr/0002-gateway-hybrid.md`, `docs/adr/0003-jwt-verify-at-service.md`, `docs/adr/0004-refresh-token-rotation.md`, `services/auth/README.md`, `services/gateway/README.md`

**Nội dung bắt buộc:**
- Mỗi ADR theo khuôn: Context / Decision / Consequences / Alternatives considered. Nội dung lấy từ spec §3 (ADR 0003), §4.6–4.7 (ADR 0004, kèm việc Register trả 409 lộ email), §5.2–5.4 (ADR 0002, kèm fail-open và việc đặt `OptionalAuth` trước rate limit), §1 + Global Constraints (ADR 0001). Thêm `docs/adr/README.md` (mục lục) ghi rõ: ADR đánh số theo thứ tự viết; các chủ đề dự kiến ở spec.md §10 (grpc-for-sync, saga-orchestration, outbox-pattern, cqrs-job-search, account-deletion-choreography, when-not-to-use-saga) nhận số khi milestone tương ứng viết chúng.
- `README.md`: mô tả 1 đoạn; sơ đồ mermaid kiến trúc mục tiêu (copy spec.md §4) và ghi chú phần đã có ở M0; Quick start: `make bootstrap`, `make infra-up obs-up up`, `make seed`, `make smoke`; bảng URL local; bảng roadmap M0–M6 (M0 = ✅ khi xong); link ADR.
- README service: trách nhiệm, env vars (bảng từ Task 9/10), port, cách chạy test.

- [ ] **Step 1: Viết tài liệu**
- [ ] **Step 2: Kiểm tra** — làm theo đúng Quick start trên một clone mới: `git clone . /tmp/sh && cd /tmp/sh && make bootstrap && make infra-up obs-up up && make seed && make smoke` — Expected: PASS. Sau đó `make down`.
- [ ] **Step 3: Commit** `docs: add README, service READMEs and ADRs 0001-0004`

---

## Definition of Done (đối chiếu spec §7)
1. Task 16 Step 2 pass trên clone mới.
2. Trace `GET /v1/me` trên Tempo: Gateway → Auth → Postgres (Task 13 Step 4).
3. `make lint test test-int` xanh; workflow CI xanh khi push.
4. `make cover-auth` ≥ 70%.
5. 4 ADR + README gốc + README từng service.
