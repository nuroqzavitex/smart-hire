# Auth Service

Service Go sở hữu danh tính người dùng: đăng ký, đăng nhập, cấp access token (JWT RS256), refresh token có rotate và phát hiện dùng lại, logout, JWKS và RBAC. Đây là **bản mẫu hexagonal** (`internal/domain` → `internal/app` → `internal/adapter/{grpc,postgres,crypto,httpx}` → `internal/bootstrap`) để các service Go sau sao chép. Dữ liệu nằm trong `auth_db`, do role `auth_svc` sở hữu (không kết nối bằng `postgres`).

## Trách nhiệm

- gRPC `smarthire.auth.v1.AuthService`: `Register`, `Login`, `Refresh`, `Logout`, `GetMe`. `Register` và `Login` là RPC public; `Refresh` và `Logout` cũng public (xác thực bằng refresh token); `GetMe` cần access token hợp lệ (interceptor `pkg/grpcx` tự verify lại bằng JWKS, xem [ADR 0003](../../docs/adr/0003-jwt-verify-at-service.md)).
- Access token: RS256, TTL 15 phút, `iss=smarthire-auth`, `aud=smarthire-api`, header `kid`. Refresh token: 32 byte ngẫu nhiên base64url, DB chỉ lưu `sha256`, TTL 7 ngày (xem [ADR 0004](../../docs/adr/0004-refresh-token-rotation.md)).
- Mật khẩu hash bằng argon2id (tham số cấu hình được). Admin không đăng ký được qua API; tạo bằng `make seed`.
- Ghi sự kiện `user.registered` vào bảng `outbox` trong cùng transaction với việc tạo user. Relay outbox (`pkg/outbox`, có từ M1a) đẩy sự kiện lên topic Kafka `user.registered`, key = `user_id` (xem [ADR 0005](../../docs/adr/0005-outbox-relay-headers.md)).
- Không log PII (email, mật khẩu, token); `user_id` được phép.

## Cổng

| Cổng | Giao thức | Nội dung |
|---|---|---|
| `9001` | gRPC | `AuthService` + health chuẩn |
| `8001` | HTTP nội bộ | `/.well-known/jwks.json`, `/healthz` (liveness), `/readyz` (kiểm tra DB) |

Trong Compose, hai cổng này chỉ có trong mạng Docker (`auth:9001`, `auth:8001`), không publish ra host.

## Biến môi trường

Prefix `AUTH_`. Config chỉ đọc qua env (`github.com/caarlos0/env/v11`); biến không prefix không bao giờ thoả config. Nguồn: `internal/config/config.go`.

| Biến | Mặc định | Bắt buộc | Ý nghĩa |
|---|---|---|---|
| `AUTH_DB_DSN` | | có | DSN Postgres của `auth_db` (role `auth_svc`) |
| `AUTH_JWT_PRIVATE_KEY_PATH` | | có | File PEM khoá RSA dùng ký access token |
| `AUTH_JWT_EXTRA_PUBLIC_KEY_PATHS` | | không | Danh sách file PEM khoá công khai cũ, phân tách bằng dấu phẩy, để xoay khoá (JWKS công bố cả khoá cũ) |
| `AUTH_GRPC_PORT` | `9001` | không | Cổng gRPC |
| `AUTH_HTTP_PORT` | `8001` | không | Cổng HTTP nội bộ |
| `AUTH_JWT_ISSUER` | `smarthire-auth` | không | Claim `iss` |
| `AUTH_JWT_AUDIENCE` | `smarthire-api` | không | Claim `aud` |
| `AUTH_ACCESS_TTL` | `15m` | không | TTL access token |
| `AUTH_REFRESH_TTL` | `168h` | không | TTL refresh token (7 ngày) |
| `AUTH_KAFKA_BROKERS` | `redpanda:9092` | không | Danh sách broker Kafka, phân tách bằng dấu phẩy (relay outbox) |
| `AUTH_OUTBOX_INTERVAL` | `500ms` | không | Chu kỳ quét bảng `outbox` của relay |
| `AUTH_JWKS_URL` | `http://localhost:8001/.well-known/jwks.json` | không | URL JWKS mà interceptor của chính Auth dùng để verify lại token |
| `AUTH_ARGON2_MEMORY_KIB` | `19456` | không | Bộ nhớ argon2id (KiB) |
| `AUTH_ARGON2_ITERATIONS` | `2` | không | Số vòng argon2id |
| `AUTH_ARGON2_PARALLELISM` | `1` | không | Độ song song argon2id |
| `AUTH_LOG_LEVEL` | `info` | không | Mức log |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | không | Có giá trị thì bật trace OTLP gRPC (Compose đặt `http://otel-collector:4317`) |

Lệnh seed (`cmd/seed`, chạy qua dịch vụ Compose `auth-seed`) dùng `AUTH_DB_DSN`, các biến `AUTH_ARGON2_*` và hai biến prefix `SEED_`:

| Biến | Bắt buộc | Ý nghĩa |
|---|---|---|
| `SEED_ADMIN_EMAIL` | có | Email admin |
| `SEED_ADMIN_PASSWORD` | có | Mật khẩu admin |

Seed idempotent: user đã tồn tại thì bỏ qua. Ngoài admin, nó tạo `candidate@smarthire.local` và `recruiter@smarthire.local` (mật khẩu dev `Password123!`).

## Chạy và test

Chạy cả stack từ thư mục gốc (xem [README](../../README.md#quick-start)): `make bootstrap`, `make infra-up obs-up up`, `make seed`.

```bash
make migrate     # chạy lại migration (container auth-migrate)
make sqlc        # sinh lại code sqlc (qua Docker)
make test        # unit test mọi module, không cần Docker
make test-int    # integration test (build tag `integration`, testcontainers Postgres, cần Docker)
make cover-auth  # coverage domain + app, phải đạt >= 70%
```

Chạy một test (trong `services/auth`):

```bash
go test ./internal/app -run TestRefresh_ReuseRevokesFamily -v
go test -tags integration ./test/integration -run TestRefresh_ConcurrentSameToken -v
```

Migration nằm ở `migrations/`, append-only sau khi merge.
