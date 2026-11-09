# API Gateway

Điểm vào HTTP duy nhất của hệ thống. Gateway là service Go dùng **chi** cho route viết tay (cookie, health) và **grpc-gateway v2** làm fallback cho mọi route `/v1/*` còn lại (xem [ADR 0002](../../docs/adr/0002-gateway-hybrid.md)). Nó verify JWT qua JWKS của Auth, rate limit bằng Redis, xử lý CORS, request ID, trace OTel, rồi chuyển lời gọi xuống service bằng gRPC kèm nguyên JWT trong metadata `authorization` (xem [ADR 0003](../../docs/adr/0003-jwt-verify-at-service.md)). Gateway không có DB nghiệp vụ; Redis dùng key prefix `gateway:`.

## Route

| Route | Triển khai | Ghi chú |
|---|---|---|
| `POST /v1/auth/register` | grpc-gateway | public, trả `201` |
| `GET /v1/me` | grpc-gateway | bắt buộc đăng nhập |
| `POST /v1/auth/login` | chi | body chỉ có `access_token`, `expires_in`; set cookie `sh_rt` và `sh_session` |
| `POST /v1/auth/refresh` | chi | đọc cookie `sh_rt`, kiểm tra `Origin` thuộc allowlist (chống CSRF), rotate, set lại cookie |
| `POST /v1/auth/logout` | chi | thu hồi refresh token, xoá cả hai cookie |
| `GET /healthz` | chi | liveness |
| `GET /readyz` | chi | `503` nếu kết nối gRPC tới Auth đã đóng |

Cookie: `sh_rt` (`HttpOnly; SameSite=Strict; Path=/v1/auth; Max-Age=604800`, giá trị là refresh token) và `sh_session=1` (`HttpOnly; SameSite=Strict; Path=/`, chỉ là cờ, không chứa token). Cờ `Secure` theo `GATEWAY_COOKIE_SECURE` (mặc định `true`; Compose dev đặt `false` vì chạy HTTP).

Chuỗi middleware: `Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes`. Bearer không hợp lệ bị rate limit theo IP rồi mới trả 401; ba route cookie miễn `RejectInvalidAuth`.

Lỗi trả về theo envelope `{"error":{"code":"<GRPC_CODE_NAME>","message":"...","details":[],"request_id":"..."}}`.

Rate limit: 100 req/phút/user (khoá theo `sub` đã verify, chưa đăng nhập thì theo IP) và 10 req/phút/IP cho `POST /v1/auth/login`. Bị chặn trả `429` + `Retry-After`; response có `X-RateLimit-Limit`, `X-RateLimit-Remaining`. Redis lỗi hoặc chậm quá 100ms thì fail-open.

## Cổng

| Cổng | Nội dung |
|---|---|
| `8080` | HTTP (Compose publish ra `127.0.0.1:8080`) |

Timeout mặc định cho mọi lời gọi gRPC xuống service: 3 giây.

## Biến môi trường

Prefix `GATEWAY_`. Config chỉ đọc qua env (`github.com/caarlos0/env/v11`); biến không prefix không bao giờ thoả config. Nguồn: `internal/config/config.go`.

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `GATEWAY_HTTP_PORT` | `8080` | Cổng HTTP |
| `GATEWAY_AUTH_GRPC_ADDR` | `auth:9001` | Địa chỉ gRPC của Auth |
| `GATEWAY_AUTH_JWKS_URL` | `http://auth:8001/.well-known/jwks.json` | JWKS để verify access token. Gateway chỉ bắt đầu phục vụ sau khi lấy được JWKS (thử lại trong lúc Auth khởi động) |
| `GATEWAY_JWT_ISSUER` | `smarthire-auth` | `iss` mong đợi |
| `GATEWAY_JWT_AUDIENCE` | `smarthire-api` | `aud` mong đợi |
| `GATEWAY_REDIS_ADDR` | `redis:6379` | Redis cho rate limit |
| `GATEWAY_CORS_ORIGINS` | `http://localhost:3001` | Origin được phép (phân tách bằng dấu phẩy); cũng là allowlist `Origin` cho `/v1/auth/refresh`. `credentials: true` |
| `GATEWAY_COOKIE_SECURE` | `true` | Cờ `Secure` của cookie |
| `GATEWAY_TRUSTED_PROXIES` | rỗng | CIDR/IP proxy được tin `X-Forwarded-For`; rỗng thì chỉ dùng `RemoteAddr` |
| `GATEWAY_RATE_LIMIT_USER_PER_MIN` | `100` | Giới hạn theo user/IP |
| `GATEWAY_RATE_LIMIT_LOGIN_PER_MIN` | `10` | Giới hạn `POST /v1/auth/login` theo IP |
| `GATEWAY_LOG_LEVEL` | `info` | Mức log |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | | Có giá trị thì bật trace OTLP gRPC |

## Chạy và test

Chạy cả stack từ thư mục gốc (xem [README](../../README.md#quick-start)). Kiểm thử end-to-end qua Gateway thật: `make smoke`.

```bash
make test       # unit test mọi module, không cần Docker
make test-int   # integration test (build tag `integration`; rate limit với Redis thật, gồm Redis dừng -> fail-open)
```

Chạy một test (trong `services/gateway`): `go test ./internal/router -v`.

OpenAPI của Gateway: `contracts/openapi/gateway.yaml` (sinh từ proto, gộp với `gateway.handwritten.yaml` bằng `make openapi`); ở M0 chỉ để làm tài liệu.
