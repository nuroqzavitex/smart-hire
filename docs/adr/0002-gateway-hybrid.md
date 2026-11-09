# ADR 0002: Gateway hybrid chi + grpc-gateway

- Trạng thái: Chấp nhận (M0)
- Ngày: 2026-11-08

## Context

Gateway là cổng HTTP duy nhất, nói gRPC với các service phía sau. Phần lớn route chỉ là ánh xạ REST sang một RPC, nhưng có những việc grpc-gateway không làm tốt: đặt và xoá cookie (`sh_rt`, `sh_session`), kiểm tra `Origin` chống CSRF cho refresh, health check, và về sau là WebSocket proxy (M4). Gateway còn phải rate limit bằng Redis và verify JWT, và rate limit cần khoá theo danh tính **đã verify**.

## Decision

**Hybrid:** `chi` v5 là router ngoài; route cần logic HTTP đặc thù viết tay (`/v1/auth/login|refresh|logout`, `/healthz`, `/readyz`); mọi route còn lại dưới `/v1/*` rơi về mux của **grpc-gateway v2**, sinh từ annotation `google.api.http` trong proto. Route viết tay đăng ký trước nên thắng. Một error handler dùng chung cho cả hai nhánh (map gRPC code sang HTTP + envelope `{"error":{"code","message","details","request_id"}}`).

### Chuỗi middleware

```
Recovery → RequestID → otelhttp → Logging → CORS → ClientIP → OptionalAuth → ratelimit → RejectInvalidAuth → routes
```

- **`OptionalAuth` đứng trước `ratelimit`** để khoá rate limit theo `sub` đã verify. Nếu khoá theo `sub` chưa verify, kẻ tấn công đổi `sub` ở mỗi request để lách giới hạn. Request không có token (hoặc token sai) rơi về khoá theo IP.
- **Bearer không hợp lệ không bị từ chối ngay.** `OptionalAuth` chỉ đánh dấu "xác thực thất bại" lên context. Request vẫn đi qua `ratelimit` (nên bị giới hạn theo IP, chặn brute-force token), rồi `RejectInvalidAuth` mới trả 401. Nhờ vậy việc thử token sai cũng tốn quota.
- **Ba route cookie `/v1/auth/login|refresh|logout` được miễn `RejectInvalidAuth`** (vẫn bị rate limit theo IP). Lý do: SPA thường gửi kèm access token đã hết hạn khi gọi refresh; nếu bị 401 vì token cũ thì không bao giờ phục hồi được phiên. Các route này xác thực bằng cookie refresh, không bằng bearer.
- `RequireAuth` / `RequireRole` gắn theo từng route cần đăng nhập (ví dụ `GET /v1/me`).

### Rate limit

- Token bucket trên Redis bằng Lua script (atomic). 100 req/phút/user (khoá `gateway:rl:user:<sub>`, hoặc `gateway:rl:ip:<ip>` nếu chưa đăng nhập) và 10 req/phút/IP cho `POST /v1/auth/login` (`gateway:rl:login:<ip>`). Key luôn có tiền tố `gateway:` (spec.md §4.2). Bị chặn trả `429` + `Retry-After`; mọi response có `X-RateLimit-Limit`, `X-RateLimit-Remaining`.
- **Fail-open:** Redis lỗi thì cho request qua, log warning và ghi nhận lỗi (span attribute ở M0, metric Prometheus ở M4).
- **Mỗi lời gọi Redis bị chặn ở 100ms.** Redis chết hay chậm không được kéo theo độ trễ của mọi request.
- **Fail-open một lần cho mỗi request:** khi một rule gặp lỗi Redis thì các rule còn lại của request đó bỏ qua luôn, để request không trả thêm timeout 100ms cho từng rule.
- IP lấy từ `RemoteAddr`; chỉ tin `X-Forwarded-For` khi `GATEWAY_TRUSTED_PROXIES` được cấu hình.

### Truyền xuống service

Gateway chuyển `authorization` (nguyên JWT, xem ADR 0003) và `x-request-id` qua metadata gRPC; `traceparent` do `otelgrpc` lo. Timeout mặc định 3 giây cho mọi lời gọi gRPC.

## Consequences

Tích cực:

- Logic cookie/CSRF viết tay, test bằng `httptest` + AuthService giả qua `bufconn`, không phải ép vào grpc-gateway.
- Phần lớn route mới chỉ cần thêm annotation vào proto; OpenAPI sinh tự động (gộp với `gateway.handwritten.yaml` cho route viết tay).
- Thứ tự middleware, đặc biệt `OptionalAuth` trước `ratelimit`, được ghi và test rõ ràng.

Đánh đổi:

- Hai cơ chế route song song: người đọc phải biết route nào nằm ở đâu, và route viết tay phải được mô tả riêng trong OpenAPI.
- **Fail-open đánh đổi bảo vệ lấy tính sẵn sàng:** khi Redis chết, giới hạn tốc độ tạm thời biến mất, kể cả giới hạn đăng nhập 10 req/phút/IP, nên brute-force mật khẩu không bị chặn ở tầng Gateway trong lúc đó. Chấp nhận được ở quy mô MVP vì argon2id làm mỗi lần thử đắt, và cảnh báo log cho phép phát hiện. Fail-closed sẽ làm Redis thành điểm chết duy nhất của toàn bộ API.
- Một bearer sai vẫn tiêu hao quota theo IP; NAT dùng chung có thể bị ảnh hưởng lẫn nhau.

## Alternatives considered

- **Chỉ grpc-gateway:** đơn giản hơn, nhưng đặt/xoá cookie, đọc cookie vào request và kiểm tra `Origin` phải làm bằng `ForwardResponseOption`/`Metadata` hook rất gượng; WebSocket proxy không phù hợp.
- **Chỉ chi, viết tay toàn bộ route:** kiểm soát tối đa nhưng phải viết và giữ đồng bộ tay từng handler, validation, map lỗi và OpenAPI cho mỗi RPC; mất lợi thế contract-first.
- **Gateway có sẵn (Kong, Envoy, KrakenD):** mạnh nhưng thêm một thành phần vận hành và cấu hình riêng; logic cookie/refresh đặc thù vẫn phải có code. Với portfolio Go, tự viết Gateway là một phần giá trị trình bày.
- **Fail-closed khi Redis lỗi:** an toàn hơn cho brute-force nhưng một sự cố Redis làm sập toàn API; bị loại.
- **Rate limit trước auth (khoá theo `sub` chưa verify hoặc chỉ theo IP):** đơn giản hơn nhưng khoá theo `sub` chưa verify bị lách, còn chỉ theo IP thì không công bằng cho user sau NAT.
