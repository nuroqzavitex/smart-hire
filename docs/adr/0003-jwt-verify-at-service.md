# ADR 0003: Verify JWT ở cả Gateway và service

- Trạng thái: Chấp nhận (M0)
- Ngày: 2026-11-08

## Context

Auth cấp access token JWT RS256 (TTL 15 phút, `iss=smarthire-auth`, `aud=smarthire-api`, header `kid`). Gateway là điểm vào, nhưng `spec.md` §14.4 yêu cầu **RBAC và kiểm tra quyền sở hữu tại service**, không chỉ ở biên. Cần quyết định danh tính người gọi đi từ Gateway xuống service theo cách nào, và ai là bên verify.

## Decision

**Verify hai lớp.**

1. Gateway verify JWT bằng JWKS của Auth (cache, tự refresh khi gặp `kid` lạ) để từ chối sớm, rate limit theo `sub` đã verify và trả lỗi HTTP gọn.
2. Gateway chuyển **nguyên JWT** xuống service qua metadata gRPC `authorization`. Interceptor `auth` trong `pkg/grpcx` của mỗi service **verify lại** bằng JWKS đã cache (`pkg/authz`: RS256, kiểm tra `iss`, `aud`, `exp`), rồi đặt `Claims{Sub, Role, ...}` vào context. `RequireRole` và kiểm tra ownership dùng claims này.
3. RPC public (`Register`, `Login`, `Refresh`, `Logout`) khai báo trong **allowlist** của interceptor; mọi RPC khác mặc định yêu cầu token hợp lệ (deny by default).

Gateway **không** gửi header kiểu `x-user-id` rồi bắt service tin.

JWKS hỗ trợ nhiều khoá (khoá đang ký và khoá cũ qua `AUTH_JWT_EXTRA_PUBLIC_KEY_PATHS`) để xoay khoá không làm hỏng token đang lưu hành. Khoá riêng chỉ Auth giữ; Gateway và service chỉ cần khoá công khai.

## Consequences

Tích cực:

- Service không phụ thuộc vào việc "chỉ Gateway mới gọi được tôi". Kẻ có đường mạng vào thẳng service (container bị xâm nhập, cấu hình mạng sai) vẫn không giả được user vì không ký được JWT.
- Chính sách xác thực nằm ở một chỗ (`pkg/grpcx` + `pkg/authz`), service mới nhận được miễn phí.
- Một nguồn sự thật cho danh tính: claims từ token đã ký, không phải header tự khai.

Đánh đổi:

- Mỗi service verify chữ ký ở mỗi request (RS256 verify rẻ, JWKS có cache) và phải truy cập được URL JWKS của Auth khi khởi động hoặc khi gặp `kid` mới.
- Thu hồi tức thì access token không có sẵn: token còn hiệu lực tối đa 15 phút sau logout hoặc khoá tài khoản. Chấp nhận nhờ TTL ngắn; denylist token ở Gateway được lên kế hoạch cho M4 (xoá tài khoản).
- Hai nơi cấu hình `iss`/`aud` phải khớp (`GATEWAY_JWT_*`, `AUTH_JWT_*`).

## Alternatives considered

- **Gateway verify, service tin `x-user-id`/`x-user-role`:** đơn giản và nhanh, nhưng ai gọi được gRPC của service là giả mạo được bất kỳ user nào, vi phạm yêu cầu RBAC tại service. Bị loại.
- **mTLS giữa Gateway và service + header danh tính:** chứng minh được "Gateway gọi", nhưng không chứng minh được "user này"; thêm gánh nặng quản lý chứng chỉ. Có thể bổ sung sau như lớp phòng thủ chiều sâu, không thay thế verify JWT.
- **Introspection (service hỏi Auth mỗi request):** thu hồi tức thì nhưng thêm một hop đồng bộ vào mọi request và biến Auth thành điểm nghẽn.
- **Token nội bộ riêng (Gateway đổi JWT ngoài lấy token nội bộ):** thêm một loại token và một chỗ ký khoá; không có lợi ích rõ ở quy mô này.
