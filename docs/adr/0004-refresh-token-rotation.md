# ADR 0004: Refresh token rotation + phát hiện dùng lại

- Trạng thái: Chấp nhận (M0)
- Ngày: 2026-11-08

## Context

Access token sống 15 phút nên cần một refresh token sống 7 ngày để giữ phiên. Refresh token là credential giá trị cao: bị đánh cắp là chiếm được phiên đến khi hết hạn. Cần giảm thiệt hại nếu bị lộ, và phát hiện được việc đó. Refresh token được chuyển cho trình duyệt qua cookie `sh_rt` (`HttpOnly; SameSite=Strict; Path=/v1/auth`), không để JavaScript đọc.

## Decision

**Token opaque, lưu băm, xoay vòng theo family, phát hiện dùng lại.**

- Refresh token là 32 byte ngẫu nhiên, mã hoá base64url (RawURLEncoding), không mang thông tin. DB chỉ lưu `sha256(token)` (`token_hash`, UNIQUE); lộ DB không lộ token dùng được. TTL 7 ngày.
- Mỗi lần **Login** tạo `family_id` mới. Mọi token sinh ra từ chuỗi refresh sau đó kế thừa `family_id`.
- **Refresh** chạy trong một transaction:
  1. `SELECT ... FOR UPDATE` theo `token_hash`. Không thấy thì `Unauthenticated`.
  2. `revoked_at` khác NULL: **token đã xoay bị dùng lại**. Thu hồi toàn bộ token cùng `family_id`, log cảnh báo (`event=refresh_token_reuse`, `user_id`, `family_id`), trả `Unauthenticated`. Bước này kiểm tra **trước** hạn dùng (R14): token đã xoay mà bị phát lại sau khi hết hạn vẫn là dấu hiệu bị đánh cắp.
  3. Hết hạn thì `Unauthenticated`.
  4. Nạp user; user không `ACTIVE` thì `Unauthenticated` (không thu hồi family).
  5. Hợp lệ: đặt `revoked_at` cho token cũ, tạo token mới cùng family, ghi `replaced_by`, trả cặp token mới.
- Việc thu hồi family phải **commit** dù lời gọi trả lỗi: closure transaction trả nil, lỗi `Unauthenticated` được trả sau khi transaction đã commit. Nếu trả lỗi từ trong closure, rollback sẽ huỷ chính việc thu hồi.
- `SELECT ... FOR UPDATE` giải quyết tranh chấp: hai request đồng thời cùng một token chỉ một cái thành công, cái kia đi vào nhánh "dùng lại" và thu hồi cả family (có integration test `TestRefresh_ConcurrentSameToken`).
- **Logout** thu hồi token hiện tại, idempotent (token không tồn tại vẫn trả OK).

### Mật khẩu, đăng nhập, đăng ký

- argon2id, tham số cấu hình qua env (`AUTH_ARGON2_*`).
- Login chống dò user: email không tồn tại vẫn chạy hash giả; mọi lỗi xác thực là "sai email hoặc mật khẩu".
- Trạng thái tài khoản chỉ kiểm tra **sau khi** mật khẩu đúng, để không lộ thông tin cho người dò: `LOCKED` trả `PermissionDenied` "account is locked", `DELETION_PENDING` trả "account is pending deletion".
- **Register trùng email trả `AlreadyExists` (HTTP 409), tức là lộ việc email đã đăng ký.** Đây là đánh đổi UX có chủ ý (người dùng biết ngay phải đăng nhập thay vì chờ email). Xem "Không làm" bên dưới.

### Không làm (chủ ý)

- **Chống liệt kê email ở Register.** Cách chuẩn là luôn trả 201/202 và gửi email xác nhận cho cả hai trường hợp. M0 chưa có Notification và luồng xác minh email, nên chấp nhận 409 lộ email. Giảm nhẹ: rate limit 100 req/phút/IP trên Gateway. Làm lại khi có luồng xác minh email.
- Không có giới hạn số phiên đồng thời hay quản lý thiết bị.

## Consequences

Tích cực:

- Token bị đánh cắp có tuổi thọ ngắn: dùng một lần rồi bị xoay; nếu kẻ trộm và chủ cùng dùng, một bên sẽ chạm token đã thu hồi và cả family bị vô hiệu hoá.
- Lộ DB không lộ refresh token.
- Có tín hiệu phát hiện xâm nhập (`refresh_token_reuse`) có thể đưa vào cảnh báo.

Đánh đổi:

- **Dương tính giả đã biết: refresh sau logout được log là `refresh_token_reuse`.** Logout đặt `revoked_at`, nên dùng lại token đó (ví dụ tab thứ hai chưa biết đã logout) đi vào nhánh "dùng lại" và thu hồi family, dù không có tấn công. Hậu quả vô hại (family vốn đã hết hiệu lực) nhưng làm nhiễu cảnh báo. Hướng cải thiện: phân biệt bằng `replaced_by IS NULL` (token bị thu hồi bởi logout không có token kế nhiệm, còn token đã xoay luôn có `replaced_by`), và chỉ coi trường hợp có `replaced_by` là dùng lại thật. Chưa làm ở M0.
- Mạng chập chờn có thể gây "dùng lại" thật: client gửi refresh, server xoay xong nhưng phản hồi mất, client thử lại bằng token cũ và bị đăng xuất cả family. Chưa có cửa sổ ân hạn (grace window).
- Mỗi lần refresh ghi DB và giữ khoá hàng; chấp nhận được ở quy mô này.
- Hàng token cũ tích luỹ trong `refresh_tokens`; cần job dọn dẹp (ngoài phạm vi M0).

## Alternatives considered

- **Refresh token là JWT:** verify không cần DB, nhưng không thu hồi/xoay được nếu không lưu trạng thái, khi đó mất lợi thế; JWT dài hơn và lộ claims.
- **Không xoay (token cố định 7 ngày):** đơn giản nhưng token bị lộ dùng được tới hết hạn mà không ai biết.
- **Xoay nhưng không có family (chỉ thu hồi token bị dùng lại):** kẻ trộm đã giữ token mới hơn vẫn dùng tiếp được. Family cho phép cắt cả chuỗi.
- **Lưu token thô trong DB:** dễ tra cứu nhưng lộ DB là lộ phiên. Dùng `sha256` đủ vì token có entropy cao (32 byte ngẫu nhiên), không cần hash chậm như mật khẩu.
- **Token trong `localStorage`:** JavaScript đọc được nên XSS lấy được. Dùng cookie `HttpOnly` + `SameSite=Strict` + kiểm tra `Origin` ở `/v1/auth/refresh`.
- **Chống liệt kê email ngay từ M0:** cần email xác nhận, chưa có Notification; lùi lại.
