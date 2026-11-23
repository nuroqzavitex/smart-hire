# ADR 0012: Notification — bản sao cục bộ, email outbox, WebSocket qua gateway bằng ticket

- Trạng thái: Chấp nhận (M4a)
- Ngày: 2026-11-24

## Context

M4a thêm Notification service (spec §5.6). Service này nhận các sự kiện của các service khác và tạo thông báo trong app, đẩy realtime tới người dùng đang online và gửi email cho những việc cần hành động. Có ba khó khăn:

- Sự kiện thường không chứa đủ thông tin để viết thông báo. Ví dụ `application.status_changed` chỉ có `candidate_id`, không có email hay tên tin tuyển dụng.
- Gửi email là side effect ra bên ngoài. SMTP có thể sập, và việc này không được làm mất thông báo hay chặn consumer.
- Trình duyệt không gửi được header `Authorization` khi mở WebSocket, còn đặt JWT trên URL thì token lộ vào access log.

## Decision

**Ai nhận gì.** Bảng ở thiết kế M4a §4, đã chốt với user:

| Sự kiện | Người nhận | Email |
|---|---|---|
| `user.registered` | chào mừng người đăng ký | có |
| `cv.parsed`, `cv.parse_failed` | ứng viên | không |
| `application.submitted` | recruiter của tin | có |
| `application.status_changed` | ứng viên; chỉ khi recruiter chuyển sang `INTERVIEW`, `OFFERED`, `HIRED` hoặc `REJECTED`, không báo các bước nội bộ của saga | có |
| `match.completed` | recruiter, khi `top` không rỗng | không |

**Bản sao cục bộ thay cho gọi đồng bộ.** Notification giữ hai bản sao:

- `users(user_id, email, role)`, lấy từ `user.registered`.
- `jobs(job_id, recruiter_id, title, version, deleted)`, lấy từ `job.created|updated|deleted`. Bản sao này có version guard: chỉ ghi khi version mới hơn. Khi tin bị xoá, dòng không bị xoá mà chuyển thành tombstone, để một `job.updated` đến muộn không làm tin sống lại.

Nhờ hai bản sao này, thông báo ghi được tên tin, `match.completed` tìm được recruiter, và email có địa chỉ người nhận mà không cần gọi gRPC sang Auth hay Job lúc xử lý sự kiện.

Hệ quả:

- User đăng ký trước M4a chưa có email trong bản sao, nên chỉ nhận thông báo trong app (không backfill, vì dữ liệu là giả).
- Bản sao `users` chứa PII (email). M4b sẽ xoá nó khi người dùng xoá tài khoản.

**Một transaction cho mỗi sự kiện.** Mỗi sự kiện được xử lý trong một transaction gồm `processed_events`, bản sao, `notifications` và `email_outbox`. Nhờ vậy sự kiện gửi lại không tạo thông báo hay email trùng. Chỉ sau khi commit, thông báo mới được đẩy lên hub WebSocket. Việc đẩy là best-effort: user không online thì thôi, họ vẫn đọc được qua `GET /v1/notifications`.

**Email outbox.**

- Email không được gửi trong consumer. Nó được ghi vào `email_outbox` trong cùng transaction với thông báo.
- Một vòng lặp trong process lấy các email đến hạn. Nó giữ chỗ (lease) bằng `next_attempt_at` và `FOR UPDATE SKIP LOCKED`, giống saga worker ở ADR 0011, nên không giữ transaction trong lúc gửi SMTP.
- Mỗi email được gửi qua SMTP với timeout 10s. Nếu lỗi thì thử lại với backoff 5s·2ⁿ, tối đa 10 phút. Sau 8 lần thất bại, email chuyển sang `FAILED`, kèm metric `emails.failed` và alert.
- Email chỉ được đánh dấu `SENT` sau khi SMTP nhận thư. Đây là at-least-once: hiếm khi một thư có thể bị gửi trùng, và đổi lại thì không bao giờ mất thư.
- Môi trường dev dùng Mailpit, nên không thư nào ra khỏi máy.

**WebSocket qua gateway bằng ticket dùng một lần.**

1. Client gọi `POST /v1/ws/ticket` kèm JWT.
2. Gateway lưu 32 byte ngẫu nhiên vào Redis tại `gateway:ws:ticket:<t>` = `user_id`, sống 30 giây.
3. Client mở `GET /v1/ws?ticket=…`. Gateway kiểm tra `Origin` có thuộc danh sách CORS không (chống cross-site WebSocket hijacking; client không phải trình duyệt thì không gửi Origin nên được cho qua). Sau đó gateway đổi ticket bằng `GETDEL`, nên mỗi ticket chỉ dùng được một lần.
4. Gateway proxy kết nối (`httputil.ReverseProxy`) sang `notification:8005/ws`, đặt `X-User-ID` và bỏ query, `Origin`, `Cookie`, `Authorization`.

Endpoint `/ws` của Notification tin `X-User-ID` vì nó chỉ chạy trong mạng compose, cùng mô hình tin cậy với ADR 0010/0011.

Hub giữ danh sách kết nối theo user, trong bộ nhớ. Mỗi kết nối có hàng đợi giới hạn: client chậm làm đầy hàng đợi thì kết nối đó bị đóng, và `Push` không bao giờ chặn consumer. Kết nối được ping mỗi 30 giây. Gateway xoá read/write deadline của `http.Server` trước khi upgrade (`net/http` cũng tự xoá khi hijack), và có test kiểm tra một kết nối sống lâu hơn `WriteTimeout` vẫn nhận được tin.

## Consequences

- Notification chỉ chạy được một replica, vì hub giữ kết nối trong bộ nhớ. Muốn scale thì cần Redis pub/sub để phát tin giữa các replica.
- Nếu không mở thông báo trong 30 giây sau khi lấy ticket thì phải lấy ticket mới.
- Không có tuỳ chọn tắt email, không có template HTML phức tạp. Chuông thông báo ở frontend để M5.
- Hai bản sao có thể chậm hơn nguồn một chút. Khi đó thông báo dùng câu chung chung ("tin tuyển dụng của bạn") thay vì bỏ qua.
