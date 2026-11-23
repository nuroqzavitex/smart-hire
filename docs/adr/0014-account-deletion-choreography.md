# ADR 0014: Xoá tài khoản bằng saga choreography — Auth chỉ theo dõi, mỗi service tự purge

- Trạng thái: Chấp nhận (M4b)
- Ngày: 2026-11-24

## Context

NĐ 13/2023 cho người dùng quyền xoá dữ liệu cá nhân, và hệ thống phải **chứng minh được** đã xoá (`spec.md` §6.5, §14.4). Dữ liệu của một người nằm rải rác ở 6 service:

| Service | Dữ liệu |
|---|---|
| Auth | email, mật khẩu, refresh token |
| Candidate | hồ sơ, file CV trên MinIO, dữ liệu CV đã parse |
| Job | JD của recruiter |
| Application | đơn và lịch sử đơn |
| AI | vector, kết quả parse và kết quả match |
| Notification | bản sao email, thông báo, email đang chờ gửi |

Mỗi service sở hữu database riêng, nên không thể xoá tất cả trong một transaction.

Theo tiêu chí §6.6, việc xoá ghi vào nhiều service nhưng các bước không phụ thuộc nhau, không có thứ tự và không có gì phải hoàn tác: xoá chỉ có chiều đi tới.

## Decision

**Choreography, Auth làm tracker.**

1. `DELETE /v1/me` (nhập lại mật khẩu). Auth làm trong **một transaction**:
   - đặt `users.status=DELETION_PENDING`;
   - thu hồi mọi refresh token;
   - ghi `deletion_requests(PENDING, expected=[candidate, job, application, ai, notification])`;
   - ghi `user.deleted` vào outbox.

   Kết quả trả về là 202. Gọi lại với cùng mật khẩu trả về đúng yêu cầu cũ. Admin không được tự xoá.
2. Mỗi service consume `user.deleted`. Trong **một transaction** gồm `processed_events`, phần purge và outbox `user.data_purged{service}`. Mọi service purge **theo `user_id`, bất kể role**, nên không có nhánh nào bị bỏ sót.
3. Auth consume `user.data_purged`, thêm service vào `purged_services` theo kiểu tập hợp. Khi đủ 5, Auth xoá cứng user (refresh token bị xoá theo cascade) và các dòng outbox `user.registered` của user đó (vì chứa email), rồi đặt `COMPLETED`. `deletion_requests` chỉ chứa UUID và được giữ lại làm bằng chứng. Admin xem qua `GET /v1/admin/deletion-requests/{id}`.
4. **Tracker** chạy mỗi phút (`FOR UPDATE SKIP LOCKED`):
   - Yêu cầu chưa xong mà lần phát gần nhất đã quá 15 phút: phát lại `user.deleted` với **event_id mới**. Mỗi service xử lý lại từ đầu và luôn trả lời lại, kể cả khi không còn gì để xoá.
   - Yêu cầu `PENDING` quá 24 giờ: chuyển sang `STUCK`, đo bằng metric `account_deletions{status}`, có alert `AccountDeletionStuck` và runbook. Yêu cầu `STUCK` vẫn được phát lại và vẫn hoàn tất được.
5. **Gateway** giữ denylist `gateway:denylist:user:<id>` trên Redis, TTL 1 giờ (lớn hơn TTL 15 phút của access token).
   - Gateway ghi denylist **trước khi** trả 202. Nếu Redis lỗi thì trả 500 và client gọi lại.
   - Middleware `Denylist` (đứng sau `OptionalAuth`) từ chối token của user nằm trong denylist bằng 401. Nếu Redis lỗi khi đọc thì cho qua (fail-open) vì token sống ngắn và Auth đã chặn refresh.

**Xoá hay ẩn danh.** Mọi PII bị xoá: email, tên, SĐT, file CV, tên file, dữ liệu parse, nội dung thông báo, và các dòng outbox cũ có chứa chúng (`user.registered`, `cv.updated`, `cv.parsed`). UUID ngẫu nhiên không phải PII nên được giữ trong **tombstone tối thiểu**, để sự kiện đến muộn không làm dữ liệu sống lại.

| Service | Purge |
|---|---|
| candidate | xoá file trên MinIO **trong** transaction (lỗi thì rollback rồi retry); CV thành tombstone `DELETED` (không tên file, không dữ liệu parse) kèm `cv.updated{DELETED}`; xoá hồ sơ và quota |
| job | mọi JD của recruiter: đóng, soft-delete, `recruiter_id = uuid.Nil`, phát `job.deleted` (projector, AI và application đã có sẵn xử lý cho sự kiện này) |
| application | đơn còn sống của ứng viên: `CANCELLED (account_deleted)`, saga chuyển `COMPENSATED` và **tăng version** để worker đang giữ lease bị version guard chặn; mỗi đơn nhận `candidate_id` ngẫu nhiên riêng, bỏ CV, idempotency key và breakdown; `actor` trong lịch sử thành `deleted-user`; `recruiter_id = uuid.Nil`. Không phát `application.status_changed` vì không còn ai để báo |
| ai (matcher) | tombstone `match_cvs` với **version lớn nhất (int64)**; xoá vector, `match_results`, `cv_parse_results` (cột mới `candidate_id`) và các dòng outbox `cv.parsed`; `match_jobs.recruiter_id = nil`. Worker không ghi kết quả parse cho CV đã là tombstone |
| notification | xoá thông báo và email đang chờ gửi; bản sao `users` thành tombstone (email rỗng, `deleted`), nên sự kiện đến muộn không tạo thông báo và `user.registered` đến muộn không đưa email trở lại; đóng WebSocket của user; thêm outbox và relay |

## Consequences

- Xoá tài khoản chạy được mà không service nào phải gọi đồng bộ tới service khác. Một service sập thì yêu cầu xoá chờ lâu hơn, không bị mất: consumer retry, DLQ và tracker phát lại.
- Có bằng chứng: `deletion_requests` ghi ai đã xác nhận và lúc nào, có metric và alert cho yêu cầu bị treo.
- Event `user.deleted` mới luôn được trả lời lại, nên việc phát lại an toàn. Đổi lại, Auth nhận nhiều `user.data_purged` trùng; chúng được dedupe theo `event_id` và theo tập hợp service.
- **Giới hạn đã chấp nhận:**
  - Log Kafka vẫn giữ sự kiện cũ (gồm `user.registered` có email, `cv.parsed` có dữ liệu CV) cho tới hết retention. Backup không được xử lý.
  - Service phía sau chỉ verify JWT, không đọc denylist, nên một token còn hạn gọi trực tiếp vào mạng nội bộ vẫn qua được (ADR 0003).
  - Một parse đang chạy dở ghi kết quả đúng lúc matcher tạo tombstone (cùng thời điểm, isolation READ COMMITTED) có thể để lại một dòng `cv_parse_results` mồ côi.
  - Tombstone giữ UUID của người đã bị xoá.
- Một sự kiện đến muộn có thể khiến một số bản sao giữ lại UUID chủ cũ: `job.updated` cũ ở Notification hoặc AI, hay quota mà saga đặt ngay trước khi purge. Đây là UUID, không phải PII.

## Alternatives considered

- **Orchestration (Auth gọi tuần tự 5 RPC nội bộ)**: trạng thái tập trung, nhưng thêm 5 RPC không có JWT, Auth phụ thuộc đồng bộ vào mọi service, và không bước nào cần compensation. Theo §6.6 đây là trường hợp nên dùng choreography.
- **Chỉ xoá ở Auth, các service lọc khi đọc**: PII vẫn nằm trong database của các service khác, nên không chứng minh được đã xoá.
- **Thời gian ân hạn, cho huỷ yêu cầu xoá**: hay cho người dùng, nhưng cần thêm trạng thái và một lệnh "khôi phục" mà mọi service phải hiểu. Để sau M5.
- **Ẩn danh thay vì tombstone ở Candidate và AI**: sự kiện đến muộn, như kết quả parse hay `cv.updated` cũ, sẽ tạo lại dữ liệu. Tombstone cùng version guard chặn được việc này mà không cần thêm bảng.
