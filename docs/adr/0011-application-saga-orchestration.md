# ADR 0011: Nộp đơn bằng Saga orchestration tự viết — worker quét bảng, lease, version guard

- Trạng thái: Chấp nhận (M3)
- Ngày: 2026-11-23

## Context

Nộp đơn (spec §5.5, §6.4) ghi vào hai service: Application tạo đơn, Candidate giữ một suất trong quota nộp đơn mỗi ngày. Nếu bước sau thất bại thì suất đó phải được trả lại và đơn bị huỷ. Ngoài ra đơn cần điểm từ AI, nhưng AI chậm hoặc sập không được làm mất đơn: spec cho đơn sang `NEEDS_MANUAL_REVIEW`. Theo tiêu chí ở §6.6 (ghi vào nhiều service, cần hoàn tác, nhiều bước phụ thuộc) đây là chỗ dùng Saga orchestration. Spec Q2 chọn tự viết thay vì Temporal để học pattern.

## Decision

**Request chỉ làm T1, saga chạy nền.** `POST /v1/applications` kiểm tra JD đang mở (Job `GetJob`) và CV đã parse, thuộc người gọi (Candidate `GetCV`, owner-only), cả hai bằng JWT của người gọi. Sau đó một transaction ghi đơn `PENDING`, `saga_instances` (bước `RESERVE_QUOTA`), lịch sử và outbox (`application.submitted`, `application.status_changed`), rồi trả 202. Các bước sau không phụ thuộc thời gian của request. Đánh đổi: hết quota không trả 429 ngay mà đơn chuyển `CANCELLED` (`reason=quota_exceeded`).

**Worker quét bảng `saga_instances`, lease bằng `next_run_at`.** Mỗi vòng, worker `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` để nhận tối đa 10 saga đến hạn, đẩy `next_run_at` thêm 30 giây (lease), tăng `attempts` và `version`. Lệnh claim commit trước khi gọi ra ngoài, nên không giữ transaction trong lúc chờ gRPC. Mỗi lần claim chạy một bước, rồi ghi kết quả trong một transaction gồm saga, đơn, lịch sử và outbox. Lệnh ghi có điều kiện `version = <version lúc claim>`: nếu một worker khác đã nhận lại saga sau khi lease hết, hoặc consumer `job.closed` đã chuyển saga sang bù trừ, thì kết quả cũ bị bỏ. Recovery không cần code riêng: process chết giữa bước thì lease hết, saga lại đến hạn và bước chạy lại. Vì vậy mọi bước đều idempotent. Request nộp đơn đánh thức worker qua một channel trong process để không phải chờ chu kỳ quét 500 ms.

**Các bước và bù trừ.**

| Bước | Kết quả |
|---|---|
| `RESERVE_QUOTA` | OK → `SCREENING`, sang `SCORE`. Hết quota → `CANCELLED(quota_exceeded)`, không cần bù. Lỗi tạm thời → thử lại, backoff 1s·2ⁿ tối đa 30s, tới deadline 10 phút → bù trừ (`timeout`). |
| `SCORE` | OK → `SCREENED` + điểm. AI lỗi, timeout, breaker mở → `NEEDS_MANUAL_REVIEW(ai_unavailable)`, giữ quota. AI chưa có bản sao CV/JD (`NotFound`) → thử lại tối đa 2 phút rồi `NEEDS_MANUAL_REVIEW(not_matched)`. Deadline không áp cho bước này: AI chậm không bao giờ làm huỷ đơn. |
| `RELEASE_QUOTA` (bù trừ) | `ReleaseQuota` rồi `CANCELLED(<reason>)`. Thử lại mãi tới khi thành công. |

JD bị đóng hoặc xoá giữa chừng cũng dẫn tới bù trừ: Application consume `job.closed|job.deleted` (group `application`, idempotent qua `processed_events`) và chuyển mọi saga còn `RUNNING` của JD đó sang `RELEASE_QUOTA` (`job_closed`). Đơn đã qua saga thì giữ nguyên để recruiter tự xử lý. Consumer cũng ghi JD vào bảng `closed_jobs` (đóng và xoá đều là trạng thái cuối). T1 tra bảng này trước khi gọi Job, nên ứng viên nộp vào JD đã đóng nhận 409 "job is not open" thay vì 404. Bước `RESERVE_QUOTA` cũng tra bảng này, nên một `job.closed` được xử lý trước khi transaction T1 commit (lúc đó chưa có saga nào để huỷ) vẫn dẫn tới bù trừ.

**Quota ở Candidate, idempotent theo `application_id`.** `ReserveQuota` ghi dòng `quota_reservations` trước (`ON CONFLICT DO NOTHING`), rồi mới tăng `daily_quota.used` có điều kiện `used < limit`. Hai lệnh cho cùng một đơn vì vậy xếp hàng trên khoá chính, và quota chỉ bị tính một lần. Hết quota thì xoá dòng giữ chỗ. `ReleaseQuota` cho một đơn chưa từng giữ chỗ sẽ ghi một tombstone `RELEASED`, để một `ReserveQuota` đến muộn (client đã timeout nhưng server vẫn chạy) nhận `ABORTED` thay vì lấy mất quota. Spec §6.4 ghi rằng đưa quota về Application sẽ gộp T1+T2 vào một transaction cục bộ; giữ ở Candidate là có chủ ý, để minh hoạ compensation.

**RPC quota là nội bộ, không JWT.** Worker không có JWT của người dùng, và lưu JWT vào DB để dùng sau là sai. `ReserveQuota`/`ReleaseQuota` vì vậy được allow-list không cần token trong `grpcx` của Candidate. Hai RPC này không có annotation HTTP, nên gateway không route tới. Đây cùng mô hình tin cậy với AI gRPC ở ADR 0010.

**Resilience theo spec §14.2.** Timeout 3s cho Job/Candidate, 5s cho AI. Retry tối đa 3 lần, backoff mũ có jitter, chỉ cho `UNAVAILABLE`/`DEADLINE_EXCEEDED`, dùng retry policy có sẵn của grpc-go qua service config thay vì tự viết vòng lặp. Breaker `sony/gobreaker` cho AI: mở sau 5 lỗi liên tiếp, half-open sau 30s; `NotFound`, `InvalidArgument` và việc chính caller huỷ không tính là lỗi. Breaker đổi trạng thái thì ghi log; `make chaos-ai` dựa vào log này.

**Idempotency-Key lưu ở Postgres thay vì Redis 24h** (lệch spec §14.3). Key nằm trên chính dòng đơn với `UNIQUE (candidate_id, idempotency_key)` và được ghi cùng transaction T1. Nhờ vậy không có khe hở giữa "ghi key" và "ghi đơn", không phụ thuộc Redis, và key được giữ cùng đơn. Gửi lại cùng key và cùng nội dung thì nhận lại đơn cũ. Cùng key nhưng khác nội dung → 400. Hai request cùng key chạy song song: request thua nhận lỗi unique, đọc lại và trả đơn của request thắng. Mỗi `(job, ứng viên)` chỉ có một đơn "sống" (unique index bỏ qua `CANCELLED`).

## Consequences

- Saga xem được hoàn toàn trong một bảng (`step`, `state`, `attempts`, `last_error`, `next_run_at`), test được bằng testcontainers mà không cần broker.
- Nhiều replica Application chạy được: `SKIP LOCKED` chia việc, version guard chặn kết quả trễ, và worker bỏ qua saga trong batch đã hết lease trước khi tới lượt. Còn một khe nhỏ: bước bắt đầu ngay trước khi lease hết có thể chạy song song với worker khác. Mọi bước đều idempotent, trừ một trường hợp: `ReserveQuota` hết quota thì xoá dòng giữ chỗ chứ không để tombstone, nên ở khe đó một suất quota trong ngày có thể bị giữ mà không được trả lại. Compose chạy một replica.
- Các bước là lời gọi đồng bộ: Candidate sập thì saga chờ và thử lại, tới deadline thì bù trừ. Command/reply qua Kafka tách rời hơn nhưng cần thêm khoảng 4 topic, schema, consumer và outbox ở Candidate, cùng timeout tự cài đặt.
- Mỗi bước saga có một span (`saga <STEP>`), là cha của các lời gọi gRPC trong bước, và một log Info ghi kết quả. Event do bước phát ra mang trace của span đó. Chưa có metrics `saga_instances{state}` / `circuit_breaker_state` (để M4).
- Phase 2 có thể thay worker bằng Temporal mà giữ nguyên các bước và RPC quota.
