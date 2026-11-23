# M4b — Saga xoá tài khoản (choreography)

Status: approved (user giao toàn quyền: "làm hết phần m4b đi, đừng hỏi lại") · Date: 2026-11-24 · Milestone: M4b (sau M4a)

## 1. Mục đích

Người dùng có quyền xoá tài khoản và dữ liệu cá nhân (NĐ 13/2023, `spec.md` §6.5, §14.4). Hệ thống phải
xoá hoặc ẩn danh dữ liệu ở **mọi** service và **chứng minh được** đã xoá. Đây là ví dụ cho saga
**choreography** trong portfolio: các bước độc lập, chỉ đi tới, không compensation. Nó đối lập với saga
orchestration của M3.

**Mục tiêu**
- Endpoint `DELETE /v1/me` (nhập lại mật khẩu) trả 202. Auth chuyển tài khoản sang `DELETION_PENDING`,
  thu hồi refresh token, ghi `deletion_requests` và phát `user.deleted` qua outbox.
- Năm service tự dọn dữ liệu của mình (candidate, job, application, ai, notification) rồi phát
  `user.data_purged`. Auth gom các xác nhận; đủ 5/5 thì xoá cứng user và đặt `COMPLETED`.
- Gateway từ chối mọi access token của user đang bị xoá (denylist Redis).
- Có tracker: phát lại `user.deleted` cho yêu cầu chưa xong, đánh `STUCK` sau 24h, có metric, alert và runbook.
- Admin xem được bằng chứng xoá: `GET /v1/admin/deletion-requests/{id}`.

**Ngoài phạm vi**: thời gian ân hạn hay huỷ yêu cầu xoá; xoá dữ liệu đã nằm trong log Kafka (retention
mặc định 7 ngày) hoặc bản backup; admin xoá hộ người khác; FE (M5).

**Anti-goal**: không được để PII sống lại sau khi đã purge (sự kiện đến muộn hoặc parse đang chạy dở),
không dừng hẳn một service khi gặp sự kiện lỗi, không log PII.

## 2. Các cách đã cân nhắc

| Cách | Ưu | Nhược |
|---|---|---|
| **A. Choreography + Auth làm tracker** (chọn) | đúng spec §6.5; các bước độc lập, chạy song song; mỗi service tự retry qua consumer M1a (retry → DLQ) | phải có tracker và phát lại để không treo vĩnh viễn |
| B. Orchestration (Auth gọi gRPC lần lượt tới từng service) | trạng thái tập trung | thêm 5 RPC nội bộ; Auth phụ thuộc đồng bộ vào tất cả; không có bước nào cần compensation nên orchestration là thừa (spec §6.6) |
| C. Chỉ xoá ở Auth, các service lazy-check khi đọc | đơn giản | PII vẫn nằm trong DB của service khác, không chứng minh được |

## 3. Luồng

```
DELETE /v1/me {password}
 Gateway ─► Auth.DeleteMe ─ tx: verify password · status=DELETION_PENDING · revoke refresh tokens ·
          │                 deletion_requests(PENDING, expected=[5]) · outbox user.deleted
          ◄─ 202 {deletion_request_id, status}
 Gateway: SET gateway:denylist:user:<sub> (TTL 1h) rồi mới trả 202 (lỗi Redis → 500, client gọi lại; DeleteMe idempotent)
 Kafka user.deleted ─► candidate | job | application | ai(matcher) | notification
      mỗi service: 1 tx = purge + processed_events + outbox user.data_purged{service}
 Kafka user.data_purged ─► Auth (group auth-deletion-tracker)
      tx: thêm service vào purged_services; đủ expected → xoá cứng users (+refresh_tokens cascade)
          + outbox user.registered của user đó (chứa email) → COMPLETED
```

## 4. Hợp đồng

**Events** (`contracts/events`, key = `user_id`):
- `user.deleted` v1 (source `auth-service`): `user_id, role, deletion_request_id, requested_at`.
- `user.data_purged` v1: `user_id, service (candidate|job|application|ai|notification), deletion_request_id, purged_at`.
- `cv.updated` v1: enum `status` thêm `DELETED` (thay đổi additive). Lúc này `parsed` và `parsed_at` là null.

Quy tắc idempotent (spec §6.5): dedupe theo `event_id`. Một `user.deleted` **mới** (do tracker phát lại)
được xử lý lại từ đầu và luôn phát lại `user.data_purged`, kể cả khi không còn gì để xoá. Mọi service purge
**theo `user_id` bất kể `role`** (role chỉ là gợi ý), nên không có nhánh nào bị bỏ sót.

**Proto `AuthService`**
- `DeleteMe(DeleteMeRequest{password}) → DeleteMeResponse{deletion_request_id, status}`, map `delete: "/v1/me" body: "*"`.
- `GetDeletionRequest(GetDeletionRequestRequest{id}) → DeletionRequest{id, user_id, status, expected_services[], purged_services[], requested_at, completed_at}`,
  map `get: "/v1/admin/deletion-requests/{id}"`, chỉ admin.

**Lỗi**
- Sai mật khẩu → 401 `invalid password`.
- Admin tự xoá → 403 `admin accounts cannot be deleted` (seed cần admin).
- Gọi lại khi đã `DELETION_PENDING` → 202 với cùng request (sau khi verify mật khẩu).
- `GetDeletionRequest` không thấy → 404.

## 5. Dữ liệu từng service

Nguyên tắc (ADR 0014): **xoá mọi PII** (email, tên, SĐT, nội dung và tên file CV, parsed data, nội dung
thông báo). UUID ngẫu nhiên không phải PII: được phép giữ lại trong **tombstone tối thiểu** để chặn sự kiện
đến muộn. Ngoại lệ là Application: thay `candidate_id` bằng pseudonym để "giữ thống kê, bỏ liên kết cá nhân".

| Service | Purge (một tx, kèm outbox `user.data_purged`) |
|---|---|
| **auth** | `DELETION_PENDING` lúc yêu cầu; khi đủ 5/5: `DELETE users` (cascade refresh_tokens), `DELETE outbox WHERE aggregate_id=user AND event_type='user.registered'`; `deletion_requests` giữ lại làm bằng chứng (chỉ có UUID) |
| **candidate** | Xoá object MinIO của từng CV **trước** tx (Delete idempotent). Trong tx: tombstone mỗi CV (`status=DELETED`, `file_name=''`, `parsed_data/parse_error/attempt_id=NULL`, `version+1`) kèm `cv.updated{DELETED}`; xoá `candidates`, `daily_quota`, `quota_reservations` của user. Kết quả parse đến muộn cho CV `DELETED` bị coi là stale và bỏ qua. Đọc (`GetCV`, `ListCVs`) ẩn CV `DELETED` |
| **job** | Mọi job chưa xoá của recruiter: `status=closed`, `deleted_at=now`, `recruiter_id=00000000-…`, `version+1`, outbox `job.deleted` (projector, AI và application đã xử lý sẵn). Cần `processed_events` và consumer mới trong binary server |
| **application** | Đơn của candidate: đơn còn sống → `CANCELLED` (reason `account_deleted`), saga đang chạy → `COMPENSATED` (`version+1`, nên mọi lần ghi muộn của worker bị version guard chặn); `candidate_id` → UUID ngẫu nhiên riêng từng đơn, `cv_id` → nil, `idempotency_key='purged:'+id`, `request_hash=''`, `score_breakdown=NULL`. Đơn vào job của recruiter: `recruiter_id` → nil. History: `actor` của user → `deleted-user`. **Không** phát `application.status_changed` cho các thay đổi do purge |
| **ai** (matcher) | Xoá `match_results` theo candidate, vector CV trong Qdrant, `cv_parse_results` (cột mới `candidate_id`, lấy từ `cv.uploaded`, cộng thêm theo `match_cvs`); `match_cvs` của user thành tombstone (`status=DELETED`, `parsed=NULL`, `active=false`, `version=max int64`); `match_jobs.recruiter_id` → nil. Matcher coi `cv.updated{DELETED}` như CV không parse (bỏ vector và results) và xoá luôn `cv_parse_results` của CV đó; worker không ghi `cv_parse_results` cho CV đã tombstone |
| **notification** | Xoá `email_outbox` (theo notification của user), `notifications`; `users` → tombstone (`email=''`, `deleted=true`): sự kiện đến muộn không tạo thông báo cho user này nữa; `jobs.recruiter_id` → NULL; đóng WebSocket của user. Thêm `outbox` + relay (service này chưa từng phát event) |

## 6. Auth — tracker

- `deletion_requests(id, user_id, role, status PENDING|COMPLETED|STUCK, expected_services text[], purged_services text[], requested_at, last_published_at, completed_at)`,
  partial unique `(user_id) WHERE status <> 'COMPLETED'`. Thêm bảng `processed_events`.
- `HandleDataPurged`: dedupe; khoá yêu cầu; bỏ qua nếu `COMPLETED` hoặc `user_id` không khớp; `service` ngoài expected là poison.
  Thêm service (dạng set); đủ thì hard delete và đặt `COMPLETED`. Yêu cầu `STUCK` vẫn hoàn tất được nếu xác nhận đến muộn.
- Vòng tracker (mỗi `AUTH_DELETION_SWEEP`, mặc định 1m, `FOR UPDATE SKIP LOCKED`):
  - `PENDING` có `last_published_at < now - AUTH_DELETION_REPUBLISH_AFTER` (15m) → phát lại `user.deleted` (event_id mới, cùng request id).
  - `PENDING` có `requested_at < now - AUTH_DELETION_STUCK_AFTER` (24h) → `STUCK`, log Warn.
- Metric `account_deletions{status}` (gauge theo `CountByStatus`). Alert `AccountDeletionStuck`: `account_deletions{status="STUCK"} > 0`.
  Runbook `docs/runbooks/account-deletion-stuck.md`.

## 7. Gateway

- Hook trên `ForwardResponseOption` cho `AuthService_DeleteMe`: `SET gateway:denylist:user:<sub> 1 EX GATEWAY_DENYLIST_TTL` (mặc định 1h, lớn hơn TTL access 15m), rồi trả 202.
- Middleware `Denylist` chạy ngay sau `OptionalAuth`: token hợp lệ của user nằm trong denylist bị đánh dấu auth-failed (→ 401 ở `RejectInvalidAuth`).
  Redis lỗi thì **fail-open** kèm log Warn: token sống tối đa 15m và Auth đã chặn refresh.
- Hạn chế đã chấp nhận: service phía sau chỉ verify JWT, không đọc denylist (ADR 0003).

## 8. Kiểm thử và demo

- Unit: domain + app từng service (coverage ≥ 70% như cũ), middleware denylist, hook 202.
- Integration (testcontainers): repo purge từng service; Auth `DeleteMe` → `HandleDataPurged` ×5 → user biến mất.
- Smoke: candidate (có CV, đã nộp đơn) và recruiter (có job) gọi `DELETE /v1/me`.
  - Ngay sau đó: token cũ trả 401, login trả 403.
  - Admin poll `GET /v1/admin/deletion-requests/{id}` tới `COMPLETED` với đủ 5 service; sau đó login trả 401.
  - Job của recruiter không còn GET được; Kafka có 5 `user.data_purged`.
- `obs-check`: 8 rule.

## 9. Quyết định để builder tự chọn

Tên hàm, cách chia file, query sqlc cụ thể; thứ tự task. Mọi lệch khỏi tài liệu này phải ghi thành
Ruling trong ledger.
