# M3 — Application service + nộp đơn bằng Saga (orchestration) + resilience — design

Status: approved approach (A + 1), pending doc review · Date: 2026-11-23 · Milestone: M3 (after M2b)

## 1. Intent

SmartHire là dự án portfolio Backend-Go (dữ liệu giả, làm để học). M3 thêm **Application service** của
`spec.md` §5.5/§6.4: ứng viên nộp một CV đã parse vào một tin đang mở, nhà tuyển dụng xử lý đơn theo state
machine. Giá trị portfolio nằm ở **Saga orchestration tự viết** (spec Q2: chưa dùng Temporal) có compensation
và recovery, cùng các pattern resilience: timeout, retry chỉ cho lỗi tạm thời, circuit breaker, fallback
`NEEDS_MANUAL_REVIEW`, idempotency key. Tiêu chí M3 trong M0 design: *kill AI → `NEEDS_MANUAL_REVIEW`, CB mở*.

## 2. Goals / non-goals / anti-goals

Goals
- Service mới `services/application` (Go, hexagonal như Auth/Job/Candidate), DB `application_db`.
- `POST /v1/applications` (header `Idempotency-Key`) trả **202** ngay với đơn `PENDING`; saga chạy nền:
  giữ quota ở Candidate → AI chấm điểm → `SCREENED` + điểm, hoặc `NEEDS_MANUAL_REVIEW` khi AI lỗi.
- Compensation thật: hết hạn saga hoặc tin bị đóng/xoá giữa chừng → trả quota (`ReleaseQuota`) + `CANCELLED`.
- Recovery: service crash giữa saga → khởi động lại tự chạy tiếp từ bước đang dở.
- Recruiter đổi trạng thái theo state machine; mọi thay đổi ghi `application_history` và phát
  `application.status_changed` qua outbox.
- Candidate có quota nộp đơn/ngày (`daily_quota`) và RPC nội bộ `ReserveQuota`/`ReleaseQuota`.
- Smoke mở rộng + một kịch bản chaos (`make chaos-ai`).

Non-goals (để sau)
- Notification/consumer của `application.*` (M4), xoá tài khoản (M4), FE/Kanban (M5), Temporal (phase 2).
- Metrics Prometheus `saga_instances{state}` / `circuit_breaker_state` (observability đầy đủ ở M4); M3 chỉ
  log + span cho mỗi bước saga và mỗi lần breaker đổi trạng thái.
- Ứng viên tự rút đơn; ghi chú của recruiter; đơn trên tin đã đóng tự bị từ chối (xem §6.4).

Anti-goals
- Một request bị lặp (client retry, double click) không bao giờ tạo hai đơn hay trừ quota hai lần.
- AI sập không được làm đơn kẹt hay bị huỷ: đơn vẫn vào tay recruiter (`NEEDS_MANUAL_REVIEW`).
- Không lưu JWT của người dùng vào DB để worker dùng sau.
- Test/CI/smoke không gọi Anthropic/Gemini thật (giữ quy tắc cũ; AI trong compose dùng fake).

## 3. Approaches considered

Saga chạy ở đâu (user chọn **A**):
- **A. Chạy nền** — *được chọn*. Request chỉ làm T1 (kiểm tra + ghi đơn `PENDING` và saga trong một
  transaction), trả 202; worker chạy các bước sau. Không phụ thuộc AI chậm/sập; crash thì chạy tiếp. Đánh đổi:
  hết quota không trả 429 ngay mà đơn chuyển `CANCELLED` (`reason=quota_exceeded`).
- B. Đồng bộ trong request: đơn giản, có 429 ngay, nhưng request chờ AI tới 5s × retry; recovery chỉ là dọn dẹp.
- C. Lai (T1+T2 đồng bộ, T3+ chạy nền): có 429 ngay nhưng hai đường thực thi cho cùng một saga.

Worker điều phối thế nào (user chọn **1**):
- **1. Worker quét bảng `saga_instances` + gọi gRPC** — *được chọn*. Lease bằng `next_run_at`,
  `FOR UPDATE SKIP LOCKED` như outbox relay; mỗi bước là một lời gọi gRPC có timeout/retry/breaker; recovery là
  chính vòng quét đó. Dùng lại pattern có sẵn, test bằng testcontainers, nhìn tiến độ saga trong một bảng.
- 2. Command/reply qua Kafka (`quota.reserve` → `quota.reserved`): tách rời hơn nhưng thêm ~4 topic, schema,
  consumer + outbox ở Candidate và phải tự làm timeout — gần gấp đôi khối lượng, không thêm giá trị trình diễn.

## 4. Architecture

```
client ─HTTP─▶ gateway ─gRPC(JWT)─▶ application ──T1 (JWT người dùng)──▶ job.GetJob, candidate.GetCV
                                        │ tx: applications + saga_instances + outbox
                                        ▼
                               saga worker (cùng process)
                                 ├─ T2 ─gRPC nội bộ─▶ candidate.ReserveQuota / ReleaseQuota
                                 └─ T3 ─gRPC nội bộ─▶ ai-api.ScoreMatch  (timeout 5s, retry, breaker)
                               outbox relay ──▶ Kafka application.submitted | application.status_changed
                               consumer ◀── Kafka job.closed | job.deleted  (group "application")
```

- Service: gRPC `:9004`, HTTP `:8004` (health), env prefix `APPLICATION_`, role/DB `application_svc` /
  `application_db`, proto `contracts/proto/smarthire/application/v1/application.proto`. Một binary
  (`cmd/server`) chạy gRPC server + saga worker + outbox relay + consumer, như Candidate.
- Gọi T1 bằng JWT người dùng (forward metadata `authorization`, như Candidate → Job ở M2b). Worker **không có
  JWT**, nên `ReserveQuota`/`ReleaseQuota` là RPC nội bộ không JWT (allow-list trong `grpcx` của Candidate,
  không có annotation HTTP nên gateway không route tới) — cùng mô hình tin cậy với AI gRPC ở ADR 0010.
- Gateway: đăng ký `ApplicationService` handler, forward header `Idempotency-Key` → metadata
  `idempotency-key`, trả `202` cho `SubmitApplication` (theo RPC method, như `CreateJob` → 201).

## 5. Data model (`application_db`)

```sql
applications(
  id uuid PK, job_id uuid, candidate_id uuid,      -- candidate_id = user id (như candidate_db)
  recruiter_id uuid,                               -- chụp từ Job lúc nộp, để recruiter lọc không cần gọi Job
  cv_id uuid, status text, reason text NULL,       -- reason: quota_exceeded | job_closed | timeout | ai_unavailable | ...
  score numeric(5,2) NULL, score_breakdown jsonb NULL, model_version text NULL,
  idempotency_key text, request_hash text,
  version bigint, created_at, updated_at,
  UNIQUE (candidate_id, idempotency_key))
-- một đơn "sống" cho mỗi (job, ứng viên); đơn CANCELLED không chặn nộp lại:
CREATE UNIQUE INDEX ... ON applications (job_id, candidate_id) WHERE status <> 'CANCELLED';

application_history(id bigserial PK, application_id uuid, from_status text NULL, to_status text,
                    actor text,  -- user id, hoặc 'system' cho saga
                    reason text NULL, at timestamptz)

saga_instances(id uuid PK, application_id uuid UNIQUE, step text, state text,
               payload jsonb, attempts int, next_run_at timestamptz, deadline_at timestamptz,
               last_error text NULL, version bigint, created_at, updated_at)
-- index (next_run_at) WHERE state IN ('RUNNING','COMPENSATING')

processed_events(...), outbox(...)   -- như các service Go khác (pkg/idempotency, pkg/outbox)
```

Candidate thêm (migration `000002`):
```sql
daily_quota(candidate_id uuid, day date, used int, PRIMARY KEY (candidate_id, day))
quota_reservations(application_id uuid PK, candidate_id uuid, day date,
                   state text,  -- RESERVED | RELEASED
                   created_at, updated_at)
```

## 6. Saga nộp đơn

### 6.1 T1 — trong request `SubmitApplication(job_id, cv_id)`
1. Chỉ role `candidate`; thiếu/sai `Idempotency-Key` (1–128 ký tự) → 400.
2. Nếu đã có đơn với `(candidate_id, idempotency_key)`: cùng `request_hash` (sha256 của job_id+cv_id) → trả
   lại đơn đó (202, trạng thái hiện tại); khác hash → 400 (`ErrValidation`, "idempotency key reused with a
   different request").
3. `job.GetJob` (JWT người dùng): NotFound → 404; status ≠ OPEN → 409 "job is not open". Lấy `recruiter_id`.
4. `candidate.GetCV` (owner-only, nên `candidate_id = sub`): NotFound → 404; status ≠ PARSED → 409
   "cv is not parsed".
5. Một transaction: insert `applications(PENDING)` + `application_history(NULL→PENDING, actor=sub)` +
   `saga_instances(step=RESERVE_QUOTA, state=RUNNING, next_run_at=now, deadline_at=now+10m)` + outbox
   `application.submitted` và `application.status_changed(NULL→PENDING)`. Vi phạm unique (job, candidate)
   → 409 "already applied". Vi phạm unique idempotency (hai request cùng key chạy song song) → đọc lại và trả
   như bước 2.
6. Đánh thức worker (channel trong process) để không phải chờ chu kỳ quét; trả 202.

Job/Candidate lỗi tạm thời ở T1 → 503 (`ErrUnavailable`), chưa ghi gì.

### 6.2 Worker
- Mỗi `APPLICATION_SAGA_POLL` (500ms) hoặc khi được đánh thức: **claim** tối đa `APPLICATION_SAGA_BATCH` (10)
  saga đến hạn trong một transaction ngắn:
  `UPDATE saga_instances SET next_run_at = now()+lease, attempts = attempts+1, version = version+1
   WHERE id IN (SELECT id ... WHERE state IN ('RUNNING','COMPENSATING') AND next_run_at <= now()
   ORDER BY next_run_at FOR UPDATE SKIP LOCKED LIMIT n) RETURNING *`. Lease `APPLICATION_SAGA_LEASE` (30s)
  > thời gian tối đa một bước. Không giữ transaction trong lúc gọi gRPC.
- Chạy bước của từng saga (tuần tự hoặc vài goroutine — builder chọn), rồi ghi kết quả trong **một
  transaction** gồm: saga (bước kế/state/`next_run_at`), đơn (status/score/reason, `version+1`),
  `application_history`, outbox `application.status_changed`. Ghi có điều kiện `version = <version lúc claim>`;
  0 dòng → có ai khác (consumer job.closed, worker khác sau khi lease hết) đã đổi saga → bỏ kết quả, không lỗi.
- **Recovery** chính là vòng quét này: crash giữa bước → lease hết → saga được claim lại và bước chạy lại.
  Mọi bước vì vậy phải **idempotent** (§6.3).
- Quá `deadline_at` (10 phút, `APPLICATION_SAGA_DEADLINE`) mà vẫn ở bước `RESERVE_QUOTA` → chuyển
  `COMPENSATING` (`reason=timeout`). Đây là "job recovery quét saga treo quá 10 phút" của spec §6.4. Deadline
  **không** áp cho bước `SCORE`: quá hạn ở đó thì đơn ra `NEEDS_MANUAL_REVIEW` (AI chậm không được làm huỷ đơn).

### 6.3 Các bước

| Bước (`step`) | Hành động | Kết quả |
|---|---|---|
| `RESERVE_QUOTA` | `candidate.ReserveQuota(application_id, candidate_id)` | OK → đơn `SCREENING`, step `SCORE`. `RESOURCE_EXHAUSTED` → đơn `CANCELLED(quota_exceeded)`, saga `COMPLETED` (chưa giữ gì nên không cần bù). `ABORTED` (đã bị release trước) → `COMPENSATING`. Lỗi tạm thời → thử lại với backoff 1s×2^n (tối đa 30s) tới deadline. |
| `SCORE` | `ai.ScoreMatch(cv_id, job_id)` | OK → đơn `SCREENED` + score/breakdown/model_version, saga `COMPLETED`. AI lỗi tạm thời sau retry, timeout, hoặc breaker mở → đơn `NEEDS_MANUAL_REVIEW(ai_unavailable)`, saga `COMPLETED` (**không** trả quota — spec §6.4). `NotFound` (AI chưa có bản sao job/CV) → thử lại với backoff tới `APPLICATION_SCORE_NOT_FOUND_WINDOW` (2 phút) kể từ lúc vào bước, sau đó `NEEDS_MANUAL_REVIEW(not_matched)`. |
| `RELEASE_QUOTA` (state `COMPENSATING`) | `candidate.ReleaseQuota(application_id)` | OK → đơn `CANCELLED(<reason>)`, saga `COMPENSATED`. Lỗi tạm thời → thử lại mãi với backoff (bù trừ phải hoàn tất; không có deadline). |

Idempotency phía Candidate (một transaction mỗi RPC):
- `ReserveQuota`: có `quota_reservations[application_id]` = RESERVED → OK (không trừ lần hai); = RELEASED →
  `ABORTED`. Chưa có → `daily_quota.used < limit` thì `used+1` + insert RESERVED, ngược lại `RESOURCE_EXHAUSTED`.
- `ReleaseQuota`: RESERVED → RELEASED + `used-1`; RELEASED → OK; **chưa có → insert RELEASED (tombstone)**
  để một `ReserveQuota` đến muộn (bị timeout phía client nhưng vẫn chạy trên server) không trừ quota.
- Ngày quota theo **UTC** (`day = current_date` của Postgres UTC); limit `CANDIDATE_DAILY_APPLICATION_QUOTA`
  (mặc định 10). Quota tính theo ngày giữ chỗ, không trả lại khi đơn sau đó bị recruiter từ chối.

### 6.4 Tin bị đóng/xoá giữa chừng
Application consume `job.closed` và `job.deleted` (idempotent qua `processed_events`). Với mọi saga
`RUNNING` của đơn thuộc job đó: chuyển `COMPENSATING`, `reason=job_closed`, `next_run_at=now`, `version+1`
(một transaction). Đơn đã `SCREENED`/`NEEDS_MANUAL_REVIEW`/sau đó **không bị đụng** — recruiter tự xử lý.
Bù trừ luôn gọi `ReleaseQuota` (an toàn cả khi T2 chưa từng thành công nhờ tombstone).

## 7. State machine & recruiter

```
PENDING ─▶ SCREENING ─▶ SCREENED ───────────┬─▶ INTERVIEW ─▶ OFFERED ─▶ HIRED
   │           └─▶ NEEDS_MANUAL_REVIEW ──────┤        │           │
   └─▶ CANCELLED (saga)   SCREENING ─▶ CANCELLED       └───────────┴─▶ REJECTED
                                     (SCREENED / NEEDS_MANUAL_REVIEW cũng ─▶ REJECTED)
```
- Saga sở hữu `PENDING`, `SCREENING` và các chuyển sang `SCREENED`, `NEEDS_MANUAL_REVIEW`, `CANCELLED`.
- Recruiter (chủ tin = `recruiter_id` của đơn) hoặc admin được: `SCREENED|NEEDS_MANUAL_REVIEW → INTERVIEW|REJECTED`,
  `INTERVIEW → OFFERED|REJECTED`, `OFFERED → HIRED|REJECTED`. Chuyển khác → 409 (`ErrConflict`,
  "invalid status transition"). Recruiter khác → 404. Đổi trạng thái khoá dòng (`FOR UPDATE`), ghi history
  (actor = sub) + outbox `application.status_changed`.
- Logic chuyển trạng thái nằm trong `internal/domain` (thuần, test bảng).

## 8. gRPC & REST

`smarthire.application.v1.ApplicationService` (JWT, qua gateway):

| RPC | REST | Ai | Ghi chú |
|---|---|---|---|
| `SubmitApplication` | `POST /v1/applications` body `{job_id, cv_id}` | candidate | header `Idempotency-Key`; 202 |
| `GetApplication` | `GET /v1/applications/{id}` | ứng viên chủ đơn, recruiter chủ tin, admin | người khác → 404 |
| `ListApplications` | `GET /v1/applications?job_id=&status=&page_size=&page_token=` | candidate: đơn của mình; recruiter: đơn trên tin của mình (`job_id` tuỳ chọn); admin: tất cả | cursor phân trang như Job/Candidate, mới nhất trước |
| `UpdateApplicationStatus` | `PATCH /v1/applications/{id}/status` body `{status}` | recruiter chủ tin, admin | §7 |

`Application` message: id, job_id, candidate_id, cv_id, status, reason, score (optional), breakdown (dùng lại
dạng `MatchBreakdown` hiện có hoặc message riêng — builder chọn), model_version, version, created_at, updated_at.
`GetApplication` có thêm `history[]` (from, to, actor, reason, at).

Candidate thêm RPC nội bộ (không annotation HTTP, allow-list không JWT): `ReserveQuota(application_id,
candidate_id)`, `ReleaseQuota(application_id)`.

## 9. Events (contracts/events, schema + fixtures như các event khác)

- `application.submitted` (key `application_id`): `application_id, job_id, candidate_id, recruiter_id, cv_id`.
- `application.status_changed` (key `application_id`): `application_id, job_id, candidate_id, recruiter_id,
  from (null khi tạo), to, reason?, score?, version, actor`.
- Topic thêm vào `create-topics.sh`. Chưa có consumer (Notification ở M4) — tự kiểm bằng smoke đọc Kafka.
- Application consume `job.closed`, `job.deleted` (group `application`), retry/DLQ theo `pkg/kafka` (ADR 0006).

## 10. Resilience (spec §14.2)

- **Timeout**: T1 và quota 3s (`APPLICATION_CALL_TIMEOUT`), AI 5s (`APPLICATION_AI_TIMEOUT`).
- **Retry**: tối đa 3 lần, exponential backoff + jitter, **chỉ** `UNAVAILABLE`/`DEADLINE_EXCEEDED` — dùng
  retry policy có sẵn của grpc-go qua service config khi dial (không tự viết vòng retry), cho cả ba client.
  Đây là retry *trong một lần thử của bước*; việc thử lại bước (backoff tới deadline) do worker đảm nhận.
- **Circuit breaker** Application → AI: `sony/gobreaker/v2`, mở sau 5 lỗi liên tiếp, half-open sau 30s
  (`APPLICATION_AI_BREAKER_FAILURES`, `APPLICATION_AI_BREAKER_OPEN`), `NotFound`/`InvalidArgument` không tính
  là lỗi (như aiclient của Job). Breaker mở → bước `SCORE` ra `NEEDS_MANUAL_REVIEW` ngay, không chờ timeout.
- **Fallback**: `NEEDS_MANUAL_REVIEW` (đơn vẫn đến tay recruiter).
- **Idempotency key**: lưu trong `application_db` (unique `(candidate_id, idempotency_key)`, cùng transaction
  với T1) thay vì Redis 24h của spec §14.3 — không có khe hở giữa "ghi key" và "ghi đơn", không phụ thuộc Redis;
  key giữ vĩnh viễn cùng đơn. Ghi trong ADR.

## 11. Ops, test, docs

- Compose: service `application` (+ `application-migrate`), init SQL `04-create-application-db.sql` + biến
  `APPLICATION_DB_PASSWORD` trong `.env.example`. Init script của Postgres chỉ chạy trên volume mới → thêm
  hướng dẫn (hoặc target make) tạo role/DB trên volume đang có. Gateway có `GATEWAY_APPLICATION_ADDR`.
- CI: module mới vào `ci-go` (lint, test, `-race`, cover ≥ 70% cho domain+app, `make cover-application`).
- Unit test: state machine (bảng), use case T1 (idempotency, 404/409/503), từng bước saga với fake client
  (quota hết, AI lỗi/breaker mở, NotFound rồi hết cửa sổ, version mismatch, deadline → compensate).
- Integration (testcontainers Postgres + bufconn fake Candidate/AI): claim/lease với 2 worker không chạy trùng;
  crash giả (bỏ dở sau khi claim) → lease hết → chạy tiếp; Candidate sập tới deadline → `CANCELLED(timeout)` và
  `ReleaseQuota` được gọi; job.closed giữa chừng → `CANCELLED(job_closed)`. Candidate: quota idempotent,
  tombstone, đồng thời nhiều reserve không vượt limit.
- Smoke (`tools/cmd/smoke`): nộp đơn → poll tới `SCREENED` có điểm; lặp request cùng key → cùng đơn; nộp lại
  cùng job khác key → 409; recruiter `INTERVIEW` → `OFFERED`; chuyển sai → 409; người khác → 404;
  `application.status_changed` có trên Kafka.
- Chaos `make chaos-ai`: `docker compose stop ai-api` → nộp ≥ 6 đơn (6 tin) → tất cả `NEEDS_MANUAL_REVIEW`,
  log breaker mở, các đơn sau ra kết quả ngay (không chờ timeout) → `start ai-api`. Quota smoke đủ lớn
  (`CANDIDATE_DAILY_APPLICATION_QUOTA` trong compose cho dev, ví dụ 50).
- ADR `0011-application-saga-orchestration.md`: A+1, idempotency trong Postgres, RPC quota nội bộ không JWT,
  tại sao giữ quota ở Candidate (spec §6.4 ghi chú: để minh hoạ compensation), không Temporal.
- Cập nhật `README.md`, `CLAUDE.md`, `docs/adr/README.md`, `spec.md` không sửa.

## 12. Left to the builder

Tên package/hàm, chia file, số goroutine của worker, dạng message breakdown, format cursor, chi tiết
backoff (miễn trong giới hạn trên), cách smoke đọc Kafka (dùng lại helper có sẵn), nội dung log
(không PII: chỉ id, status, reason, error type).
