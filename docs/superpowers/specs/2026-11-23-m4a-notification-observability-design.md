# M4a — Notification service + observability (metrics, logs, dashboards, alerts) — design

Status: approved approach (tách M4a/M4b; telemetry qua OTel collector), pending doc review · Date: 2026-11-23 · Milestone: M4a (after M3; M4b = saga xoá tài khoản)

## 1. Intent

SmartHire là dự án portfolio Backend-Go (dữ liệu giả, làm để học). M4a thêm hai thứ của `spec.md` §5.6 và §14.5:
**Notification service** (consume event → thông báo trong app, đẩy realtime qua WebSocket, email qua Mailpit) và
**observability đầy đủ** (metrics Prometheus, log tập trung trên Loki, dashboard Grafana, alert). Giá trị portfolio:
idempotent consumer + local replica, "email outbox" cho side effect ra ngoài, WebSocket qua gateway bằng ticket dùng
một lần, và một đường ống OTel duy nhất cho trace + metrics + log ở cả Go lẫn Python. M4b (saga xoá tài khoản) sẽ
dùng lại metrics/alert của M4a.

## 2. Goals / non-goals / anti-goals

Goals
- Service mới `services/notification` (Go, hexagonal như các service Go khác), DB `notification_db`.
- Thông báo theo bảng đã chốt với user (§4). Đọc bằng `GET /v1/notifications`, đánh dấu đã đọc
  `POST /v1/notifications/{id}:read` (spec §13.1).
- Realtime: `POST /v1/ws/ticket` + `GET /v1/ws?ticket=…` ở gateway, proxy WebSocket sang Notification.
- Email qua SMTP (Mailpit trong compose), gửi bằng "email outbox" có retry; test/CI/smoke không gửi mail thật.
- Metrics + log của mọi service (Go và Python) đi OTLP → collector → Prometheus / Loki; dashboard và alert rules
  được provision sẵn.

Non-goals (để sau)
- Saga xoá tài khoản, `deletion_requests`, denylist token, xoá bản sao email (M4b).
- Alertmanager / gửi alert ra ngoài (chỉ xem alert ở Prometheus và Grafana). FE chuông thông báo (M5).
- Nhiều replica Notification (hub WS trong bộ nhớ; muốn scale thì thêm Redis pub/sub sau).
- Backfill email cho user đăng ký trước M4a (chỉ nhận thông báo trong app). Tuỳ chọn nhận email của user.
- `ai_llm_tokens_total` thật (LLM mặc định là fake); chỉ đếm khi adapter Anthropic trả `usage`.

Anti-goals
- Không log PII: email, nội dung thông báo không vào log; metrics không có nhãn theo user.
- Event lặp lại không tạo thông báo/email trùng (dedupe theo `event_id`).
- SMTP hay WebSocket lỗi không được làm mất thông báo hay chặn consumer.
- Token JWT không bao giờ nằm trên URL.

## 3. Approaches considered

Phạm vi: user chọn **tách M4 thành M4a (Notification + observability) và M4b (saga xoá tài khoản)** — mỗi phần
review và push riêng; M4b đụng cả năm service nên để riêng.

Realtime (một cách hợp lý, theo spec §4.3/§12): **WebSocket qua gateway bằng ticket**. Trình duyệt không gửi được
header JWT khi mở WS, còn token trên URL lộ vào log. SSE đơn giản hơn nhưng lệch spec và M5 đã định dùng WS.

Telemetry (user chọn **1**):
- **1. Tất cả qua OTel collector** — *được chọn*. SDK OTel đẩy metrics + log bằng OTLP lên collector có sẵn; collector
  mở endpoint Prometheus và đẩy log sang Loki. Một đường ống cho cả Go và Python; log mang `trace_id` để nhảy sang trace.
  Đánh đổi: tên metric bị collector chuyển đổi (vd `rpc_server_duration_milliseconds`), cầu nối slog → OTel còn mới.
- 2. Prometheus client `/metrics` mỗi service + Grafana Alloy đọc log Docker → Loki: phổ biến, tên metric đúng spec,
  nhưng metrics/trace hai đường riêng và thêm agent + mount Docker socket.

## 4. Notification — ai nhận gì (đã chốt với user)

| Event | Người nhận | Trong app + WS | Email |
|---|---|---|---|
| `user.registered` | người đăng ký | "Chào mừng" | ✓ |
| `cv.parsed` / `cv.parse_failed` | ứng viên (`candidate_id`) | "CV đã phân tích xong" / "phân tích lỗi, thử lại" | — |
| `application.submitted` | recruiter (`recruiter_id`) | "Có hồ sơ mới cho <tên tin>" | ✓ |
| `application.status_changed` | ứng viên | chỉ khi `to` ∈ {INTERVIEW, OFFERED, HIRED, REJECTED} (actor ≠ system) | ✓ |
| `match.completed` | recruiter của tin (từ bản sao job) | "Có N ứng viên phù hợp cho <tên tin>" (bỏ qua khi `top` rỗng) | — |

- Bản sao cục bộ: `users(user_id, email, role)` từ `user.registered`; `jobs(job_id, recruiter_id, title, version)`
  từ `job.created|updated` (bỏ bản có `version` không mới hơn; `job.deleted` xoá dòng). Thiếu bản sao (tin chưa
  biết, user cũ) → thông báo vẫn tạo với chữ chung chung ("tin tuyển dụng của bạn"), email bị bỏ nếu không có địa chỉ.
- Nội dung tiếng Việt, template Go (`text/template` cho title/body, `html/template` cho email), embed trong binary.

## 5. Notification service

- `services/notification`: gRPC `:9005` (`smarthire.notification.v1.NotificationService`, JWT, qua grpc-gateway),
  HTTP `:8005` (health + `GET /ws` nội bộ cho gateway), env prefix `NOTIFICATION_`, role/DB `notification_svc` /
  `notification_db`. Một binary: gRPC server, consumer, email sender, hub WS.
- RPC: `ListNotifications({unread_only, page_size, page_token}) → {notifications[], next_page_token, unread_count}`
  GET `/v1/notifications`; `MarkNotificationRead({id}) → Notification` POST `/v1/notifications/{id}:read`
  (của người khác → 404; đã đọc → giữ `read_at` cũ). `Notification{id, type, title, body, data(map<string,string>),
  read_at, created_at}`; `type` ∈ `welcome, cv_parsed, cv_parse_failed, application_received, application_status,
  candidates_matched`.
- Bảng: `users`, `jobs` (bản sao), `notifications(id, user_id, type, title, body, data jsonb, read_at, created_at)`,
  `email_outbox(id, notification_id, to_addr, subject, html_body, text_body, status PENDING|SENT|FAILED, attempts,
  next_attempt_at, last_error, created_at, sent_at)`, `processed_events`.
- Consumer group `notification` trên `user.registered, job.created, job.updated, job.deleted, cv.parsed,
  cv.parse_failed, application.submitted, application.status_changed, match.completed` (retry/DLQ `pkg/kafka`).
  Mỗi event: một transaction gồm `processed_events` + bản sao/thông báo + `email_outbox`. **Sau commit** đẩy thông báo
  lên hub WS (best-effort: user không online thì thôi, vẫn đọc được qua REST).
- Email sender: vòng lặp như outbox relay (`FOR UPDATE SKIP LOCKED`, 1s), gửi SMTP (`net/smtp`, Mailpit không auth),
  timeout 10s, retry backoff 5s·2ⁿ tối đa 10 phút, sau 8 lần → `FAILED`. Gửi xong mới đánh dấu `SENT` (at-least-once;
  chấp nhận hiếm khi trùng mail).
- Hub WS: map `user_id → connections` trong bộ nhớ, mỗi connection có hàng đợi gửi giới hạn (chậm/đầy → đóng kết nối).
  Thông điệp: `{"type":"notification.created","notification":{…như REST…}}`; ping mỗi 30s. Thư viện `github.com/coder/websocket`.
  Endpoint `/ws` tin header `X-User-ID` do gateway đặt — chỉ trong mạng compose (cùng mô hình tin cậy ADR 0010/0011).

## 6. Gateway: WS ticket + proxy

- `POST /v1/ws/ticket` (route viết tay, cần JWT hợp lệ): sinh 32 byte ngẫu nhiên (base64url), lưu Redis
  `gateway:ws:ticket:<t>` = `user_id`, TTL 30s; trả `{"ticket": "...", "expires_in": 30}`.
- `GET /v1/ws?ticket=…`: kiểm tra `Origin` thuộc `GATEWAY_CORS_ORIGINS` (chống CSWSH; không có Origin → cho qua,
  dành cho client không phải trình duyệt như smoke), `GETDEL` ticket (dùng một lần), thiếu/sai/hết hạn → 401 trước khi
  upgrade; hợp lệ → `httputil.ReverseProxy` sang `notification:8005/ws`, bỏ query `ticket`, đặt `X-User-ID`. Không
  áp `RejectInvalidAuth`/body limit cho route này; vẫn qua rate limit theo IP. Redis lỗi → 503.
- Gateway thêm `GATEWAY_NOTIFICATION_GRPC_ADDR`, `GATEWAY_NOTIFICATION_WS_URL`.
- Lưu ý khi làm: mọi lớp bọc `ResponseWriter` trên đường tới route WS (logging, otelhttp, recovery) phải giữ
  `http.Hijacker`, và `WriteTimeout`/`ReadTimeout` của `http.Server` gateway không được cắt kết nối WS đang mở
  (kiểm bằng test: kết nối sống quá timeout vẫn nhận được tin).

## 7. Observability

Đường ống: service → OTLP gRPC → `otel-collector` → (traces) Tempo · (metrics) exporter Prometheus `:8889` ←
Prometheus scrape · (logs) `otlphttp` → Loki 3 (OTLP native). Grafana có datasource Tempo, Prometheus, Loki (Loki
`derivedFields`/trace_id → Tempo; Tempo → logs).

- `pkg/otel.Setup` thêm MeterProvider (OTLP, 10s) và LoggerProvider (OTLP, batch); `pkg/logger` ghi JSON ra stdout
  **và** qua cầu nối `otelslog` (giữ `trace_id/span_id`, `service`). Tắt được bằng env khi không có collector
  (OTel SDK đã tự bỏ qua lỗi export).
- Python AI: `MeterProvider` + `LoggerProvider` OTLP và `LoggingHandler`, cùng resource `service.name`.
- Metrics (tên theo OTel; collector đổi `.` → `_` và thêm hậu tố đơn vị):

| Metric | Loại | Ở đâu |
|---|---|---|
| `rpc.server.duration`, `http.server.request.duration` | histogram | tự có từ `otelgrpc` / `otelhttp` |
| `outbox.pending` | observable gauge | `pkg/outbox` (đếm `published_at IS NULL`), mọi service có relay |
| `kafka.consumer.lag` | observable gauge `{group,topic}` | `pkg/kafka` (franz-go `kadm` Lag, 15s) |
| `dlq.messages` | counter `{topic}` | `pkg/kafka` khi ghi DLQ (Go) + Python consumer |
| `circuit_breaker.state` | gauge `{breaker}` 0 closed/1 half-open/2 open | job (opensearch, ai), candidate (ai), application (ai) |
| `saga.instances` | observable gauge `{state}` | application (count theo `state`) |
| `search.fallback` | counter | job `ListJobs` khi `degraded` |
| `projection.lag` | histogram (s) | job-projector: `now − occurred_at` khi index |
| `notifications.created` | counter `{type}` | notification |
| `emails.sent` / `emails.failed` | counter | notification |
| `ai.parse.duration` | histogram (s) | ai-worker |
| `ai.match.score` | histogram | ai-matcher khi chấm điểm |
| `ai.llm.tokens` | counter `{direction}` | ai-worker, chỉ adapter Anthropic |

- Alert rules (Prometheus, spec §14.5): consumer lag > 1000 trong 5 phút; DLQ tăng (> 0 trong 5 phút); tỉ lệ lỗi
  gRPC > 2% trong 5 phút; breaker open > 2 phút; outbox pending > 500; projection lag p95 > 30s; email FAILED > 0.
  `deletion_requests` để M4b.
- Dashboard provision sẵn: "SmartHire — overview" (RED theo service, lag, outbox, DLQ, breaker, saga, search
  fallback, projection lag, notifications/email, AI parse/score) và panel log Loki lọc theo service.
- Compose obs thêm `prometheus` (`127.0.0.1:9090`), `loki` (`127.0.0.1:3100`); infra thêm `mailpit`
  (UI `127.0.0.1:8025`, SMTP nội bộ `mailpit:1025`). Image pin phiên bản.

## 8. Ops, test, docs

- Compose: `notification-migrate`, `notification`, init SQL `06-create-notification-db.sql` (idempotent, `make db-init`
  chạy luôn), `NOTIFICATION_DB_PASSWORD` trong `.env.example`. CI: module mới vào `ci-go` + `make cover-notification`.
- Unit: chọn người nhận/kênh theo bảng §4 (bảng test), template, dedupe, bản sao job theo version, hub (đăng ký, gửi,
  đóng khi đầy), email sender backoff, ticket (sinh, GETDEL, Origin), metrics bằng `sdk/metric` ManualReader.
- Integration: repo + consumer với Postgres thật; email sender gửi tới một SMTP server giả trong test.
- Smoke: đăng ký → thông báo "welcome" + mail trong Mailpit (API `:8025/api/v1/search`); mở WS bằng ticket → recruiter
  chuyển hồ sơ sang INTERVIEW → ứng viên nhận `notification.created` qua WS; dùng lại ticket → 401; đánh dấu đã đọc;
  `make obs-check`: Prometheus có `saga_instances`, `outbox_pending`, `rpc_server_duration_*`; Loki có log của
  `application`; Prometheus đã nạp alert rules.
- ADR `0012-notification-ws-ticket-email-outbox.md` và `0013-telemetry-through-otel-collector.md`; cập nhật README,
  CLAUDE.md, `docs/adr/README.md`.

## 9. Left to the builder

Tên package/hàm, câu chữ template, bố cục dashboard, cách đặt tên instrument trong code (miễn khớp bảng §7), kích
thước hàng đợi WS, cấu hình batch của collector, retention Loki/Prometheus cho dev.
