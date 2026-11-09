# M1a — Event backbone + Job Service — Design

| Mục | Giá trị |
|---|---|
| Ngày | 2026-11-10 |
| Trạng thái | Đã duyệt (tự chốt theo chỉ đạo "không hỏi, làm luôn" của người dùng ngày 2026-11-10) |
| Spec gốc | [`spec.md`](../../../spec.md) v0.3.0 — §5.3, §6.6, §7, §8.2, §14.3 |
| Kế thừa | M0: [`2026-11-08-m0-foundation-auth-gateway-design.md`](2026-11-08-m0-foundation-auth-gateway-design.md), ADR 0001–0004 |

---

## 1. Phân rã M1

M1 trong roadmap ("Job + Candidate + AI parse, `pkg/kafka`, `pkg/outbox`, DLQ") quá lớn cho một plan. Nó được chia thành hai sub-project, mỗi cái có spec, plan và code riêng:

| Sub-project | Nội dung | Demo được |
|---|---|---|
| **M1a (tài liệu này)** | Kafka (Redpanda) + `pkg/kafka` + `pkg/outbox` relay + `pkg/idempotency` + DLQ; relay cho Auth (`user.registered`); **Job service** (CRUD, state machine, tìm kiếm `tsvector`, event `job.*`); route Gateway | Recruiter đăng JD → JD xuất hiện ở `GET /v1/jobs?q=`; event `job.created` hiện trên Redpanda Console |
| M1b | Candidate service (hồ sơ, upload CV presigned MinIO, `cv.uploaded`), AI service Python (worker parse CV → `cv.parsed`/`cv.parse_failed`), consumer Candidate | Upload PDF → JSON có cấu trúc |

Lý do làm M1a trước: M1b cần sẵn event backbone (relay, consumer, DLQ, idempotency). Job service là service đầu tiên vừa phát event vừa có business logic thật, nên nó kiểm chứng backbone trước khi thêm Python vào.

---

## 2. Phạm vi M1a

### 2.1 Trong phạm vi
- **Hạ tầng:**
  - **Redpanda** (Kafka API, single node, dev mode) và **Redpanda Console** (`127.0.0.1:8081`) trong `docker-compose.infra.yml`.
  - Container `topics-init` tạo topic bằng `rpk`.
  - Role `job_svc` làm owner của `job_db`.
- **`pkg/kafka`:** producer, consumer group, envelope codec, truyền trace qua header, retry trong process và chuyển sang DLQ.
- **`pkg/outbox`:** relay dùng chung (poll `FOR UPDATE SKIP LOCKED`), chạy như goroutine trong `cmd/server` của mỗi service phát event.
- **`pkg/idempotency`:** helper `processed_events`, dùng chung trong transaction xử lý. M1a chỉ cung cấp và test helper này, vì chưa service Go nào consume event có side effect DB. M1b dùng nó.
- **Auth:**
  - Bật relay để `user.registered` thực sự lên Kafka.
  - Migration `000002` thêm cột `headers` vào `outbox`.
  - **Sửa tồn đọng từ M0 (R26):** transaction Refresh hiện bị tách hẳn khỏi cancellation và không có giới hạn thời gian. Cần sửa lại như sau:
    - Đường xoay vòng bình thường dùng ctx của request (bị huỷ thì rollback, RT cũ vẫn còn hiệu lực).
    - Riêng nhánh phát hiện dùng lại thì thu hồi family trên `context.WithTimeout(context.WithoutCancel(ctx), 5s)`.
    - Siết ngưỡng của `TestLimiter_HangingRedisFailsFast` xuống ≤ 150ms.
- **Job service** (port gRPC `9002`, HTTP nội bộ `8002` cho health):
  - CRUD JD, ownership theo recruiter.
  - State machine `draft → open → closed`, xoá mềm.
  - Tìm kiếm full-text Postgres `tsvector` có bỏ dấu tiếng Việt, cộng filter và cursor pagination.
  - Ghi event `job.created|updated|closed|deleted` (có `version`) qua outbox.
- **Gateway:**
  - Mount `JobService` qua grpc-gateway.
  - `GET /v1/jobs` và `GET /v1/jobs/{id}` cho phép ẩn danh.
- **Smoke:** thêm luồng recruiter.
- **Contract:** JSON Schema cho các event M1a trong `contracts/events/`, có test validate.
- **Tài liệu:** ADR 0005 (outbox relay + headers), ADR 0006 (Kafka client và chiến lược retry/DLQ).

### 2.2 Ngoài phạm vi
| Hạng mục | Milestone |
|---|---|
| OpenSearch read model, projector (CQRS) | M2 |
| `FindCandidates` (Job → AI) | M2 |
| Consumer `job.*` trong AI/notification | M1b/M2/M4 |
| `user.deleted` (ẩn danh recruiter) | M4 |
| Prometheus metric `outbox_pending_count`, `kafka_consumer_lag` | M4 (M1a chỉ có span + log) |

---

## 3. Event backbone

### 3.1 Broker
- Image `redpandadata/redpanda` single node, `--mode dev-container`.
- Listener nội bộ `redpanda:9092`. Không publish broker ra host. Chỉ Console được bind `127.0.0.1:8081`.
- `topics-init` (image redpanda, `rpk topic create`, chạy một lần rồi `restart: "no"`) tạo trước các topic M1a: `user.registered`, `job.created`, `job.updated`, `job.closed`, `job.deleted`, kèm `<topic>.dlq` cho mỗi topic.
  - Mỗi topic có 3 partition, replication 1.
  - Tắt auto-create topic để topic sai tên lộ ra ngay.

### 3.2 `pkg/kafka` (thư viện `github.com/twmb/franz-go`)
- `NewProducer(brokers []string, opts...) (*Producer, error)`:
  - Bật idempotent producer, `acks=all`.
  - `Produce(ctx, Record) error` gửi đồng bộ, chờ ack.
  - `Record{Topic, Key, Value []byte, Headers map[string]string}`.
- Trace:
  - Producer inject W3C `traceparent` và `tracestate` từ ctx vào header, trừ khi `Record.Headers` đã có sẵn. Relay dùng header lưu sẵn trong outbox (xem §3.3).
  - Consumer extract header thành ctx mới, mở span consumer `<topic> process`.
- `NewConsumer(cfg ConsumerConfig) (*Consumer, error)`:
  - `ConsumerConfig{Brokers, Group, Topics, Handler, MaxAttempts=3, Backoff=200ms×2^n (tối đa 2s), Logger}`.
  - `Run(ctx) error` chặn cho tới khi ctx bị huỷ. Commit offset sau khi handler trả nil, hoặc sau khi message đã vào DLQ.
- `Handler func(ctx context.Context, env event.Envelope, rec *kgo.Record) error`.
  - Envelope decode lỗi: chuyển thẳng DLQ, không retry.
  - Handler trả lỗi bọc `kafka.Permanent(err)`: DLQ ngay.
  - Lỗi khác: retry trong process với backoff, hết lượt thì DLQ.
- DLQ:
  - Topic là `<topic>.dlq`. Giữ nguyên key và value.
  - Thêm header `dlq-error` (chuỗi lỗi, cắt còn 512 ký tự, không chứa PII), `dlq-attempts`, `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset`.
  - Ghi DLQ thất bại: không commit offset, trả lỗi để `Run` dừng (fail loud).
- Thứ tự: xử lý tuần tự trong từng partition, nên giữ được thứ tự theo key (`spec.md` §7.2).

### 3.3 `pkg/outbox`
- Schema chuẩn cho mọi service:
  ```sql
  outbox(id uuid PK, aggregate_id text, event_type text, payload jsonb, headers jsonb NOT NULL DEFAULT '{}',
         created_at timestamptz, published_at timestamptz NULL)
  ```
  Auth thêm cột `headers` bằng migration `000002_outbox_headers`.
- `outbox.Writer`:
  - `Write(ctx, q Querier, env event.Envelope, aggregateID string) error`. Lấy W3C carrier từ ctx và lưu vào `headers`.
  - `Querier` là interface `Exec`, mà `pgx.Tx` và `*pgxpool.Pool` đều thoả. Service gọi trong transaction nghiệp vụ.
  - Auth thay `OutboxWriter` riêng bằng `pkg/outbox.Writer`. Port `app.OutboxWriter` giữ nguyên, adapter chỉ uỷ quyền xuống.
- `outbox.Relay`:
  - `NewRelay(pool *pgxpool.Pool, producer Publisher, opts RelayOptions)`, với `RelayOptions{BatchSize=100, Interval=500ms, Logger}`.
  - `Run(ctx) error`: mỗi tick, trong một transaction:
    1. `SELECT … WHERE published_at IS NULL ORDER BY created_at LIMIT $batch FOR UPDATE SKIP LOCKED`.
    2. Publish từng bản ghi với topic = `event_type`, key = `aggregate_id`, value = `payload`, headers = `headers`.
    3. `UPDATE … SET published_at = now() WHERE id = ANY($ids)`.
    4. Commit.
  - Publish lỗi giữa batch: các bản ghi đã gửi vẫn được đánh dấu, phần còn lại rollback và thử ở tick sau. Đây là at-least-once.
  - Batch đầy thì chạy tick tiếp ngay, không chờ hết `Interval`.
- Nhiều replica relay chạy song song an toàn nhờ `SKIP LOCKED`. Thứ tự chỉ đảm bảo theo key trong một replica. Thứ tự toàn cục do consumer xử lý bằng `version` (§4.4).

### 3.4 `pkg/idempotency`
- Bảng chuẩn `processed_events(event_id text PK, processed_at timestamptz)`.
- `Once(ctx, q Querier, eventID string) (first bool, err error)` chạy `INSERT … ON CONFLICT DO NOTHING` và trả về số dòng bị ảnh hưởng. Gọi trong cùng transaction với side effect.

---

## 4. Job service

### 4.1 Domain
- `Job{ID, RecruiterID uuid; Title, Description string; RequiredSkills, NiceToHaveSkills []string; Level, Location string; SalaryMin, SalaryMax *int; Status; Version int64; CreatedAt, UpdatedAt, PublishedAt, DeletedAt *time}`.
- `Level` ∈ `intern|junior|middle|senior|lead`.
- `Status` ∈ `draft|open|closed`. Chuyển trạng thái hợp lệ:
  - `draft → open` (publish)
  - `open → closed` (close)
  - `draft → (deleted)`, `closed → (deleted)`
  - Sửa nội dung được khi `draft` hoặc `open`, không được khi `closed`.
  - Chuyển trạng thái sai trả `apperr.ErrValidation` (→ 400, `FAILED_PRECONDITION` không có trong apperr).
- Validate:
  - `title` 5–200 ký tự; `description` 20–20 000 ký tự, là plain text (M1a chưa có Tiptap).
  - Tối đa 30 skill. Skill được chuẩn hoá: trim, gộp khoảng trắng, bỏ trùng không phân biệt hoa thường. Bảng alias để M2.
  - `salary_min ≤ salary_max`, cả hai ≥ 0. `location` 1–100 ký tự.
- Mọi thay đổi đều tăng `version` thêm 1 trong cùng transaction.

### 4.2 Quyền
| RPC | Ai được gọi |
|---|---|
| `CreateJob` | role `recruiter` |
| `UpdateJob`, `PublishJob`, `CloseJob`, `DeleteJob` | recruiter là owner (`recruiter_id == sub`); không phải owner → `NotFound`, không lộ sự tồn tại |
| `GetJob` | public nếu `open`; owner xem được mọi trạng thái trừ deleted; người khác xem JD không `open` → `NotFound` |
| `ListJobs` | public, chỉ trả JD `open` |
| `ListMyJobs` | recruiter, chỉ JD của mình, lọc theo `status` |

Admin moderation để M4/M5.

### 4.3 Tìm kiếm (`ListJobs`)
- Cột generated:
  ```sql
  search_vector tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', unaccent_immutable(title)), 'A') ||
    setweight(to_tsvector('simple', unaccent_immutable(array_to_string(required_skills,' '))), 'B') ||
    setweight(to_tsvector('simple', unaccent_immutable(description)), 'C')) STORED
  ```
  - Có GIN index.
  - `unaccent` là trusted extension, owner tự cài được. `unaccent_immutable` là wrapper `IMMUTABLE` (pattern chuẩn để dùng trong generated column).
- Tham số:
  - `q`: `websearch_to_tsquery('simple', unaccent_immutable(q))`, xếp hạng bằng `ts_rank`.
  - `skills[]`: `required_skills && $skills`, so sánh theo chữ thường.
  - `location`, `level`: so sánh bằng, không phân biệt hoa thường.
  - `salary_min`: `salary_max IS NULL OR salary_max >= $salary_min`.
- Sắp xếp:
  - Có `q`: `rank DESC, published_at DESC, id`.
  - Không có `q`: `published_at DESC, id`.
- Phân trang: cursor (keyset) encode base64url JSON `{r?, p, id}`. `page_size` mặc định 20, tối đa 100. Trả `next_page_token`.
  - Token sai → `InvalidArgument`.
  - Không lọc theo tiền tố trong M1a.

### 4.4 Event
| Event | Khi nào | Payload (theo `spec.md` §7.2) |
|---|---|---|
| `job.created` | `CreateJob` | `job_id, recruiter_id, title, description, required_skills, nice_to_have_skills, level, location, salary_min, salary_max, status, version` |
| `job.updated` | `UpdateJob`, `PublishJob` | như trên |
| `job.closed` | `CloseJob` | `job_id, version` |
| `job.deleted` | `DeleteJob` | `job_id, version` |

- Mỗi event được ghi vào outbox trong cùng transaction với thay đổi. Key là `job_id`. `source` là `job-service`.
- JSON Schema nằm ở `contracts/events/{job.created,job.updated,job.closed,job.deleted,user.registered}.v1.json`.
- Integration test validate payload thật (đọc từ bảng outbox) theo schema. Thư viện: `github.com/santhosh-tekuri/jsonschema/v6`.

### 4.5 Data model `job_db`
```sql
jobs(id uuid PK, recruiter_id uuid NOT NULL, title text, description text, required_skills text[] NOT NULL DEFAULT '{}',
     nice_to_have_skills text[] NOT NULL DEFAULT '{}', level text CHECK(...), location text,
     salary_min int NULL, salary_max int NULL, status text CHECK(status IN ('draft','open','closed')),
     version bigint NOT NULL DEFAULT 1, published_at timestamptz NULL, deleted_at timestamptz NULL,
     created_at, updated_at, search_vector tsvector GENERATED … STORED)
INDEX jobs_search_gin USING GIN(search_vector) WHERE status='open' AND deleted_at IS NULL
INDEX jobs_open_published (published_at DESC, id) WHERE status='open' AND deleted_at IS NULL
INDEX jobs_recruiter (recruiter_id, created_at DESC) WHERE deleted_at IS NULL
INDEX jobs_skills_gin USING GIN(required_skills)
outbox(...)  -- schema chuẩn §3.3
processed_events(...)  -- tạo sẵn cho M2
```
Xoá mềm: `deleted_at` được đặt; dòng không bị xoá vật lý.

### 4.6 Proto `smarthire/job/v1` (annotation REST)
| RPC | REST |
|---|---|
| `CreateJob` | `POST /v1/jobs` |
| `UpdateJob` (field mask kiểu "chỉ field có mặt"; dùng `optional` cho scalar, wrapper cho mảng) | `PATCH /v1/jobs/{id}` |
| `PublishJob` | `POST /v1/jobs/{id}:publish` |
| `CloseJob` | `POST /v1/jobs/{id}:close` |
| `DeleteJob` | `DELETE /v1/jobs/{id}` |
| `GetJob` | `GET /v1/jobs/{id}` |
| `ListJobs` | `GET /v1/jobs` |
| `ListMyJobs` | `GET /v1/me/jobs` |

- `protovalidate` kiểm tra độ dài, enum và giới hạn `page_size`.
- `CreateJob` trả 201 qua forward option, giống `Register`.

### 4.7 Cấu trúc
- Sao chép template `services/auth` (`domain → app → adapter{grpc,postgres,httpx} → bootstrap`, `cmd/server`, `cmd`).
- `PublicMethods`: `GetJob`, `ListJobs`, health `Check`.
- Relay chạy trong `cmd/server` (goroutine cạnh gRPC server, cùng vòng đời shutdown).
- Config prefix `JOB_`: `GRPC_PORT=9002`, `HTTP_PORT=8002`, `DB_DSN` (bắt buộc), `JWKS_URL`, `JWT_ISSUER`, `JWT_AUDIENCE`, `KAFKA_BROKERS=redpanda:9092`, `OUTBOX_INTERVAL=500ms`, `LOG_LEVEL`.

---

## 5. Gateway
- Config thêm `GATEWAY_JOB_GRPC_ADDR=job:9002`.
- `RegisterJobServiceHandlerClient` trên cùng mux. Route `/v1/me/jobs` cũng đi qua mux (không phải `/v1/me` của Auth).
- Chain middleware giữ nguyên. `GET /v1/jobs*` ẩn danh vẫn bị rate limit theo IP.
- `/readyz` kiểm tra cả hai gRPC conn.

---

## 6. Auth thay đổi
- Migration `000002_outbox_headers.up.sql`: `ALTER TABLE outbox ADD COLUMN headers jsonb NOT NULL DEFAULT '{}'`, kèm file down.
- `OutboxWriter` uỷ quyền cho `pkg/outbox.Writer`.
- Bootstrap chạy `outbox.Relay` với `kafka.Producer`. Config thêm `AUTH_KAFKA_BROKERS`, `AUTH_OUTBOX_INTERVAL`.
- Auth không consume event nào trong M1a.

---

## 7. Compose, Makefile, CI
- Infra:
  - Thêm `redpanda`, `redpanda-console` (`127.0.0.1:8081`), `topics-init`.
  - Script init Postgres tạo thêm role `job_svc` làm owner của `job_db`, mật khẩu `JOB_DB_PASSWORD`.
- App: thêm `job-migrate` và `job`. `auth` và `job` phụ thuộc `topics-init` (`service_completed_successfully`).
- Makefile:
  - `sqlc` sinh code cho cả auth và job.
  - `test-int` và `cover-*` bao cả job. `cover-job` yêu cầu ≥ 70%.
- CI:
  - Thêm `job` vào path filter và matrix.
  - Docker build thêm job.
  - `pkg/**` thay đổi thì kích hoạt mọi module.
- Smoke có thêm 6 bước, sau 7 bước Auth:
  1. Login recruiter seed.
  2. Tạo JD (201, `draft`).
  3. JD chưa xuất hiện ở `GET /v1/jobs?q=…`.
  4. Publish JD.
  5. `GET /v1/jobs?q=<từ trong title, gõ không dấu>` tìm thấy.
  6. Close JD rồi tìm lại thì không thấy.

  Smoke cũng kiểm tra event lên Kafka: đọc topic `job.created` bằng franz-go qua listener `127.0.0.1:19092` publish riêng cho dev, so key = `job_id`, timeout 10s.

---

## 8. Testing
- **`pkg/kafka`** (integration, testcontainers Redpanda):
  - Produce/consume round-trip.
  - Trace header được truyền.
  - Lỗi transient: retry rồi thành công.
  - Lỗi permanent: vào DLQ với đủ header.
  - Decode lỗi: vào DLQ.
  - Thứ tự theo key.
- **`pkg/outbox`** (integration, Postgres + Redpanda):
  - Ghi trong transaction bị rollback thì không publish.
  - Relay publish rồi đánh dấu.
  - Hai relay song song không publish trùng: mỗi id được publish đúng 1 lần, kiểm bằng cách đếm trên topic.
  - Publish lỗi giữa batch thì phần còn lại thử lại.
  - Header trace được giữ nguyên.
- **`pkg/idempotency`** (integration): lần đầu trả `first=true`, lần hai `false`; rollback thì lần sau lại `true`.
- **Job unit** (domain/app, fake port):
  - Bảng chuyển trạng thái.
  - Validate.
  - Chuẩn hoá skill.
  - Ownership (không phải owner → `NotFound`).
  - Version tăng.
  - Ghi event đúng loại.
  - Coverage `domain` + `app` ≥ 70%.
- **Job integration** (Postgres):
  - Migration up/down.
  - Tìm kiếm có dấu/không dấu (`"Lập trình viên Go"` tìm bằng `"lap trinh"`).
  - Filter skill/location/level/salary.
  - Cursor ổn định qua nhiều trang.
  - JD không `open` không xuất hiện.
  - Event trong outbox khớp JSON Schema.
- **Gateway:** route job qua fake JobService, gồm `GET /v1/jobs` ẩn danh 200, `POST` không token 401, recruiter 201.
- **Smoke** như §7.

## 9. Definition of Done
1. `make bootstrap && make infra-up obs-up up && make seed && make smoke` pass trên volume sạch, gồm 7 bước Auth và 6 bước Job.
2. Redpanda Console thấy event `user.registered` và `job.*`. Trace Tempo của `POST /v1/jobs` đi gateway → job → pgx, rồi có span relay produce và trace tiếp nối qua header.
3. `make lint test test-int` xanh. `go test -race` chạy trong container Linux xanh. `cover-auth`, `cover-job` ≥ 70%.
4. ADR 0005 và 0006. README cập nhật roadmap (M1a ✅) và Quick start. CLAUDE.md cập nhật.

## 10. Rulings (tự chốt)
| # | Quyết định | Lý do |
|---|---|---|
| D1 | Redpanda thay Apache Kafka cho local | `spec.md` R4: nhẹ, Kafka API tương thích, có console |
| D2 | franz-go | `spec.md` §9.1 |
| D3 | Relay là goroutine trong `cmd/server` | `spec.md` §10 cho phép; ít process hơn. Tách ra `cmd/relay` khi cần scale riêng |
| D4 | Cột `headers jsonb` trong outbox | Envelope §7.1 chỉ có `trace_id`, không đủ để nối span. Lưu carrier W3C giúp trace xuyên Kafka (DoD 2) |
| D5 | Retry trong process + DLQ, không có topic retry trễ | Đủ cho MVP. Topic retry trễ để khi có nhu cầu |
| D6 | Chuyển trạng thái sai → `InvalidArgument` | apperr chưa có `FailedPrecondition`; thêm sentinel là việc của M3 (state machine đơn) |
| D7 | Tìm kiếm `simple` + `unaccent` | Postgres không có dictionary tiếng Việt. Bỏ dấu đủ cho M1a; OpenSearch ICU ở M2 |
| D8 | Không phải owner → `NotFound` | Không lộ sự tồn tại của JD nháp |
| D9 | Publish listener Redpanda `127.0.0.1:19092` cho dev/smoke | Smoke cần kiểm event từ host; chỉ bind localhost |
