# M1b — Candidate Service + AI CV parsing — Design

| Mục | Giá trị |
|---|---|
| Ngày | 2026-11-11 |
| Trạng thái | Đã duyệt (tự chốt theo chỉ đạo "không hỏi, làm luôn" của người dùng, 2026-11-10) |
| Spec gốc | [`spec.md`](../../../spec.md) v0.3.0: §5.4, §5.7, §6.2, §7.2, §8.3, §8.5, §9.2, §13.3, §14.4 |
| Kế thừa | M0, M1a ([`2026-11-10-m1a-events-job-design.md`](2026-11-10-m1a-events-job-design.md)), ADR 0001–0006 |

---

## 1. Mục tiêu

Ứng viên upload CV (PDF) qua presigned URL. Hệ thống phát `cv.uploaded`. AI service (Python) đọc PDF, trích xuất JSON có cấu trúc bằng LLM, rồi phát `cv.parsed` hoặc `cv.parse_failed`. Candidate service consume các event này, cập nhật trạng thái CV và lưu dữ liệu đã parse. Ứng viên xem và sửa được dữ liệu đó.

**Demo:** smoke đăng nhập ứng viên → upload một PDF mẫu → chờ CV sang `PARSED` → thấy skills đã trích xuất → sửa một field. Trace Tempo đi xuyên Go → Kafka → Python → Kafka → Go.

## 2. Phạm vi

### 2.1 Trong phạm vi
- **Hạ tầng:**
  - MinIO trong compose, console bind `127.0.0.1:9001`, S3 API bind `127.0.0.1:9000`.
  - Container `minio-init` tạo bucket private `cvs` và hai user có policy tối thiểu:
    - `candidate-svc`: put/get/stat/delete trên `cvs/*`.
    - `ai-svc`: chỉ get trên `cvs/*`.
  - Topic mới: `cv.uploaded`, `cv.parsed`, `cv.parse_failed`, kèm `.dlq` cho từng topic.
  - Role `candidate_svc` sở hữu `candidate_db`, role `ai_svc` sở hữu `ai_db`.
- **Candidate service (Go, port gRPC `9003`, HTTP `8003`):**
  - Hồ sơ ứng viên.
  - Upload CV: presigned PUT, xác nhận hoàn tất, outbox `cv.uploaded`.
  - Consumer `cv.parsed` / `cv.parse_failed`, idempotent qua `pkg/idempotency`.
  - Xem CV, danh sách CV, sửa dữ liệu đã parse, parse lại.
- **AI service (Python 3.12, `services/ai`):**
  - Process `worker`: consumer `cv.uploaded` → tải PDF từ MinIO → PyMuPDF trích text → LLM parse ra `ParsedCV` → chuẩn hoá skill → lưu `ai_db` → outbox → relay Python đẩy `cv.parsed` / `cv.parse_failed`.
  - Process `api`: chỉ FastAPI `/healthz`, `/readyz`, `/metrics`.
- **Contracts:**
  - Proto `smarthire/candidate/v1` có annotation REST.
  - JSON Schema cho `cv.uploaded`, `cv.parsed`, `cv.parse_failed`. Test cả phía Go lẫn Python validate theo cùng một file.
- **Gateway:** route `CandidateService`.
- **Smoke:** thêm luồng CV, chạy với LLM giả lập (§5.4).
- **CI:**
  - Job Python dùng `uv`, `ruff`, `mypy` (strict cho `domain/` và `services/`), `pytest`, integration test bằng testcontainers.
  - Thêm module candidate vào matrix Go.
- **Tài liệu:** ADR 0007 (AI service Python + cổng LLM + adapter giả lập), ADR 0008 (upload presigned / valet key), README, CLAUDE.md.

### 2.2 Ngoài phạm vi
| Hạng mục | Milestone |
|---|---|
| Embedding, Qdrant, matching, gRPC `AIService` | M2 |
| Quota nộp đơn (`daily_quota`) | M3 |
| OCR cho PDF dạng ảnh | sau M2 (M1b trả `parse_failed` với `reason=no_text`) |
| `user.deleted` (xoá CV và dữ liệu) | M4 |
| Sửa các mục tồn đọng M1a đã dời sang M1b (§8) | M1b — task đầu tiên |

---

## 3. Candidate service

### 3.1 Domain
- `Candidate{UserID (PK, = sub của JWT), FullName, Phone, Location, CreatedAt, UpdatedAt}`.
  - Hồ sơ được tạo lười ở lần đầu `PUT /v1/me/profile`. Chưa có hồ sơ thì `GET` trả 404.
- `CV{ID, CandidateID, ObjectKey, Status, ParsedData (JSON), ParseError, ConsentAt, SizeBytes, ParsedAt, CreatedAt, UpdatedAt, Version}`.
- `Status` ∈ `UPLOADING | UPLOADED | PARSED | FAILED`. Các chuyển trạng thái:
  - `UPLOADING → UPLOADED` khi complete.
  - `UPLOADED → PARSED | FAILED` khi nhận event.
  - `PARSED | FAILED → UPLOADED` khi reparse.
  - Chuyển sai trả `ErrValidation`.
- **Event cũ:** event `cv.parsed` / `cv.parse_failed` mang `attempt_id`. Candidate chỉ áp dụng event có `attempt_id` trùng `cv.attempt_id` hiện tại. Event khác bị bỏ qua: không lỗi, vẫn đánh dấu đã xử lý.
  - Mỗi lần complete hoặc reparse sinh một `attempt_id` (ULID) mới, đưa vào payload `cv.uploaded`. AI copy giá trị này sang event kết quả.
- **Quyền:** mọi RPC yêu cầu role `candidate` và quyền sở hữu. Không phải owner thì trả `NotFound`.

### 3.2 Luồng upload (spec §6.2, Valet Key)
1. `POST /v1/cvs/upload-url` với body `{consent: true, file_name}`.
   - `consent` phải là `true` (NĐ 13/2023), nếu không trả 400.
   - Tạo CV ở trạng thái `UPLOADING` với `object_key = cvs/{candidate_id}/{cv_id}.pdf`.
   - Trả `{cv_id, upload_url, expires_at}`: presigned PUT 10 phút, ký với `Content-Type: application/pdf`.
2. Client `PUT` file lên `upload_url`.
3. `POST /v1/cvs/{id}:complete`:
   - Candidate gọi `StatObject`. Không có object → 400. `size > 5 MiB` hoặc `content-type ≠ application/pdf` → xoá object, CV giữ `UPLOADING`, trả 400.
   - Đọc 5 byte đầu để kiểm magic `%PDF-`.
   - Hợp lệ: status `UPLOADED`, `attempt_id` mới, và trong cùng transaction ghi outbox `cv.uploaded {cv_id, candidate_id, object_key, attempt_id}`.
   - Gọi `complete` lần hai khi đã `UPLOADED` là idempotent: trả trạng thái hiện tại.
4. **Endpoint ký URL:** presign dùng một client MinIO cấu hình endpoint public `CANDIDATE_S3_PUBLIC_ENDPOINT` (mặc định `http://127.0.0.1:9000`). Ký không cần kết nối mạng. Stat, get và delete dùng endpoint nội bộ `minio:9000`.

### 3.3 API (`smarthire/candidate/v1`)
| RPC | REST |
|---|---|
| `GetMyProfile` | `GET /v1/me/profile` |
| `UpsertMyProfile` | `PUT /v1/me/profile` |
| `CreateUploadURL` | `POST /v1/cvs/upload-url` |
| `CompleteUpload` | `POST /v1/cvs/{id}:complete` |
| `GetCV` | `GET /v1/cvs/{id}` |
| `ListMyCVs` | `GET /v1/cvs` (cursor, `page_size` ≤ 50) |
| `UpdateParsedData` | `PATCH /v1/cvs/{id}/parsed`: chỉ khi `PARSED`; body là `ParsedCV` đầy đủ, validate theo schema §13.3; tăng `version` |
| `ReparseCV` | `POST /v1/cvs/{id}:reparse`: chỉ khi `PARSED` hoặc `FAILED` |

- `GetCV` trả `parsed` (khi có), `status`, `parse_error`, `updated_at`. Không bao giờ trả `object_key` hay URL tải.
- `UpdateParsedData` không phát event nào trong M1b. Event `cv.updated` cho re-embedding thuộc M2.

### 3.4 Consumer
- Group `candidate-cv-results`, topic `cv.parsed` và `cv.parse_failed`, dùng `pkg/kafka.Consumer`.
- Trong một transaction:
  1. `idempotency.Once(event_id)`. Event đã xử lý thì bỏ qua.
  2. `SELECT … FOR UPDATE` CV. Không có CV → `kafka.Permanent` (vào DLQ).
  3. Kiểm `attempt_id`, cập nhật status, `parsed_data` hoặc `parse_error`, `parsed_at`, tăng `version`.
- Payload không hợp lệ theo schema → `kafka.Permanent`.
- Consumer chạy goroutine cùng vòng đời với server, giống relay (D3 của M1a).

### 3.5 Data model `candidate_db`
```sql
candidates(user_id uuid PK, full_name text, phone text NULL, location text NULL, created_at, updated_at)
cvs(id uuid PK, candidate_id uuid NOT NULL, object_key text NOT NULL UNIQUE, file_name text NOT NULL,
    status text CHECK(status IN ('UPLOADING','UPLOADED','PARSED','FAILED')), attempt_id text NULL,
    parsed_data jsonb NULL, parse_error text NULL, size_bytes bigint NULL, consent_at timestamptz NOT NULL,
    parsed_at timestamptz NULL, version bigint NOT NULL DEFAULT 1, created_at, updated_at)
INDEX cvs_candidate (candidate_id, created_at DESC, id DESC)
outbox(...), processed_events(...)   -- schema chuẩn pkg/outbox, pkg/idempotency
```
PII (tên, số điện thoại, nội dung CV) không bao giờ được log. Chỉ log `cv_id` / `user_id`.

---

## 4. Event (bổ sung vào `contracts/events/`)
| Event | Producer | Payload |
|---|---|---|
| `cv.uploaded` | candidate | `cv_id, candidate_id, object_key, attempt_id` |
| `cv.parsed` | ai | `cv_id, candidate_id, attempt_id, parsed` (đúng schema `ParsedCV` §5.3), `model_version` |
| `cv.parse_failed` | ai | `cv_id, candidate_id, attempt_id, reason` ∈ `no_text \| not_pdf \| too_large \| llm_refused \| llm_invalid_output \| download_failed`, `detail` (không chứa PII, ≤ 200 ký tự) |

Key của cả ba event là `cv_id`. `source` lần lượt là `candidate-service` và `ai-service`.

---

## 5. AI service (Python)

### 5.1 Cấu trúc (spec §10)
```
services/ai/
  pyproject.toml, uv.lock, Dockerfile, README.md
  src/ai_service/
    config.py            # pydantic-settings, prefix AI_
    main_api.py          # FastAPI health/metrics
    main_worker.py       # consumer + relay
    domain/models.py     # ParsedCV, Education, Experience (pydantic)
    domain/skills.py     # bảng alias + normalize (pure)
    services/cv_parser.py   # use case: bytes → ParsedCV | ParseFailure
    adapters/pdf.py         # PyMuPDF
    adapters/llm.py         # port LlmParser + AnthropicLlmParser + FakeLlmParser
    adapters/storage.py     # MinIO (chỉ get)
    adapters/kafka_consumer.py, adapters/outbox.py (writer + relay), adapters/db.py
    observability.py
  migrations/            # alembic
  tests/unit, tests/integration
```

### 5.2 Worker
- **Consumer `confluent-kafka`:**
  - Group `ai-cv-parser`, topic `cv.uploaded`, `enable.auto.commit=false`.
  - Xử lý từng message rồi commit đồng bộ.
  - Đi theo ngữ nghĩa của `pkg/kafka` (ADR 0006):
    - Retry trong process 3 lần với backoff 200ms×2ⁿ, tối đa 2s.
    - Lỗi permanent hoặc hết lượt retry → DLQ `cv.uploaded.dlq`, đủ các header `dlq-*` giống Go.
    - Ghi DLQ thất bại → dừng worker (fail loud).
    - Đang shutdown thì không đẩy DLQ và không commit.
- **Trace:** extract W3C `traceparent` từ header Kafka, mở span `cv.uploaded process`. OTel Python SDK export OTLP tới `otel-collector:4317`.
- **Idempotency:** bảng `processed_events` trong `ai_db`, ghi trong cùng transaction với kết quả.
- **Luồng xử lý một CV:**
  1. Lỗi khi tải từ MinIO: retry. Hết lượt → `cv.parse_failed reason=download_failed` (phát event, không vào DLQ).
  2. `size > 5 MiB` → `too_large`. Không phải PDF → `not_pdf`.
  3. PyMuPDF trích text; tổng text sau `strip` dưới 50 ký tự → `no_text`.
  4. Text cắt còn 60 000 ký tự trước khi gửi LLM. Việc cắt được ghi lại trong `cv_parse_results.truncated`.
  5. LLM parse → `ParsedCV` → chuẩn hoá skill theo alias (`golang → Go`, `k8s → Kubernetes`, …), bỏ trùng không phân biệt hoa thường.
  6. Trong một transaction: `cv_parse_results` (upsert theo `cv_id` và `attempt_id`) + `processed_events` + outbox (`cv.parsed` hoặc `cv.parse_failed`).
- **Relay Python:** cùng thuật toán với `pkg/outbox` (ADR 0005), chạy thread trong process worker:
  - Poll `FOR UPDATE SKIP LOCKED`, batch 100, chu kỳ 500ms.
  - Publish đồng bộ bằng `confluent-kafka` Producer (`acks=all`, `enable.idempotence=true`).
  - Dừng ở bản ghi lỗi đầu tiên. Đánh dấu và commit chỉ phần đã gửi.
  - Khôi phục carrier trace đã lưu.

### 5.3 `ParsedCV` (spec §13.3)
```python
class Education(BaseModel): school: str; degree: str | None; major: str | None; end_year: int | None
class Experience(BaseModel): company: str; title: str; start: str | None; end: str | None; highlights: list[str]
class ParsedCV(BaseModel):
    full_name: str | None; email: str | None; skills: list[str]; years_of_experience: float | None
    current_title: str | None; education: list[Education]; experiences: list[Experience]
    languages: list[str]; location: str | None
```
- Giới hạn:
  - `skills` tối đa 50 phần tử, mỗi phần tử tối đa 50 ký tự.
  - `highlights` tối đa 10 phần tử, mỗi phần tử tối đa 300 ký tự.
  - `years_of_experience` trong khoảng 0–60.
  - `start` / `end` theo dạng `YYYY-MM` hoặc null.
- JSON Schema `cv.parsed` được sinh ra (hoặc viết tay) khớp với model này. Một test Python so schema pydantic với file trong `contracts/events/` để chống lệch.

### 5.4 Cổng LLM (port) và hai adapter
- `LlmParser` (Protocol): `parse(cv_text: str) -> ParsedCV`. Lỗi trả về là `LlmRefused` hoặc `LlmInvalidOutput`.
- **`AnthropicLlmParser`** (mặc định khi chạy thật):
  - SDK `anthropic`, model `claude-opus-5-5`.
  - `client.messages.parse(model=…, max_tokens=16000, output_format=ParsedCV, output_config={"effort": "medium"}, system=…, messages=[…])`, đọc kết quả ở `response.parsed_output`.
  - Kiểm `stop_reason == "refusal"` trước → `LlmRefused`. `parsed_output` là None hoặc validate lỗi → `LlmInvalidOutput`.
  - Bật server-side fallback mặc định nếu SDK hỗ trợ trên `parse` (`betas=["server-side-fallback-2026-07-01"]`, `fallbacks="default"`). Không hỗ trợ thì ghi lại trong ADR 0007 và bỏ qua.
  - Client tạo với `timeout=60s`, `max_retries=2`; SDK tự retry 429/5xx. Lỗi `APIStatusError` 4xx khác 429 → `LlmInvalidOutput` (permanent). Lỗi kết nối / timeout sau khi hết retry → ném lên để consumer retry, rồi vào DLQ.
  - **Chống prompt injection:** system prompt nói rõ nội dung giữa `<cv_document>` … `</cv_document>` là dữ liệu chưa tin cậy cần trích xuất, không phải chỉ dẫn. Text CV được escape chuỗi `</cv_document>` trước khi bọc. Output bị ràng buộc bởi schema, và kết quả được validate lại bằng pydantic.
  - `model_version` = `"anthropic:claude-opus-5-5"`.
- **`FakeLlmParser`** (`AI_LLM_PROVIDER=fake`; mặc định trong compose dev, test và smoke):
  - Parser quy tắc, xác định (deterministic).
  - Tìm skill bằng cách so khớp từ điển alias trên text đã bỏ dấu và lowercase.
  - `full_name` lấy từ dòng đầu không rỗng; email lấy bằng regex.
  - Các field còn lại để rỗng.
  - `model_version = "fake:rules-v1"`.
  - Đây cũng chính là "fallback rule/NER" mà spec.md §5.7 nhắc tới.
- `AI_LLM_PROVIDER=anthropic` mà không có credential (`ANTHROPIC_API_KEY` hay profile) → worker thoát khi khởi động với lỗi rõ ràng.

### 5.5 Config (`AI_`, pydantic-settings)
`AI_DB_DSN` (bắt buộc), `AI_KAFKA_BROKERS=redpanda:9092`, `AI_S3_ENDPOINT=minio:9000`, `AI_S3_ACCESS_KEY` / `AI_S3_SECRET_KEY` (bắt buộc), `AI_S3_BUCKET=cvs`, `AI_LLM_PROVIDER=fake|anthropic` (mặc định `fake`), `AI_LLM_MODEL=claude-opus-5-5`, `AI_LLM_TIMEOUT_S=60`, `AI_OUTBOX_INTERVAL_MS=500`, `AI_HTTP_PORT=8100`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `AI_LOG_LEVEL`.

### 5.6 Data model `ai_db` (alembic)
```sql
cv_parse_results(cv_id uuid, attempt_id text, status text CHECK(status IN ('PARSED','FAILED')), reason text NULL,
                 parsed jsonb NULL, model_version text, text_chars int, truncated bool, created_at,
                 PRIMARY KEY (cv_id, attempt_id))
processed_events(event_id text PK, processed_at)
outbox(...)  -- cùng schema chuẩn pkg/outbox, gồm headers jsonb
```
Không lưu `raw_text` trong M1b. Bỏ khác biệt này so với spec.md §8.5 vì lý do giảm thiểu PII; M2 có thể cần lại cho embedding thì thêm khi đó.

---

## 6. Gateway, compose, smoke
- **Gateway:** `GATEWAY_CANDIDATE_GRPC_ADDR=candidate:9003`, `RegisterCandidateServiceHandlerClient`.
  - `/v1/me/profile` và `/v1/cvs*` đi qua mux. Kiểm lại thứ tự route với `/v1/me` (exact) và `/v1/me/jobs`.
- **Compose:**
  - Thêm `minio`, `minio-init` (profile infra), `candidate-migrate`, `candidate`, `ai-migrate` (alembic), `ai-worker`, `ai-api`.
  - Script init Postgres tạo thêm `candidate_svc` / `candidate_db` và `ai_svc` / `ai_db`.
  - Biến mới trong `.env.example`: `CANDIDATE_DB_PASSWORD`, `AI_DB_PASSWORD`, `MINIO_ROOT_USER/PASSWORD`, `CANDIDATE_S3_ACCESS_KEY/SECRET`, `AI_S3_ACCESS_KEY/SECRET` (giá trị dev).
- **Smoke:** thêm các bước theo thứ tự:
  1. Login candidate seed.
  2. `PUT` profile, rồi `GET` profile.
  3. Lấy upload-url.
  4. `PUT` một PDF nhỏ do smoke tự sinh (text: tên, email, "Golang, k8s, PostgreSQL").
  5. Complete.
  6. `cv.uploaded` xuất hiện trên Kafka.
  7. Poll `GET /v1/cvs/{id}` tới `PARSED` (timeout 60s).
  8. Skills chứa `Go`, `Kubernetes`, `PostgreSQL`.
  9. `PATCH` parsed rồi đọc lại.
  10. Upload một file không phải PDF → complete trả 400.

## 7. Testing
- **Candidate unit:** domain (bảng chuyển trạng thái, `attempt_id`), app (ownership, consent, complete idempotent, event cũ bị bỏ qua). Coverage domain+app ≥ 70%.
- **Candidate integration:** Postgres + MinIO + Redpanda qua testcontainers.
  - Presign → PUT → complete → outbox có `cv.uploaded` khớp schema.
  - Quá cỡ / sai magic bị từ chối và object bị xoá.
  - Consumer áp dụng `cv.parsed`; event trùng bị bỏ qua; event `attempt_id` cũ bị bỏ qua.
  - Payload hỏng → DLQ.
- **AI unit:**
  - `skills.normalize`; `FakeLlmParser`; `cv_parser` với mỗi `reason`.
  - Escape delimiter.
  - `AnthropicLlmParser` với client giả (refusal, `parsed_output` None, lỗi 4xx).
  - Schema pydantic khớp file `contracts/events`.
  - mypy strict cho `domain` + `services`. Coverage ≥ 70%.
- **AI integration:** Postgres + Redpanda + MinIO.
  - Đặt PDF vào bucket, phát `cv.uploaded` → nhận `cv.parsed` với key `cv_id`, trace header nối tiếp.
  - Trùng `event_id` → không phát lần hai.
  - Payload hỏng → `cv.uploaded.dlq`.
- **Không có test nào gọi API Anthropic thật.** Một script thủ công `services/ai/scripts/try_anthropic.py` để thử bằng tay khi có credential (không chạy trong CI).

## 8. Tồn đọng M1a làm trong M1b (task đầu tiên, trước khi có consumer thật)
- Relay Go: đánh dấu và commit dưới `context.WithTimeout(context.WithoutCancel(ctx), 5s)`; rollback có timeout. Log cảnh báo kèm `event_type` và outbox id.
- `pkg/kafka`:
  - Span consumer ghi nhận kết quả DLQ / decode.
  - DLQ bỏ header `dlq-*` cũ trước khi thêm mới.
  - `NewConsumer` validate Brokers / Group / Topics rỗng.
  - Doc `Handler` ghi rõ lỗi trả về không được chứa PII.

## 9. Definition of Done
1. `make down-v && make bootstrap && make infra-up obs-up up && make seed && make smoke` pass: auth 8 + job 7 + CV 10 bước.
2. Tempo có một trace `POST /v1/cvs/{id}:complete` → candidate → outbox relay → `cv.uploaded process` (Python) → `cv.parsed publish` (Python) → `cv.parsed process` (Go candidate), cùng một trace id.
3. `make lint test test-int` xanh, kể cả Python (`make ai-lint ai-test ai-test-int`). Race test Go trong container xanh. `cover-candidate` ≥ 70%. Coverage Python ≥ 70%.
4. ADR 0007, 0008; README (roadmap M1b ✅, URL MinIO console); CLAUDE.md.

## 10. Rulings (tự chốt)
| # | Quyết định | Lý do |
|---|---|---|
| E1 | LLM: Anthropic SDK, `claude-opus-5-5`, `messages.parse` + pydantic, effort `medium` | spec.md §9.2 chỉ cần "structured output"; dùng SDK chính thức theo hướng dẫn claude-api; Opus 5.5 là mặc định |
| E2 | `FakeLlmParser` mặc định ở dev / CI / smoke | Không cần API key để demo hay chạy CI; xác định, nên test ổn định; đóng vai fallback rule của spec |
| E3 | Process `api` của AI chưa có gRPC trong M1b | Chưa có RPC nào cần (ScoreMatch / FindCandidates thuộc M2); YAGNI |
| E4 | Python có outbox + relay riêng (cùng thuật toán với Go) | Giữ nguyên tắc spec §4.1 #4 (event qua outbox), thể hiện pattern xuyên ngôn ngữ |
| E5 | Presigned PUT + kiểm size / magic khi `complete` (thay vì POST policy) | PUT presign đơn giản cho client; S3 PUT không giới hạn được size nên kiểm phía server và xoá nếu sai |
| E6 | `attempt_id` để bỏ qua kết quả cũ | Reparse có thể tạo nhiều kết quả song song; không cần lock phân tán |
| E7 | Không lưu `raw_text` | Giảm PII; khác spec §8.5, thêm lại khi M2 cần |
| E8 | User MinIO tách theo service, quyền tối thiểu | spec §4.2: hạ tầng dùng chung, credential riêng |
