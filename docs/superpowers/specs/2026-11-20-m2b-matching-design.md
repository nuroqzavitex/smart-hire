# M2b — CV ↔ JD matching (embedding, Qdrant, scoring) — design

Status: approved approach (option 2), pending doc review · Date: 2026-11-20 · Milestone: M2b (after M2a)

## 1. Intent

SmartHire là dự án portfolio Backend-Go (dữ liệu giả, làm để học). M2b thêm phần "AI matching" của `spec.md` §5.7/§6.3:
nhà tuyển dụng xem **ứng viên phù hợp** cho một tin của mình, ứng viên xem **tin gợi ý** cho một CV của mình,
và `ScoreMatch(cv, job)` sẵn sàng cho saga nộp đơn ở M3. Giá trị portfolio nằm ở các pattern backend:
event-driven local replica, idempotent consumer, Go → Python gRPC có timeout + circuit breaker, vector search,
và vận hành được khi AI/Gemini lỗi.

## 2. Goals / non-goals / anti-goals

Goals
- Embed CV và JD bằng **Gemini embedding** (free tier của user), lưu vector trong **Qdrant**.
- Điểm 0–100 theo công thức §5.7 kèm breakdown + kỹ năng khớp/thiếu.
- Precompute `match_results` theo sự kiện (tin mới/sửa, CV mới/sửa) và phát `match.completed`.
- Hai endpoint qua gateway: `GET /v1/jobs/{id}/candidates`, `GET /v1/cvs/{id}/recommended-jobs`.
- Mỗi ứng viên chỉ một dòng trong danh sách của recruiter (CV "active" = CV PARSED mới nhất).
- Sửa CV (candidate) → AI embed + chấm lại.

Non-goals (để sau)
- Cache Redis 5 phút, bộ eval nDCG/MRR, giải thích bằng LLM, FE.
- Xoá vector khi xoá tài khoản (saga M4), consumer của `match.completed` (Notification M4).
- Gọi `ScoreMatch` từ saga (M3) — chỉ cần RPC chạy được.
- Tên ứng viên trong danh sách recruiter (cần gọi Candidate; FE M5 sẽ quyết).
- Backfill CV đã parse trước M2b (chưa từng có `cv.updated`): chỉ cần reparse/sửa là vào matching. Tin cũ thì
  không cần backfill vì group `ai-matcher` mới đọc `job.*` từ đầu topic (`auto.offset.reset=earliest`).

Anti-goals
- Test/CI/smoke **không bao giờ** gọi Gemini thật (giống quy tắc với Anthropic).
- Gemini lỗi/429 không được làm mất sự kiện (không đẩy vào DLQ vì lỗi hạ tầng).
- AI sập không được làm hỏng các API khác của Job/Candidate; chỉ hai endpoint matching trả 503.

## 3. Approaches considered

1. **Gateway → AI trực tiếp** (AI Python tự verify JWT, tự kiểm tra quyền dựa trên bản sao). Ít chặng, nhưng
   lặp lại logic `pkg/grpcx` trong Python và đặt quyết định quyền ở service không sở hữu dữ liệu.
2. **Go services đứng trước, AI gRPC nội bộ** — *được chọn*. Job kiểm tra chủ tin rồi gọi `AI.FindCandidates`;
   Candidate kiểm tra chủ CV rồi gọi `AI.RecommendJobs` và `Job.BatchGetJobs` để lấy chi tiết tin. Đúng spec
   ("Job Svc → AI.FindCandidates"), quyền nằm ở chủ dữ liệu, và đường Go → AI dùng lại được ở M3.
3. AI qua HTTP/FastAPI thay gRPC — loại vì lệch contracts-first (proto là nguồn sự thật).

Mô hình embedding: user chọn **Gemini free tier** (thay `bge-m3` của spec). Dữ liệu là giả nên gửi CV lên Gemini
không phải vấn đề; vẫn bỏ họ tên/email/điện thoại/trường khỏi văn bản embed vì không giúp matching và giảm bias (R3).

## 4. Architecture

```
job.* (Job outbox) ─┐                                ┌─► Qdrant: jobs (768d, cosine)
cv.updated (Cand.) ─┼─► ai-matcher (Python worker) ──┼─► Qdrant: cvs  (768d, cosine)
                    │     Gemini embed (port)        ├─► ai_db: match_jobs, match_cvs, match_results
                    │                                └─► outbox → match.completed
gateway ─► Job.ListMatchedCandidates ─(owner check)─► ai-api gRPC :9100 FindCandidates  (reads match_results)
gateway ─► Candidate.ListRecommendedJobs ─(owner)──► ai-api gRPC :9100 RecommendJobs   (Qdrant search + score)
                                          └─────────► Job.BatchGetJobs (title, level, location, salary…)
(M3) Application ─────────────────────────────────► ai-api gRPC ScoreMatch
```

Processes (same `services/ai` image):
- `ai-worker` (có sẵn): parse CV, không đổi.
- `ai-matcher` (**mới**, entrypoint riêng): consumer group `ai-matcher`, topics `job.created|updated|closed|deleted`,
  `cv.updated`. Tách khỏi `ai-worker` để rate limit Gemini không chặn việc parse và scale độc lập.
- `ai-api` (có sẵn, FastAPI health/metrics :8100) **thêm gRPC server :9100** (compose-internal, không JWT — ghi ADR).

## 5. Events

### 5.1 `cv.updated` (mới, Candidate phát, key = `cv_id`)
Candidate là nguồn sự thật của dữ liệu CV đã parse (nó áp `cv.parsed` theo `attempt_id` và cho sửa). Vì vậy
matcher **không** nghe `cv.parsed` trực tiếp (có thể là attempt cũ đã bị bỏ) mà nghe `cv.updated`, phát trong cùng
transaction với mọi thay đổi trạng thái/dữ liệu parse của CV:
- áp `cv.parsed` thành công (status PARSED), áp `cv.parse_failed` (FAILED), `UpdateParsedData` (PARSED, dữ liệu mới),
  `ReparseCV` (UPLOADED, parsed = null).

Payload: `cv_id, candidate_id, version (cv.version), status, parsed (ParsedCV | null), parsed_at (| null)`.
Schema `contracts/events/cv.updated.v1.json` dùng lại `$defs.ParsedCV` của `cv.parsed.v1.json` (copy, test giữ đồng bộ
như M1b). Matcher bỏ qua bản có `version` ≤ bản đã lưu.

### 5.2 `job.*` (có sẵn)
Matcher giữ bản sao trong `match_jobs` (job_id, recruiter_id, version, status, title, description, required/nice skills,
level, location, salary, published_at). Chỉ tin `open` được embed/match; `closed|deleted` → xoá khỏi Qdrant và
`match_results`. Bỏ qua version ≤ bản đã lưu.

### 5.3 `match.completed` (mới, AI phát, key = `job_id`)
Sau khi tính lại top cho một tin: `job_id, model_version, top[{candidate_id, cv_id, score}]` (≤ 20 phần tử, theo
§7.2 spec). Chưa có consumer.

## 6. Matching

### 6.1 Văn bản đem embed
- JD: `title`, `level`, `required_skills`, `nice_to_have_skills`, `description` (cắt ~6.000 ký tự).
- CV: `current_title`, `skills`, `years_of_experience`, các `experiences` (title + highlights, bỏ company),
  `education` chỉ `degree/major`, `languages`. **Không** có `full_name`, `email`, school, location (location chấm riêng).

Gemini: task type `RETRIEVAL_DOCUMENT` cho cả hai (so sánh đối xứng CV↔JD), `output_dimensionality = 768`,
chuẩn hoá L2 phía client. Model mặc định `gemini-embedding-001` (cấu hình `AI_EMBEDDING_MODEL`).
`model_version` = tên model + `@768` được lưu kèm mỗi vector và mỗi dòng `match_results`.

Port `Embedder` (`embed(texts: list[str]) -> list[list[float]]`, `model_version`) với:
- `GeminiEmbedder` (SDK `google-genai`, batch, timeout), dùng khi `AI_EMBEDDING_PROVIDER=gemini` + `AI_GEMINI_API_KEY`.
- `FakeEmbedder` (mặc định): vector xác định từ hashing bag-of-words đã unaccent (cùng chữ → gần nhau), để smoke và
  demo không cần key nhưng kết quả vẫn có nghĩa.

### 6.2 Qdrant
Collections `jobs_<model-slug>` và `cvs_<model-slug>` (tên theo model → đổi model là collection mới, R5), 768d,
cosine. Point id = job_id / cv_id. Payload CV: `cv_id, candidate_id, active (bool), version`; payload job:
`job_id, recruiter_id, version`. Chỉ CV `active` (PARSED mới nhất của ứng viên, theo `parsed_at`) được tìm khi
match cho một tin; mọi CV PARSED đều có thể xin `RecommendJobs`.

### 6.3 Điểm (spec §5.7, giữ nguyên trọng số)
```
score = 100 × (0.55·semantic + 0.30·skill_coverage + 0.10·experience_fit + 0.05·location_fit)
nếu skill_coverage < 0.5 (thiếu > 50% kỹ năng bắt buộc) → score = min(score, 40)
```
- `semantic` = max(0, cosine).
- `skill_coverage` = |required ∩ cv.skills| / |required| sau khi chuẩn hoá bằng `domain/skills.normalize`
  (alias/synonym có sẵn từ M1b); tin không có kỹ năng bắt buộc → 1.0. `matched_skills`/`missing_skills` từ đây.
- `experience_fit`: số năm tối thiểu theo level — intern 0, junior 0, middle 2, senior 5, lead 7;
  `min(1, years/min)` (min = 0 → 1); CV không có số năm → 0.5.
- `location_fit`: unaccent + lowercase; bằng nhau hoặc chứa nhau → 1; một bên trống → 0.5; khác → 0.
Điểm làm tròn 2 chữ số. Hàm thuần trong `domain/scoring.py`, test bảng.

### 6.4 Khi nào tính
- Tin `open` mới/sửa: embed → Qdrant search 50 CV `active` gần nhất → chấm → thay toàn bộ `match_results` của tin
  → outbox `match.completed`.
- `cv.updated` PARSED: embed → cập nhật `active` cho các CV của ứng viên → search 50 tin open gần nhất → chấm →
  upsert các dòng (job_id, cv_id); xoá dòng của các CV không còn active của ứng viên đó.
- `cv.updated` khác PARSED: xoá vector + dòng của CV; CV PARSED mới nhất còn lại (nếu có) thành active.
- `job.closed|deleted`: xoá vector + dòng.
Mọi thay đổi Postgres của một sự kiện + `processed_events` + outbox trong một transaction; Qdrant ghi trước,
idempotent (upsert/delete theo id) nên chạy lại an toàn.

### 6.5 Lỗi
- Gemini 429/5xx/timeout, Qdrant/Postgres không tới được → **thử lại có giãn cách (0.2s → 10s) cho tới khi dừng**,
  không DLQ (như projector M2a). Payload sai schema / version lạ → DLQ ngay. Cần mở rộng `RetryingConsumer` hiện có
  (đang retry 3 lần rồi DLQ) bằng một lớp lỗi "transient" và cho nhận nhiều topic; `ai-worker` giữ hành vi cũ.
- Key thiếu khi provider = gemini → service không khởi động.

## 7. gRPC & API

`contracts/proto/smarthire/ai/v1/ai.proto` theo spec §13.2 (`ScoreMatch`, `FindCandidates`, `RecommendJobs`), mở rộng:
`RankedJob` thêm `ScoreBreakdown breakdown`; `top_k` mặc định 20, tối đa 50. Không có HTTP annotation (nội bộ).
Python code sinh bằng buf (plugins `protocolbuffers/python`, `protocolbuffers/pyi`, `grpc/python`) vào `services/ai`.
- `FindCandidates(job_id, top_k)`: đọc `match_results` (chỉ CV active) theo score giảm dần.
- `RecommendJobs(cv_id, top_k)`: lấy vector CV từ Qdrant → search tin → chấm on-demand. CV chưa có vector → `NOT_FOUND`.
- `ScoreMatch(cv_id, job_id)`: on-demand từ hai vector + bản sao. Thiếu một bên → `NOT_FOUND`.

Job service (Go): `ListMatchedCandidates` — `GET /v1/jobs/{id}/candidates?top_k=` — chỉ recruiter chủ tin (hoặc admin);
`BatchGetJobs(ids)` — nội bộ (không HTTP annotation, tối đa 50 id), cần JWT hợp lệ bất kỳ role, chỉ trả tin `open`
(dữ liệu công khai), giữ thứ tự id đầu vào.
Candidate service (Go): `ListRecommendedJobs` — `GET /v1/cvs/{id}/recommended-jobs?top_k=` — chỉ chủ CV, CV phải
PARSED; ghép kết quả AI với `Job.BatchGetJobs` (forward JWT), bỏ tin không còn open.
AI client trong Go: `grpcx.Dial`, timeout 2s, `sony/gobreaker/v2` (5 lỗi liên tiếp, mở 30s) như M2a; AI lỗi/breaker
mở → `Unavailable` → 503 qua gateway. Breaker không đếm caller cancel và lỗi `NOT_FOUND`/`INVALID_ARGUMENT`.

## 8. Ops

- Compose: `qdrant` (ảnh chính thức, pin version, 127.0.0.1:6333, volume), `ai-matcher`, `ai-api` mở :9100 trong mạng;
  env `JOB_AI_ADDR`, `CANDIDATE_AI_ADDR`, `CANDIDATE_JOB_ADDR`, `AI_QDRANT_URL`, `AI_EMBEDDING_PROVIDER`,
  `AI_GEMINI_API_KEY` (rỗng mặc định trong `.env.example`).
- Migration alembic mới cho `match_jobs`, `match_cvs`, `match_results`; migration Candidate không cần (chỉ outbox).
- Smoke: thêm bước tạo tin + upload CV (fake LLM, fake embedder) → recruiter thấy ứng viên, ứng viên thấy tin;
  người khác gọi → 403/404.
- OTel: trace đi tiếp qua Kafka (`cv.updated`, `job.*`) và Go → Python gRPC (instrumentation grpc Python).
- ADR 0010: matching (Gemini thay bge-m3, cv.updated thay vì cv.parsed, AI gRPC nội bộ không JWT, precompute vs on-demand).
- CI: unit Python dùng fake; integration Python dùng testcontainers Qdrant + Postgres; Go dùng AI server giả (bufconn).

## 9. Left to the builder

Tên file/module cụ thể, dạng bảng SQL chi tiết, cách đóng gói code proto Python, chọn version pin (Qdrant, google-genai,
grpcio), chia task. Các con số (50/20/768/ngưỡng 40/years theo level) là quyết định của mình (Claude), có thể chỉnh
khi review.
