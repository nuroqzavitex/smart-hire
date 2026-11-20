# M2a — CQRS job search (OpenSearch read model + projector) — Design

- Ngày: 2026-11-20
- Trạng thái: chờ duyệt
- Nguồn: `spec.md` §4.1, §5.2, §6.7, §8.2, R6, R7; ADR 0005–0006 (outbox, consumer/DLQ).
- M2 được chia đôi (user chọn 2026-11-20): **M2a** = CQRS tìm kiếm JD (Go), **M2b** = embedding + Qdrant + matching (Python), spec/plan riêng.
- Cách triển khai sau khi có plan: inline trong session chính (superpowers:executing-plans), không subagent.

## 1. Mục tiêu

Tìm kiếm JD là luồng đọc nhiều nhất. Mục tiêu của M2a là thể hiện pattern **CQRS + projection** một cách đúng và kiểm chứng được trong repo portfolio Backend-Go:

- ghi vẫn vào Postgres (write model) như M1a;
- đọc tìm kiếm từ OpenSearch (read model) được cập nhật **bất đồng bộ** qua Kafka bởi một projector;
- xử lý đúng các ca khó của projection: event trùng, event đến sai thứ tự, xoá rồi event cũ đến sau, rebuild không downtime và không mất event, OpenSearch sập.

Thành công = smoke chứng minh luồng tạo/tìm/đóng JD qua OpenSearch; test integration chứng minh các ca khó trên; video demo được "tắt OpenSearch vẫn tìm được (fallback)" và "rebuild không gián đoạn".

## 2. Phạm vi

### 2.1 Trong phạm vi
- Job service thêm binary **`cmd/projector`** (process riêng, container riêng `job-projector`) với 3 lệnh: chạy projector (mặc định), `rebuild`, `reconcile`.
- Adapter OpenSearch trong Job service: mapping/analyzer tiếng Việt, upsert có versioning, search + facet, quản lý alias.
- `ListJobs` (GET `/v1/jobs`) chuyển sang đọc OpenSearch qua circuit breaker; fallback Postgres `tsvector` (code M1a giữ nguyên làm fallback).
- Bổ sung `published_at` vào payload `job.created`/`job.updated` (§4).
- Response `ListJobs` thêm `facets` và `degraded` (§5.3).
- Compose: OpenSearch (image tự build = image chính thức + plugin `analysis-icu`), `job-projector`; smoke thêm bước tìm kiếm; CI thêm OpenSearch cho test integration.
- ADR 0009 (CQRS read model, versioning, tombstone, alias kép), cập nhật README/CLAUDE.md.

### 2.2 Ngoài phạm vi (non-goals)
- Matching, embedding, Qdrant, `FindCandidates`/`RecommendJobs` → M2b.
- OpenSearch Dashboards, dashboard Grafana riêng cho projector (M4 observability).
- Cron đối soát hằng đêm (chỉ có lệnh `reconcile` chạy tay/smoke).
- Bảo mật OpenSearch (TLS/user): dev tắt security plugin, chỉ trong mạng compose (ghi nhận nợ trong ADR).
- Tìm kiếm cho chủ JD (`ListMyJobs`) vẫn đọc Postgres (cần dữ liệu mới nhất, kể cả draft).
- `company_name` trong spec §8.2: Job domain chưa có công ty → không index ở M2a.

### 2.3 Anti-goals (hỏng dù "chạy được")
- Kết quả tìm kiếm hiện JD đã đóng/xoá lâu dài, hoặc một event cũ làm "sống lại" JD đã xoá.
- Rebuild làm mất thay đổi xảy ra trong lúc rebuild.
- OpenSearch sập kéo theo `GET /v1/jobs` lỗi 5xx.
- Projector lặp vô hạn / chặn partition vì một message hỏng (phải vào DLQ như ADR 0006).

## 3. Kiến trúc

```
Recruiter → Gateway → job-server (command) → Postgres jobs + outbox
                                                 │ outbox relay (M1a)
                                                 ▼
                                    Kafka job.created|updated|closed|deleted
                                                 │ consumer group "job-projector"
                                                 ▼
                                   job-projector → OpenSearch alias jobs_write
Public → Gateway → job-server (query ListJobs) → [breaker] → OpenSearch alias jobs
                                                      └─ open/timeout → Postgres tsvector (degraded=true)
```

- `job-server` = binary hiện có (`cmd/server`), thêm client OpenSearch chỉ để **đọc**.
- `job-projector` = binary mới (`cmd/projector`), dùng chung `internal/` của Job service (domain, adapter postgres để rebuild/reconcile, adapter opensearch). Không import service khác.
- Projector chỉ cần Postgres cho `rebuild`/`reconcile`; khi chạy consumer thì không đọc Postgres.

## 4. Event (thay đổi `contracts/events/`)

- `job.created.v1` và `job.updated.v1`: thêm `published_at` (`string` `date-time` | `null`) vào `payload`, **required** (luôn có mặt, `null` khi draft). Giữ `event_version: 1`.
  - Lý do giữ v1: consumer duy nhất của `job.*` là projector viết ở M2a; event cũ đã nằm trên topic (M1a) không có trường này → projector chấp nhận thiếu `published_at` (coi như `null`, rồi lấy `occurred_at` nếu `status=open`, xem §6.2). Ghi rõ trong ADR 0009.
  - Fixture dùng chung trong `contracts/events/testdata` cập nhật theo.
- `job.closed.v1`, `job.deleted.v1` giữ nguyên (`job_id`, `version`).
- Job service `payloadOf` thêm `PublishedAt`.

## 5. OpenSearch read model

### 5.1 Index, alias, mapping
- Index vật lý `jobs_v{n}` (bắt đầu `jobs_v1`). Hai alias:
  - `jobs` (đọc): trỏ đúng **một** index.
  - `jobs_write` (ghi): bình thường trỏ cùng index với `jobs`; trong lúc rebuild trỏ **cả cũ lẫn mới**.
- Mapping + settings nằm trong file JSON embed vào binary (`internal/adapter/opensearch/index.json`), `dynamic: strict`:
  - `job_id` keyword; `version` long; `status` keyword (`draft|open|closed|deleted`);
  - `title`: text `vi` + sub-field `.raw` text `vi_exact` (giữ dấu) để boost khớp có dấu;
  - `description`: text `vi`;
  - `required_skills`, `nice_to_have_skills`: keyword với normalizer `folded` (= `lowercase` + `icu_folding`, dùng cho lọc + facet) và sub-field `.text` text `vi_skills` (tìm full-text có synonym);
  - `level` keyword; `location` keyword normalizer `folded` + sub-field `.text` text `vi` (để `q=ha noi` khớp "Hà Nội");
  - Facet trả giá trị đã normalize (vd `golang`, `ha noi`) — chấp nhận ở M2a (FE M5 có thể hiển thị nguyên dạng này); ghi trong ADR.
  - `salary_min`, `salary_max` integer; `published_at` date; `recruiter_id` keyword (không dùng tìm kiếm, phục vụ reconcile/debug).
- Analyzer:
  - `vi`: `icu_tokenizer` + `icu_folding` (bỏ dấu, lowercase) + filter synonym kỹ năng;
  - `vi_exact`: `icu_tokenizer` + `lowercase` (không bỏ dấu);
  - `vi_skills`: `keyword` tokenizer + `icu_folding` + synonym.
  - Synonym (graph, file inline trong settings): `golang, go`; `k8s, kubernetes`; `js, javascript`; `ts, typescript`; `postgres, postgresql`; `nodejs, node.js, node`. Danh sách ngắn, mở rộng sau.
- `number_of_shards: 1`, `number_of_replicas: 0` (dev, 1 node).

### 5.2 Ghi có versioning (thay cho "so version rồi ghi" + processed_events)
- Mọi ghi là **index full document** với `version = payload.version`, `version_type = external`. OpenSearch từ chối (409) nếu version ≤ version hiện có → projector coi là **đã áp dụng** (thành công, commit offset). Nhờ vậy:
  - event trùng → 409 → bỏ qua (idempotent, không cần bảng `processed_events`);
  - event cũ đến sau event mới → 409 → bỏ qua.
- **Không xoá document** (lệch spec §8.2 "xoá document"): `job.closed` / `job.deleted` ghi **tombstone** — giữ document với `status=closed|deleted` và version mới. Lý do: delete xoá luôn version → một `job.updated` cũ đến sau sẽ tạo lại JD đã xoá. Tìm kiếm luôn lọc `status=open`.
  - Tombstone của `job.closed`/`job.deleted` chỉ có `job_id`, `version`, `status` (payload không có trường khác). Nếu sau đó không có event mới hơn thì document chỉ gồm 3 trường đó — vô hại vì bị lọc.
  - Tombstone tồn tại lâu dài (một document nhỏ cho mỗi JD đã xoá, tương ứng dòng soft-delete vẫn còn trong Postgres); rebuild cũng ghi lại tombstone (§6.3). Dọn tombstone = nợ, ghi trong ADR.
- Ghi vào alias `jobs_write`: projector resolve danh sách index của alias (cache 5s, `ProjectorAliasTTL`) và ghi lần lượt từng index; 409 ở index nào thì coi index đó đã có bản mới hơn. Lỗi khác ở bất kỳ index nào → lỗi tạm thời → retry/DLQ theo `pkg/kafka`.
- `refresh`: không ép refresh mỗi ghi; dựa vào `refresh_interval: 1s` (spec: JD xuất hiện sau ~1–2s).

### 5.3 Query (`ListJobs`)
- Lọc bắt buộc `status=open`. Bộ lọc giữ **đúng ngữ nghĩa SQL M1a** (`SearchOpenJobs`):
  - `skills`: JD có **ít nhất một** skill bắt buộc trong danh sách (SQL `&&`) → `terms` trên `required_skills` (keyword normalizer `folded`);
  - `location`: bằng nhau không phân biệt hoa thường và dấu (SQL `lower(unaccent(..)) =`) → `term` trên `location` (normalizer `folded`);
  - `level`: bằng nhau;
  - `salary_min = x`: `salary_max` null **hoặc** `salary_max >= x`.
- `q` rỗng → sort `published_at desc, job_id desc`. `q` có → `multi_match` kiểu `cross_fields`, `operator: and` (mọi từ phải khớp ở đâu đó — giống `websearch_to_tsquery` của M1a), fields `title^3`, `title.raw^4`, `required_skills.text^2`, `description`, `location.text`; sort `_score desc, published_at desc, job_id desc`.
- Phân trang `search_after`. Page token = base64 JSON `{"b":"os","sa":[...]}` (OpenSearch) hoặc `{"b":"pg", ...cursor M1a}` (Postgres). Token của backend khác backend đang phục vụ → trả **trang đầu** của backend hiện tại (không lỗi). Token hỏng → 400 như M1a.
- **Facets** (chỉ khi phục vụ bằng OpenSearch): terms aggregation `required_skills` (top 20), `location` (top 20), `level` (đủ 5), tính trên cùng bộ lọc (không loại trừ facet của chính nó — đơn giản, ghi nhận).
- Proto `ListJobsResponse` thêm:
  ```proto
  message Facet { string value = 1; int64 count = 2; }
  message ListJobsFacets { repeated Facet skills = 1; repeated Facet locations = 2; repeated Facet levels = 3; }
  ListJobsFacets facets = 3;   // rỗng khi degraded
  bool degraded = 4;           // true khi kết quả lấy từ fallback Postgres
  ```
  Field mới, không đổi field cũ → client cũ không ảnh hưởng. `ListMyJobs` dùng chung message, luôn `degraded=false`, facets rỗng.

### 5.4 Circuit breaker + fallback
- `sony/gobreaker` bọc lời gọi search OpenSearch: mở sau 5 lỗi liên tiếp, half-open sau 30s, 1 request thử. Timeout mỗi lời gọi 2s (`JOB_SEARCH_TIMEOUT`).
- Lỗi được tính là "lỗi backend": timeout, lỗi mạng, 5xx, `index_not_found`. Lỗi 4xx do query (không nên xảy ra) → log + tính là lỗi luôn (an toàn: fallback vẫn phục vụ).
- Khi breaker mở hoặc lời gọi lỗi → gọi `JobRepo.Search` (Postgres, code M1a) và trả `degraded=true`. Log warning (không PII: chỉ `error_type`, trạng thái breaker). Metric/attr span `smarthire.search.backend=opensearch|postgres`.
- Nếu cả fallback lỗi → lỗi như M1a (500 internal).

## 6. Projector (`cmd/projector`)

### 6.1 Chạy (mặc định)
- Consumer `pkg/kafka` group `job-projector`, topics `job.created`, `job.updated`, `job.closed`, `job.deleted`; retry/DLQ/trace theo ADR 0006 (không thay đổi `pkg/kafka`).
- Validate envelope bằng `pkg/event/eventschema` (như Candidate M1b). Sai schema / payload không parse được → `kafka.Permanent` → DLQ ngay; thông báo lỗi cố định, không echo payload (E-R6 của M1b).
- HTTP `:8012` `/healthz`, `/readyz` (ready khi đã kết nối Kafka và `jobs_write` tồn tại). Port gRPC: không có.
- Khởi động: nếu alias `jobs`/`jobs_write` chưa có → tạo `jobs_v1` + hai alias (idempotent; hai projector cùng khởi động thì một bên nhận "đã tồn tại" và bỏ qua).

### 6.2 Ánh xạ event → document
| Event | Document ghi (version = payload.version) |
|---|---|
| `job.created`, `job.updated` | full: mọi trường payload; `published_at` = payload nếu có, nếu thiếu/null mà `status=open` → `occurred_at` của envelope, ngược lại null |
| `job.closed` | tombstone `{job_id, version, status:"closed"}` |
| `job.deleted` | tombstone `{job_id, version, status:"deleted"}` |

Skills được ghi nguyên chữ (keyword normalizer lo lowercase), facet trả giá trị đã lowercase.

### 6.3 `job-projector rebuild`
1. Tạo index mới `jobs_v{n+1}` (n = số lớn nhất hiện có) từ mapping hiện tại.
2. Thêm index mới vào alias `jobs_write` (giữ index cũ).
3. Chờ `ProjectorAliasTTL + 2s` để mọi projector đang chạy thấy alias mới.
4. Đọc Postgres (mọi dòng `jobs`, theo keyset `id`, batch 500) và bulk index với `version_type=external` (version = cột `version`, bằng version của event cuối cùng của JD đó) → bản nào cũ hơn bản projector vừa ghi bị 409 (bỏ qua). JD đã soft-delete (`deleted_at` not null) được ghi thành **tombstone** `status=deleted` để event cũ đến muộn không hồi sinh nó trong index mới.
5. Đổi alias atomically (`_aliases` một request): `jobs` → index mới; `jobs_write` → chỉ index mới.
6. Xoá index cũ (cờ `--keep-old` để giữ).
- Đúng đắn: mọi thay đổi commit trước bước 4 nằm trong snapshot Postgres; mọi event xử lý sau bước 3 được ghi vào cả hai index; external version đảm bảo thứ tự không quan trọng.
- Chạy trong compose: `docker compose run --rm job-projector rebuild` (Makefile `make search-rebuild`).
- Postgres rebuild dùng role `job_svc` hiện có (đọc `jobs`).

### 6.4 `job-projector reconcile`
- Đếm JD `status=open AND deleted_at IS NULL` ở Postgres và document `status=open` ở alias `jobs`; liệt kê tối đa 20 `job_id` lệch (có ở một bên) hoặc lệch version. In kết quả (chỉ id/số), exit 0 nếu khớp, 2 nếu lệch. `make search-reconcile`. Smoke chạy nó ở cuối (sau khi chờ projector bắt kịp).

## 7. Config (`JOB_`)
Thêm vào `services/job/internal/config` (server và projector dùng chung struct, mỗi binary chỉ bắt buộc phần nó cần — plan quyết định tách struct hay không):
- `JOB_OPENSEARCH_URL` (mặc định `http://opensearch:9200`)
- `JOB_SEARCH_TIMEOUT` (2s), `JOB_SEARCH_BREAKER_FAILURES` (5), `JOB_SEARCH_BREAKER_OPEN` (30s)
- `JOB_PROJECTOR_HTTP_PORT` (8012), `JOB_PROJECTOR_ALIAS_TTL` (5s)

## 8. Hạ tầng, compose, smoke, CI
- `deploy/docker/opensearch/Dockerfile`: `FROM opensearchproject/opensearch:2.19.2` + `opensearch-plugin install --batch analysis-icu`. Tag `smarthire/opensearch:2.19.2-icu`, `make opensearch-image` (infra-up và test-int phụ thuộc, giống `minio-image`).
- Compose infra: `opensearch` (single-node, `DISABLE_SECURITY_PLUGIN=true`, `OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m`, healthcheck `_cluster/health`, publish `127.0.0.1:9200` để debug theo quy ước 127.0.0.1 của repo, volume `opensearch-data`).
- Compose app: `job-projector` (cùng image `smarthire/job:dev`, entrypoint binary projector — Dockerfile Job build cả hai binary), depends `opensearch` healthy + `topics-init` completed; `job` thêm `JOB_OPENSEARCH_URL`.
- Smoke (sau bước Job hiện có): tạo + publish JD với `required_skills ["Golang"]`, location "Hà Nội", tiêu đề có dấu → poll `GET /v1/jobs?q=go` tới khi thấy (timeout 15s) và `degraded=false`; `?q=ha noi` thấy; facet `skills` chứa `golang`; close JD → poll tới khi biến mất; cuối cùng `reconcile` exit 0.
- Test integration (testcontainers, image tự build): xem §9.
- CI `ci-go.yml`: build image OpenSearch (gha cache) cho module `services/job` trước test integration; filter thêm `deploy/docker/opensearch/**`.

## 9. Testing
- Unit:
  - mapping event → document (bảng §6.2, kể cả thiếu `published_at`);
  - page token encode/decode, token khác backend → trang đầu;
  - service query: breaker mở → gọi Postgres, `degraded=true`; OpenSearch OK → không gọi Postgres;
  - projector handler: 409 = thành công; lỗi khác = lỗi tạm thời; schema sai = Permanent.
- Integration (OpenSearch thật):
  - tìm không dấu ra có dấu; synonym `golang`→`Go`; facet đúng; phân trang `search_after` không trùng/không sót;
  - event trùng → 1 document, version không đổi;
  - sai thứ tự: `updated(v3)` rồi `updated(v2)` → giữ v3; `deleted(v4)` rồi `updated(v3)` → vẫn tombstone, không hiện trong tìm kiếm;
  - rebuild trong khi projector đang ghi (goroutine bắn update liên tục) → sau rebuild, index mới có đúng version cuối của mọi JD; alias `jobs` đổi, tìm kiếm không lỗi trong suốt quá trình;
  - reconcile phát hiện lệch (xoá tay một document) và khớp sau khi sửa;
  - fallback: dừng container OpenSearch → `ListJobs` vẫn trả kết quả với `degraded=true`.
- Coverage gate `make cover-job` ≥ 70% giữ nguyên.
- Smoke toàn luồng như §8.

## 10. Definition of Done
- `make lint test test-int cover-job` pass; `make smoke` pass trên volume sạch (thêm các bước tìm kiếm).
- ADR 0009 viết xong; README (sơ đồ, URL OpenSearch, lệnh rebuild/reconcile), CLAUDE.md, `services/job/README` cập nhật.
- Trace: một span `job.created process` (projector) cùng trace với `POST /v1/jobs` (gateway → job → outbox → Kafka → projector).

## 11. Quyết định đã chốt (để user kiểm tra)
1. Chia M2 → M2a/M2b (user chọn).
2. Projector là binary `cmd/projector` trong Job service (spec §5.2), không phải service mới hay goroutine trong server.
3. Thêm `published_at` vào payload `job.created|updated` v1 (không bump v2); projector chịu được event cũ thiếu trường.
4. Versioning bằng `version_type=external` của OpenSearch thay cho `processed_events` + so version thủ công.
5. Tombstone thay vì xoá document (lệch spec §8.2) để chống "hồi sinh".
6. Alias kép `jobs` (đọc) / `jobs_write` (ghi) để rebuild không mất event.
7. Response `ListJobs` thêm `facets` + `degraded`; page token ghi backend, đổi backend → trang đầu.
8. `sony/gobreaker`, client `opensearch-go`; OpenSearch 2.19.2 + `analysis-icu` build local; luôn bật trong compose, heap 512 MB.
9. Không làm: company_name, cron đối soát, Dashboards, security plugin.
