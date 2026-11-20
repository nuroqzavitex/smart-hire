# ADR 0009: CQRS cho tìm kiếm JD — read model OpenSearch + projector

- Trạng thái: Chấp nhận (M2a)
- Ngày: 2026-11-20

## Context

`GET /v1/jobs` là luồng đọc nhiều nhất (trang công khai). M1a tìm bằng Postgres `tsvector` + `unaccent`: được mức cơ bản nhưng yếu ở tiếng Việt, không có facet hay đồng nghĩa kỹ năng, và tải đọc tranh chấp với tải ghi trên cùng database (spec §6.7). Spec chọn CQRS: Postgres là write model, OpenSearch là read model cập nhật bất đồng bộ qua Kafka. Thách thức nằm ở các ca khó của projection: event trùng (outbox at-least-once), event đến sai thứ tự, JD đã xoá bị một event cũ "hồi sinh", rebuild index mà không mất thay đổi và không gián đoạn tìm kiếm, và OpenSearch sập.

## Decision

**Binary `cmd/projector` trong Job service.** Process riêng (container `job-projector`) consume `job.created|updated|closed|deleted` (group `job-projector`, validate envelope bằng `contracts/events`, lỗi schema hoặc payload hỏng → DLQ ngay với thông điệp cố định).

**OpenSearch sập khi đang project: chặn partition, không DLQ.** Lỗi hạ tầng (lỗi mạng/timeout, 5xx, 429, index chưa có) được projector tự thử lại với backoff 200ms → tối đa 10s cho tới khi OpenSearch sống lại hoặc process dừng (khi dừng: không commit, không DLQ, theo ADR 0006). Nếu để `pkg/kafka` retry 3 lần rồi DLQ như consumer thường, mọi JD tạo trong lúc sập sẽ mất khỏi tìm kiếm cho tới khi có người chạy rebuild. Chỉ lỗi 4xx khác (document/mapping hỏng, retry không cứu được) mới vào DLQ. Mỗi lời gọi OpenSearch có timeout 10s để OpenSearch treo không chặn mãi. Đã kiểm trên compose: publish JD khi OpenSearch tắt → projector log "retrying", không có DLQ → bật lại thì JD vào read model. Cùng binary có lệnh `rebuild` và `reconcile`. `job-server` chỉ đọc OpenSearch. Không tách service riêng (thêm module, proto, DB mà không được gì) và không chạy projector như goroutine trong server (mất ý nghĩa tách đọc/ghi).

**Versioning của OpenSearch thay cho `processed_events`.** Mọi ghi là index nguyên document với `version = payload.version`, `version_type = external`. OpenSearch trả 409 khi đã có version bằng hoặc mới hơn → projector coi là đã áp dụng và commit offset. Một cơ chế giải quyết cả event trùng lẫn event sai thứ tự, không cần đọc trước khi ghi hay bảng dedupe riêng.

**Tombstone thay vì xoá document** (lệch spec §8.2 "xoá document"). Xoá document thì mất luôn version, nên một `job.updated` cũ đến sau sẽ tạo lại JD đã xoá. `job.closed`/`job.deleted` ghi document chỉ gồm `{job_id, version, status: closed|deleted}`; mọi truy vấn lọc `status=open`. Rebuild cũng ghi tombstone cho dòng soft-delete.

**Hai alias: `jobs` (đọc) và `jobs_write` (ghi).** Projector resolve `jobs_write` (cache `JOB_PROJECTOR_ALIAS_TTL`, mặc định 5s) và ghi vào mọi index phía sau. `rebuild`:
1. tạo `jobs_v{n+1}`;
2. gắn nó vào `jobs_write` (giữ index cũ) → projector đang chạy ghi cả hai;
3. chờ `TTL + 2s` để mọi projector thấy alias mới;
4. đọc toàn bộ `jobs` từ Postgres theo keyset `id`, bulk index với external version (bản cũ hơn bản projector vừa ghi bị 409, bỏ qua);
5. đổi alias trong **một** request `_aliases`: `jobs` → index mới, `jobs_write` → chỉ index mới;
6. chờ thêm `TTL + 2s` rồi xoá index cũ.

Đúng đắn: mọi thay đổi commit trước bước 4 nằm trong snapshot Postgres; mọi event xử lý sau bước 3 vào cả hai index; external version khiến thứ tự không còn quan trọng. Bước chờ thứ hai được thêm sau khi test `TestRebuild_ConcurrentUpdatesSurvive` bắt được projector ghi vào index cũ vừa bị xoá (404). Ngoài ra projector gặp `index_not_found` thì bỏ cache và resolve lại alias một lần.

**Reconcile.** `projector reconcile` so số JD `open` (và version từng JD) giữa Postgres và alias `jobs`, in tối đa 20 `job_id` lệch, exit 2 nếu lệch; `make smoke` chạy nó ở cuối. Cách sửa khi lệch là `rebuild` (ghi lại cùng version sau khi xoá tay sẽ bị 409, vì OpenSearch nhớ version của document đã xoá một thời gian).

**Tiếng Việt và đồng nghĩa.** Analyzer `vi` = `icu_tokenizer` + `icu_folding` (bỏ dấu, lowercase) + synonym kỹ năng (`golang, go`; `k8s, kubernetes`; …); `title.raw` giữ dấu để boost khớp chính xác. `required_skills` và `location` là keyword với normalizer `folded` (lowercase + bỏ dấu) cho lọc và facet. Image = `opensearchproject/opensearch:2.19.2` + plugin `analysis-icu` (`make opensearch-image`).

**Bộ lọc cùng ngữ nghĩa M1a.** skills = có ít nhất một skill (SQL `&&`), location = bằng nhau không phân biệt hoa thường/dấu, level bằng nhau, `salary_min=x` ⇒ `salary_max` null hoặc `≥ x`. Test integration so cùng bộ dữ liệu trên cả hai backend. Khác biệt đã biết: OpenSearch bỏ dấu cả khi lọc skill (Postgres chỉ lowercase), nên với skill có dấu kết quả OpenSearch là tập cha; phần tìm theo text (`q`) khác nhau theo bản chất hai engine (đồng nghĩa, xếp hạng).

**Circuit breaker + fallback.** `sony/gobreaker` bọc search: mở sau 5 lỗi liên tiếp, half-open sau 30s, timeout 2s mỗi lời gọi. Client tự huỷ request (hoặc hết deadline của chính nó) không bị tính là lỗi OpenSearch và không chạy fallback; timeout 2s của breaker thì vẫn tính. Lỗi hoặc breaker mở → `ListJobs` chạy truy vấn Postgres M1a và trả `degraded: true` (không có facet). OpenSearch sập lúc khởi động không làm `job-server` fail.

**Page token ghi backend.** Token là base64url JSON `{"b":"os","q":<có text query>,"sa":[search_after]}` hoặc `{"b":"pg","c":<cursor M1a>}`. Token của backend khác backend đang phục vụ → trả trang đầu (không lỗi). Token OpenSearch được kiểm hình dạng trước khi dùng: không có `q` thì `search_after` phải là `[số nguyên, chuỗi]`, có `q` thì `[số, số nguyên, chuỗi]` (job_id ≤ 64 ký tự); sai hình dạng hoặc sai chế độ truy vấn → 400. Nếu không, bất kỳ client ẩn danh nào cũng gửi được token giả làm OpenSearch trả 400/500, đủ 5 lần là breaker mở và mọi người dùng bị đẩy sang fallback.

**`published_at` trong payload.** Index cần `published_at` để sắp xếp nên `job.created|updated` v1 có thêm trường này (không lên v2: projector là consumer duy nhất). Trường là **tuỳ chọn** trong schema: event phát trước M2a không có nó và vẫn phải hợp lệ (chạy thật trên compose đã đẩy các event cũ đó vào DLQ khi trường còn bắt buộc). Thiếu hoặc null mà JD `open` thì projector lấy `occurred_at`.

**API.** `ListJobsResponse` thêm `facets` (skills top 20, locations top 20, levels) và `degraded`; field cũ giữ nguyên. Facet trả giá trị đã chuẩn hoá (`golang`, `ha noi`).

## Consequences

**Tích cực**
- Tìm kiếm tiếng Việt không dấu, đồng nghĩa kỹ năng, facet; tải đọc rời khỏi Postgres.
- Xử lý trùng/sai thứ tự/hồi sinh bằng một cơ chế (external version + tombstone), kiểm bằng test integration trên OpenSearch thật.
- Rebuild không gián đoạn, không mất event (test rebuild giữa lúc 200 cập nhật đang chạy và tìm kiếm liên tục không lỗi).
- OpenSearch sập vẫn phục vụ được, kết quả nhất quán với read model nhờ bộ lọc giống hệt.
- Trace liền mạch `gateway → job → Kafka → job-projector` (`job.created process`).

**Tiêu cực / nợ**
- OpenSearch sập thì projector dừng tiến (lag Kafka tăng) cho tới khi OpenSearch sống lại; read model đứng yên trong thời gian đó, tìm kiếm chạy fallback.
- Eventual consistency: JD mới xuất hiện sau khoảng 1–2s (`refresh_interval: 1s`); FE sau khi tạo JD nên mở trang chi tiết (đọc Postgres), không mở trang tìm kiếm.
- Tombstone tồn tại lâu dài (một document nhỏ mỗi JD đã xoá); chưa có việc dọn.
- Thứ tự theo key chỉ đảm bảo trong một replica relay (ADR 0005); external version bù lại cho read model.
- Reconcile chạy tay hoặc trong smoke, chưa có cron hằng đêm.
- Dev tắt security plugin của OpenSearch (chỉ trong mạng compose, publish `127.0.0.1:9200`); production cần TLS + user.
- Facet tính trên cùng bộ lọc, kể cả bộ lọc của chính facet đó (đơn giản, chưa làm "post_filter" kiểu e-commerce).
- Thêm một container OpenSearch (heap 512 MB) vào stack dev.
- `company_name` trong spec §8.2 chưa có vì Job domain chưa có thông tin công ty.

## Alternatives considered

- **So version thủ công + `processed_events`** (spec gốc): phải đọc document trước khi ghi (race giữa các replica) và vẫn cần tombstone; external version của OpenSearch làm việc này nguyên tử.
- **Xoá document khi đóng/xoá JD** (spec §8.2): gọn index nhưng cho phép event cũ hồi sinh JD.
- **Rebuild bằng một alias rồi replay Kafka từ offset lúc bắt đầu**: phụ thuộc retention của topic và phải dừng projector; alias ghi kép đơn giản và không cần dừng.
- **Projector chạy trong `job-server`**: ít container hơn nhưng không scale/tắt riêng được, mất ý nghĩa tách đọc/ghi.
- **Bump `job.*` lên v2 để thêm `published_at`**: đúng theo sách nhưng không có consumer nào khác cần bảo vệ; trường tuỳ chọn ở v1 tương thích ngược.
- **Postgres-only nâng cấp (pg_trgm, unaccent tốt hơn)**: rẻ hơn nhưng không có facet/đồng nghĩa và không thể hiện pattern CQRS mà milestone nhắm tới.
