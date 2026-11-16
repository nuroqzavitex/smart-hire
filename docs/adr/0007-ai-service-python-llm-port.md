# ADR 0007: AI service bằng Python + cổng `LlmParser`

- Trạng thái: Chấp nhận (M1b)
- Ngày: 2026-11-16

## Context

M1b cần một service đọc CV (PDF), trích văn bản và dùng LLM để chuyển thành JSON có cấu trúc (`ParsedCV`, spec.md §13.3). Hệ sinh thái PDF/LLM tốt nhất nằm ở Python (PyMuPDF, SDK `anthropic` có `messages.parse` + pydantic). Đồng thời cần: chạy được ở dev/CI/smoke mà không có API key, không đưa PII vào log/DLQ, chịu được prompt injection trong nội dung CV, và giữ nguyên tắc "sự kiện đi qua outbox" của spec §4.1 ngay cả khi service không viết bằng Go.

## Decision

**Ngôn ngữ và cấu trúc.** `services/ai` là Python 3.12 quản lý bằng `uv`, layout `src/ai_service` theo hexagonal (`domain` → `services` → `adapters`); mypy strict cho `domain` và `services`. Hai process từ cùng một image: `ai-worker` (consumer `cv.uploaded` + relay outbox) và `ai-api` (FastAPI chỉ có `/healthz`, `/readyz`, `/metrics`, cổng `8100`, nội bộ). Chưa có gRPC `AIService` vì chưa có RPC nào cần trước M2 (E3).

**Cổng LLM.** `LlmParser` (Protocol, `parse(cv_text) -> ParsedCV`) với hai adapter:

- `AnthropicLlmParser`: model `claude-opus-5-5`, structured output bằng pydantic, `effort=medium`, `max_tokens=16000`, client `timeout=60s`, `max_retries=2` (SDK tự retry 429/5xx). Gọi qua `client.beta.messages.with_raw_response.parse(...)` với `betas=["server-side-fallback-2026-07-01"]` và `fallbacks="default"`.
- `FakeLlmParser` (`AI_LLM_PROVIDER=fake`, mặc định ở compose dev, test và smoke): parser quy tắc, xác định; `full_name` lấy từ dòng đầu không rỗng, email bằng regex, skill bằng từ điển alias; `model_version = "fake:rules-v1"`. Đây cũng là "fallback rule/NER" của spec §5.7. `AI_LLM_PROVIDER=anthropic` mà không có credential thì worker thoát ngay lúc khởi động với thông báo rõ ràng. Không test nào gọi API thật; `scripts/try_anthropic.py` chỉ để thử tay.

**Server-side fallback: bật.** Task 10 kiểm tra SDK `anthropic` 1.17.0 đã cài: `client.beta.messages.parse` nhận `betas` và `fallbacks`, nên điều kiện "SDK hỗ trợ trên `parse`" thoả và fallback được bật (decision #4 của plan). Hệ quả: model thực sự trả lời có thể khác model cấu hình, nên `model_version` lấy từ trường `model` của response (cắt tối đa 100 ký tự, khớp schema `cv.parsed`), không cố định `"anthropic:claude-opus-5-5"`. Test dựng client `anthropic` thật trên `httpx2.MockTransport` để khoá hình dạng request (header `anthropic-beta`, `?beta=true`, `fallbacks`, `output_config`).

**Ánh xạ lỗi** (`cv.parse_failed` là kết quả terminal, không retry):

- Từ chối (refusal) được kiểm **trước** khi SDK validate: adapter đọc `stop_reason == "refusal"` trên response thô (`with_raw_response`) rồi mới gọi `raw.parse()`. Lý do: SDK validate JSON của mọi text block ngay trong `parse()`, nên một lời từ chối dạng văn xuôi sẽ ném `ValidationError` và bị gán nhầm là `llm_invalid_output` thay vì `llm_refused`. → `llm_refused` (kèm category).
- `parsed_output` là None, `ValidationError` của SDK hoặc của `sanitize`, body không phải JSON object, hoặc 4xx **khác** 401/403/404/429 (ví dụ 400/413/422) → `llm_invalid_output`.
- **401/403/404 là lỗi triển khai** (sai key, thiếu quyền, sai model), không phải lỗi của một CV cụ thể, nên **được ném lên** như lỗi bất ngờ: consumer retry 3 lần rồi vào DLQ để sự cố hiện rõ (E-R12; thay dòng "mọi 4xx khác 429 → `llm_invalid_output`" của design §5.4).
- 429, 5xx, lỗi kết nối/timeout (sau 2 lần retry của SDK) → ném lên, consumer retry 3 lần rồi DLQ.

**Chống prompt injection.** System prompt khai báo nội dung giữa `<cv_document>…</cv_document>` là dữ liệu chưa tin cậy, mọi yêu cầu trong đó bị bỏ qua. Text CV bị escape thẻ `<cv_document>` / `</cv_document>` (không phân biệt hoa thường, kể cả khoảng trắng bên trong thẻ) thành `&lt;…&gt;` trước khi bọc. Output bị ràng buộc bằng JSON schema, rồi được `sanitize` và validate lại bằng pydantic.

**`ParsedCVDraft` + `sanitize`.** LLM chỉ sinh `ParsedCVDraft` (không ràng buộc độ dài/khoảng giá trị, để schema gửi đi đơn giản); `sanitize` cắt chuỗi quá dài, bỏ phần tử thừa, đưa số ngoài khoảng và ngày sai định dạng về `null`, chuẩn hoá skill. Kết quả mới là `ParsedCV` thật, nên một `cv.parsed` đã phát luôn qua được schema.

**Outbox + relay Python (E4).** Kết quả (`cv_parse_results`), `processed_events` và bản ghi outbox ghi trong cùng một transaction. Relay chạy thread trong process worker, cùng thuật toán `pkg/outbox` (ADR 0005): poll `FOR UPDATE SKIP LOCKED`, batch 100, chu kỳ 500ms, publish đồng bộ (`acks=all`, idempotent), dừng ở lỗi đầu tiên và chỉ đánh dấu phần đã gửi, khôi phục carrier trace từ cột `headers`. Consumer Python theo ngữ nghĩa ADR 0006 (retry 3 lần, backoff 200ms×2ⁿ trần 2s, DLQ với header `dlq-*`, ghi DLQ lỗi thì dừng, shutdown không đẩy DLQ).

**Dữ liệu và PII.** Không lưu `raw_text` (E7, khác spec §8.5) để giảm PII. Văn bản lỗi đưa vào `cv.parse_failed.detail`, `dlq-error` và log là chuỗi cố định, không chứa nội dung CV; `dlq-error` bị cắt 512 ký tự (`APIStatusError` mang cả body của Anthropic). `cv.uploaded` đến được validate theo `contracts/events/cv.uploaded.v1.json` và `object_key == cvs/{candidate_id}/{cv_id}.pdf`; sai thì là poison (DLQ, không retry).

**Hợp đồng chung.** JSON Schema trong `contracts/events` là nguồn sự thật cho cả Go lẫn Python; hai phía test chéo bằng cùng bộ fixture (`contracts/events/testdata`), và một test Python so schema sinh từ pydantic với file schema để chống lệch.

## Consequences

Tích cực:
- Dùng đúng công cụ cho PDF/LLM mà vẫn giữ pattern outbox/idempotent consumer/DLQ xuyên ngôn ngữ; trace đi liền một mạch Go → Kafka → Python → Kafka → Go.
- Demo và CI không cần API key hay mạng; bật Claude thật chỉ bằng `AI_LLM_PROVIDER=anthropic` + `ANTHROPIC_API_KEY`.
- Sự cố triển khai (sai key/model) vào DLQ, không bị che thành hàng loạt CV "lỗi".

Tiêu cực / nợ đã biết:
- Thêm một runtime (Python/uv) vào CI và Docker, cùng một bản relay phải giữ đồng bộ thủ công với bản Go.
- `FakeLlmParser` khớp cả các từ tiếng Anh thông dụng (REST, React, Node, Java) nên có thể trích nhầm skill trên văn bản thường; chỉ dùng cho dev/CI.
- `timeout=60s` cho lời gọi không streaming có thể không đủ khi đầu ra gần `max_tokens=16000`; khi đó lỗi timeout đi theo đường retry rồi DLQ. Cần xem lại (streaming hoặc timeout theo kích thước) trước khi chạy model thật ở quy mô.
- Trích văn bản PDF chưa giới hạn số trang/thời gian (chỉ giới hạn 5 MiB đầu vào và 60 000 ký tự gửi LLM).
- Worker xử lý đơn luồng vì `AnthropicLlmParser` giữ trạng thái theo từng lời gọi (`model_version`); muốn song song phải tách trạng thái đó ra trước.
- `fallbacks="default"` gắn với beta header `server-side-fallback-2026-07-01`; nếu SDK bỏ beta này thì phải sửa adapter.

## Alternatives considered

- **Candidate gọi LLM đồng bộ**: loại; nặng trong request, không có retry/DLQ, PII đi vào luồng API.
- **Go thuần**: loại; thiếu thư viện PDF tương đương PyMuPDF và SDK structured output chín muồi.
- **Tool use thay structured output**: loại; `parse` + pydantic cho schema ràng buộc và validate sẵn, ít code hơn.
- **Bỏ qua beta server-side fallback**: loại vì SDK hỗ trợ trên `parse`; đổi lại chấp nhận `model_version` biến động.
- **Gán mọi 4xx (kể cả 401/403/404) thành `llm_invalid_output`**: loại (E-R12), vì che lỗi cấu hình thành lỗi từng CV.
