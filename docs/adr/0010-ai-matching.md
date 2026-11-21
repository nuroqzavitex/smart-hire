# ADR 0010: Matching CV ↔ JD — embedding Gemini, Qdrant, điểm tính sẵn

- Trạng thái: Chấp nhận (M2b)
- Ngày: 2026-11-21

## Context

M2b thêm phần matching của spec (§5.7, §6.3): nhà tuyển dụng xem ứng viên hợp với một JD của mình, ứng viên xem JD gợi ý cho một CV của mình, và `ScoreMatch(cv, job)` sẵn cho saga nộp đơn ở M3. Điểm 0–100 theo công thức §5.7: `100 × (0.55·semantic + 0.30·skill_coverage + 0.10·experience_fit + 0.05·location_fit)`, thiếu quá nửa kỹ năng bắt buộc thì tối đa 40. Phần khó không nằm ở công thức mà ở chỗ giữ cho dữ liệu ghép luôn đúng khi JD và CV thay đổi liên tục, và ở chỗ AI service (Python) hoặc nhà cung cấp embedding sập không được kéo các service Go sập theo.

## Decision

**Embedding bằng Gemini (`gemini-embedding-001`, 768 chiều) sau một port `Embedder`.** Spec chọn `BAAI/bge-m3` chạy tại chỗ, nhưng mô hình 2,3 GB cùng PyTorch quá nặng cho máy dev 8 GB RAM. User chọn free tier của Gemini (dự án học, dữ liệu giả). Có hai adapter: `GeminiEmbedder` (task type `RETRIEVAL_DOCUMENT` cho cả hai phía, chuẩn hoá L2 ở client) và `FakeEmbedder` (bag-of-words băm vào 768 ô, bỏ dấu tiếng Việt). Mặc định dùng fake, nên test, CI và smoke không bao giờ gọi Gemini. Văn bản đem embed bỏ họ tên, email, tên trường, tên công ty và địa điểm (địa điểm được chấm riêng). Những trường này không giúp ghép mà còn đưa thiên vị vào điểm (spec R3).

**Qdrant đặt tên collection theo model** (`jobs_<model>` / `cvs_<model>`). Đổi model sẽ ghi sang collection mới thay vì trộn vector của hai model (spec R5). `model_version` được lưu kèm mỗi dòng điểm. Point id là `job_id` / `cv_id`, nên mọi lần ghi là idempotent.

**`cv.updated` thay cho `cv.parsed` làm nguồn dữ liệu CV.** Candidate service mới là nơi giữ bản CV đúng: nó bỏ qua kết quả của attempt cũ, cho ứng viên sửa dữ liệu đã parse, và cho parse lại. Vì vậy Candidate phát `cv.updated` (trạng thái + bản parse hiện tại + `version`) trong cùng transaction với mọi thay đổi: áp `cv.parsed`, áp `cv.parse_failed`, sửa, parse lại. Matcher bỏ qua bản có `version` không mới hơn bản đã lưu.

**Process riêng `ai-matcher`** (cùng image, consumer group `ai-matcher` trên `job.*` và `cv.updated`), giữ bản sao `match_jobs` / `match_cvs`, vector trong Qdrant và bảng `match_results`. Mỗi sự kiện được áp trong một transaction Postgres gồm `processed_events`, bản sao, điểm và outbox (`match.completed`). Embedding được tính trước transaction. Lệnh ghi Qdrant chạy bên trong transaction, trước khi commit: nếu Qdrant hoặc lệnh commit lỗi thì cả sự kiện được làm lại, và vì ghi Qdrant là idempotent nên làm lại không gây hại. Tách khỏi `ai-worker` để Gemini giới hạn request không chặn việc parse CV.

**Lỗi hạ tầng chặn partition, không đẩy vào DLQ** (như projector ở ADR 0009). `RetryingConsumer` có thêm một lớp lỗi "transient": Gemini 429/5xx/timeout, Qdrant hoặc Postgres không tới được. Các lỗi này thử lại với backoff 0,2s → 10s cho tới khi process dừng và không bị tính vào 3 lần thử. Chỉ payload sai, nguồn sai hoặc version lạ mới vào DLQ.

**Tính sẵn cho recruiter, tính lúc gọi cho ứng viên.** Khi một JD đang mở được tạo hoặc sửa: lấy 50 CV *active* gần nhất (mỗi ứng viên một CV, là CV parse gần nhất), chấm điểm, thay toàn bộ điểm của JD, rồi phát `match.completed` (tối đa 20 ứng viên). Khi một CV được parse: lấy 50 JD đang mở gần nhất rồi upsert điểm. `FindCandidates` đọc bảng điểm, chỉ lấy CV active. `RecommendJobs` và `ScoreMatch` chấm ngay từ vector và bản sao đã lưu, không gọi Gemini. Điểm của một CV đã parse nhưng không còn active vẫn được giữ (bị ẩn), để khi ứng viên parse lại CV mới hơn thì CV cũ lập tức có thứ hạng trở lại.

**AI gRPC chỉ chạy nội bộ, không xác thực JWT; các service Go đứng trước.** `ai-api` mở `smarthire.ai.v1.AIService` trên cổng 9100, chỉ trong mạng compose. Job kiểm tra chủ JD (recruiter khác nhận 404 để không dò được id) rồi gọi `FindCandidates`. Candidate kiểm tra chủ CV và trạng thái `PARSED`, gọi `RecommendJobs`, rồi gọi `Job.BatchGetJobs` (forward JWT của người gọi) để lấy chi tiết JD và bỏ những JD đã đóng. Quyền vẫn nằm ở service sở hữu dữ liệu và `pkg/grpcx` không phải viết lại bằng Python. M3 dùng lại đúng đường Go → AI này cho `ScoreMatch`.

**Timeout + circuit breaker ở phía Go.** Mỗi lời gọi AI có timeout 2s và đi qua `sony/gobreaker`: 5 lỗi liên tiếp thì mở, sau 30s cho một yêu cầu thử. Caller tự huỷ, `NotFound` và `InvalidArgument` không bị tính là lỗi. Khi breaker mở hoặc AI lỗi, endpoint trả `apperr.ErrUnavailable` (sentinel mới, sang gRPC `Unavailable`, HTTP 503). Các API khác của Job/Candidate không bị ảnh hưởng.

## Consequences

- Tích cực: dữ liệu ghép luôn theo đúng bản CV/JD mới nhất mà không cần job batch. AI hoặc Gemini sập chỉ làm hai endpoint matching trả 503 và làm sự kiện chờ, không mất sự kiện. Trace đi liền từ gateway qua job/candidate tới ai-api, và từ Kafka (`cv.updated`, `job.*`) tới ai-matcher.
- Tiêu cực: điểm phụ thuộc vào thứ tự đến của sự kiện. CV parse sau JD chỉ được ghép với 50 JD gần nhất, và ngược lại. CV đã parse trước M2b không có `cv.updated`, nên phải sửa hoặc parse lại thì mới vào matching. Đổi provider embedding cần phát lại sự kiện (sửa/parse lại CV, sửa JD) vì collection mới bắt đầu trống. Free tier của Gemini cho phép Google dùng dữ liệu gửi lên, chấp nhận được vì dự án dùng dữ liệu giả.
- AI gRPC không có xác thực, chỉ an toàn khi cổng 9100 không bị publish ra ngoài mạng compose.

## Alternatives considered

- **`bge-m3` tại chỗ (PyTorch hoặc ONNX/fastembed):** đúng spec và không phụ thuộc mạng, nhưng nặng 2–4 GB cho image và RAM. Vẫn có thể thêm sau như một adapter khác của `Embedder`.
- **Gateway gọi thẳng AI (AI tự verify JWT):** ít chặng hơn, nhưng phải viết lại phần xác thực bằng Python, và quyết định quyền lại dựa trên bản sao có thể trễ.
- **Nghe `cv.parsed`:** không thấy được các lần sửa và có thể embed kết quả của một attempt đã bị bỏ.
- **Tính hết lúc gọi (không có `match_results`):** đơn giản hơn, nhưng không có `match.completed` cho Notification (M4), và mỗi lần mở trang lại phải tìm và chấm điểm lại.
