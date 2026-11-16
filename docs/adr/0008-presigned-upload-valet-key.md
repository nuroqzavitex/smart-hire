# ADR 0008: Upload CV bằng presigned URL (valet key) + MinIO

- Trạng thái: Chấp nhận (M1b)
- Ngày: 2026-11-16

## Context

Ứng viên upload CV PDF (tối đa 5 MiB). Đi qua Gateway tốn băng thông và buộc phải nới giới hạn body; spec §6.2 chọn mẫu Valet Key: client gửi file thẳng lên object storage bằng URL ký sẵn. Cần thêm: xác nhận file hợp lệ, cấp cho mỗi service đúng quyền tối thiểu, và bỏ qua kết quả parse cũ khi ứng viên parse lại. Upload còn cần sự đồng ý xử lý dữ liệu cá nhân (NĐ 13/2023).

## Decision

**Luồng.** `POST /v1/cvs/upload-url` (`consent: true` bắt buộc, nếu không thì 400) tạo CV `UPLOADING` với `object_key = cvs/{candidate_id}/{cv_id}.pdf` và trả presigned PUT hạn 10 phút, **ký kèm `Content-Type: application/pdf`**. Client PUT thẳng lên MinIO. `POST /v1/cvs/{id}:complete` kiểm trên server: object tồn tại, `size ≤ 5 MiB`, `content-type = application/pdf`, 5 byte đầu là `%PDF-`. Sai size/content-type/magic thì **xoá object**, CV giữ `UPLOADING`, trả 400 (E5). Hợp lệ thì CV sang `UPLOADED`, sinh `attempt_id` (ULID) mới và ghi outbox `cv.uploaded` trong cùng transaction; gọi `complete` lần hai là idempotent. API không bao giờ trả `object_key` hay URL tải.

**Chỉ `NoSuchKey` nghĩa là "chưa upload".** Adapter lưu trữ chỉ ánh xạ `NoSuchKey` thành lỗi nghiệp vụ "uploaded file not found" (400). `AccessDenied` (và `NoSuchBucket`) là lỗi cấu hình/quyền thật nên thành lỗi nội bộ (Go: 500; Python: không retry trong adapter, consumer retry rồi DLQ) để sự cố hiện rõ. Kiểm chứng trên MinIO thật: user không có `s3:ListBucket` vẫn nhận `NoSuchKey` cho key không tồn tại (E-R9; thay giả định ban đầu "AccessDenied ≡ không tìm thấy").

**Lỗi lưu trữ không chứa key/URL** (E-R8): `minio-go` đưa cả URL vào `*url.Error`, nên adapter bọc thành thông điệp cố định, không bao giờ đưa object key hay presigned URL vào lỗi, log hay `dlq-error`.

**Hai endpoint, ký không cần mạng.** Presign dùng client MinIO riêng với endpoint public `CANDIDATE_S3_PUBLIC_ENDPOINT` (mặc định `http://127.0.0.1:9000`) và **region cố định** (`CANDIDATE_S3_REGION`, mặc định `us-east-1`), nên ký không phải gọi server. Stat/get/delete dùng endpoint nội bộ `minio:9000`. Vì lý do tương tự, AI service có `AI_S3_REGION` (mặc định `us-east-1`) truyền vào `Minio(region=...)`: nếu không, `minio-py` gọi `GetBucketLocation`, thứ user quyền tối thiểu `ai-svc` không có → `AccessDenied` → DLQ (E-R16; lỗi này lộ ra ở smoke Task 13 vì integration test trước đó dùng root).

**Quyền tối thiểu theo service** (E8). Bucket `cvs` private; `minio-init` (idempotent) tạo hai user: `candidate-svc` (`PutObject`/`GetObject`/`DeleteObject` trên `cvs/*`) và `ai-svc` (chỉ `GetObject` trên `cvs/*`). Root chỉ dùng trong `minio-init`.

**MinIO build từ mã nguồn (E-R2, quyết định của người dùng).** Image chính thức `minio/minio` và `minio/mc` đã bị gỡ khỏi Docker Hub, và không dùng image cộng đồng/bên thứ ba. `deploy/docker/minio/Dockerfile` clone `github.com/minio/minio` (`RELEASE.2025-04-22T22-12-26Z`) và `github.com/minio/mc` (`RELEASE.2025-04-16T18-13-26Z`) ở tag cố định, build bằng Go và đóng vào một image `smarthire/minio:RELEASE.2025-04-22T22-12-26Z` dùng cho cả `minio` lẫn `minio-init`. `make minio-image` build image này; `infra-up`, `test-int`, `ai-test-int`, `ai-cover` phụ thuộc vào nó; CI build trước khi chạy integration test; testcontainers (Go và Python) dùng chính tag này.

**`attempt_id` chống kết quả cũ** (E6). Mỗi lần complete/reparse có `attempt_id` mới đi vào `cv.uploaded`; AI copy sang `cv.parsed`/`cv.parse_failed`. Candidate chỉ áp dụng event có `attempt_id` trùng hiện tại; event khác bị bỏ qua (không lỗi, vẫn đánh dấu đã xử lý). Không cần khoá phân tán cho reparse.

**Reparse CV kẹt ở `UPLOADED`** (E-R18, lệch có chủ đích so với design M1b §3.1 và §3.3: chỉ reparse từ `PARSED`/`FAILED`). Nếu `cv.uploaded` hoặc event kết quả bị đẩy vào DLQ, CV ở `UPLOADED` mãi mãi. Vì vậy `Reparse` được phép từ `UPLOADED` khi lần thử hiện tại đã cũ ít nhất `15 phút` (`domain.StaleUploadAfter`, đo từ `updated_at` — thời điểm complete/reparse bắt đầu lần thử — theo đồng hồ của app). Ngưỡng lớn hơn `max.poll.interval.ms` của worker AI (900s) nên lần thử còn đang xử lý không bị khởi động lại; trước ngưỡng vẫn trả 400 như chuyển trạng thái sai. Reparse sinh `attempt_id` mới như bình thường, nên kết quả muộn của lần thử cũ bị bỏ qua.

## Consequences

Tích cực:
- Gateway không gánh băng thông file; client upload bằng một `PUT`.
- Dù client lách ràng buộc size của PUT, file sai bị phát hiện và xoá ở `complete` trước khi có event.
- Credential tách theo service: lộ `ai-svc` không cho ghi/xoá CV.
- Không phụ thuộc image MinIO không còn được phát hành.

Tiêu cực / nợ đã biết:
- Presigned PUT không giới hạn được kích thước, nên file lớn vẫn được ghi rồi mới bị xoá ở `complete`; M1b chưa có cơ chế dọn object `UPLOADING` bị bỏ dở.
- Build MinIO từ nguồn làm lần đầu chậm và `make up` lạnh cần truy cập `github.com`; ghim theo **tag** chứ không phải commit SHA, và base image (`golang:1.27-bookworm`, `debian:bookworm-slim`) trôi theo bản mới (chưa ghim digest).
- Image MinIO chạy bằng root trong container (dev).
- `minio-init` chạy lại không gỡ policy cũ đã gắn vào user.
- Secret truyền qua argv của `mc` trong `minio-init` (chỉ dev; cần cơ chế secret thật nếu đưa ra môi trường khác).
- Mặc định `CANDIDATE_S3_PUBLIC_ENDPOINT=http://127.0.0.1:9000` chạy tốt trên máy dev; môi trường khác phải đặt lại.
- Cho reparse từ `UPLOADED` sau 15 phút là lệch so với spec (chỉ `PARSED`/`FAILED`): một lần thử thật sự chậm hơn 15 phút (vd. consumer AI tụt hậu lâu) có thể bị ứng viên khởi động lại; kết quả của nó khi đó bị bỏ qua như kết quả cũ, chỉ tốn thêm một lần gọi LLM.
- URL PUT ký sẵn còn hiệu lực tới ~10 phút sau `complete`, nên object có thể bị thay giữa lúc Candidate kiểm và lúc AI đọc; vì vậy cả hai phía đều tự kiểm lại size/magic (AI đọc tối đa `5 MiB + 1` byte và coi phần vượt là `too_large`).

## Alternatives considered

- **Upload qua Gateway**: loại; tốn băng thông, buộc nới giới hạn body và giữ kết nối dài ở Gateway.
- **POST policy** (ràng buộc size/content-type ngay ở storage): loại; client phức tạp hơn (multipart form), và vẫn phải kiểm magic ở server.
- **Khoá phân tán cho reparse**: loại; `attempt_id` đủ để bỏ kết quả cũ mà không thêm hạ tầng.
- **Image MinIO cộng đồng/bên thứ ba**: loại theo quyết định người dùng (chuỗi cung ứng).
- **Coi `AccessDenied` như "không tìm thấy"**: loại (E-R9), vì che lỗi cấu hình quyền.
