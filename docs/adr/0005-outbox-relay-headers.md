# ADR 0005: Outbox relay trong process + cột `headers`

- Trạng thái: Chấp nhận (M1a)
- Ngày: 2026-11-10

## Context

Từ M0, Auth ghi sự kiện vào bảng `outbox` cùng transaction với dữ liệu nghiệp vụ. M1a cần đẩy các dòng đó sang Kafka (`user.registered`, `job.created|updated|closed|deleted`) mà không mất sự kiện khi Kafka hoặc process chết giữa chừng, và để trace của request ban đầu (Gateway → service) đi tiếp qua Kafka sang consumer.

## Decision

**Relay là một goroutine trong `cmd/server` của từng service (`pkg/outbox.Relay`), poll bảng `outbox` bằng `SELECT ... FOR UPDATE SKIP LOCKED`.**

- Mỗi vòng: mở transaction, lấy tối đa `BatchSize` (mặc định 100) dòng `published_at IS NULL` theo `ORDER BY created_at, id`, gọi `Produce` (đồng bộ) lần lượt từng dòng, rồi `UPDATE outbox SET published_at = now()` cho các dòng đã gửi và commit. Nếu một dòng lỗi, các dòng đã gửi trước đó vẫn được đánh dấu, vòng sau thử lại dòng lỗi. Chu kỳ poll `Interval` mặc định `500ms` (Job: `JOB_OUTBOX_INTERVAL`).
- Ánh xạ: topic = `event_type`, key = `aggregate_id`, value = `payload` (envelope JSON đầy đủ của `pkg/event`), headers = cột `headers`.
- **At-least-once**: crash giữa lúc gửi và lúc commit đánh dấu thì dòng được gửi lại. Consumer phải idempotent (`pkg/idempotency`, khoá theo `event_id` ULID trong envelope).
- `SKIP LOCKED` cho phép chạy nhiều replica song song: replica khác bỏ qua các dòng đang bị khoá.
- **Cột `headers jsonb NOT NULL DEFAULT '{}'`** (Auth: migration `000002`, append-only; Job: có sẵn trong `000001`). `outbox.Writer` inject carrier W3C (`traceparent`, `tracestate`) của `ctx` hiện tại vào cột này khi ghi. Relay **khôi phục** carrier đã lưu (bỏ `traceparent`/`tracestate` khỏi header gửi đi) thành parent context bằng `Extract`; span publish nối vào trace của request gốc và producer inject `traceparent` mới vào header Kafka; consumer extract tiếp. Trace liên tục: Gateway → service → outbox → Kafka → consumer.

### Đánh đổi chấp nhận

- **Row lock giữ suốt các lần `ProduceSync` tuần tự.** Transaction và khoá dòng mở trong cả batch; một record kẹt có thể giữ khoá tới 10s (`RecordDeliveryTimeout`) trước khi lỗi trả về.
- **Poison record chặn outbox (head-of-line).** Dòng không gửi được khiến các dòng sau nó không được gửi, vì relay dừng ở lỗi đầu tiên. Đây là cái giá của việc giữ thứ tự theo key. Chưa có cơ chế quarantine ở M1a.
- **Thứ tự chỉ đảm bảo theo key và trong một replica.** Với nhiều replica relay, hai batch của cùng một aggregate có thể do hai replica gửi và đảo thứ tự. Consumer phải đối chiếu theo `version` trong payload thay vì tin thứ tự đến. Ngoài ra `created_at` lấy từ đồng hồ của app (không phải của Postgres): giữa các replica lệch giờ, các dòng chưa gửi cùng key có thể đi ra sai thứ tự, và consumer cũng xử lý bằng cách đối chiếu `version`.
- **Latency phụ thuộc `Interval`** (khoảng 500ms cộng thời gian gửi), chấp nhận được cho nghiệp vụ tuyển dụng.
- Bảng `outbox` chưa có job dọn các dòng đã `published_at`.

### Khi nào tách `cmd/relay`

Chưa cần ở M1a: một relay trong process đủ cho Compose với một replica. Tách thành binary riêng (cùng `pkg/outbox.Relay`) khi cần scale relay độc lập với API, hoặc khi relay ảnh hưởng tải của request path. `SKIP LOCKED` đã sẵn sàng cho việc đó.

## Consequences

Tích cực:
- Không mất sự kiện: ghi outbox nguyên tử với dữ liệu nghiệp vụ; chỉ đánh dấu sau khi Kafka ack (`acks=all`, idempotent producer, xem ADR 0006).
- Không thêm hạ tầng ngoài Postgres + Kafka; có integration test (testcontainers) cho hai relay đồng thời.
- Trace liên tục xuyên Kafka nhờ carrier lưu cùng dòng outbox.

Tiêu cực:
- Poll tốn một truy vấn mỗi `Interval` kể cả khi rỗng.
- Các đánh đổi ở trên (khoá giữ lâu, head-of-line, thứ tự theo replica).

## Alternatives considered

- **CDC (Debezium) đọc WAL**: loại. Thêm Kafka Connect + Debezium để vận hành, nặng so với quy mô portfolio và che mất pattern Outbox mà dự án muốn thể hiện.
- **Publish thẳng sau commit (dual write)**: loại. Crash giữa commit và publish làm mất sự kiện.
- **Relay là binary riêng ngay từ đầu**: hoãn (xem trên).
- **`LISTEN/NOTIFY` thay poll**: hoãn; vẫn cần poll để bắt dòng bị lỡ, và chưa cần latency thấp hơn.
