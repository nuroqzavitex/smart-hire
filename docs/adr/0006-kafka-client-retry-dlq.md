# ADR 0006: Kafka client (franz-go) + retry in-process + DLQ

- Trạng thái: Chấp nhận (M1a)
- Ngày: 2026-11-10

## Context

M1a cần một lớp Kafka dùng chung (`pkg/kafka`) cho producer của outbox relay và consumer của các service sau. Cần: giữ thứ tự theo key, xử lý lỗi tạm thời, cô lập message hỏng (poison) mà không chặn partition vĩnh viễn, và chạy nhẹ trên máy dev.

## Decision

**Broker local: Redpanda** (một container, tương thích Kafka API, kèm Redpanda Console ở `127.0.0.1:8081`). Listener nội bộ `redpanda:9092`; listener dev `127.0.0.1:19092` chỉ cho smoke/dev trên host. Topic tạo trước bằng `topics-init` (3 partition, replication 1), tắt auto-create; mỗi topic có `<topic>.dlq`.

**Client: franz-go (`github.com/twmb/franz-go`)**, thuần Go (không cgo), cho phép kiểm soát commit và rebalance.

### Producer
- Idempotent producer (franz-go default, không bị tắt), `acks=all`, `RecordDeliveryTimeout` 10s, gửi đồng bộ để relay biết chắc đã ack trước khi đánh dấu `published_at`.
- Luôn mở span publish và đưa `traceparent` vào header record (xem ADR 0005).

### Consumer
- Consumer group, `DisableAutoCommit`: chỉ commit sau khi record xử lý xong hoặc đã vào DLQ.
- **Xử lý tuần tự** để giữ thứ tự theo key: cả batch poll (có thể gồm record của nhiều partition) được xử lý lần lượt trên **một goroutine**. Vì vậy backoff in-process của một record làm trễ mọi partition mà member đang giữ, không chỉ partition của record đó. Worker riêng cho từng partition (franz-go `EachPartition` + goroutine/partition) là hướng mở rộng sau này.
- `BlockRebalanceOnPoll` kết hợp poll batch nhỏ (`pollBatch = 10`): rebalance chỉ xảy ra giữa các batch, nên không có hai member cùng xử lý một partition. Ràng buộc: `pollBatch × (MaxAttempts × (thời gian handler + MaxBackoff))` phải nhỏ hơn nhiều so với rebalance timeout của group (mặc định 60s), nếu không member bị đá khỏi group.
- **Retry trong process**: tối đa `MaxAttempts = 3` lần, backoff `200ms × 2^n` (n từ 0), trần `2s`. Lỗi bọc bằng `kafka.Permanent(err)` bỏ qua retry và đi thẳng DLQ. Envelope không giải mã được hoặc thiếu `event_id`/`event_type` cũng vào DLQ ngay.
- **DLQ**: sau khi hết lần thử, ghi record gốc (key, value, header gốc) vào `<topic>.dlq` kèm header `dlq-error` (cắt 512 ký tự), `dlq-attempts`, `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset`, rồi commit offset gốc.
- **Ghi DLQ lỗi thì `Run` trả lỗi và consumer dừng**: không commit, không bỏ qua message, tránh mất message âm thầm.
- **Shutdown không bao giờ gửi record vào DLQ.** Khi `ctx` bị huỷ, handler lỗi hoặc đang backoff thì record để chưa commit và sẽ được giao lại (kể cả ở lần thử cuối); lỗi ghi DLQ do shutdown cũng không bị coi là lỗi.
- **Lỗi của handler không được chứa PII** (email, mật khẩu, token, nội dung CV): `err.Error()` được log và ghi vào `dlq-error`. Handler bọc lỗi bằng ID (`user_id`, `job_id`, `event_id`), không đưa dữ liệu người dùng vào message.
- Consumer có side effect DB dùng `pkg/idempotency` (bảng `processed_events`, khoá theo `event_id`) vì giao hàng là at-least-once.

## Consequences

Tích cực:
- Partition không bị chặn quá vài giây bởi lỗi tạm thời; message hỏng được cô lập, còn nguyên nội dung và metadata để điều tra/replay.
- Giữ thứ tự theo key trong một consumer; không mất message khi shutdown hoặc rebalance.
- Chạy được trên máy dev với một container Redpanda.

Tiêu cực:
- Retry in-process làm chậm mọi partition của member trong lúc backoff (tối đa vài giây mỗi record), vì cả batch chạy trên một goroutine.
- Lỗi ghi DLQ dừng consumer (ưu tiên đúng đắn hơn sẵn sàng).
- M1a chưa có consumer nghiệp vụ nào; `pkg/kafka` được kiểm bằng integration test (testcontainers) và smoke.

## Alternatives considered

- **Topic retry trễ (`<topic>.retry.5s`, ...)**: hoãn. Cần thêm topic và cơ chế trì hoãn, và làm vỡ thứ tự theo key (record retry đến sau record mới hơn). Lỗi tạm thời ngắn đã được retry in-process; lỗi kéo dài vào DLQ rồi replay thủ công. Xem lại khi có consumer gọi dịch vụ ngoài chậm.
- **segmentio/kafka-go, sarama, confluent-kafka-go**: loại; confluent cần cgo (máy dev Windows không có), các lib còn lại kém ergonomics cho idempotent producer và kiểm soát rebalance.
- **Apache Kafka (KRaft) cho local**: nặng hơn Redpanda, không thêm giá trị cho demo.
- **Auto-commit**: loại, có thể mất message khi crash giữa commit và xử lý.
