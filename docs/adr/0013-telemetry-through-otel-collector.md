# ADR 0013: Metrics và log đi qua OTel collector tới Prometheus và Loki

- Trạng thái: Chấp nhận (M4a)
- Ngày: 2026-11-24

## Context

Từ M0 mọi service đã đẩy trace bằng OTLP qua OTel collector sang Tempo; log chỉ là JSON ra stdout (có `trace_id`). M4a cần metrics (RED + nghiệp vụ, spec §14.5), log tập trung, dashboard và alert cho cả service Go lẫn Python.

## Decision

**Một đường ống OTLP cho cả ba tín hiệu** (user chọn, thay cho Prometheus client `/metrics` mỗi service + Grafana Alloy đọc log Docker).

- `pkg/otel.Setup` cài thêm `MeterProvider` (OTLP gRPC, đẩy mỗi 10s) và `LoggerProvider` (OTLP gRPC, batch) khi có `OTEL_EXPORTER_OTLP_ENDPOINT`; không có biến này thì mọi thứ là no-op như trước.
- `pkg/logger.New` ghi JSON ra stdout **và** qua cầu nối `otelslog` tới logger provider toàn cục (provider cài sau vẫn nhận được); cùng level, cùng thuộc tính `trace_id/span_id/request_id/user_id/service`.
- Python: `setup_otel` cài meter + logger provider và gắn `LoggingHandler` vào root logger; instrument nghiệp vụ nằm trong `ai_service/metrics.py` và được inject vào component (provider toàn cục chỉ set được một lần mỗi process, nên test truyền reader trong bộ nhớ).
- Collector (`otel/opentelemetry-collector-contrib`, cần exporter `prometheus`): traces → Tempo; metrics → exporter Prometheus `:8889` với `resource_to_telemetry_conversion` (nhãn `service_name`), Prometheus scrape mỗi 10s; logs → `otlphttp` tới Loki 3 (OTLP native, `trace_id` thành structured metadata). Grafana có datasource Tempo ↔ Loki ↔ Prometheus liên kết theo `trace_id`.

**Metrics** (tên OTel, collector đổi `.` → `_` và thêm hậu tố đơn vị/`_total`): `rpc.server.call.duration` và `http.server.request.duration` tự có từ `otelgrpc`/`otelhttp`; dùng chung trong `pkg`: `outbox.pending` (observable, đếm `published_at IS NULL`), `kafka.consumer.lag{group,topic}` (observable, `kadm.Lag`), `dlq.messages{topic}`, `circuit_breaker.state{breaker}` (0/1/2, ghi từ hook đổi trạng thái của mỗi breaker và lúc khởi tạo); riêng từng service: `saga.instances{state}`, `search.fallback`, `projection.lag`, `notifications.created{type}`, `emails.sent|failed`, `ai.parse.duration`, `ai.match.score`, `ai.llm.tokens{direction}`. Không có nhãn theo user.

**Alert** (Prometheus rules, chưa có Alertmanager — xem trên Prometheus/Grafana): consumer lag > 1000 trong 5 phút; có bản ghi vào DLQ; lỗi phía server của gRPC > 2% trong 5 phút (chỉ `INTERNAL|UNAVAILABLE|DEADLINE_EXCEEDED|UNKNOWN|DATA_LOSS`, lỗi của client như sai quyền không tính); breaker mở > 2 phút; outbox pending > 500; projection lag p95 > 30s; email bị bỏ cuộc. `deletion_requests` thuộc M4b. Dashboard "SmartHire — overview" provision sẵn; `make obs-check` kiểm tra metric, log và rule trên stack đang chạy.

## Consequences

- Tên metric do collector chuyển đổi (vd `rpc_server_call_duration_seconds_count`, nhãn `rpc_response_status_code`) và đổi theo phiên bản semconv của thư viện instrumentation; query trong alert/dashboard phải theo tên thật, `obs-check` bắt khi chúng biến mất.
- Collector là điểm nghẽn duy nhất của telemetry; collector sập thì service vẫn chạy (exporter bỏ dữ liệu), chỉ mất quan sát.
- Log vừa ở stdout vừa ở Loki (stdout vẫn dùng cho `docker compose logs`).
- `LoggingHandler` của Python SDK bị đánh dấu deprecated (nên dùng `opentelemetry-instrumentation-logging`); giữ vì chạy đúng và không thêm phụ thuộc.
