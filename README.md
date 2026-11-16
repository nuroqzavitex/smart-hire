# SmartHire

SmartHire là nền tảng tuyển dụng (phân tích CV, so khớp CV với JD, theo dõi đơn ứng tuyển) xây dựng như một **dự án portfolio Backend Go**: monorepo gồm nhiều microservice Go (cùng một service AI bằng Python từ M1b), giao tiếp đồng bộ qua gRPC và bất đồng bộ qua Kafka. Trọng tâm là các pattern backend (Transactional Outbox, idempotent consumer, Saga, circuit breaker, CQRS, tracing xuyên service), không phải độ rộng tính năng. Đặc tả đầy đủ nằm ở [`spec.md`](spec.md); mỗi milestone có design và plan riêng trong [`docs/superpowers/`](docs/superpowers/).

## Kiến trúc mục tiêu

Sơ đồ dưới là kiến trúc đích (spec.md §4). Phần **đã có (M0 + M1a + M1b)** được tô xanh: API Gateway, Auth Service, Job Service, Candidate Service, AI Service (Python, chỉ parse CV), Postgres `auth_db`, `job_db`, `candidate_db` và `ai_db`, Redis, Kafka (Redpanda), MinIO. Các thành phần còn lại (Application, Notification, OpenSearch, Qdrant, Web App) thuộc các milestone sau.

```mermaid
flowchart LR
    User[Browser] -->|HTTPS| WEB[Web App<br/>Next.js SSR/RSC]
    User -->|REST + WS<br/>từ client components| GW
    WEB -->|REST server-side| GW[API Gateway<br/>Go]
    User -->|PUT CV presigned| S3

    GW -->|gRPC| AUTH[Auth Service<br/>Go]
    GW -->|gRPC| JOB[Job Service<br/>Go]
    GW -->|gRPC| CAND[Candidate Service<br/>Go]
    GW -->|gRPC| APP[Application Service<br/>Go + Saga Orchestrator]
    GW -->|WS proxy| NOTI[Notification Service<br/>Go]

    APP -->|gRPC| AI[AI Service<br/>Python]
    JOB -->|gRPC| AI

    AUTH --- PGA[(Postgres auth)]
    JOB --- PGJ[(Postgres job<br/>write model)]
    JOB --- OS[(OpenSearch<br/>read model)]
    CAND --- PGC[(Postgres candidate)]
    CAND --- S3[(MinIO / S3)]
    APP --- PGP[(Postgres application)]
    AI --- QD[(Qdrant)]
    AI --- PGAI[(Postgres ai)]
    GW --- RD[(Redis)]

    JOB -. events .-> K{{Kafka}}
    CAND -. events .-> K
    APP -. events .-> K
    AI -. events .-> K
    K -. events .-> NOTI
    K -. events .-> AI
    K -. events .-> APP
    K -. job.* .-> JOB
    K -. user.deleted .-> CAND
    K -. user.deleted .-> AUTH

    classDef m0 fill:#d3f9d8,stroke:#2f9e44,color:#000;
    class GW,AUTH,PGA,RD,JOB,PGJ,K,CAND,AI,S3,PGC,PGAI m0;
```

Từ M1a, Auth và Job **ghi** sự kiện (`user.registered`, `job.created|updated|closed|deleted`) vào bảng `outbox` trong cùng transaction với dữ liệu nghiệp vụ; một relay chạy trong process của mỗi service (poll `FOR UPDATE SKIP LOCKED`, at-least-once) đẩy chúng sang Kafka (Redpanda), kèm carrier W3C trong cột `headers` để trace đi tiếp qua Kafka. Consumer retry trong process rồi chuyển vào `<topic>.dlq`. Xem [ADR 0005](docs/adr/0005-outbox-relay-headers.md) và [ADR 0006](docs/adr/0006-kafka-client-retry-dlq.md).

Từ M1b, ứng viên upload CV (PDF) thẳng lên MinIO bằng presigned URL ([ADR 0008](docs/adr/0008-presigned-upload-valet-key.md)); Candidate ghi `cv.uploaded` vào outbox, AI service (Python, [ADR 0007](docs/adr/0007-ai-service-python-llm-port.md)) tải PDF, trích văn bản, parse bằng LLM rồi phát `cv.parsed` / `cv.parse_failed` qua outbox riêng; Candidate consume kết quả (idempotent, bỏ qua kết quả của `attempt_id` cũ). Trace đi liền Go → Kafka → Python → Kafka → Go. Mặc định LLM là parser quy tắc giả lập (không cần API key); xem mục Quick start để bật Claude thật.

## Quick start

Yêu cầu: Go (bản stable mới nhất), `make`, `curl`, Docker với Compose v2. Trên Windows chạy từ Git Bash (Makefile viết cho POSIX shell).

```bash
make bootstrap            # cài buf, golangci-lint, migrate; sinh khoá JWT dev; tạo .env từ .env.example
make infra-up obs-up up   # Postgres + Redis + Redpanda (+ Console, tạo topic) + MinIO (build từ mã nguồn, tạo bucket/user); OTel Collector + Tempo + Grafana; auth + job + candidate + ai + gateway
make seed                 # tạo admin và user mẫu (dịch vụ auth-seed)
make smoke                # luồng auth (register → login → /v1/me → refresh → reuse → logout) rồi luồng recruiter tạo/đăng/đóng job, rồi luồng CV (profile → upload-url → PUT PDF → complete → `PARSED` → sửa dữ liệu parse → từ chối file không phải PDF), kiểm tra sự kiện trên Kafka
make down                 # dừng stack
```

`make bootstrap` cài tool vào `$(go env GOPATH)/bin`; thư mục này phải nằm trong `PATH`. Khoá JWT sinh ở `deploy/docker/secrets/jwt.pem` và `.env` đều bị gitignore; giá trị trong `.env.example` chỉ dùng cho máy local.

Sau `make seed` có ba tài khoản: admin (`SEED_ADMIN_EMAIL` / `SEED_ADMIN_PASSWORD` trong `.env`, mặc định `admin@smarthire.local` / `Admin-Password-123!`), `candidate@smarthire.local` và `recruiter@smarthire.local` (mật khẩu dev `Password123!`). Admin không đăng ký được qua API.

**Volume dev cũ:** DB `job_db` và role `job_svc` chỉ được tạo khi Postgres khởi tạo volume lần đầu. Nếu đã chạy stack từ trước M1a, thêm `JOB_DB_PASSWORD` vào `.env` (xem `.env.example`) rồi chạy `make down-v` (xoá volume của project Compose, mất dữ liệu dev) trước khi `make infra-up obs-up up`.

**Nâng từ M1a:** M1b thêm script khởi tạo Postgres (`candidate_db`/`candidate_svc`, `ai_db`/`ai_svc`) chỉ chạy khi volume được tạo lần đầu. Thêm các biến mới trong `.env.example` (`CANDIDATE_DB_PASSWORD`, `AI_DB_PASSWORD`, `MINIO_*`, `CANDIDATE_S3_*`, `AI_S3_*`) vào `.env`, rồi chạy `make down-v` (mất dữ liệu dev) trước `make infra-up obs-up up`.

**MinIO:** image `minio/minio` và `minio/mc` chính thức đã bị gỡ khỏi Docker Hub, nên MinIO và `mc` được build từ mã nguồn GitHub chính thức ở tag cố định (`make minio-image`, tự chạy trong `make infra-up`; lần đầu cần truy cập `github.com` và mất vài phút). Xem [ADR 0008](docs/adr/0008-presigned-upload-valet-key.md).

**Bật Claude thật cho AI parse:** mặc định `AI_LLM_PROVIDER=fake` (parser quy tắc, xác định). Để dùng Claude, đặt `AI_LLM_PROVIDER=anthropic` và `ANTHROPIC_API_KEY=...` trong `.env` rồi `make up`; thiếu credential thì worker thoát khi khởi động. Không bao giờ commit `.env`.

**Mã trả về của OpenAPI:** `POST /v1/jobs` (CreateJob) và `POST /v1/auth/register` thực tế trả **201**, trong khi `contracts/openapi/gateway.yaml` (sinh từ proto) ghi 200. `openapimerge` chỉ hợp nhất path/method và báo lỗi khi trùng, không ghi đè được response của operation sinh ra, nên sai lệch này được ghi nhận ở đây thay vì vá trong `gateway.handwritten.yaml`.

Lệnh khác: `make test` (unit, không cần Docker), `make test-int` (integration, cần Docker), `make cover-auth` / `make cover-job` / `make cover-candidate` (coverage domain + app, ngưỡng 70%), `make ai-sync ai-lint ai-test ai-test-int ai-cover` (service Python AI, cần Docker cho `ai-test-int` và `ai-cover`), `make lint`, `make proto` (buf generate trên `buf.lock` đã commit, rồi gộp OpenAPI), `make proto-deps` (chủ động nâng deps buf, ghi lại `buf.lock`), `make sqlc` (chạy sqlc qua Docker), `make migrate`, `make logs`.

## URL local

Mọi cổng publish ra host đều bind vào `127.0.0.1`.

| Thành phần | URL | Ghi chú |
|---|---|---|
| Gateway | http://localhost:8080 | `/healthz`, `/readyz`, `/v1/...` |
| Grafana | http://localhost:3000 | anonymous Admin, datasource Tempo có sẵn |
| Tempo | http://localhost:3200 | API truy vấn trace |
| OTel Collector | `localhost:4317` | OTLP gRPC |
| Redpanda Console | http://127.0.0.1:8081 | xem topic, message, DLQ |
| Kafka (broker, listener dev) | `127.0.0.1:19092` | chỉ cho smoke/dev trên host; trong Compose dùng `redpanda:9092` |
| Auth gRPC / HTTP | `9001` / `8001` | chỉ trong mạng Compose (`auth:9001`, `auth:8001`), không publish ra host |
| Job gRPC / HTTP | `9002` / `8002` | chỉ trong mạng Compose (`job:9002`, `job:8002`), không publish ra host |
| Candidate gRPC / HTTP | `9003` / `8003` | chỉ trong mạng Compose (`candidate:9003`, `candidate:8003`), không publish ra host |
| AI API | `8100` | chỉ trong mạng Compose (`ai-api:8100`: `/healthz`, `/readyz`, `/metrics`), không publish ra host |
| MinIO S3 API | http://127.0.0.1:9000 | nơi client PUT CV bằng presigned URL |
| MinIO Console | http://127.0.0.1:9001 | đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env` |

Xem trace của một request: gọi `GET /v1/me`, mở Grafana → Explore → Tempo, tìm theo service `gateway`.

## Roadmap

| # | Milestone | Trạng thái |
|---|---|---|
| M0 | Nền móng + Auth + Gateway: register/login/refresh/logout qua Gateway, có trace | ✅ |
| M1a | Nền sự kiện (`pkg/kafka`, `pkg/outbox`, `pkg/idempotency`, DLQ, Redpanda) + Job Service (tìm kiếm Postgres full-text) | ✅ |
| M1b | Candidate + AI parse (upload CV presigned, AI service Python, `cv.*` events) | ✅ |
| M2 | Matching (embedding, Qdrant) + CQRS tìm kiếm JD (OpenSearch) | Chưa bắt đầu |
| M3 | Application + Saga (orchestration) + resilience | Chưa bắt đầu |
| M4 | Notification + observability đầy đủ + saga xoá tài khoản (choreography) | Chưa bắt đầu |
| M5 | Frontend tối giản + E2E + k6 + video demo | Chưa bắt đầu |
| M6 | (tuỳ chọn) Helm/K8s | Chưa bắt đầu |

## Tài liệu

- Service: [`services/auth`](services/auth/README.md), [`services/gateway`](services/gateway/README.md), [`services/job`](services/job/README.md), [`services/candidate`](services/candidate/README.md), [`services/ai`](services/ai/README.md)
- ADR ([mục lục](docs/adr/README.md)):
  - [0001 — Monorepo + Go workspace](docs/adr/0001-monorepo.md)
  - [0002 — Gateway hybrid chi + grpc-gateway](docs/adr/0002-gateway-hybrid.md)
  - [0003 — Verify JWT ở cả Gateway và service](docs/adr/0003-jwt-verify-at-service.md)
  - [0004 — Refresh token rotation + phát hiện dùng lại](docs/adr/0004-refresh-token-rotation.md)
  - [0005 — Outbox relay trong process + cột `headers`](docs/adr/0005-outbox-relay-headers.md)
  - [0006 — Kafka client (franz-go) + retry in-process + DLQ](docs/adr/0006-kafka-client-retry-dlq.md)
  - [0007 — AI service bằng Python + cổng `LlmParser`](docs/adr/0007-ai-service-python-llm-port.md)
  - [0008 — Upload CV bằng presigned URL (valet key) + MinIO](docs/adr/0008-presigned-upload-valet-key.md)
- Thiết kế M0: [`docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md`](docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md)
- Thiết kế M1a: [`docs/superpowers/specs/2026-11-10-m1a-events-job-design.md`](docs/superpowers/specs/2026-11-10-m1a-events-job-design.md); plan: [`docs/superpowers/plans/2026-11-10-m1a-events-job.md`](docs/superpowers/plans/2026-11-10-m1a-events-job.md)
- Thiết kế M1b: [`docs/superpowers/specs/2026-11-11-m1b-candidate-ai-parse-design.md`](docs/superpowers/specs/2026-11-11-m1b-candidate-ai-parse-design.md); plan: [`docs/superpowers/plans/2026-11-11-m1b-candidate-ai.md`](docs/superpowers/plans/2026-11-11-m1b-candidate-ai.md)
- OpenAPI của Gateway: [`contracts/openapi/gateway.yaml`](contracts/openapi/gateway.yaml)
