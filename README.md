# SmartHire

SmartHire là nền tảng tuyển dụng (phân tích CV, so khớp CV với JD, theo dõi đơn ứng tuyển) xây dựng như một **dự án portfolio Backend Go**: monorepo gồm nhiều microservice Go (cùng một service AI bằng Python từ M1b), giao tiếp đồng bộ qua gRPC và bất đồng bộ qua Kafka. Trọng tâm là các pattern backend (Transactional Outbox, idempotent consumer, Saga, circuit breaker, CQRS, tracing xuyên service), không phải độ rộng tính năng. Đặc tả đầy đủ nằm ở [`spec.md`](spec.md); mỗi milestone có design và plan riêng trong [`docs/superpowers/`](docs/superpowers/).

## Kiến trúc mục tiêu

Sơ đồ dưới là kiến trúc đích (spec.md §4). Phần **đã có (M0 + M1a)** được tô xanh: API Gateway, Auth Service, Job Service, Postgres `auth_db` và `job_db`, Redis, Kafka (Redpanda). Các thành phần còn lại (Candidate, Application, Notification, AI, OpenSearch, Qdrant, MinIO, Web App) thuộc các milestone sau.

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
    class GW,AUTH,PGA,RD,JOB,PGJ,K m0;
```

Từ M1a, Auth và Job **ghi** sự kiện (`user.registered`, `job.created|updated|closed|deleted`) vào bảng `outbox` trong cùng transaction với dữ liệu nghiệp vụ; một relay chạy trong process của mỗi service (poll `FOR UPDATE SKIP LOCKED`, at-least-once) đẩy chúng sang Kafka (Redpanda), kèm carrier W3C trong cột `headers` để trace đi tiếp qua Kafka. Consumer retry trong process rồi chuyển vào `<topic>.dlq`. Xem [ADR 0005](docs/adr/0005-outbox-relay-headers.md) và [ADR 0006](docs/adr/0006-kafka-client-retry-dlq.md).

## Quick start

Yêu cầu: Go (bản stable mới nhất), `make`, `curl`, Docker với Compose v2. Trên Windows chạy từ Git Bash (Makefile viết cho POSIX shell).

```bash
make bootstrap            # cài buf, golangci-lint, migrate; sinh khoá JWT dev; tạo .env từ .env.example
make infra-up obs-up up   # Postgres + Redis + Redpanda (+ Console, tạo topic); OTel Collector + Tempo + Grafana; auth + job + gateway
make seed                 # tạo admin và user mẫu (dịch vụ auth-seed)
make smoke                # luồng auth (register → login → /v1/me → refresh → reuse → logout) rồi luồng recruiter tạo/đăng/đóng job, kiểm tra sự kiện trên Kafka
make down                 # dừng stack
```

`make bootstrap` cài tool vào `$(go env GOPATH)/bin`; thư mục này phải nằm trong `PATH`. Khoá JWT sinh ở `deploy/docker/secrets/jwt.pem` và `.env` đều bị gitignore; giá trị trong `.env.example` chỉ dùng cho máy local.

Sau `make seed` có ba tài khoản: admin (`SEED_ADMIN_EMAIL` / `SEED_ADMIN_PASSWORD` trong `.env`, mặc định `admin@smarthire.local` / `Admin-Password-123!`), `candidate@smarthire.local` và `recruiter@smarthire.local` (mật khẩu dev `Password123!`). Admin không đăng ký được qua API.

**Volume dev cũ:** DB `job_db` và role `job_svc` chỉ được tạo khi Postgres khởi tạo volume lần đầu. Nếu đã chạy stack từ trước M1a, thêm `JOB_DB_PASSWORD` vào `.env` (xem `.env.example`) rồi chạy `make down-v` (xoá volume của project Compose, mất dữ liệu dev) trước khi `make infra-up obs-up up`.

**Mã trả về của OpenAPI:** `POST /v1/jobs` (CreateJob) và `POST /v1/auth/register` thực tế trả **201**, trong khi `contracts/openapi/gateway.yaml` (sinh từ proto) ghi 200. `openapimerge` chỉ hợp nhất path/method và báo lỗi khi trùng, không ghi đè được response của operation sinh ra, nên sai lệch này được ghi nhận ở đây thay vì vá trong `gateway.handwritten.yaml`.

Lệnh khác: `make test` (unit, không cần Docker), `make test-int` (integration, cần Docker), `make cover-auth` / `make cover-job` (coverage domain + app của Auth / Job, ngưỡng 70%), `make lint`, `make proto` (buf generate trên `buf.lock` đã commit, rồi gộp OpenAPI), `make proto-deps` (chủ động nâng deps buf, ghi lại `buf.lock`), `make sqlc` (chạy sqlc qua Docker), `make migrate`, `make logs`.

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

Xem trace của một request: gọi `GET /v1/me`, mở Grafana → Explore → Tempo, tìm theo service `gateway`.

## Roadmap

| # | Milestone | Trạng thái |
|---|---|---|
| M0 | Nền móng + Auth + Gateway: register/login/refresh/logout qua Gateway, có trace | ✅ |
| M1a | Nền sự kiện (`pkg/kafka`, `pkg/outbox`, `pkg/idempotency`, DLQ, Redpanda) + Job Service (tìm kiếm Postgres full-text) | ✅ |
| M1b | Candidate + AI parse | Chưa bắt đầu |
| M2 | Matching (embedding, Qdrant) + CQRS tìm kiếm JD (OpenSearch) | Chưa bắt đầu |
| M3 | Application + Saga (orchestration) + resilience | Chưa bắt đầu |
| M4 | Notification + observability đầy đủ + saga xoá tài khoản (choreography) | Chưa bắt đầu |
| M5 | Frontend tối giản + E2E + k6 + video demo | Chưa bắt đầu |
| M6 | (tuỳ chọn) Helm/K8s | Chưa bắt đầu |

## Tài liệu

- Service: [`services/auth`](services/auth/README.md), [`services/gateway`](services/gateway/README.md), [`services/job`](services/job/README.md)
- ADR ([mục lục](docs/adr/README.md)):
  - [0001 — Monorepo + Go workspace](docs/adr/0001-monorepo.md)
  - [0002 — Gateway hybrid chi + grpc-gateway](docs/adr/0002-gateway-hybrid.md)
  - [0003 — Verify JWT ở cả Gateway và service](docs/adr/0003-jwt-verify-at-service.md)
  - [0004 — Refresh token rotation + phát hiện dùng lại](docs/adr/0004-refresh-token-rotation.md)
  - [0005 — Outbox relay trong process + cột `headers`](docs/adr/0005-outbox-relay-headers.md)
  - [0006 — Kafka client (franz-go) + retry in-process + DLQ](docs/adr/0006-kafka-client-retry-dlq.md)
- Thiết kế M0: [`docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md`](docs/superpowers/specs/2026-11-08-m0-foundation-auth-gateway-design.md)
- Thiết kế M1a: [`docs/superpowers/specs/2026-11-10-m1a-events-job-design.md`](docs/superpowers/specs/2026-11-10-m1a-events-job-design.md); plan: [`docs/superpowers/plans/2026-11-10-m1a-events-job.md`](docs/superpowers/plans/2026-11-10-m1a-events-job.md)
- OpenAPI của Gateway: [`contracts/openapi/gateway.yaml`](contracts/openapi/gateway.yaml)
