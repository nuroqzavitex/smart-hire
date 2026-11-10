# job service

Job postings (JD) for recruiters: create, edit, publish, close, soft-delete, and public search over open jobs (Postgres full-text search with `unaccent`).

## Ports

| Port | Purpose |
|------|---------|
| 9002 | gRPC (`smarthire.job.v1.JobService`, health) |
| 8002 | HTTP, internal only: `/healthz`, `/readyz` |

## Environment (prefix `JOB_`)

| Variable | Default | Notes |
|----------|---------|-------|
| `JOB_DB_DSN` | required | role `job_svc` on `job_db` |
| `JOB_GRPC_PORT` | `9002` | |
| `JOB_HTTP_PORT` | `8002` | |
| `JOB_JWKS_URL` | `http://auth:8001/.well-known/jwks.json` | tokens are re-verified here |
| `JOB_JWT_ISSUER` | `smarthire-auth` | |
| `JOB_JWT_AUDIENCE` | `smarthire-api` | |
| `JOB_KAFKA_BROKERS` | `redpanda:9092` | comma separated |
| `JOB_OUTBOX_INTERVAL` | `500ms` | outbox relay poll interval |
| `JOB_LOG_LEVEL` | `info` | |

## RPC and REST

| RPC | REST | Auth |
|-----|------|------|
| `CreateJob` | `POST /v1/jobs` | recruiter |
| `UpdateJob` | `PATCH /v1/jobs/{id}` | owner |
| `PublishJob` | `POST /v1/jobs/{id}:publish` | owner |
| `CloseJob` | `POST /v1/jobs/{id}:close` | owner |
| `DeleteJob` | `DELETE /v1/jobs/{id}` | owner |
| `GetJob` | `GET /v1/jobs/{id}` | public (open jobs); owner sees any state |
| `ListJobs` | `GET /v1/jobs` | public |
| `ListMyJobs` | `GET /v1/me/jobs` | recruiter |

## Events (outbox, relayed to Kafka)

`job.created`, `job.updated`, `job.closed`, `job.deleted` (source `job-service`, key = `job_id`).
