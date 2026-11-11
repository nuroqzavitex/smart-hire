# candidate service

Candidate profiles and CVs: the profile (name, phone, location), CV upload through a presigned MinIO URL (valet key), and the parse results produced by the AI service (`cv.parsed` / `cv.parse_failed`), which the candidate can view, edit and re-run.

## Ports

| Port | Purpose |
|------|---------|
| 9003 | gRPC (`smarthire.candidate.v1.CandidateService`, health) |
| 8003 | HTTP, internal only: `/healthz` (liveness), `/readyz` (ready only once gRPC is serving and the DB answers) |

## Environment (prefix `CANDIDATE_`)

| Variable | Default | Notes |
|----------|---------|-------|
| `CANDIDATE_DB_DSN` | required | role `candidate_svc` on `candidate_db` |
| `CANDIDATE_GRPC_PORT` | `9003` | |
| `CANDIDATE_HTTP_PORT` | `8003` | |
| `CANDIDATE_JWKS_URL` | `http://auth:8001/.well-known/jwks.json` | tokens are re-verified here |
| `CANDIDATE_JWT_ISSUER` | `smarthire-auth` | |
| `CANDIDATE_JWT_AUDIENCE` | `smarthire-api` | |
| `CANDIDATE_KAFKA_BROKERS` | `redpanda:9092` | comma separated |
| `CANDIDATE_OUTBOX_INTERVAL` | `500ms` | outbox relay poll interval |
| `CANDIDATE_S3_ENDPOINT` | `minio:9000` | in-network MinIO `host:port` for server-side calls |
| `CANDIDATE_S3_PUBLIC_ENDPOINT` | `http://127.0.0.1:9000` | URL clients reach; used only to sign upload URLs |
| `CANDIDATE_S3_ACCESS_KEY` | required | MinIO user `candidate-svc` (Put/Get/Delete on `cvs/*` only) |
| `CANDIDATE_S3_SECRET_KEY` | required | |
| `CANDIDATE_S3_BUCKET` | `cvs` | private bucket |
| `CANDIDATE_S3_REGION` | `us-east-1` | set so presigning never looks the region up |
| `CANDIDATE_S3_USE_SSL` | `false` | for the internal endpoint |
| `CANDIDATE_UPLOAD_URL_TTL` | `10m` | presigned PUT lifetime |
| `CANDIDATE_LOG_LEVEL` | `info` | |

## RPC and REST

Every RPC requires a `candidate` token; there are no public methods (besides gRPC health).

| RPC | REST | Notes |
|-----|------|-------|
| `GetMyProfile` | `GET /v1/me/profile` | 404 before the first upsert |
| `UpsertMyProfile` | `PUT /v1/me/profile` | |
| `CreateUploadURL` | `POST /v1/cvs/upload-url` | `consent` must be `true`; creates the CV in `UPLOADING` |
| `CompleteUpload` | `POST /v1/cvs/{id}:complete` | checks size ≤ 5 MiB, `application/pdf`, `%PDF-` magic; `UPLOADED` + `cv.uploaded` |
| `GetCV` | `GET /v1/cvs/{id}` | owner only (others get 404) |
| `ListMyCVs` | `GET /v1/cvs` | newest first, `page_size` default 20, max 50 |
| `UpdateParsedData` | `PATCH /v1/cvs/{id}/parsed` | `PARSED` only; body validated against `cv.parsed#/$defs/ParsedCV` |
| `ReparseCV` | `POST /v1/cvs/{id}:reparse` | from `PARSED`/`FAILED`; new `attempt_id`, emits `cv.uploaded` |

A CV response never contains the storage object key or a download URL.

## Upload flow

1. `POST /v1/cvs/upload-url` → `{cv_id, upload_url, expires_at}`.
2. The client `PUT`s the PDF to `upload_url` with `Content-Type: application/pdf` (the header is part of the signature).
3. `POST /v1/cvs/{cv_id}:complete` → the service stats and sniffs the object; an invalid object is deleted and the CV stays `UPLOADING` (the client may PUT again while the URL is valid).
4. The AI service parses the PDF and publishes the result; the CV becomes `PARSED` or `FAILED`.

## Events

| Direction | Topic | Notes |
|-----------|-------|-------|
| emits (outbox → Kafka) | `cv.uploaded` | source `candidate-service`, key `cv_id` |
| consumes (group `candidate-cv-results`) | `cv.parsed`, `cv.parse_failed` | validated against `contracts/events` at runtime; deduplicated by `event_id` (`processed_events`); results for an older `attempt_id` are ignored |

Invalid results (schema violation, unknown CV, wrong `candidate_id`) are poison events and go straight to `<topic>.dlq`; other errors are retried 3 times first ([ADR 0006](../../docs/adr/0006-kafka-client-retry-dlq.md)). A failed DLQ write stops the consumer and shuts the service down.

The image copies `contracts/events` to `/contracts/events` and runs from `/`; the service refuses to start if the schemas are missing.

## Privacy

Never logged, nor put in errors or `dlq-error`: names, emails, phone numbers, file names, object keys, presigned URLs, CV content or parsed data. Schema validation errors echo offending values, so they are replaced by fixed messages. `user_id`, `cv_id`, `event_id` and `attempt_id` may be logged.
