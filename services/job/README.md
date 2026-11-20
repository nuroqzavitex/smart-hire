# job service

Job postings (JD) for recruiters: create, edit, publish, close, soft-delete, and public search over open jobs. Since M2a public search reads an OpenSearch read model kept in step by the `job-projector` binary (CQRS, [ADR 0009](../../docs/adr/0009-cqrs-job-search-read-model.md)); the M1a Postgres full-text search (`unaccent`) is the circuit-breaker fallback.

## Binaries

| Binary | Compose service | Purpose |
|--------|-----------------|---------|
| `/server` (`cmd/server`) | `job` | gRPC API, outbox relay |
| `/projector` (`cmd/projector`) | `job-projector` | consumes `job.*` into OpenSearch (group `job-projector`); `projector rebuild [--keep-old]` rebuilds the index from Postgres with zero downtime (`make search-rebuild`); `projector reconcile` compares open jobs, exit 2 on mismatch (`make search-reconcile`) |

## Ports

| Port | Purpose |
|------|---------|
| 9002 | gRPC (`smarthire.job.v1.JobService`, health) |
| 8002 | HTTP, internal only: `/healthz`, `/readyz` |
| 8012 | projector HTTP, internal only: `/healthz`, `/readyz` (ready once the index and aliases exist) |

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
| `JOB_OPENSEARCH_URL` | `http://opensearch:9200` | server + projector |
| `JOB_SEARCH_TIMEOUT` | `2s` | per search call |
| `JOB_SEARCH_BREAKER_FAILURES` | `5` | consecutive failures that open the breaker |
| `JOB_SEARCH_BREAKER_OPEN` | `30s` | open time before one trial request |
| `JOB_PROJECTOR_HTTP_PORT` | `8012` | projector |
| `JOB_PROJECTOR_ALIAS_TTL` | `5s` | projector cache of the `jobs_write` alias; rebuild waits TTL + 2s twice |

## RPC and REST

| RPC | REST | Auth |
|-----|------|------|
| `CreateJob` | `POST /v1/jobs` | recruiter |
| `UpdateJob` | `PATCH /v1/jobs/{id}` | owner |
| `PublishJob` | `POST /v1/jobs/{id}:publish` | owner |
| `CloseJob` | `POST /v1/jobs/{id}:close` | owner |
| `DeleteJob` | `DELETE /v1/jobs/{id}` | owner |
| `GetJob` | `GET /v1/jobs/{id}` | public (open jobs); owner sees any state |
| `ListJobs` | `GET /v1/jobs` | public; OpenSearch read model with `facets`, `degraded: true` when served by the Postgres fallback |
| `ListMyJobs` | `GET /v1/me/jobs` | recruiter |

## Events (outbox, relayed to Kafka)

`job.created`, `job.updated`, `job.closed`, `job.deleted` (source `job-service`, key = `job_id`). `job.created|updated` carry `published_at` since M2a (optional in the schema for older events).

## Read model

Index `jobs_v{n}` behind the read alias `jobs` and the write alias `jobs_write`. Every write uses `version_type=external` (409 = already applied); closed and deleted jobs stay as tombstones filtered out by `status=open`. Analyzer: `icu_tokenizer` + `icu_folding` + skill synonyms; image `smarthire/opensearch:2.19.2-icu` (`make opensearch-image`). Page tokens record which backend issued them; a token from the other backend restarts at page 1.

## Notes

- **Status codes:** `CreateJob` returns HTTP 201 through the gateway, although the generated OpenAPI (`contracts/openapi/gateway.yaml`) documents 200 (`openapimerge` cannot override generated responses).
- **`unaccent` and reindexing:** the full-text column `search_vector` is generated through `unaccent_immutable(text)`, an `IMMUTABLE` wrapper around `unaccent` with a fixed dictionary. If the `unaccent` rules change (extension upgrade or edited rules file), stored values are not recomputed: rewrite the column and `REINDEX` the dependent indexes.
- **Dev volumes:** `job_db` and the `job_svc` role exist only on a freshly initialised Postgres volume; older dev volumes need `make down-v` plus `JOB_DB_PASSWORD` in `.env` (see the root README).
- Events and relay behaviour: [ADR 0005](../../docs/adr/0005-outbox-relay-headers.md), [ADR 0006](../../docs/adr/0006-kafka-client-retry-dlq.md). Coverage: `make cover-job` (threshold 70%).
