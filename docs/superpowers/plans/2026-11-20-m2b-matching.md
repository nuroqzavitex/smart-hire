# M2b — CV ↔ JD Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (user chose inline execution, 2026-11-20). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Score CVs against open jobs (Gemini embeddings in Qdrant + §5.7 formula), precompute matches on `job.*`/`cv.updated`, and serve `GET /v1/jobs/{id}/candidates` (Job) and `GET /v1/cvs/{id}/recommended-jobs` (Candidate) through an internal AI gRPC service.

**Architecture:** Candidate emits a new state-carrying `cv.updated`. A new Python process `ai-matcher` keeps local replicas (`match_jobs`, `match_cvs`), vectors in Qdrant and precomputed `match_results`, publishing `match.completed` via the AI outbox. `ai-api` gains a gRPC server (`AIService`) that Job and Candidate call through a timeout + circuit breaker after their own ownership checks.

**Tech Stack:** Python 3.12 (uv): `google-genai>=3.6`, `qdrant-client>=1.20`, `grpcio>=1.85`, `opentelemetry-instrumentation-grpc`, testcontainers (generic Qdrant). Go 1.27: `sony/gobreaker/v2`, `pkg/grpcx`. Qdrant `qdrant/qdrant:v1.20.2`. buf remote plugins `protocolbuffers/python`, `protocolbuffers/pyi`, `grpc/python`.

**Spec:** `docs/superpowers/specs/2026-11-20-m2b-matching-design.md` (read it; this plan argues from it).

## Global Constraints

- Score: `100 × (0.55·semantic + 0.30·skill_coverage + 0.10·experience_fit + 0.05·location_fit)`; `skill_coverage < 0.5` → `min(score, 40)`; round 2 decimals. `semantic = max(0, cosine)`. Min years by level: intern 0, junior 0, middle 2, senior 5, lead 7. Missing years / missing location → 0.5.
- Embedding: `gemini-embedding-001`, task type `RETRIEVAL_DOCUMENT`, `output_dimensionality=768`, L2-normalised client side. `model_version = f"{model}@768"`; fake: `fake-hash@768`.
- Qdrant collections `jobs_<slug>` / `cvs_<slug>` where slug = model_version lowercased with non-alnum → `_` (e.g. `jobs_gemini_embedding_001_768`); 768d, cosine; point id = job_id / cv_id (UUID).
- Candidate pool per job: 50 nearest active CVs. CV → jobs: 50 nearest open jobs. `top_k` default 20, max 50 (larger → clamp, ≤ 0 → default). `match.completed.top` ≤ 20.
- Embed text excludes `full_name`, `email`, `education.school`, `experiences.company`, `location`. JD text truncated to 6000 chars.
- Never call Gemini in tests/CI/smoke; default `AI_EMBEDDING_PROVIDER=fake`. Provider `gemini` without `AI_GEMINI_API_KEY` → startup error.
- Matcher errors: Gemini 429/5xx/timeout, Qdrant/Postgres unreachable → retry with backoff 0.2s→10s until stop, never DLQ; schema/version/source errors → DLQ (PermanentError).
- Env: `AI_QDRANT_URL` (`http://qdrant:6333`), `AI_EMBEDDING_PROVIDER` (`fake|gemini`), `AI_EMBEDDING_MODEL` (`gemini-embedding-001`), `AI_GEMINI_API_KEY` (SecretStr, empty), `AI_GRPC_PORT` (9100); `JOB_AI_ADDR` (`ai-api:9100`), `CANDIDATE_AI_ADDR` (`ai-api:9100`), `CANDIDATE_JOB_ADDR` (`job:9002`), `JOB_AI_TIMEOUT`/`CANDIDATE_AI_TIMEOUT` (2s), breaker 5 consecutive failures / 30s open.
- Consumer group `ai-matcher`, topics `job.created`, `job.updated`, `job.closed`, `job.deleted`, `cv.updated`; `auto.offset.reset=earliest`.
- AI gRPC has no JWT (compose-internal); Job/Candidate check ownership before calling it. Go breaker ignores caller cancel, `NotFound`, `InvalidArgument`.
- Logs/errors never carry PII or payload values (M1b E-R6).
- Commits: Conventional Commits with scope + `Co-Authored-By` trailer (local history).

## Review Focus

1. A stale `cv.updated` (lower version) arriving after a newer one must not overwrite replica/vector → Task 5 test `test_cv_updated_older_version_ignored`.
2. Candidate with two PARSED CVs appears once in `FindCandidates` (newest `parsed_at` active); reparse of the newest makes the older one active again → Task 5 `test_active_cv_switches_on_reparse`.
3. Editing an open job's required skills (`job.updated`) replaces its old results (no stale rows from the previous skill set) → Task 5 `test_job_updated_replaces_results`.
4. Gemini 429 during a `job.created` keeps the partition blocked and later succeeds, no DLQ → Task 5 `test_transient_error_retries_without_dlq`.
5. `RecommendJobs` with a CV of another candidate → 404 via Candidate (not leaked by AI) → Task 8 `TestListRecommendedJobs_NotOwner`.

---

### Task 1: Contracts (proto + events) and code generation

**Files:**
- Create: `contracts/proto/smarthire/ai/v1/ai.proto` (spec §13.2 + `RankedJob.breakdown = 3`; no `google.api.http`)
- Modify: `contracts/proto/smarthire/job/v1/job.proto` — `rpc ListMatchedCandidates(ListMatchedCandidatesRequest{id, top_k}) returns (ListMatchedCandidatesResponse{repeated MatchedCandidate candidates; string model_version})` `get: "/v1/jobs/{id}/candidates"`; `MatchedCandidate{candidate_id, cv_id, score, MatchBreakdown breakdown}`; `MatchBreakdown` mirrors `ai.v1.ScoreBreakdown` (own copy: no cross-service proto import); `rpc BatchGetJobs(BatchGetJobsRequest{repeated string ids}) returns (BatchGetJobsResponse{repeated Job jobs})` no HTTP.
- Modify: `contracts/proto/smarthire/candidate/v1/candidate.proto` — `rpc ListRecommendedJobs(ListRecommendedJobsRequest{id, top_k}) returns (ListRecommendedJobsResponse{repeated RecommendedJob jobs; string model_version})` `get: "/v1/cvs/{id}/recommended-jobs"`; `RecommendedJob{job_id, title, level, location, salary_min?, salary_max?, score, MatchBreakdown breakdown}`.
- Create: `contracts/proto/buf.gen.python.yaml` (python, pyi, grpc/python → `../../services/ai/src`); Makefile `proto` also runs `buf generate --template buf.gen.python.yaml --path smarthire/ai`.
- Modify: `services/ai/pyproject.toml` — `module-name = ["ai_service", "smarthire"]` (uv_build multi-module); ruff/mypy exclude `src/smarthire`.
- Create: `contracts/events/cv.updated.v1.json` (payload `cv_id, candidate_id` uuid; `version` int ≥1; `status` enum `UPLOADED|PARSED|FAILED`; `parsed` `$defs.ParsedCV | null` copied from `cv.parsed.v1.json`; `parsed_at` date-time|null; all required), `contracts/events/match.completed.v1.json` (`job_id` uuid, `model_version` string, `top` array ≤20 of `{candidate_id, cv_id, score 0..100}`), fixtures `testdata/cv.updated/{valid_parsed,valid_reparse}.json`, `testdata/match.completed/valid.json`.
- Test: `pkg/event/eventschema/eventschema_test.go` (fixture loop), `services/ai/tests/unit/test_contract_schema.py` (ParsedCV `$defs` of `cv.updated` equals `cv.parsed`'s).

**Interfaces:** Produces Go packages `gen/go/smarthire/ai/v1` (`aiv1.AIServiceClient`), new job/candidate RPCs; Python `smarthire.ai.v1.ai_pb2` / `ai_pb2_grpc`; event types `cv.updated` v1, `match.completed` v1.

- [ ] **Step 1:** Tests: `TestValidate_CvUpdated` (both fixtures valid; `status:"PARSED"` with `parsed:null` still schema-valid — semantic rule enforced by consumers), python `test_cv_updated_parsed_defs_match_cv_parsed`.
- [ ] **Step 2:** Run `cd pkg && go test ./event/eventschema` and `make ai-test` → FAIL (schemas missing).
- [ ] **Step 3:** Write protos/schemas/fixtures/template; `make proto`; `make lint` (buf lint).
- [ ] **Step 4:** `go build ./...` in `gen/go`; `cd services/ai && uv run python -c "from smarthire.ai.v1 import ai_pb2_grpc"`; tests PASS. If uv_build rejects a module list → Ruling: generate into `src/ai_service/gen` and add the dir to `sys.path` in `ai_service/gen/__init__.py`.
- [ ] **Step 5:** Commit `feat(contracts): add ai matching proto, cv.updated and match.completed events`.

### Task 2: Candidate emits `cv.updated`

**Files:**
- Modify: `services/candidate/internal/app/events.go` (`evUpdated = "cv.updated"`, `cvUpdatedPayload{CVID, CandidateID, Version int64, Status string, Parsed json.RawMessage (null when empty), ParsedAt *time.Time}`, `func (s *Service) emitUpdated(ctx, cv domain.CV, at time.Time) error`)
- Modify: `services/candidate/internal/app/results.go` (after `CVs.Update` in `HandleParseResult` → `emitUpdated`; stale result → no event), `cvs.go` (`mutate` emits `cv.updated` after every successful Update of `UpdateParsedData` and `ReparseCV`; `CompleteUpload` does not)
- Test: `services/candidate/internal/app/results_test.go`, `cvs_test.go`; integration `services/candidate/test/integration` existing event test extended to validate `cv.updated` with `eventschema`.

**Interfaces:** Produces `cv.updated` envelopes, key `cv_id`, `version` = the CV's version **after** the update.

- [ ] **Step 1:** Tests `TestHandleParseResult_EmitsCvUpdated` (parsed → one `cv.updated` with status PARSED, parsed equals stored doc, version = cv.Version), `TestHandleParseResult_FailedEmitsCvUpdated` (FAILED, parsed null), `TestHandleParseResult_StaleEmitsNothing`, `TestUpdateParsedData_EmitsCvUpdated`, `TestReparseCV_EmitsUploadedAndUpdated` (both events, `cv.updated` status UPLOADED parsed null). Each emitted envelope validates against the schema.
- [ ] **Step 2:** `cd services/candidate && go test ./internal/app` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `go test ./...` + `go test -tags integration ./test/integration` → PASS; `make cover-candidate` ≥ 70%.
- [ ] **Step 5:** Commit `feat(candidate): emit cv.updated on every parse state change`.

### Task 3: AI domain — scoring, embed text, embedders

**Files:**
- Create: `services/ai/src/ai_service/domain/matching.py`: `@dataclass(frozen=True) JobProfile(job_id: UUID, recruiter_id: UUID, version: int, status: str, title: str, description: str, required_skills: list[str], nice_to_have_skills: list[str], level: str, location: str | None, salary_min: int | None, salary_max: int | None)`; `CvProfile(cv_id: UUID, candidate_id: UUID, version: int, status: str, parsed: ParsedCV | None, parsed_at: datetime | None)`; `Breakdown(semantic_similarity, required_skill_coverage, experience_fit, location_fit: float, matched_skills, missing_skills: list[str])`; `score(semantic: float, job: JobProfile, cv: ParsedCV) -> tuple[float, Breakdown]`; `job_text(job) -> str`; `cv_text(cv: ParsedCV) -> str`; `MIN_YEARS: dict[str, int]`.
- Create: `services/ai/src/ai_service/services/embedding.py` (port `Embedder` Protocol: `model_version: str`, `embed(texts: Sequence[str]) -> list[list[float]]`; `EmbeddingUnavailable(Exception)` = transient)
- Create: `services/ai/src/ai_service/adapters/embedder.py`: `FakeEmbedder` (unaccent+lowercase tokens, each token hashed with sha256 into 768 buckets ± sign, L2-normalised; empty text → zero vector), `GeminiEmbedder(client, model, timeout_s)` (`client.models.embed_content(model=, contents=texts, config=EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT", output_dimensionality=768))`, batches of 100, L2-normalise; `google.genai.errors.APIError` with code 429/5xx, timeouts, connection errors → `EmbeddingUnavailable`; other 4xx → re-raise), `build_embedder(settings) -> Embedder`.
- Modify: `services/ai/src/ai_service/config.py` (fields from Global Constraints; validator: provider gemini needs key).
- Test: `tests/unit/test_matching.py`, `tests/unit/test_embedder.py`, `tests/unit/test_config.py`.

**Interfaces:** Produces `score`, `job_text`, `cv_text`, `JobProfile`, `CvProfile`, `Breakdown`, `Embedder`, `EmbeddingUnavailable`, `build_embedder` for Tasks 5–6.

- [ ] **Step 1:** Tests (table): perfect match (cos 1, all skills, years ≥ min, same location) → 100.0; coverage 1/3 with cos 0.9 → capped 40.0; no required skills → coverage 1.0; `"Golang"` in CV matches required `"go"` (normalize synonyms); years None → exp 0.5; level middle with 1 year → exp 0.5; location `"Hà Nội"` vs `"ha noi"` → 1, None → 0.5, `"Huế"` → 0; negative cosine → semantic 0. `cv_text` of a CV with full_name/email/school/company never contains them. Fake: same text → identical vector, norm 1.0, "golang backend" closer to "backend golang developer" than to "chef". Gemini with fake client: 429 → `EmbeddingUnavailable`, 400 → raised as is, 150 texts → 2 calls. Config: `AI_EMBEDDING_PROVIDER=gemini` without key → ValidationError.
- [ ] **Step 2:** `make ai-test` → FAIL.
- [ ] **Step 3:** Implement; add deps `google-genai>=3.6` (`uv add`).
- [ ] **Step 4:** `make ai-lint ai-test` → PASS.
- [ ] **Step 5:** Commit `feat(ai): add match scoring, embed text and gemini/fake embedders`.

### Task 4: AI storage — migration, Qdrant adapter, compose Qdrant

**Files:**
- Create: `services/ai/migrations/versions/0002_matching.py`: `match_jobs(job_id uuid PK, recruiter_id uuid, version bigint, status text, doc jsonb, updated_at)`, `match_cvs(cv_id uuid PK, candidate_id uuid, version bigint, status text, parsed jsonb, parsed_at timestamptz, active bool NOT NULL DEFAULT false, updated_at)` + index `(candidate_id)`, `match_results(job_id uuid, cv_id uuid, candidate_id uuid, score numeric(5,2), breakdown jsonb, model_version text, computed_at timestamptz, PRIMARY KEY(job_id, cv_id))` + index `(job_id, score DESC)`.
- Create: `services/ai/src/ai_service/adapters/match_store.py` (`PgMatchStore` — replica upserts guarded by `version`, active recompute, results replace/upsert/delete, outbox write — all methods take a `Connection` so the handler controls the tx; reads for gRPC).
- Create: `services/ai/src/ai_service/adapters/vectors.py` (`QdrantVectors(client, model_version)`: `ensure_collections()`, `upsert_job(job_id, vec, payload)`, `upsert_cv(cv_id, vec, payload)`, `set_cv_active(cv_ids_active: set, cv_ids_inactive: set)`, `delete_job(id)`, `delete_cv(id)`, `get_job_vector(id) -> list[float] | None`, `get_cv_vector(id)`, `nearest_cvs(vec, limit=50) -> list[tuple[UUID, float]]` (filter `active=true`), `nearest_jobs(vec, limit=50)`; connection/timeouts → `VectorStoreUnavailable` transient)
- Modify: `deploy/docker/docker-compose.infra.yml` (`qdrant`: `qdrant/qdrant:v1.20.2`, `127.0.0.1:6333:6333`, volume `qdrant-data:/qdrant/storage`, healthcheck `bash -c ':> /dev/tcp/127.0.0.1/6333'`), Makefile `infra-up` includes qdrant.
- Test: `tests/integration/test_match_store.py`, `tests/integration/test_vectors.py` (conftest fixture `qdrant_url` via `testcontainers.core.container.DockerContainer("qdrant/qdrant:v1.20.2")`, wait for `/readyz`).

**Interfaces:** Produces `PgMatchStore`, `QdrantVectors`, `VectorStoreUnavailable` for Tasks 5–6. Exact store method names are the executor's choice; record them in the ledger for Task 5.

- [ ] **Step 1:** Tests: `test_upsert_job_older_version_ignored`, `test_active_is_newest_parsed_per_candidate`, `test_replace_results_for_job`, `test_nearest_cvs_only_active`, `test_collection_name_from_model_version`, `test_upsert_idempotent`.
- [ ] **Step 2:** `make ai-test-int` → FAIL.
- [ ] **Step 3:** Implement; `uv add qdrant-client>=1.20`; alembic upgrade in integration conftest.
- [ ] **Step 4:** `make ai-test-int` PASS; `make infra-up` → qdrant healthy.
- [ ] **Step 5:** Commit `feat(ai): add match replicas, results table and qdrant vector store`.

### Task 5: `ai-matcher` — handler, consumer, process

**Files:**
- Create: `services/ai/src/ai_service/services/matcher.py` (`MatchHandler(store, vectors, embedder, engine, clock, trace_id).handle(env)`: dispatch on `event_type`; validate source (`job-service` for `job.*`, `candidate-service` for `cv.updated`), version 1 and payload (pydantic models in `domain/events.py`: `JobPayload`, `JobRefPayload`, `CvUpdatedPayload`) → `PermanentError`; idempotency via `processed_events`; flows per spec §6.4; `match.completed` built with `domain/events.new_envelope` source `ai-service`).
- Modify: `adapters/kafka_consumer.py` — `RetryingConsumer(..., transient: tuple[type[Exception], ...] = (), transient_cap_s: float = 10.0)`: a transient error retries with backoff capped at `transient_cap_s` without counting attempts until stop; `make_consumer(brokers, group_id=GROUP_ID, topics=(TOPIC,))`.
- Create: `services/ai/src/ai_service/main_matcher.py` (+ `[project.scripts] ai-matcher`), same relay/stop pattern as `main_worker.py`; `transient=(EmbeddingUnavailable, VectorStoreUnavailable, sqlalchemy.exc.OperationalError)`.
- Modify: `deploy/docker/docker-compose.yml` (`ai-matcher` service: same image, `command: ["ai-matcher"]`, env DSN/brokers/qdrant/embedding, depends_on ai-migrate + qdrant + redpanda).
- Test: `tests/unit/test_matcher.py` (in-memory fakes for store/vectors), `tests/unit/test_kafka_consumer.py`, `tests/integration/test_matcher_flow.py` (Postgres + Qdrant + fake embedder, events fed to handler directly).

**Interfaces:** Consumes Tasks 3–4. Produces rows in `match_results`, `match.completed` outbox rows.

- [ ] **Step 1:** Tests: `test_job_created_scores_active_cvs_and_emits_completed` (top sorted desc, ≤20), `test_cv_updated_parsed_upserts_results`, `test_cv_updated_older_version_ignored`, `test_active_cv_switches_on_reparse`, `test_job_closed_removes_vector_and_results`, `test_job_updated_replaces_results`, `test_draft_job_not_matched`, `test_duplicate_event_is_noop`, `test_bad_payload_is_permanent`, consumer `test_transient_error_retries_without_dlq` (3 transient failures then success → committed, no DLQ publish, backoff ≤ 10s), `test_permanent_still_dlq`.
- [ ] **Step 2:** `make ai-test` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make ai-lint ai-test ai-test-int` → PASS.
- [ ] **Step 5:** Commit `feat(ai): add ai-matcher consuming job and cv events into precomputed matches`.

### Task 6: AI gRPC server

**Files:**
- Create: `services/ai/src/ai_service/adapters/grpc_server.py` (`AIServicer(store, vectors, embedder_version)` implementing `FindCandidates`, `RecommendJobs`, `ScoreMatch` per spec §7; invalid UUID → `INVALID_ARGUMENT`; missing CV/job/vector → `NOT_FOUND`; `top_k` clamp; `serve(settings) -> grpc.Server` with `ThreadPoolExecutor(max_workers=8)`, OTel `GrpcInstrumentorServer`, health service `grpc.health.v1`).
- Modify: `main_api.py` (start gRPC on `AI_GRPC_PORT` next to uvicorn; stop with 5s grace on shutdown); compose `ai-api` gets qdrant env + depends_on qdrant; add deps `grpcio`, `grpcio-health-checking`, `opentelemetry-instrumentation-grpc`, dev `grpc-stubs`/`types-protobuf`.
- Test: `tests/unit/test_grpc_server.py` (in-process server on a free port, fakes), `tests/integration/test_grpc_flow.py` (real store + Qdrant).

**Interfaces:** Produces `ai-api:9100` `smarthire.ai.v1.AIService`.

- [ ] **Step 1:** Tests: `test_find_candidates_sorted_and_limited`, `test_find_candidates_unknown_job_empty` (job unknown → empty list, not error), `test_recommend_jobs_cv_without_vector_not_found`, `test_recommend_jobs_only_open_jobs`, `test_score_match_matches_domain_score`, `test_bad_uuid_invalid_argument`, `test_top_k_clamped`.
- [ ] **Step 2:** `make ai-test` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `make ai-lint ai-test ai-test-int ai-cover` → PASS, coverage ≥ 70%.
- [ ] **Step 5:** Commit `feat(ai): serve matching over grpc from ai-api`.

### Task 7: Job — `ListMatchedCandidates`, `BatchGetJobs`

**Files:**
- Create: `services/job/internal/adapter/aiclient/client.go` (`New(conn grpc.ClientConnInterface, failures uint32, open, timeout time.Duration) *Client`; `FindCandidates(ctx, jobID uuid.UUID, topK int) (app.MatchList, error)`; gobreaker like M2a `guard.go` — `NotFound`/`InvalidArgument`/caller cancel are successes; breaker open / `Unavailable` / deadline → `apperr` unavailable error mapped to gRPC `Unavailable`). If `pkg/apperr` lacks an unavailable sentinel, add `ErrUnavailable` (→ `codes.Unavailable` → HTTP 503) in `pkg/apperr` + `pkg/grpcx` + gateway `httperr`.
- Modify: `internal/app/ports.go` (`Matcher` port, `MatchedCandidate`, `MatchBreakdown`, `MatchList{Candidates, ModelVersion}`), `service.go` (`Deps.Matcher`), new `internal/app/matching.go` (`ListMatchedCandidates(ctx, jobID, topK)`: recruiter owner or admin, else `ErrNotFound` for non-owner recruiter (don't leak existence) / `ErrForbidden` for candidate; `BatchGetJobs(ctx, ids []uuid.UUID) ([]domain.Job, error)`: any authenticated caller, ≤50 ids else `ErrValidation`, only open, input order), postgres query `GetOpenJobsByIDs` (sqlc), grpc handler + mapping, config `AIAddr`, `AITimeout`, breaker vars; bootstrap dials AI with `grpcx.Dial` (otel stats handler).
- Test: `internal/app/matching_test.go`, `internal/adapter/aiclient/client_test.go` (bufconn fake AI server), `internal/adapter/grpc/handler_test.go`, integration `BatchGetJobs` query test.

**Interfaces:** Produces `GET /v1/jobs/{id}/candidates`, internal `JobService.BatchGetJobs` used by Task 8.

- [ ] **Step 1:** Tests: `TestListMatchedCandidates_Owner`, `_OtherRecruiterNotFound`, `_CandidateForbidden`, `_AdminAllowed`, `_AIUnavailable` (→ apperr unavailable), `TestBatchGetJobs_OnlyOpenInOrder`, `_TooManyIDs`, client `TestClient_BreakerOpensAfterFailures`, `TestClient_NotFoundDoesNotTrip`.
- [ ] **Step 2:** `cd services/job && go test ./...` → FAIL.
- [ ] **Step 3:** Implement; `make sqlc`.
- [ ] **Step 4:** `go test ./...`, integration, `make lint`, `make cover-job` ≥ 70% → PASS.
- [ ] **Step 5:** Commit `feat(job): list matched candidates via ai service and add batch get jobs`.

### Task 8: Candidate — `ListRecommendedJobs`

**Files:**
- Create: `services/candidate/internal/adapter/aiclient/client.go` (same breaker shape as Task 7; `RecommendJobs(ctx, cvID, topK) (app.Recommendations, error)`), `internal/adapter/jobclient/client.go` (`BatchGetJobs(ctx, ids) ([]app.JobSummary, error)`, forwards incoming `authorization` metadata via `metadata.FromIncomingContext` → outgoing).
- Modify: candidate `app` (`ports.go` `Recommender`, `JobDirectory`; new `recommend.go`: `ListRecommendedJobs(ctx, cvID, topK)`: owner only else `ErrNotFound`; status must be PARSED else `ErrValidation` "cv is not parsed"; merge AI order with job details, drop ids missing from BatchGetJobs), grpc handler/mapping, config `AIAddr`, `JobAddr`, timeout/breaker, bootstrap dials both.
- Test: `internal/app/recommend_test.go`, client tests (bufconn), handler test.

**Interfaces:** Consumes Task 7 `BatchGetJobs`, Task 6 `RecommendJobs`. Produces `GET /v1/cvs/{id}/recommended-jobs`.

- [ ] **Step 1:** Tests: `TestListRecommendedJobs_MergesInAIOrder`, `_NotOwner` (404), `_NotParsed` (400), `_DropsClosedJobs`, `_AIUnavailable` (503), `TestJobClient_ForwardsAuthorization`.
- [ ] **Step 2:** `cd services/candidate && go test ./...` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `go test ./...`, integration, lint, `make cover-candidate` → PASS.
- [ ] **Step 5:** Commit `feat(candidate): recommend jobs for a parsed cv via ai and job services`.

### Task 9: Wiring, smoke, CI, docs

**Files:**
- Modify: `deploy/docker/docker-compose.yml` (job/candidate env + depends_on `ai-api`), `.env.example` (`AI_EMBEDDING_PROVIDER=fake`, commented `AI_GEMINI_API_KEY=`), gateway openapi regenerated (`make proto`), `services/gateway` router test covers the two new routes reaching handlers.
- Modify: `tools/cmd/smoke` new `matching.go`: recruiter creates + publishes job with skills from the fake-parsed CV; candidate uploads CV (fake LLM); poll ≤ 30s `GET /v1/jobs/{id}/candidates` until candidate present with score > 0 and breakdown; `GET /v1/cvs/{cv}/recommended-jobs` contains the job; another recruiter → 404; another candidate → 404.
- Modify: `.github/workflows/ci-ai.yml` (integration job needs Qdrant image pull only — testcontainers), `ci-go.yml` filters include `contracts/proto/smarthire/ai/**`.
- Create: `docs/adr/0010-ai-matching.md` (Gemini over bge-m3, `cv.updated` over `cv.parsed`, internal AI gRPC without JWT, precompute for recruiter vs on-demand for candidate/ScoreMatch, transient retry without DLQ); update `docs/adr/README.md`, `README.md`, `CLAUDE.md`, `services/ai/README.md`.

- [ ] **Step 1:** Run `make down && make infra-up obs-up up migrate seed smoke` → new matching steps FAIL before wiring, PASS after.
- [ ] **Step 2:** Verify Tempo shows a trace gateway → job → ai-api (gRPC), and `cv.updated` → ai-matcher span.
- [ ] **Step 3:** `make lint test test-int ai-lint ai-test ai-test-int` → PASS; actionlint clean.
- [ ] **Step 4:** Commit `feat(smoke): cover matching end to end` and `docs: add ADR 0010 and update docs for M2b`.
