# M2a — CQRS Job Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (user chose inline execution, 2026-11-20). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve `GET /v1/jobs` from an OpenSearch read model kept up to date by a Kafka projector, with versioned writes, tombstones, zero-loss rebuild, reconcile and a circuit-breaker fallback to Postgres.

**Architecture:** Job service gains a second binary `cmd/projector` (consumer + `rebuild` + `reconcile`) and an `internal/adapter/opensearch` adapter; projection logic lives in `internal/projection`. `ListJobs` reads OpenSearch through a breaker-guarded `SearchBackend` port and falls back to the M1a Postgres search.

**Tech Stack:** Go 1.27, `github.com/opensearch-project/opensearch-go/v4` (+ `opensearchapi`), `github.com/sony/gobreaker/v2`, franz-go via `pkg/kafka`, testcontainers-go (generic container), OpenSearch 2.19.2 + `analysis-icu`.

**Spec:** `docs/superpowers/specs/2026-11-20-m2a-cqrs-job-search-design.md` (read it; this plan argues from it).

## Global Constraints

- Index names: physical `jobs_v{n}` starting `jobs_v1`; read alias `jobs`; write alias `jobs_write`. `number_of_shards: 1`, `number_of_replicas: 0`, `refresh_interval: 1s`, `dynamic: strict`.
- Every write: `version = payload.version`, `version_type = external`; HTTP 409 = already applied (success).
- No document is ever deleted by the projector: `job.closed` → tombstone `{job_id, version, status:"closed"}`, `job.deleted` → `{job_id, version, status:"deleted"}`. Every search filters `status=open`.
- Filter semantics copy M1a `SearchOpenJobs` exactly: skills = any-of (`terms`), location = case/accent-insensitive equality, level = equality, `salary_min=x` ⇒ `salary_max IS NULL OR salary_max >= x`.
- Text query: `multi_match` `cross_fields`, `operator: and`, fields `title^3`, `title.raw^4`, `required_skills.text^2`, `description`, `location.text`. Sort: q empty → `published_at desc, job_id desc`; q set → `_score desc, published_at desc, job_id desc`.
- Synonyms: `golang, go` · `k8s, kubernetes` · `js, javascript` · `ts, typescript` · `postgres, postgresql` · `nodejs, node.js, node`.
- Facets: `required_skills` top 20, `location` top 20, `level` top 5; only when served by OpenSearch.
- Breaker: open after 5 consecutive failures, half-open after 30s, 1 trial request; per-call timeout 2s.
- Env (`JOB_` prefix): `JOB_OPENSEARCH_URL` (`http://opensearch:9200`), `JOB_SEARCH_TIMEOUT` (2s), `JOB_SEARCH_BREAKER_FAILURES` (5), `JOB_SEARCH_BREAKER_OPEN` (30s), `JOB_PROJECTOR_HTTP_PORT` (8012), `JOB_PROJECTOR_ALIAS_TTL` (5s).
- Consumer group `job-projector`; topics `job.created`, `job.updated`, `job.closed`, `job.deleted`.
- OpenSearch image `smarthire/opensearch:2.19.2-icu` from `deploy/docker/opensearch/Dockerfile` (`FROM opensearchproject/opensearch:2.19.2` + `opensearch-plugin install --batch analysis-icu`); heap `-Xms512m -Xmx512m`; `DISABLE_SECURITY_PLUGIN=true`, `DISABLE_INSTALL_DEMO_CONFIG=true`, `discovery.type=single-node`.
- Errors/logs never contain PII or payload values; schema errors → fixed message (M1b E-R6).
- Commits: Conventional Commits with scope, ending with the `Co-Authored-By` trailer (local history).

## Review Focus

1. A `job.updated` older than a later `job.deleted`, arriving after it, must not resurrect the job in search → Task 3 test `TestPut_OldUpdateAfterTombstone`.
2. Edits made while `rebuild` runs must survive in the new index → Task 6 test `TestRebuild_ConcurrentUpdatesSurvive`.
3. A page token issued by OpenSearch, presented while the breaker is open (or vice versa), returns page 1 of the current backend, not 400/500 → Task 5 test `TestListOpen_TokenFromOtherBackend`.
4. Search with a query that has no matches / skills filter with different case/accents → same results as Postgres fallback → Task 4 test `TestSearch_FilterParityWithPostgres`.
5. A malformed `job.*` message must DLQ once, not block the partition → Task 3 test `TestHandle_InvalidEnvelopeIsPermanent`.

---

### Task 1: `published_at` in job events

**Files:**
- Modify: `contracts/events/job.created.v1.json`, `contracts/events/job.updated.v1.json` (add `"published_at": {"type": ["string","null"], "format": "date-time"}` to payload `properties` and `required`)
- Modify: `contracts/events/testdata/**` job fixtures (add `published_at`)
- Modify: `services/job/internal/app/events.go` (`jobPayload.PublishedAt *time.Time \`json:"published_at"\``, set in `payloadOf`)
- Test: `services/job/internal/app/events_test.go` (create if absent), `pkg/event/eventschema/eventschema_test.go` (existing fixture loop covers new fixtures)

**Interfaces:** Produces payload field `published_at` (RFC3339 UTC or `null`) used by Task 3.

- [ ] **Step 1:** Test `TestPayloadOf_PublishedAt`: draft job → marshalled payload has `"published_at":null`; open job with `PublishedAt = 2026-11-20T10:00:00Z` → `"published_at":"2026-11-20T10:00:00Z"`; marshalled `job.created` envelope validates with `eventschema.Validate("job.created", b)`.
- [ ] **Step 2:** `cd services/job && go test ./internal/app -run TestPayloadOf_PublishedAt` → FAIL (field missing / schema rejects).
- [ ] **Step 3:** Implement schema + payload + fixture changes. Check `services/job/test/integration/events_test.go` still matches (it validates emitted events against the schema).
- [ ] **Step 4:** `go test ./internal/app ./...` in `services/job` and `pkg` → PASS.
- [ ] **Step 5:** Commit `feat(job): add published_at to job.created and job.updated payloads`.

### Task 2: OpenSearch image, compose infra, test container helper

**Files:**
- Create: `deploy/docker/opensearch/Dockerfile`
- Modify: `deploy/docker/docker-compose.infra.yml` (service `opensearch`: build context `./opensearch`, image tag from Global Constraints, env from Global Constraints, ports `127.0.0.1:9200:9200`, volume `opensearch-data:/usr/share/opensearch/data`, healthcheck `curl -fsS http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s`)
- Modify: `Makefile` (`opensearch-image: $(COMPOSE) build opensearch`; `infra-up` depends on it and starts `opensearch` with `--wait`; `test-int` depends on it)
- Create: `services/job/test/integration/opensearch_test.go` (helper `startOpenSearch(ctx) (url string, stop func(), err error)` using `testcontainers.GenericContainer`, image `smarthire/opensearch:2.19.2-icu`, env as compose, wait `wait.ForHTTP("/_cluster/health").WithPort("9200/tcp").WithStartupTimeout(120*time.Second)`); `TestMain` in `main_test.go` starts it once and sets package var `osURL`.

**Interfaces:** Produces `osURL string` (integration package) and `startOpenSearch` for Task 7's own container.

- [ ] **Step 1:** Test `TestOpenSearch_ICUInstalled` (integration): `GET {osURL}/_cat/plugins` body contains `analysis-icu`.
- [ ] **Step 2:** `make opensearch-image && cd services/job && go test -tags integration ./test/integration -run TestOpenSearch_ICUInstalled` → FAIL before helper exists, PASS after Step 3.
- [ ] **Step 3:** Implement Dockerfile, compose, Makefile, helper.
- [ ] **Step 4:** `make infra-up` → `opensearch` healthy (`docker compose ps`), `curl 127.0.0.1:9200/_cat/plugins` lists `analysis-icu`.
- [ ] **Step 5:** Commit `feat(deploy): add opensearch with analysis-icu for the job read model`.

### Task 3: Projection core + OpenSearch writes

**Files:**
- Create: `services/job/internal/projection/document.go`, `projector.go`, `projector_test.go`
- Create: `services/job/internal/adapter/opensearch/client.go` (client, `EnsureIndex`, alias helpers, `Put`), `index.json` (embedded settings+mapping per spec §5.1, analyzers `vi`, `vi_exact`, `vi_skills`, normalizer `folded`, synonym filter), `client_test.go` (unit, `httptest` server for 409 handling)
- Test: `services/job/test/integration/projection_test.go`

**Interfaces:**
- `projection.Document`: `JobID string json:"job_id"`, `Version int64 json:"version"`, `Status string json:"status"`, and `omitempty` pointer/slice fields `RecruiterID, Title, Description *string`… (plain `string` with `omitempty` is fine), `RequiredSkills, NiceToHaveSkills []string`, `Level, Location string`, `SalaryMin, SalaryMax *int`, `PublishedAt *time.Time json:"published_at,omitempty"`.
- `func projection.FromEnvelope(env event.Envelope) (Document, error)` — rules of spec §6.2; unparsable payload or unknown type → `kafka.Permanent(errors.New("job event payload invalid"))`.
- `func projection.FromJob(j domain.Job) Document` — `DeletedAt != nil` → tombstone `status:"deleted"`; else full document with `Status: string(j.Status)`.
- `type projection.Store interface { WriteIndices(ctx context.Context) ([]string, error); Put(ctx context.Context, index string, d Document) (applied bool, err error) }`
- `type projection.Projector struct { Store Store; Validate func(eventType string, doc []byte) error; AliasTTL time.Duration; Now func() time.Time; Log *slog.Logger }` with `func (p *Projector) Handle(ctx context.Context, env event.Envelope, rec *kgo.Record) error` (= `kafka.Handler`). Validate failure → `kafka.Permanent` (fixed message). Caches `WriteIndices` for `AliasTTL`.
- `opensearch.Client`: `func New(url string) (*Client, error)`; `const ReadAlias = "jobs"`, `WriteAlias = "jobs_write"`, `IndexPrefix = "jobs_v"`; `EnsureIndex(ctx) error` (creates `jobs_v1` + both aliases when `jobs` alias is missing; "already exists" = ok); `WriteIndices(ctx) ([]string, error)`; `Put(ctx, index, d) (bool, error)` (`PUT /{index}/_doc/{job_id}?version=N&version_type=external`; 409 → `false, nil`).

- [ ] **Step 1:** Unit tests in `projector_test.go`:
  - `TestFromEnvelope_Table`: created/updated full doc; closed/deleted tombstones; open event without `published_at` → `PublishedAt == env.OccurredAt`; draft without → nil.
  - `TestHandle_InvalidEnvelopeIsPermanent`: validator error → `kafka.IsPermanent(err)` and `err.Error()` does not contain payload text.
  - `TestHandle_WritesEveryWriteIndex`: fake store with `["jobs_v1","jobs_v2"]` → `Put` called for both; one returns `applied=false` → `Handle` returns nil.
  - `TestHandle_StoreErrorIsTransient`: `Put` error → returned error, `!kafka.IsPermanent`.
  - `TestHandle_AliasCachedForTTL`: two calls within TTL → `WriteIndices` called once.
- [ ] **Step 2:** `go test ./internal/projection` → FAIL.
- [ ] **Step 3:** Implement `projection` package.
- [ ] **Step 4:** Integration tests (`projection_test.go`, fresh index per test via `opensearch.Client` on a unique prefix is not possible with fixed alias names → each test calls `resetIndices(t)` that deletes `jobs_v*` then `EnsureIndex`):
  - `TestPut_DuplicateIsNoop`: put v1 twice → second `applied=false`, `_doc` version 1.
  - `TestPut_OutOfOrderKeepsNewest`: v3 then v2 → stored version 3, title of v3.
  - `TestPut_OldUpdateAfterTombstone`: tombstone v4 then full v3 → stored status `deleted`.
  - `TestEnsureIndex_Idempotent`: call twice → one index `jobs_v1`, aliases `jobs`, `jobs_write` both on it.
- [ ] **Step 5:** Implement `opensearch/client.go` + `index.json`; run unit + integration → PASS.
- [ ] **Step 6:** Commit `feat(job): add opensearch projection with external versioning and tombstones`.

### Task 4: OpenSearch search + facets

**Files:**
- Create: `services/job/internal/adapter/opensearch/search.go`, `search_test.go` (unit: query body builder golden JSON for q empty / q set / all filters)
- Modify: `services/job/internal/app/ports.go` (add types below)
- Test: `services/job/test/integration/opensearch_search_test.go`

**Interfaces:**
- In `app`: `type FacetCount struct{ Value string; Count int64 }`; `type Facets struct{ Skills, Locations, Levels []FacetCount }`; `type SearchQuery struct{ Filter SearchFilter; After json.RawMessage; Size int }`; `type SearchResult struct{ Jobs []domain.Job; Next json.RawMessage; Facets Facets }` (`Next` = `search_after` of the last hit when `len(Jobs) == Size`, else nil); `type SearchBackend interface{ Search(ctx context.Context, q SearchQuery) (SearchResult, error) }`.
- `func (c *Client) Search(ctx context.Context, q app.SearchQuery) (app.SearchResult, error)` — reads alias `jobs`; maps documents back to `domain.Job` (ID, RecruiterID, fields, Status, Version, PublishedAt); requests `Size` hits (pagination "more" decided by `Next != nil`, see Task 5).

- [ ] **Step 1:** Unit test `TestBuildQuery_Golden` (3 cases) asserting JSON equals spec §5.3 (filters, multi_match, sort, aggs, `search_after` only when `After` set).
- [ ] **Step 2:** Integration tests (seed via `Put`, then `POST /jobs/_refresh`):
  - `TestSearch_UnaccentedMatchesAccented`: title "Kỹ sư Go tại Hà Nội", `q="ky su"` and `q="ha noi"` → hit.
  - `TestSearch_SkillSynonym`: required skill "Golang", `q="go"` → hit; required "Kubernetes", `q="k8s"` → hit.
  - `TestSearch_ExcludesNonOpen`: draft, closed tombstone, deleted tombstone → not returned.
  - `TestSearch_FilterParityWithPostgres`: same 6 jobs inserted in Postgres (repo `Create`) and OpenSearch; for filters {skills `["GOLANG","rust"]`}, {location `"ha noi"`}, {level senior}, {salary_min 3000 with one null salary_max} → same ID sets from `JobRepo.Search` and `Client.Search`.
  - `TestSearch_PaginationNoDupNoGap`: 25 jobs, size 10 → three pages, 25 unique IDs, `Next` nil on the last.
  - `TestSearch_Facets`: counts per skill/location/level match seed.
- [ ] **Step 3:** Run → FAIL; implement `search.go`; run → PASS.
- [ ] **Step 4:** Commit `feat(job): search open jobs in opensearch with vietnamese analysis and facets`.

### Task 5: Query use case — breaker, fallback, tokens, proto

**Files:**
- Modify: `contracts/proto/smarthire/job/v1/job.proto` (`Facet`, `ListJobsFacets`, `ListJobsResponse.facets = 3`, `degraded = 4` per spec §5.3); run `make proto` (commit `gen/go`, openapi)
- Modify: `services/job/internal/app/{queries.go,cursor.go,service.go}`; `services/job/internal/adapter/grpc/mapping.go` (`pageToProto` maps facets/degraded)
- Create: `services/job/internal/adapter/opensearch/guard.go` (breaker wrapper)
- Test: `services/job/internal/app/queries_test.go`, `services/job/internal/adapter/opensearch/guard_test.go`, `services/job/internal/adapter/grpc/handler_test.go`

**Interfaces:**
- `app.Deps.Search SearchBackend` (nil ⇒ Postgres only, used by existing tests).
- `app.Page` gains `Facets Facets; Degraded bool`.
- Search token (ListOpen only): base64url JSON `{"b":"os","sa":<raw>}` or `{"b":"pg","c":<Cursor>}`; `func encodeSearchToken(t searchToken) string`, `func decodeSearchToken(s string) (searchToken, error)` (malformed → `ErrValidation "invalid page token"`). `ListMine` keeps `EncodeCursor/DecodeCursor`.
- `ListOpen`: if `Search != nil` → call it (with `After` only when token backend is `os`); success → page from result (`more` = `Next != nil`, fetch `Size` only), `Degraded=false`; error → log warn `{"error_type": fmt.Sprintf("%T", err)}` and run the existing Postgres path with `Degraded=true` (cursor only when token backend is `pg`).
- `opensearch.Guarded`: `func NewGuarded(b app.SearchBackend, failures uint32, open, timeout time.Duration) *Guarded` implementing `app.SearchBackend` via `gobreaker.CircuitBreaker[app.SearchResult]` (`ReadyToTrip: consecutive >= failures`, `Timeout: open`, `MaxRequests: 1`), each call under `context.WithTimeout(ctx, timeout)`.

- [ ] **Step 1:** Tests:
  - `TestListOpen_UsesSearchBackend`: fake backend returns 2 jobs + facets → page has them, `Degraded=false`, repo `Search` not called.
  - `TestListOpen_FallbackOnError`: backend error → repo called, `Degraded=true`, `Facets` empty.
  - `TestListOpen_TokenFromOtherBackend`: `pg` token while backend healthy → backend called with `After == nil`; `os` token while backend fails → repo called with `after == nil`.
  - `TestListOpen_MalformedToken`: `"x"` → `ErrValidation`.
  - `TestGuarded_OpensAfterFailures`: 5 failing calls → 6th returns `gobreaker.ErrOpenState` without calling inner; after `open` elapsed (use 50ms) one trial call reaches inner.
  - `TestGuarded_Timeout`: inner blocks on ctx → returns within timeout+margin with `context.DeadlineExceeded`.
  - `TestListJobs_MapsFacetsAndDegraded` (grpc handler).
- [ ] **Step 2:** Run → FAIL; implement; `make proto`; run → PASS; `make cover-job` ≥ 70%.
- [ ] **Step 3:** Commit `feat(job): read job search from opensearch with circuit breaker fallback to postgres`.

### Task 6: Rebuild + reconcile

**Files:**
- Create: `services/job/internal/projection/rebuild.go`, `reconcile.go`, `rebuild_test.go` (unit with fakes)
- Modify: `services/job/internal/adapter/opensearch/client.go` (admin ops), `services/job/internal/adapter/postgres/jobs.go` + `queries/jobs.sql` (`ScanAll`, `OpenVersions`; `make sqlc`)
- Test: `services/job/test/integration/rebuild_test.go`

**Interfaces:**
- `type projection.JobSource interface { ScanAll(ctx context.Context, afterID uuid.UUID, limit int) ([]domain.Job, error); OpenVersions(ctx context.Context) (map[uuid.UUID]int64, error) }` — `ScanAll` includes soft-deleted rows, ordered by `id`; `OpenVersions` = `status='open' AND deleted_at IS NULL`.
- `type projection.IndexAdmin interface { Indices(ctx) ([]string, error); CreateIndex(ctx, name string) error; AddToWriteAlias(ctx, index string) error; SwapAliases(ctx, newIndex string, old []string) error; DeleteIndex(ctx, name string) error; BulkPut(ctx, index string, docs []Document) error; OpenVersions(ctx) (map[uuid.UUID]int64, error) }` — implemented by `opensearch.Client`; `BulkPut` uses `_bulk` with external versions and ignores per-item 409; `SwapAliases` is one `_aliases` call.
- `type projection.RebuildOptions struct{ AliasTTL time.Duration; Batch int; KeepOld bool; Sleep func(time.Duration) }`; `func projection.Rebuild(ctx, src JobSource, idx IndexAdmin, o RebuildOptions) (newIndex string, err error)` — steps 1–6 of spec §6.3; next name = `jobs_v{max+1}`; waits `AliasTTL + 2s` after step 2.
- `type projection.Report struct{ PGOpen, IndexOpen int; Diff []string }`; `func projection.Reconcile(ctx, src JobSource, idx IndexAdmin) (Report, error)` — `Diff` holds up to 20 job IDs missing on one side or with different versions; `Report.OK() bool`.

- [ ] **Step 1:** Unit `TestRebuild_Sequence` (fake admin records calls: Create `jobs_v2` → AddToWriteAlias → Sleep(TTL+2s) → BulkPut batches → SwapAliases(`jobs_v2`, [`jobs_v1`]) → DeleteIndex `jobs_v1`; `KeepOld` skips delete); `TestReconcile_Diff`.
- [ ] **Step 2:** Integration:
  - `TestRebuild_ConcurrentUpdatesSurvive`: 50 jobs in Postgres + index; goroutine applies 200 random updates (bump version in Postgres via repo + `Projector.Handle` with the matching event) while `Rebuild` runs with `AliasTTL` 200ms → afterwards alias `jobs` → `jobs_v2`, `jobs_v1` gone, every doc version == Postgres version; concurrent `Client.Search` calls during rebuild never error.
  - `TestRebuild_SoftDeletedBecomeTombstones`.
  - `TestReconcile_DetectsMissingDoc`: delete one doc by hand → `!OK()`, Diff contains its ID; after `Put` → `OK()`.
- [ ] **Step 3:** Run → FAIL; implement; run → PASS.
- [ ] **Step 4:** Commit `feat(job): add zero-downtime index rebuild and reconcile for the job read model`.

### Task 7: Wiring — config, binaries, Docker, compose, fallback IT

**Files:**
- Modify: `services/job/internal/config/config.go` (server fields `OpenSearchURL`, `SearchTimeout`, `SearchBreakerFailures`, `SearchBreakerOpen`; `type ProjectorConfig` with `DBDSN`, `KafkaBrokers`, `OpenSearchURL`, `HTTPPort` (8012), `AliasTTL` (5s), `LogLevel`; `func LoadProjector() (ProjectorConfig, error)`)
- Modify: `services/job/internal/bootstrap/bootstrap.go` (server builds `opensearch.NewGuarded(client, …)` into `app.Deps.Search`; OpenSearch unreachable at start must NOT fail startup)
- Create: `services/job/internal/bootstrap/projector.go` (`func RunProjector(ctx, cfg ProjectorConfig) error`: EnsureIndex with retry until ctx done, `kafka.NewConsumer` group/topics per Global Constraints with handler `Projector.Handle`, HTTP `/healthz` 200, `/readyz` 200 after EnsureIndex succeeded; graceful shutdown like server), `func RunRebuild(ctx, cfg, keepOld bool) error`, `func RunReconcile(ctx, cfg) (projection.Report, error)`
- Create: `services/job/cmd/projector/main.go` (args: none → run; `rebuild [--keep-old]`; `reconcile` → print `pg_open=N index_open=M diff=[...]`, exit 2 when not OK)
- Modify: `services/job/Dockerfile` (build `/out/server` and `/out/projector`, copy both; ENTRYPOINT stays `/server`)
- Modify: `deploy/docker/docker-compose.yml` (`job` env `JOB_OPENSEARCH_URL`, depends `opensearch: service_healthy`; new `job-projector` with `image: smarthire/job:dev`, `entrypoint: ["/projector"]`, env `JOB_DB_DSN`, `JOB_KAFKA_BROKERS`, `JOB_OPENSEARCH_URL`, `OTEL_EXPORTER_OTLP_ENDPOINT`, depends `opensearch` healthy, `topics-init` + `job-migrate` completed, healthcheck none (distroless))
- Modify: `Makefile` (`search-rebuild: $(COMPOSE) run --rm job-projector rebuild`; `search-reconcile: $(COMPOSE) run --rm job-projector reconcile`; `.PHONY`)
- Test: `services/job/internal/config/config_test.go`, `services/job/test/integration/fallback_test.go`

**Interfaces:** Consumes everything above.

- [ ] **Step 1:** Tests: `TestLoadProjector_Defaults`; integration `TestListOpen_FallbackWhenOpenSearchStops` — own container via `startOpenSearch`, service with real `Guarded` (failures 2, open 1s, timeout 500ms) over real client + real Postgres repo; seed one open job in both; `stop()` the container; three `ListOpen` calls all succeed, last two have `Degraded=true` and return the job from Postgres.
- [ ] **Step 2:** Run → FAIL; implement; run → PASS.
- [ ] **Step 3:** `docker build -f services/job/Dockerfile .` OK; `make up` → `docker compose ps job-projector` running; `docker compose logs job-projector` shows consumer started, no errors.
- [ ] **Step 4:** Commit `feat(job): run the job projector in compose with rebuild and reconcile commands`.

### Task 8: Smoke, CI, docs

**Files:**
- Create: `tools/cmd/smoke/search.go` (`func (s *smoke) searchSteps() []namedStep`), wire after job steps in `main.go`
- Modify: `Makefile` `smoke` target: after the Go smoke, run `$(COMPOSE) run --rm job-projector reconcile`
- Modify: `.github/workflows/ci-go.yml` (job module: build OpenSearch image with buildx gha cache `scope=opensearch`, `load: true`, before integration tests; filter `job` adds `deploy/docker/opensearch/**`, `contracts/events/job.*`)
- Create: `docs/adr/0009-cqrs-job-search-read-model.md`; Modify: `docs/adr/README.md`, `README.md`, `CLAUDE.md`, `services/job/README.md` (if absent, create short one)

- [ ] **Step 1:** Smoke steps (recruiter token from existing job steps): create + publish JD title `"Kỹ sư Backend " + tag`, location `"Hà Nội"`, required `["Golang","Kubernetes"]`, description containing `tag` → `search finds new job` (poll `GET /v1/jobs?q=<tag>` every 500ms up to 15s, `degraded` false) → `unaccented and synonym` (`?q=ha noi <tag>` and `?q=go <tag>` both contain the id) → `facets` (`?q=<tag>` facets.skills has `golang`) → `closed job disappears` (close, poll up to 15s until absent).
- [ ] **Step 2:** `make down-v && make bootstrap && make infra-up obs-up up && make seed && make smoke` → all PASS, reconcile exit 0. Tempo: trace of `POST /v1/jobs` contains `job.created process` from service `job-projector`.
- [ ] **Step 3:** Fallback demo check: `docker compose stop opensearch`; `curl 127.0.0.1:8080/v1/jobs` → 200 with `"degraded":true`; `docker compose start opensearch`; after ≤ 30s `degraded` false again. `make search-rebuild` while smoke-created jobs exist → `curl 127.0.0.1:9200/_cat/aliases` shows `jobs` on `jobs_v2`.
- [ ] **Step 4:** ADR 0009 (Context/Decision/Consequences/Alternatives like 0001–0008): read model + projector binary; external versioning instead of processed_events; tombstones (deviation from spec §8.2) + cleanup debt; dual alias rebuild algorithm and its correctness argument; `published_at` added to v1; breaker/fallback and token behavior; facets normalized values; dev security plugin off. README: diagram (OpenSearch, job-projector), URL `127.0.0.1:9200`, `make search-rebuild|search-reconcile`, roadmap M2a ✅. CLAUDE.md: project state M2a, design/plan paths, ADR 0009, env vars, commands.
- [ ] **Step 5:** `make lint test test-int cover-job` → PASS; actionlint via `MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd):/repo" -w /repo rhysd/actionlint:1.7.7` → clean.
- [ ] **Step 6:** Commit `feat(smoke): cover opensearch job search` / `ci: build opensearch image for job integration tests` / `docs: add ADR 0009 and update docs for M2a`.

---

## Self-review notes
- Spec coverage: §4 → T1; §5.1–5.2 → T3; §5.3 → T4–T5; §5.4 → T5, T7; §6.1–6.2 → T3, T7; §6.3–6.4 → T6; §7 → T7; §8 → T2, T7, T8; §9 → T3–T8; §10 → T8.
- Names used across tasks: `projection.Document/FromEnvelope/FromJob/Projector/Store/JobSource/IndexAdmin/Rebuild/Reconcile/Report`, `opensearch.Client/New/EnsureIndex/WriteIndices/Put/Search/NewGuarded`, `app.SearchBackend/SearchQuery/SearchResult/Facets/FacetCount`, `config.ProjectorConfig/LoadProjector`, `bootstrap.RunProjector/RunRebuild/RunReconcile`.
