# M3 Application Saga Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Application service (Go) where candidates apply with a parsed CV and a background, self-written orchestration saga reserves quota at Candidate, scores via AI, and compensates on timeout / closed job.

**Architecture:** `POST /v1/applications` validates (Job, Candidate with the caller's JWT), writes `applications` + `saga_instances` + outbox in one tx, returns 202. An in-process worker claims due sagas with a lease (`next_run_at`, `FOR UPDATE SKIP LOCKED`), runs one step per claim (gRPC call with timeout / grpc-go retry policy / breaker for AI) and writes the outcome in one version-guarded tx. A consumer of `job.closed|job.deleted` flips running sagas to compensation.

**Tech Stack:** Go 1.27, pgx/v5 + sqlc, golang-migrate, grpc-go (+ service-config retry), sony/gobreaker/v2, franz-go via `pkg/kafka`, `pkg/outbox`, testcontainers.

**Spec:** `docs/superpowers/specs/2026-11-23-m3-application-saga-design.md`

## Global Constraints

- Module `github.com/nuroqzavitex/smarthire/services/application`, hexagonal (`domain` → `app` → `adapter` → `bootstrap`), copy Candidate's patterns (tx in ctx, outbox writer, idempotency, bootstrap shutdown order).
- Ports: gRPC `9004`, HTTP `8004`; env prefix `APPLICATION_`; DB `application_db` owned by role `application_svc`.
- Timeouts: Job/Candidate calls 3s, AI 5s. Retry ≤ 3 attempts, only `UNAVAILABLE`/`DEADLINE_EXCEEDED`, via grpc-go service config. AI breaker: open after 5 consecutive failures, half-open after 30s; `NotFound`/`InvalidArgument`/caller cancel are not failures.
- Saga deadline 10 min (RESERVE_QUOTA only); NotFound window for SCORE 2 min; lease 30s; poll 500ms; batch 10; step backoff 1s×2^n capped 30s.
- Candidate quota default `CANDIDATE_DAILY_APPLICATION_QUOTA=10`, day = UTC date; compose dev sets 50.
- Never log PII (only ids, statuses, reasons, error types). Never call real Anthropic/Gemini in tests/CI/smoke.
- Conventional Commits with scope; local commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- `apperr.ErrValidation` → 400, `ErrConflict` → 409, `ErrNotFound` → 404, `ErrUnavailable` → 503.

## Review Focus

1. Two concurrent submits with the same `Idempotency-Key` → exactly one application, both callers get it (unique-violation re-read path) — test in Task 4 + Task 7 integration.
2. A worker whose lease expired finishes its step after another worker re-claimed → its write is discarded (version guard), no double history/outbox — Task 6 unit + Task 7 integration.
3. `job.closed` arriving while a saga is mid-step → saga ends `COMPENSATED`, quota released even if reserve landed after — Task 6 unit, Task 7 integration.
4. Concurrent `ReserveQuota` for the same candidate at the limit never exceed the limit; late reserve after release is rejected (tombstone) — Task 2 integration.
5. AI breaker open → SCORE fails fast to `NEEDS_MANUAL_REVIEW` without waiting for the 5s timeout — Task 5 client test + Task 9 chaos.

---

### Task 1: Contracts (proto + events)

**Files:**
- Create: `contracts/proto/smarthire/application/v1/application.proto`
- Modify: `contracts/proto/smarthire/candidate/v1/candidate.proto` (internal quota RPCs)
- Create: `contracts/events/application.submitted.v1.json`, `contracts/events/application.status_changed.v1.json`, fixtures under `contracts/events/testdata/valid/` + `invalid/` following existing naming
- Modify: `pkg/event/eventschema/eventschema_test.go`
- Regenerate: `gen/go/...`, `contracts/openapi/...` (`make proto`)

**Interfaces — Produces:**
- `ApplicationService`: `SubmitApplication(SubmitApplicationRequest{job_id, cv_id}) returns (Application)` POST `/v1/applications` body `*`; `GetApplication({id}) returns (GetApplicationResponse{application, repeated HistoryEntry history})` GET `/v1/applications/{id}`; `ListApplications({job_id, status, page_size, page_token}) returns ({repeated Application applications, next_page_token})` GET `/v1/applications`; `UpdateApplicationStatus({id, status}) returns (Application)` PATCH `/v1/applications/{id}/status` body `*`.
- `enum ApplicationStatus { APPLICATION_STATUS_UNSPECIFIED, _PENDING, _SCREENING, _SCREENED, _NEEDS_MANUAL_REVIEW, _INTERVIEW, _OFFERED, _HIRED, _REJECTED, _CANCELLED }`.
- `message Application { id, job_id, candidate_id, cv_id, status, reason, optional double score, ScoreBreakdown breakdown, model_version, int64 version, created_at, updated_at }`; `ScoreBreakdown` own message with the same 6 fields as `ai.v1.ScoreBreakdown`.
- `HistoryEntry { optional ApplicationStatus from, ApplicationStatus to, actor, reason, at }`.
- Candidate: `rpc ReserveQuota(ReserveQuotaRequest{application_id, candidate_id}) returns (google.protobuf.Empty)`; `rpc ReleaseQuota(ReleaseQuotaRequest{application_id}) returns (google.protobuf.Empty)` — no HTTP annotation.
- Events (envelope `$ref` like the others): `application.submitted` payload `{application_id, job_id, candidate_id, recruiter_id, cv_id}` all uuid, required, `additionalProperties:false`; `application.status_changed` payload `{application_id, job_id, candidate_id, recruiter_id, from (status|null), to (status), reason (string|null), score (number 0–100|null), version (int ≥1), actor (string)}`, status enum = the 9 names without prefix.

- [ ] **Step 1:** Write `TestValidate_ApplicationSubmitted` and `TestValidate_ApplicationStatusChanged` in eventschema_test (valid fixtures pass; invalid fixture with bad `to` / missing `application_id` fails), mirroring `TestValidate_MatchCompleted`.
- [ ] **Step 2:** Run `cd pkg && go test ./event/eventschema -run Application` → FAIL (schema missing).
- [ ] **Step 3:** Add the two schemas + fixtures, the proto files; validate fields with `buf.validate` (uuid strings, `page_size` 0–100, `status` defined & not unspecified on update).
- [ ] **Step 4:** `make proto && cd contracts/proto && buf lint`; `cd pkg && go test ./event/eventschema` → PASS.
- [ ] **Step 5:** Commit `feat(contracts): add application service and quota rpcs and application events`.

### Task 2: Candidate quota

**Files:**
- Create: `services/candidate/migrations/000002_quota.{up,down}.sql`, `internal/adapter/postgres/queries/quota.sql`, `internal/adapter/postgres/quota.go`, `internal/app/quota.go`, `internal/app/quota_test.go`, `test/integration/quota_test.go`
- Modify: `internal/app/ports.go`, `service.go` (Deps `Quota QuotaRepo`, `DailyQuota int`), `internal/config/config.go` (`DailyApplicationQuota int env:"DAILY_APPLICATION_QUOTA" envDefault:"10"`), `internal/adapter/grpc/handler.go` + `PublicMethods`, bootstrap, `fakes_test.go`, Makefile `sqlc` (unchanged list), regenerate sqlc.

**Interfaces — Produces:**
- `app.QuotaRepo { Reserve(ctx, appID, candidateID uuid.UUID, day time.Time, limit int) (QuotaResult, error); Release(ctx, appID uuid.UUID) error }` (runs inside `WithTx`); `QuotaResult` ∈ `QuotaReserved | QuotaExhausted | QuotaAlreadyReleased`.
- `(*Service).ReserveQuota(ctx, appID, candidateID uuid.UUID) error` returns `domain.ErrQuotaExhausted` / `domain.ErrReservationReleased`; `(*Service).ReleaseQuota(ctx, appID uuid.UUID) error`.
- gRPC mapping: `ErrQuotaExhausted` → `codes.ResourceExhausted`, `ErrReservationReleased` → `codes.Aborted` (return `status.Error` from the handler). `PublicMethods` = the two full method names (internal, no JWT, ADR 0011).
- Tables per spec §5 (`daily_quota`, `quota_reservations` with `state CHECK IN ('RESERVED','RELEASED')`).

Algorithm (one tx): Reserve — `SELECT state FROM quota_reservations WHERE application_id=$1 FOR UPDATE`; RESERVED → Reserved; RELEASED → AlreadyReleased; none → `INSERT INTO daily_quota(candidate_id, day, used) VALUES ($c,$d,1) ON CONFLICT (candidate_id, day) DO UPDATE SET used = daily_quota.used + 1 WHERE daily_quota.used < $limit RETURNING used` (no row → Exhausted), then insert RESERVED (a concurrent insert of the same application_id → unique violation → retry the read once). Release — row RESERVED → RELEASED + `used = used - 1` for its (candidate, day); RELEASED → no-op; none → insert RELEASED tombstone (`ON CONFLICT DO NOTHING`, then re-read and apply).

- [ ] **Step 1:** Unit tests in `quota_test.go` with a fake QuotaRepo: `TestReserveQuota_UsesUTCDayAndLimit` (Now = 2026-11-24T01:30:00+07:00 → repo receives day 2026-11-23 (UTC date), limit = DailyQuota), `TestReserveQuota_MapsResults` (Exhausted → ErrQuotaExhausted, AlreadyReleased → ErrReservationReleased), `TestReleaseQuota_Delegates`. Handler test: ReserveQuota exhausted → `codes.ResourceExhausted`, released → `codes.Aborted`, invalid uuid → `InvalidArgument`.
- [ ] **Step 2:** `cd services/candidate && go test ./internal/...` → FAIL (undefined).
- [ ] **Step 3:** Implement migration, queries (`make sqlc`), repo, use cases, handler, config, bootstrap wiring.
- [ ] **Step 4:** Integration tests: `TestQuota_ReserveIdempotent` (twice same app → used=1), `TestQuota_LimitUnderConcurrency` (limit 3, 10 goroutines distinct apps → exactly 3 reserved, used=3), `TestQuota_ReleaseThenLateReserveAborted` (release unknown app → tombstone; reserve same app → AlreadyReleased, used unchanged), `TestQuota_ReleaseRestoresUsed`. Run `go test ./... && go test -tags integration ./test/integration -run Quota` → PASS.
- [ ] **Step 5:** Commit `feat(candidate): add daily application quota with idempotent reserve and release`.

### Task 3: Application skeleton, domain, persistence

**Files:**
- Create: `services/application/{go.mod,Dockerfile,README.md,sqlc.yaml,cmd/server/main.go}`, `migrations/000001_init.{up,down}.sql`, `internal/config/config.go`, `internal/domain/{application.go,status.go,saga.go,errors.go}`, `internal/domain/status_test.go`, `internal/adapter/postgres/{tx.go,outbox.go,idempotency.go,applications.go,sagas.go,queries/*.sql,sqlc/}`
- Modify: `go.work`, Makefile (`sqlc` loop + `cover-application`), `.golangci` nothing new.

**Interfaces — Produces (domain):**
- `type Status string` consts `StatusPending="PENDING"`, `StatusScreening`, `StatusScreened`, `StatusNeedsManualReview="NEEDS_MANUAL_REVIEW"`, `StatusInterview`, `StatusOffered`, `StatusHired`, `StatusRejected`, `StatusCancelled`; `ParseStatus(string) (Status, error)`; `CanRecruiterMove(from, to Status) bool` (spec §7 table).
- `Application{ID, JobID, CandidateID, RecruiterID, CVID uuid.UUID; Status Status; Reason string; Score *float64; Breakdown json.RawMessage; ModelVersion string; IdempotencyKey, RequestHash string; Version int64; CreatedAt, UpdatedAt time.Time}`.
- `HistoryEntry{From *Status; To Status; Actor, Reason string; At time.Time}`.
- `SagaStep` consts `StepReserveQuota="RESERVE_QUOTA"`, `StepScore="SCORE"`, `StepReleaseQuota="RELEASE_QUOTA"`; `SagaState` consts `SagaRunning`, `SagaCompensating`, `SagaCompleted`, `SagaCompensated`.
- `Saga{ID, ApplicationID, JobID uuid.UUID; Step SagaStep; State SagaState; Attempts int; NextRunAt, DeadlineAt, StepStartedAt time.Time; CancelReason, LastError string; Version int64}` (job_id denormalized for the job.closed lookup; Ruling vs spec `payload jsonb`: explicit columns `job_id`, `step_started_at`, `cancel_reason` instead).
- Reasons consts: `ReasonQuotaExceeded="quota_exceeded"`, `ReasonJobClosed="job_closed"`, `ReasonTimeout="timeout"`, `ReasonAIUnavailable="ai_unavailable"`, `ReasonNotMatched="not_matched"`.
- Errors: `ErrApplicationNotFound`, `ErrAlreadyApplied`, `ErrIdempotencyKeyConflict` (repo returns these for unique violations on the two indexes).

**Interfaces — Produces (app ports, `internal/app/ports.go`):**
- `ApplicationRepo { Create(ctx, domain.Application) error; Get(ctx, id) (domain.Application, error); GetForUpdate(ctx, id) (domain.Application, error); GetByKey(ctx, candidateID uuid.UUID, key string) (domain.Application, error); Update(ctx, domain.Application) error; List(ctx, ListFilter, after *Cursor, limit int) ([]domain.Application, error); AddHistory(ctx, appID uuid.UUID, h domain.HistoryEntry) error; History(ctx, appID uuid.UUID) ([]domain.HistoryEntry, error) }`; `ListFilter{CandidateID, RecruiterID, JobID *uuid.UUID; Status *domain.Status}`; `Cursor{At time.Time; ID uuid.UUID}` (order created_at DESC, id DESC).
- `SagaRepo { Create(ctx, domain.Saga) error; Claim(ctx, now time.Time, lease time.Duration, limit int) ([]domain.Saga, error); Save(ctx, s domain.Saga) (bool, error) /* WHERE id AND version = s.Version; sets version+1 */; CancelRunningForJob(ctx, jobID uuid.UUID, reason string, now time.Time) (int, error) }`.
- `TxManager`, `OutboxWriter`, `Idempotency` identical to Candidate's.

- [ ] **Step 1:** `status_test.go`: table test `TestCanRecruiterMove` covering every allowed edge true and e.g. PENDING→INTERVIEW, SCREENING→REJECTED, HIRED→REJECTED, SCREENED→CANCELLED, REJECTED→INTERVIEW false; `TestParseStatus` rejects "" and lowercase.
- [ ] **Step 2:** `cd services/application && go test ./internal/domain` → FAIL.
- [ ] **Step 3:** Implement domain, migration (spec §5 + `saga_instances.job_id`, `step_started_at`, `cancel_reason`, index `(job_id) WHERE state='RUNNING'`, `CHECK` on status/step/state), sqlc queries (Claim per spec §6.2 SQL), repos (map pg unique violation constraint names → domain errors), config:
  `GRPCPort 9004, HTTPPort 8004, DBDSN required, JWKSURL/JWTIssuer/JWTAudience (as Candidate), KafkaBrokers, OutboxInterval 500ms, LogLevel, JobAddr "job:9002", CandidateAddr "candidate:9003", AIAddr "ai-api:9100", CallTimeout 3s, AITimeout 5s, AIBreakerFailures 5, AIBreakerOpen 30s, SagaPoll 500ms, SagaBatch 10, SagaLease 30s, SagaDeadline 10m, ScoreNotFoundWindow 2m`.
- [ ] **Step 4:** `go test ./internal/domain` PASS; `go build ./...`; `make sqlc` regenerates without diff on rerun.
- [ ] **Step 5:** Commit `feat(application): scaffold service with domain state machine and postgres schema`.

### Task 4: Use cases — submit, get, list, update status

**Files:** Create `internal/app/{service.go,submit.go,query.go,status.go,events.go,cursor.go,fakes_test.go,submit_test.go,query_test.go,status_test.go}`.

**Interfaces:**
- Consumes Task 3 ports. New ports: `Jobs { GetJob(ctx, id uuid.UUID) (JobInfo, error) }` with `JobInfo{ID, RecruiterID uuid.UUID; Open bool}`; `CVs { GetCV(ctx, id uuid.UUID) (CVInfo, error) }` with `CVInfo{ID uuid.UUID; Parsed bool}`. Both return `apperr` NotFound / Unavailable.
- Produces: `NewService(Deps) *Service`, `Deps{Applications, Sagas, Outbox, Idem, Tx, Jobs, CVs, Quota, Scorer, Now, NewID, Logger, Wake func(), SagaDeadline, Lease, ScoreNotFoundWindow}`;
  `Submit(ctx, SubmitInput{JobID, CVID uuid.UUID; IdempotencyKey string}) (domain.Application, error)`;
  `Get(ctx, id uuid.UUID) (domain.Application, []domain.HistoryEntry, error)`;
  `List(ctx, ListInput{JobID *uuid.UUID; Status *domain.Status; PageSize int; PageToken string}) ([]domain.Application, string, error)` (default 20, max 100);
  `UpdateStatus(ctx, id uuid.UUID, to domain.Status) (domain.Application, error)`;
  `emitStatusChanged(ctx, a domain.Application, from *domain.Status, actor string, at time.Time) error`, `emitSubmitted`.

- [ ] **Step 1:** Tests (fakes in `fakes_test.go`, authz claims helpers like Candidate's):
  - `TestSubmit_CreatesPendingWithSagaAndEvents`: app PENDING, recruiter_id from JobInfo, saga `{Step RESERVE_QUOTA, State RUNNING, NextRunAt=now, DeadlineAt=now+10m, StepStartedAt=now, JobID}`, history NULL→PENDING actor=sub, outbox `application.submitted` + `application.status_changed` (schema-valid via `eventschema.Validate`), Wake called once.
  - `TestSubmit_SameKeySameRequestReturnsExisting` (no second create, no jobs/cvs calls), `TestSubmit_SameKeyDifferentRequestRejected` (ErrValidation), `TestSubmit_ConcurrentSameKeyReReads` (repo Create returns ErrIdempotencyKeyConflict → GetByKey result returned).
  - `TestSubmit_KeyRequired` ("" and 129 chars → ErrValidation), `TestSubmit_OnlyCandidates` (recruiter → ErrForbidden, none → ErrUnauthenticated).
  - `TestSubmit_JobNotOpen` (ErrConflict, nothing written), `TestSubmit_CVNotParsed` (ErrConflict), `TestSubmit_NotFoundPassThrough`, `TestSubmit_AlreadyApplied` (Create → ErrAlreadyApplied → ErrConflict "already applied"), `TestSubmit_DependencyUnavailable` (ErrUnavailable, nothing written).
  - `TestGet_Visibility`: owner candidate ok, recruiter of job ok, admin ok, other candidate/recruiter → ErrNotFound.
  - `TestList_ScopesByRole`: candidate filter CandidateID=sub (job_id honoured), recruiter RecruiterID=sub, admin no scope; page token round trip; bad token → ErrValidation.
  - `TestUpdateStatus_RecruiterMoves`: SCREENED→INTERVIEW updates version+1, history actor=sub, outbox status_changed with from/to; `TestUpdateStatus_InvalidTransition` → ErrConflict; `TestUpdateStatus_OtherRecruiterNotFound`; `TestUpdateStatus_CandidateForbidden`.
- [ ] **Step 2:** `go test ./internal/app` → FAIL.
- [ ] **Step 3:** Implement. `request_hash` = hex sha256 of `job_id + ":" + cv_id`. Validation/role checks happen before any remote call; GetByKey before Jobs/CVs.
- [ ] **Step 4:** `go test ./internal/app` PASS.
- [ ] **Step 5:** Commit `feat(application): submit, view, list and move applications`.

### Task 5: Outbound clients with timeout, retry policy, breaker

**Files:** Create `internal/adapter/{jobclient,cvclient,quotaclient,aiclient}/client.go` + `_test.go` (bufconn fakes like Job's aiclient test, mutex/atomic-safe), `internal/adapter/dialx/dialx.go` (retry service config).

**Interfaces:**
- `dialx.ServiceConfig(service string) string` returning JSON `{"methodConfig":[{"name":[{"service":"<service>"}],"retryPolicy":{"maxAttempts":3,"initialBackoff":"0.1s","maxBackoff":"1s","backoffMultiplier":2,"retryableStatusCodes":["UNAVAILABLE","DEADLINE_EXCEEDED"]}}]}`; bootstrap dials with `grpcx.Dial(addr, grpc.WithDefaultServiceConfig(...))`. Note: per-attempt deadline is the whole call deadline (grpc retry), acceptable.
- `jobclient.New(conn, timeout) *Client` implements `app.Jobs` (forwards `authorization`; NotFound → apperr NotFound; other → ErrUnavailable; Open = status == JOB_STATUS_OPEN).
- `cvclient.New(conn, timeout)` implements `app.CVs` (same; Parsed = CV_STATUS_PARSED).
- `quotaclient.New(conn, timeout)` implements `app.Quota`: `Reserve(ctx, appID, candidateID) error`, `Release(ctx, appID) error`; ResourceExhausted → `app.ErrQuotaExhausted`, Aborted → `app.ErrReservationReleased`, other errors returned wrapped (transient).
- `aiclient.New(conn, failures uint32, open, timeout time.Duration)` implements `app.Scorer { Score(ctx, cvID, jobID uuid.UUID) (Score, error) }`, `Score{Value float64; Breakdown json.RawMessage; ModelVersion string}`; NotFound → `app.ErrNotMatched`; any other failure incl. breaker open → `app.ErrScorerUnavailable`. Breaker as Job's aiclient (callerGone, healthy). Logs breaker state changes via `OnStateChange` (name, from, to).
- Errors in `internal/app/ports.go`: `ErrQuotaExhausted`, `ErrReservationReleased`, `ErrNotMatched`, `ErrScorerUnavailable`.

- [ ] **Step 1:** Tests: `TestAI_MapsScore`, `TestAI_BreakerOpensAfterFiveFailures` (5 Unavailable → 6th returns ErrScorerUnavailable without reaching server, measured fast < 100ms), `TestAI_NotFoundDoesNotTrip`, `TestAI_TimeoutIsUnavailable`; `TestQuota_MapsCodes`; `TestJobs_ForwardsAuthAndMapsStatus`; `TestCVs_MapsParsed`; `TestRetryPolicy_RetriesUnavailableOnly` (fake returns Unavailable twice then OK → 3 calls, success; InvalidArgument → 1 call).
- [ ] **Step 2:** `go test ./internal/adapter/...` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; also `go test -race` later in container (Task 8).
- [ ] **Step 5:** Commit `feat(application): add job, cv, quota and ai clients with retry policy and breaker`.

### Task 6: Saga orchestrator + worker + job.closed handling

**Files:** Create `internal/app/{saga.go,worker.go,jobs_events.go,saga_test.go,worker_test.go,jobs_events_test.go}`.

**Interfaces:**
- `(*Service).ProcessDue(ctx) (int, error)`: `Sagas.Claim(now, Lease, batch)` then `runStep` per saga sequentially.
- `runStep(ctx, s domain.Saga) error` decides per spec §6.3 and calls `finish(ctx, s, outcome)` which in one tx: `Sagas.Save` (false → discard silently, log debug) then `Applications.GetForUpdate` + `Update` + `AddHistory(actor "system")` + `emitStatusChanged`.
- Rules (exact): RESERVE_QUOTA: past DeadlineAt → `State=COMPENSATING, Step=RELEASE_QUOTA, CancelReason=timeout, NextRunAt=now`; OK → app SCREENING, `Step=SCORE, StepStartedAt=now, NextRunAt=now, Attempts=0`; ErrQuotaExhausted → app CANCELLED(quota_exceeded), saga COMPLETED; ErrReservationReleased → COMPENSATING/RELEASE_QUOTA with `CancelReason` kept if set, else `timeout`; other → `NextRunAt=now+backoff(Attempts)`, `LastError=<error type>`.
  SCORE: OK → app SCREENED + score, saga COMPLETED; ErrNotMatched and `now < StepStartedAt+ScoreNotFoundWindow` and `now < DeadlineAt`... (deadline not applied to SCORE) → retry with backoff; ErrNotMatched past window → NEEDS_MANUAL_REVIEW(not_matched); ErrScorerUnavailable/other → NEEDS_MANUAL_REVIEW(ai_unavailable), saga COMPLETED.
  RELEASE_QUOTA: OK → app CANCELLED(CancelReason), saga COMPENSATED; error → backoff retry, no deadline.
  `backoff(n) = min(1s·2^n, 30s)`.
- `Worker{svc *Service; poll time.Duration; wake chan struct{}}`, `NewWorker(svc, poll) *Worker`, `(*Worker).Wake()` (non-blocking send), `(*Worker).Run(ctx) error` (loop: ProcessDue until it returns 0, then wait poll/wake/ctx; ProcessDue error → log, continue).
- `(*Service).HandleJobEvent(ctx, raw []byte) error`: decode envelope; types `job.closed`/`job.deleted` with `payload.job_id`; inside tx `Idem.Once(event_id)` then `Sagas.CancelRunningForJob(jobID, job_closed, now)` (sets COMPENSATING, RELEASE_QUOTA, NextRunAt=now, version+1); then Wake. Bad JSON / missing job_id → `ErrPoisonEvent`.

- [ ] **Step 1:** Tests (fakes; fake SagaRepo.Save honours versions):
  `TestSaga_ReserveOKMovesToScoring`, `TestSaga_QuotaExhaustedCancels` (history PENDING→CANCELLED reason quota_exceeded, no Release call), `TestSaga_ReserveTransientBacksOff` (NextRunAt = now+1s, then +2s on attempt 2, capped 30s at attempt 10), `TestSaga_ReserveDeadlineCompensates` (then next run calls Release and ends CANCELLED(timeout), COMPENSATED), `TestSaga_ScoreOKScreened` (score/breakdown/model on app, status_changed has score), `TestSaga_ScoreUnavailableManualReview` (no Release), `TestSaga_ScoreNotMatchedRetriesThenManualReview` (within 2m retry; at +2m1s → NEEDS_MANUAL_REVIEW not_matched), `TestSaga_ScorePastDeadlineStillManualReviewNotCancel`, `TestSaga_StaleVersionDiscarded` (Save false → no app update/outbox/history), `TestSaga_ReleaseRetriesForever`, `TestHandleJobEvent_CancelsRunningSagas` (+ duplicate event_id no second call; poison → ErrPoisonEvent; other event types ignored), `TestWorker_WakeTriggersImmediateRun`.
- [ ] **Step 2:** `go test ./internal/app` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; `make cover-application` ≥ 70%.
- [ ] **Step 5:** Commit `feat(application): orchestrate the submit saga with compensation and recovery`.

### Task 7: gRPC handler, consumer, bootstrap, integration tests

**Files:** Create `internal/adapter/grpc/{handler.go,mapping.go,handler_test.go}`, `internal/adapter/consumer/handler.go`, `internal/adapter/httpx/handler.go` (copy Candidate), `internal/bootstrap/bootstrap.go`, `test/integration/{main_test.go,repo_test.go,saga_test.go}`.

**Interfaces:**
- Handler reads `idempotency-key` from incoming metadata; maps domain ↔ proto (`APPLICATION_STATUS_` prefix). `PublicMethods = []string{}`.
- Consumer: `Group = "application"`, `Topics = []string{"job.closed","job.deleted"}`, poison → `kafka.Permanent`.
- Bootstrap = Candidate's shape + `Worker.Run` goroutine awaited on shutdown; `Deps.Wake = worker.Wake` (construct worker after service via a small closure).

- [ ] **Step 1:** Handler tests: missing metadata key → InvalidArgument; status mapping both ways; history mapping. Integration (testcontainers Postgres, migrations, real repos, fake Quota/Scorer in-process):
  `TestRepo_UniqueIndexes` (same key → ErrIdempotencyKeyConflict; same job+candidate → ErrAlreadyApplied; after CANCELLED a new application allowed),
  `TestClaim_TwoWorkersNoOverlap` (20 due sagas, two concurrent Claim(limit 10) → disjoint, union 20),
  `TestClaim_LeaseExpiryReclaims` (claim, don't save, advance `now` past lease → claimed again with Attempts=2),
  `TestSaga_EndToEndScreened` (Submit via service with fake Jobs/CVs → ProcessDue twice → SCREENED, outbox has 3 rows),
  `TestSaga_CandidateDownUntilDeadlineCancelled` (quota fake always Unavailable, deadline 1s → eventually CANCELLED(timeout), Release called),
  `TestSaga_JobClosedMidway` (HandleJobEvent after reserve → CANCELLED(job_closed), Release called),
  `TestSubmit_ConcurrentSameKeyOneRow` (10 goroutines → 1 row, all return same id).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** `go test ./... && go test -count=1 -tags integration ./test/integration` PASS.
- [ ] **Step 5:** Commit `feat(application): serve grpc, consume job events and run the saga worker`.

### Task 8: Gateway, compose, CI

**Files:** Modify `services/gateway/internal/config/config.go` (`ApplicationGRPCAddr env:"APPLICATION_GRPC_ADDR" envDefault:"application:9004"`), `cmd/gateway/main.go`, `internal/router/router.go` (+ test): register handler, header matcher adds `idempotency-key`, 202 for `ApplicationService_SubmitApplication_FullMethodName`, `ConnsReady` includes it. Create `deploy/docker/init/postgres/05-create-application-db.sql` (idempotent: `SELECT ... WHERE NOT EXISTS ... \gexec`), modify `docker-compose.infra.yml` (env), `docker-compose.yml` (`application-migrate`, `application`, gateway env, `CANDIDATE_DAILY_APPLICATION_QUOTA: 50`), `create-topics.sh` (`application.submitted`, `application.status_changed`), `.env.example` (`APPLICATION_DB_PASSWORD=application-dev-only`), Makefile (`migrate`, `up` wait for `/v1/applications` = 401, `db-init` target running all init scripts' application part on an existing volume, `cover-application`), `.github/workflows/ci-go.yml` (filter + matrix + docker + coverage gate).

- [ ] **Step 1:** Router tests: `TestRouter_ForwardsIdempotencyKey` (fake ApplicationService server records metadata), `TestRouter_SubmitApplicationReturns202`.
- [ ] **Step 2:** `cd services/gateway && go test ./...` → FAIL.
- [ ] **Step 3:** Implement gateway + deploy changes.
- [ ] **Step 4:** Gateway tests PASS; `make lint`; `make test`; race run in container for application/candidate/gateway; `make db-init up` brings `application` up and `curl -s -o /dev/null -w '%{http_code}' localhost:8080/v1/applications` → 401.
- [ ] **Step 5:** Commit `feat(deploy): wire application service into gateway, compose and ci`.

### Task 9: Smoke, chaos, docs

**Files:** Create `tools/cmd/smoke/applications.go`, `tools/cmd/smoke/chaos.go` (flag `-chaos-ai`), modify `tools/cmd/smoke/main.go`, Makefile (`chaos-ai`: stop ai-api → `go run ./tools/cmd/smoke -chaos-ai ...` → start ai-api, start runs even on failure), `docs/adr/0011-application-saga-orchestration.md`, `docs/adr/README.md`, `README.md`, `CLAUDE.md`, `services/application/README.md`.

- [ ] **Step 1:** Smoke steps (reuse matching job+CV helpers): submit → 202 PENDING; same key → same id; new key same job → 409; poll GET until SCREENED with score > 0 (≤ 45s); history contains PENDING, SCREENING, SCREENED; recruiter list includes it; recruiter PATCH INTERVIEW → OFFERED ok; PATCH HIRED→... invalid (OFFERED→INTERVIEW) → 409; other candidate GET → 404; Kafka has `application.status_changed` keyed by the id.
  Chaos: with ai-api stopped, create 6 open jobs + submit 6 applications → all reach NEEDS_MANUAL_REVIEW reason ai_unavailable within 60s; the last 1 resolves in < 3s after reaching SCORE (breaker open) — assert via total elapsed of the final application < 5s from submit.
- [ ] **Step 2:** `make smoke` against the running stack → new steps FAIL before wiring (verify), then PASS; `make chaos-ai` PASS; `make smoke` again PASS (ai-api back).
- [ ] **Step 3:** Docs: ADR 0011 (A+1, lease worker, version guard, idempotency in Postgres vs Redis, internal quota RPC without JWT, why quota stays in Candidate, no Temporal, deadline only on RESERVE_QUOTA), update CLAUDE.md project state / milestone list / commands (`make cover-application`, `make chaos-ai`, `make db-init`), README.
- [ ] **Step 4:** `make lint test test-int` green (note known flakes from M2b if they recur).
- [ ] **Step 5:** Commit `feat(smoke): cover applications end to end and add ai chaos run` and `docs: add ADR 0011 and update docs for M3`.
