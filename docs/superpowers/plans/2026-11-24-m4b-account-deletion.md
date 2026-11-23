# M4b Account Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:executing-plans (inline). Steps use checkbox (`- [ ]`) syntax.

**Goal:** `DELETE /v1/me` xoá hoặc ẩn danh dữ liệu của user ở cả 6 service bằng saga choreography
(`user.deleted` → 5 × `user.data_purged`). Auth đóng vai tracker, Gateway giữ denylist.

**Architecture:**
- Auth ghi `deletion_requests` cùng outbox `user.deleted` trong một tx.
- Mỗi service purge theo `user_id` trong một tx gồm `processed_events` và outbox `user.data_purged`.
- Auth consumer gom đủ 5 rồi xoá cứng user. Vòng tracker phát lại và đánh STUCK.

**Tech Stack:** Go 1.27 (pgx/sqlc, franz-go, go-redis), Python 3.12 (SQLAlchemy, confluent-kafka, Qdrant), buf.

**Spec:** `docs/superpowers/specs/2026-11-24-m4b-account-deletion-design.md`

## Global Constraints

- Không log PII; lỗi của consumer không được chứa giá trị payload.
- Topics: `user.deleted`, `user.data_purged` (cùng `.dlq`), key = `user_id`, event_version 1.
- Tên service trong `user.data_purged`: `candidate|job|application|ai|notification`. Auth expected = cả 5.
- UUID "đã xoá" = `00000000-0000-0000-0000-000000000000`; actor trong history = `deleted-user`.
- Env mới:
  - `AUTH_DELETION_SWEEP=1m`, `AUTH_DELETION_REPUBLISH_AFTER=15m`, `AUTH_DELETION_STUCK_AFTER=24h`;
  - `GATEWAY_DENYLIST_TTL=1h`.
- Migrations append-only; coverage domain+app ≥ 70%; thêm test cho mỗi purge.

## Review Focus

1. **`user.deleted` mới cho user đã purge xong**: phải phát lại `user.data_purged`, không báo lỗi.
   Test idempotent trong từng service.
2. **Kết quả parse hoặc `cv.updated` cũ đến sau khi purge**: không được tạo lại PII.
   Test: candidate (stale), matcher (tombstone version), notification (user tombstone).
3. **Saga M3 đang chạy khi purge application**: worker không được ghi đè.
   Test: saga bị COMPENSATED, `version` tăng.
4. **Redis lỗi ở Gateway**:
   - khi ghi denylist → 500 và DeleteMe gọi lại được;
   - khi đọc denylist → fail-open.
5. **`user.data_purged` trùng hoặc đến sau `COMPLETED`**: không lỗi, không xoá hai lần.

---

### Task 1: Contracts + pkg

**Files:**
- `contracts/events/user.deleted.v1.json`, `contracts/events/user.data_purged.v1.json`, `contracts/events/cv.updated.v1.json` (enum thêm DELETED)
- `contracts/proto/smarthire/auth/v1/auth.proto` (DeleteMe, GetDeletionRequest) → `make proto`
- `pkg/event/userdeletion/userdeletion.go` + test
- `deploy/docker/init/redpanda/create-topics.sh`

**Interfaces (Produces):**
```go
package userdeletion
const TopicDeleted = "user.deleted"; const TopicPurged = "user.data_purged"
const (ServiceCandidate="candidate"; ServiceJob="job"; ServiceApplication="application"; ServiceAI="ai"; ServiceNotification="notification")
var Services = []string{candidate, job, application, ai, notification}
var DeletedUserID = uuid.Nil
type Deleted struct{ UserID uuid.UUID; Role string; RequestID uuid.UUID; RequestedAt time.Time }
type Purged  struct{ UserID uuid.UUID; Service string; RequestID uuid.UUID; PurgedAt time.Time }
func ParseDeleted(env event.Envelope) (Deleted, error)   // error wraps ErrInvalid, no values
func ParsePurged(env event.Envelope) (Purged, error)
func NewDeletedEvent(ctx, source string, d Deleted, now time.Time) (event.Envelope, error)
func NewPurgedEvent(ctx, source string, d Deleted, service string, now time.Time) (event.Envelope, error)
var ErrInvalid = errors.New("invalid user deletion event")
```
- [ ] Test: round-trip New*/Parse*; bad uuid/unknown service/wrong type → ErrInvalid; JSON payload keys = schema (`user_id, role, deletion_request_id, requested_at` / `user_id, service, deletion_request_id, purged_at`), validated against the schema files via `pkg/event/eventschema`.
- [ ] Implement, `make proto`, `go build ./...` in gen/go, commit `feat(contracts): add user deletion events and DeleteMe rpc`.

### Task 2: Auth — DeleteMe, tracker, consumer

**Files:**
- migrations `000003_deletion_requests`
- `internal/domain/deletion.go`
- `internal/app/deletion.go`, `internal/app/ports.go`
- postgres queries + repos `deletion.go`, `idempotency.go`
- grpc handler
- `internal/adapter/consumer/handler.go`
- bootstrap (consumer, sweeper loop, gauge), config

**Interfaces:**
```go
// domain
type DeletionStatus string // PENDING|COMPLETED|STUCK
type DeletionRequest struct{ ID, UserID uuid.UUID; Role authz.Role; Status DeletionStatus; Expected, Purged []string; RequestedAt, LastPublishedAt time.Time; CompletedAt *time.Time }
func (r *DeletionRequest) MarkPurged(service string, now time.Time) (done bool, err error) // unknown service → ErrUnknownService
// app
func (s *Service) DeleteMe(ctx, userID uuid.UUID, password string) (domain.DeletionRequest, error)
func (s *Service) GetDeletionRequest(ctx, id uuid.UUID) (domain.DeletionRequest, error) // admin claims
func (s *Service) HandleDataPurged(ctx, env event.Envelope) error
func (s *Service) SweepDeletions(ctx) (republished, stuck int, err error)
// ports
type DeletionRepo interface{ Create; GetActiveByUser(ctx,userID) (DeletionRequest, error) /*ErrNotFound*/; Get; GetForUpdate; Update; ClaimSweep(ctx, republishBefore, stuckBefore time.Time, limit int) ([]DeletionRequest, error); CountByStatus(ctx) (map[string]int64, error) }
UserRepo += SetStatus(ctx,id,status,at) error; Delete(ctx,id) error
RefreshTokenRepo += RevokeAllForUser(ctx,userID,at) error
OutboxWriter += DeleteUserRegistered(ctx,userID) error (on the postgres outbox adapter)
Idempotency interface{ Once(ctx, eventID) (bool, error) }
```
- [ ] Tests (app, fakes):
  - `TestDeleteMe_MarksPendingRevokesAndEmits`
  - `TestDeleteMe_WrongPassword401`
  - `TestDeleteMe_AdminForbidden`
  - `TestDeleteMe_RepeatReturnsSameRequest`
  - `TestHandleDataPurged_CompletesAfterAllFive` (user deleted, registered outbox deleted)
  - `TestHandleDataPurged_DuplicateAndAfterCompleted`
  - `TestHandleDataPurged_UnknownServicePoison`
  - `TestSweep_RepublishesAndMarksStuck`
  - `TestGetDeletionRequest_AdminOnly`
- [ ] Domain tests for MarkPurged (set semantics).
- [ ] Integration: `TestDeletion_EndToEnd` (repo: create → 5 purges → user row gone, request COMPLETED), migrations up/down.
- [ ] Commit `feat(auth): track account deletion requests and hard-delete after all services purge`.

### Task 3: Gateway — 202 + denylist

**Files:** `internal/denylist/denylist.go` (+test with miniredis), `internal/middleware/auth.go` (Denylist mw), router (202 + hook), config `DENYLIST_TTL`, main.
**Interfaces:** `type Denylist struct; func New(rdb redis.Cmdable, ttl time.Duration) *Denylist; Add(ctx, userID) error; Contains(ctx, userID) (bool, error)`; `middleware.Denylist(checker interface{Contains}, log) func(http.Handler) http.Handler`.
- [ ] Tests:
  - `TestDenylist_AddContainsTTL`
  - `TestDenylistMiddleware_RejectsListedUser` (→ 401 via RejectInvalidAuth)
  - `TestDenylistMiddleware_FailOpenOnRedisError`
  - router test: DELETE /v1/me → 202, user then denylisted; Redis down → 500.
- [ ] Commit `feat(gateway): deny tokens of users pending deletion`.

### Task 4: Candidate purge

**Files:**
- migration `000003_cv_deleted` (CHECK adds DELETED)
- `internal/domain/cv.go` (`Tombstone(now)`, ApplyParsed/ApplyFailed on DELETED → ErrStaleResult)
- `internal/app/purge.go`
- repos: `ListAllByCandidate`, `DeleteProfile`, `DeleteQuota`
- consumer dispatch + Topics + user.deleted
- read paths hide DELETED

**Interfaces:** `func (s *Service) HandleUserDeleted(ctx, env event.Envelope) error`.
- [ ] Tests:
  - `TestPurge_DeletesObjectsTombstonesAndEmits` (cv.updated DELETED + user.data_purged)
  - `TestPurge_StorageErrorRetries` (no tx)
  - `TestPurge_RepeatEmitsPurgedAgain`
  - `TestCV_StaleResultAfterTombstone`
  - `TestGetCV_DeletedIsNotFound`
- [ ] Integration repo test for the purge queries.
- [ ] Commit `feat(candidate): purge profile, cv files and quota on user.deleted`.

### Task 5: Job purge

**Files:**
- migration `000002_processed_events`
- `internal/app/purge.go`
- repo `ListLiveByRecruiterForUpdate`, `Purge`
- `internal/adapter/consumer`
- bootstrap consumer (group `job-user-deleted`)

**Interfaces:** `func (s *Service) HandleUserDeleted(ctx, env event.Envelope) error`; domain `func (j *Job) Purge(now time.Time)` (closed + deleted_at + recruiter nil + version+1).

- [ ] Tests:
  - `TestPurge_DeletesRecruiterJobsAndEmits` (job.deleted per job + user.data_purged)
  - `TestPurge_CandidateUserOnlyEmitsPurged`
  - `TestPurge_DuplicateEventIgnored`
- [ ] Integration test.
- [ ] Commit `feat(job): delete and anonymise a deleted recruiter's jobs`.

### Task 6: Application purge

**Files:**
- `internal/app/purge.go`
- postgres `PurgeCandidate`, `PurgeRecruiter` (SQL in one tx)
- consumer Topics += user.deleted, dispatch

**Interfaces:** `func (s *Service) HandleUserDeleted(ctx, env event.Envelope) error`; port `type PurgeRepo interface{ PurgeUser(ctx, userID uuid.UUID, now time.Time) (PurgeStats, error) }`.
- [ ] Tests:
  - `TestPurge_EmitsPurged`
  - `TestPurge_Duplicate`
  - Integration `TestPurgeUser_PseudonymisesAndCancels`:
    - live app → CANCELLED, saga COMPENSATED with a bumped version;
    - candidate_id changes and differs per app;
    - history actor → `deleted-user`;
    - recruiter_id nil;
    - a terminal app keeps its status.
- [ ] Commit `feat(application): pseudonymise applications of deleted users`.

### Task 7: Notification purge

**Files:**
- migration `000002_outbox_user_tombstone` (outbox + `users.deleted`)
- `internal/app/purge.go`
- repo `PurgeUser`
- events.go skips tombstoned recipients
- hub `Disconnect(userID)`
- bootstrap relay + producer
- consumer Topics

- [ ] Tests:
  - `TestPurge_DeletesAndEmits`
  - `TestLateEventForDeletedUserCreatesNothing`
  - `TestHub_DisconnectClosesUserConns`
  - integration `TestRepo_PurgeUser`
- [ ] Commit `feat(notification): purge notifications and email of deleted users`.

### Task 8: AI purge (Python)

**Files:**
- `migrations/versions/0003_purge.py` (`cv_parse_results.candidate_id`)
- `domain/events.py` (UserDeletedPayload, DataPurgedPayload, cv.updated DELETED)
- `services/matcher.py` (user.deleted handling, DELETED cv)
- `adapters/match_store.py` (purge queries)
- worker store guard
- `main_matcher.py` topics

- [ ] Tests:
  - `test_user_deleted_purges_and_emits`
  - `test_user_deleted_repeat_emits_again`
  - `test_cv_deleted_removes_vector_and_parse_results`
  - `test_stale_cv_updated_after_purge_ignored`
  - schema lock-step tests for the new payloads
  - integration store purge test
- [ ] Commit `feat(ai): purge vectors, matches and parse results on user.deleted`.

### Task 9: Deploy, smoke, docs

**Files:**
- compose env
- `prometheus/rules/alerts.yml` (`AccountDeletionStuck`)
- `docs/runbooks/account-deletion-stuck.md`
- `tools/cmd/smoke/deletion.go`
- `tools/cmd/obscheck` (8 rules)
- ADR 0014, README, CLAUDE.md

- [ ] Smoke flow per spec §8.
- [ ] `make up`, `make smoke`, `make obs-check` green.
- [ ] Commit `feat(smoke): cover account deletion end to end` and `docs: ...`.
