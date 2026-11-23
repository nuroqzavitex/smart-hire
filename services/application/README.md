# Application service

Candidates apply to open jobs with a parsed CV; recruiters move applications
through the pipeline. Submitting runs an orchestration saga (spec §6.4,
ADR 0011): reserve the daily quota at Candidate, score with the AI service,
and compensate (release the quota, cancel) on timeout or when the job is
closed. AI failures send the application to `NEEDS_MANUAL_REVIEW`.

- gRPC `:9004` (`smarthire.application.v1.ApplicationService`), HTTP `:8004` (`/healthz`, `/readyz`).
- One process: gRPC API, saga worker (lease on `saga_instances.next_run_at`), outbox relay,
  consumer of `job.closed|job.deleted` (group `application`).
- Config: `APPLICATION_*` (see `internal/config`).

```bash
go test ./...                                         # unit
go test -count=1 -tags integration ./test/integration # Postgres via testcontainers
make cover-application                                # domain+app coverage ≥ 70%
```
