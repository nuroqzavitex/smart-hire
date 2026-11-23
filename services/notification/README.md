# Notification service

Turns events into in-app notifications, pushes them over WebSocket and
mails the important ones (ADR 0012).

- Consumes `user.registered`, `job.created|updated|deleted`, `cv.parsed|parse_failed`,
  `application.submitted|status_changed`, `match.completed` (group `notification`), deduplicated by `event_id`.
- Replicas `users` (email for mail) and `jobs` (owner and title); `notifications`; `email_outbox`
  sent by an in-process loop over SMTP (Mailpit in compose) with backoff, `FAILED` after 8 attempts.
- gRPC `:9005` (`ListNotifications`, `MarkNotificationRead`, via the gateway at `/v1/notifications`);
  HTTP `:8005` (`/healthz`, `/readyz`, `/ws` — reached only through the gateway's ticketed `/v1/ws`).
- Config: `NOTIFICATION_*` (see `internal/config`).

```bash
go test ./...                                         # unit
go test -count=1 -tags integration ./test/integration # Postgres via testcontainers
make cover-notification                               # domain+app coverage ≥ 70%
```
