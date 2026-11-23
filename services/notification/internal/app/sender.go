package app

import (
	"context"
	"io"
	"log/slog"
	"time"
)

// Email retry policy (ADR 0012).
const (
	emailBackoffBase    = 5 * time.Second
	emailBackoffCeiling = 10 * time.Minute
	emailMaxAttempts    = 8
	maxLastError        = 200
)

// EmailBackoff is the delay after the attempt-th failed send (attempt ≥ 1):
// 5s·2^(attempt-1), capped at 10 minutes.
func EmailBackoff(attempt int) time.Duration {
	d := emailBackoffBase
	for i := 1; i < attempt && d < emailBackoffCeiling; i++ {
		d *= 2
	}
	return min(d, emailBackoffCeiling)
}

// SendDue claims due emails and sends each once: success marks it SENT, a
// failure schedules a retry, and the 8th failure marks it FAILED. It returns
// how many were claimed; only a failed claim is an error.
func (s *Service) SendDue(ctx context.Context) (int, error) {
	emails, err := s.Emails.ClaimDue(ctx, s.Now().UTC(), s.EmailLease, s.EmailBatch)
	if err != nil {
		return 0, err
	}
	for _, e := range emails {
		sendErr := s.Mailer.Send(ctx, e.To, e.Subject, e.HTML, e.Text)
		if ctx.Err() != nil {
			return len(emails), nil //nolint:nilerr // shutdown: the lease expires and the email is retried
		}
		now := s.Now().UTC()
		var markErr error
		switch attempts := e.Attempts + 1; {
		case sendErr == nil:
			markErr = s.Emails.MarkSent(ctx, e.ID, now)
			count(ctx, "emails.sent", "Emails delivered to the SMTP server")
		case attempts >= emailMaxAttempts:
			markErr = s.Emails.MarkFailed(ctx, e.ID, attempts, truncate(sendErr.Error()))
			count(ctx, "emails.failed", "Emails given up after all retries")
			s.Logger.Error("email given up", "email_id", e.ID, "notification_id", e.NotificationID, "attempts", attempts)
		default:
			markErr = s.Emails.MarkRetry(ctx, e.ID, attempts, now.Add(EmailBackoff(attempts)), truncate(sendErr.Error()))
			s.Logger.Warn("email send failed, will retry", "email_id", e.ID, "attempts", attempts)
		}
		if markErr != nil {
			// The lease expires and the email is tried again.
			s.Logger.Error("email state not saved", "email_id", e.ID, "err", markErr)
		}
	}
	return len(emails), nil
}

func truncate(s string) string {
	if len(s) > maxLastError {
		return s[:maxLastError]
	}
	return s
}

// EmailSender runs SendDue until nothing is due, then waits poll.
type EmailSender struct {
	svc  *Service
	poll time.Duration
	log  *slog.Logger
}

// NewEmailSender returns a sender loop; log may be nil.
func NewEmailSender(svc *Service, poll time.Duration, log *slog.Logger) *EmailSender {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &EmailSender{svc: svc, poll: poll, log: log}
}

// Run loops until ctx is cancelled; it returns nil on shutdown.
func (w *EmailSender) Run(ctx context.Context) error {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for ctx.Err() == nil {
			n, err := w.svc.SendDue(ctx)
			if err != nil {
				if ctx.Err() == nil {
					w.log.Error("email claim failed", "err", err)
				}
				break
			}
			if n == 0 {
				break
			}
		}
		t.Reset(w.poll)
	}
}
