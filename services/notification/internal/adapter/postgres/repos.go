package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

// Repo implements app.UserRepo, app.JobRepo, app.NotificationRepo and
// app.EmailRepo on one pool.
type Repo struct{ pool *pgxpool.Pool }

// NewRepo returns a Repo over pool.
func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Users returns the user replica view of r.
func (r *Repo) Users() app.UserRepo { return userRepo{r} }

// Jobs returns the job replica view of r.
func (r *Repo) Jobs() app.JobRepo { return jobRepo{r} }

// Notifications returns the notification store view of r.
func (r *Repo) Notifications() app.NotificationRepo { return notificationRepo{r} }

// Emails returns the email outbox view of r.
func (r *Repo) Emails() app.EmailRepo { return emailRepo{r} }

type userRepo struct{ *Repo }

func (r userRepo) Upsert(ctx context.Context, u domain.User) error {
	if err := queries(ctx, r.pool).UpsertUser(ctx, sqlc.UpsertUserParams{UserID: u.ID, Email: u.Email, Role: u.Role}); err != nil {
		return fmt.Errorf("upsert user: %w", err)
	}
	return nil
}

func (r userRepo) Get(ctx context.Context, id uuid.UUID) (domain.User, error) {
	row, err := queries(ctx, r.pool).GetUser(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.User{}, domain.ErrUserNotFound
	}
	if err != nil {
		return domain.User{}, fmt.Errorf("get user: %w", err)
	}
	return domain.User{ID: row.UserID, Email: row.Email, Role: row.Role, Deleted: row.Deleted}, nil
}

func (r userRepo) Tombstone(ctx context.Context, id uuid.UUID) error {
	if err := queries(ctx, r.pool).TombstoneUser(ctx, id); err != nil {
		return fmt.Errorf("tombstone user: %w", err)
	}
	return nil
}

func (r jobRepo) ClearRecruiter(ctx context.Context, recruiterID uuid.UUID) error {
	if err := queries(ctx, r.pool).ClearJobRecruiter(ctx, &recruiterID); err != nil {
		return fmt.Errorf("clear job recruiter: %w", err)
	}
	return nil
}

func (r notificationRepo) DeleteByUser(ctx context.Context, userID uuid.UUID) (int, error) {
	q := queries(ctx, r.pool)
	if err := q.DeleteUserEmails(ctx, userID); err != nil {
		return 0, fmt.Errorf("delete emails: %w", err)
	}
	n, err := q.DeleteUserNotifications(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("delete notifications: %w", err)
	}
	return int(n), nil
}

type jobRepo struct{ *Repo }

func (r jobRepo) Upsert(ctx context.Context, j domain.Job) (bool, error) {
	p := sqlc.UpsertJobParams{JobID: j.ID, Title: j.Title, Version: j.Version, Deleted: j.Deleted}
	if j.RecruiterID != uuid.Nil {
		p.RecruiterID = &j.RecruiterID
	}
	n, err := queries(ctx, r.pool).UpsertJob(ctx, p)
	if err != nil {
		return false, fmt.Errorf("upsert job: %w", err)
	}
	return n == 1, nil
}

func (r jobRepo) Get(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	row, err := queries(ctx, r.pool).GetJob(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	j := domain.Job{ID: row.JobID, Title: row.Title, Version: row.Version, Deleted: row.Deleted}
	if row.RecruiterID != nil {
		j.RecruiterID = *row.RecruiterID
	}
	return j, nil
}

type notificationRepo struct{ *Repo }

func (r notificationRepo) Create(ctx context.Context, n domain.Notification) error {
	data, err := json.Marshal(n.Data)
	if err != nil {
		return fmt.Errorf("encode data: %w", err)
	}
	if err := queries(ctx, r.pool).CreateNotification(ctx, sqlc.CreateNotificationParams{
		ID: n.ID, UserID: n.UserID, Type: string(n.Type), Title: n.Title, Body: n.Body, Data: data, CreatedAt: n.CreatedAt,
	}); err != nil {
		return fmt.Errorf("create notification: %w", err)
	}
	return nil
}

func (r notificationRepo) List(ctx context.Context, userID uuid.UUID, unreadOnly bool, after *app.Cursor, limit int) ([]domain.Notification, error) {
	p := sqlc.ListNotificationsParams{UserID: userID, UnreadOnly: unreadOnly, RowLimit: int32(limit)} //nolint:gosec // limit ≤ 101
	if after != nil {
		p.AfterAt, p.AfterID = &after.At, &after.ID
	}
	rows, err := queries(ctx, r.pool).ListNotifications(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	out := make([]domain.Notification, len(rows))
	for i, row := range rows {
		if out[i], err = toNotification(row); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r notificationRepo) CountUnread(ctx context.Context, userID uuid.UUID) (int, error) {
	n, err := queries(ctx, r.pool).CountUnread(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("count unread: %w", err)
	}
	return int(n), nil
}

func (r notificationRepo) MarkRead(ctx context.Context, id, userID uuid.UUID, at time.Time) (domain.Notification, error) {
	row, err := queries(ctx, r.pool).MarkRead(ctx, sqlc.MarkReadParams{ID: id, UserID: userID, At: at})
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Notification{}, domain.ErrNotificationNotFound
	}
	if err != nil {
		return domain.Notification{}, fmt.Errorf("mark read: %w", err)
	}
	return toNotification(row)
}

func toNotification(row sqlc.Notification) (domain.Notification, error) {
	n := domain.Notification{ID: row.ID, UserID: row.UserID, Type: domain.Type(row.Type), Title: row.Title,
		Body: row.Body, ReadAt: row.ReadAt, CreatedAt: row.CreatedAt}
	if err := json.Unmarshal(row.Data, &n.Data); err != nil {
		return domain.Notification{}, fmt.Errorf("decode data: %w", err)
	}
	return n, nil
}

type emailRepo struct{ *Repo }

func (r emailRepo) Enqueue(ctx context.Context, e domain.Email) error {
	if err := queries(ctx, r.pool).EnqueueEmail(ctx, sqlc.EnqueueEmailParams{
		ID: e.ID, NotificationID: e.NotificationID, ToAddr: e.To, Subject: e.Subject, HtmlBody: e.HTML,
		TextBody: e.Text, NextAttemptAt: e.NextAttemptAt, CreatedAt: e.CreatedAt,
	}); err != nil {
		return fmt.Errorf("enqueue email: %w", err)
	}
	return nil
}

func (r emailRepo) ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.Email, error) {
	rows, err := queries(ctx, r.pool).ClaimDueEmails(ctx, sqlc.ClaimDueEmailsParams{
		LeaseUntil: now.Add(lease), Now: now, Batch: int32(limit), //nolint:gosec // small batch
	})
	if err != nil {
		return nil, fmt.Errorf("claim emails: %w", err)
	}
	out := make([]domain.Email, len(rows))
	for i, row := range rows {
		out[i] = domain.Email{ID: row.ID, NotificationID: row.NotificationID, To: row.ToAddr, Subject: row.Subject,
			HTML: row.HtmlBody, Text: row.TextBody, Status: domain.EmailStatus(row.Status), Attempts: int(row.Attempts),
			NextAttemptAt: row.NextAttemptAt, LastError: row.LastError, CreatedAt: row.CreatedAt}
	}
	return out, nil
}

func (r emailRepo) MarkSent(ctx context.Context, id uuid.UUID, at time.Time) error {
	return wrap("mark email sent", queries(ctx, r.pool).MarkEmailSent(ctx, sqlc.MarkEmailSentParams{ID: id, SentAt: &at}))
}

func (r emailRepo) MarkRetry(ctx context.Context, id uuid.UUID, attempts int, next time.Time, lastErr string) error {
	return wrap("mark email retry", queries(ctx, r.pool).MarkEmailRetry(ctx, sqlc.MarkEmailRetryParams{
		ID: id, Attempts: int32(attempts), NextAttemptAt: next, LastError: lastErr, //nolint:gosec // ≤ 8
	}))
}

func (r emailRepo) MarkFailed(ctx context.Context, id uuid.UUID, attempts int, lastErr string) error {
	return wrap("mark email failed", queries(ctx, r.pool).MarkEmailFailed(ctx, sqlc.MarkEmailFailedParams{
		ID: id, Attempts: int32(attempts), LastError: lastErr, //nolint:gosec // ≤ 8
	}))
}

func wrap(what string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}
