package app

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	pkgotel "github.com/nuroqzavitex/smarthire/pkg/otel"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

const meterName = "github.com/nuroqzavitex/smarthire/services/notification/internal/app"

// Deps are the collaborators of Service.
type Deps struct {
	Users         UserRepo
	Jobs          JobRepo
	Notifications NotificationRepo
	Emails        EmailRepo
	Idem          Idempotency
	Tx            TxManager
	Mailer        Mailer
	Pusher        Pusher
	Now           func() time.Time
	NewID         func() uuid.UUID
	Logger        *slog.Logger
	// EmailLease bounds how long a claimed email stays reserved for one
	// send attempt (> the SMTP timeout). EmailBatch caps a SendDue run.
	EmailLease time.Duration
	EmailBatch int
}

// Service implements the notification use cases.
type Service struct{ Deps }

// NewService wires a Service, defaulting Now, NewID, Logger and the email
// sender settings.
func NewService(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewID == nil {
		d.NewID = uuid.New
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if d.EmailLease == 0 {
		d.EmailLease = time.Minute
	}
	if d.EmailBatch == 0 {
		d.EmailBatch = 10
	}
	return &Service{Deps: d}
}

// count adds 1 to an int64 counter of the global meter provider.
func count(ctx context.Context, name, desc string, attrs ...attribute.KeyValue) {
	c, err := pkgotel.Meter(meterName).Int64Counter(name, metric.WithDescription(desc))
	if err == nil {
		c.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

func notificationCreated(ctx context.Context, t domain.Type) {
	count(ctx, "notifications.created", "Notifications created", attribute.String("type", string(t)))
}
