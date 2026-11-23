package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/kafka"
)

// Store writes documents to the read model.
type Store interface {
	// WriteIndices lists the indices behind the write alias.
	WriteIndices(ctx context.Context) ([]string, error)
	// Put indexes d with external version d.Version; applied is false when
	// the index already holds that version or a newer one.
	Put(ctx context.Context, index string, d Document) (applied bool, err error)
}

// Projector applies job.* events to every index behind the write alias. It is
// a kafka.Handler; ordering and duplicates are settled by external versions.
type Projector struct {
	Store Store
	// Validate checks the raw envelope against its JSON Schema.
	Validate func(eventType string, doc []byte) error
	// AliasTTL bounds how long a resolved write alias is reused; rebuild
	// waits longer than this after adding a new index.
	AliasTTL time.Duration
	// IndexGone reports whether a Put error means the index was deleted
	// (a rebuild finished); the alias is then re-resolved and the write
	// retried once.
	IndexGone func(error) bool
	// Transient reports whether a store error is an infrastructure failure
	// (OpenSearch down, 5xx, 429). Those are retried here with capped
	// backoff until they succeed or ctx ends, blocking the partition rather
	// than dead-lettering events and leaving holes in the read model. Other
	// store errors go to the DLQ. Nil keeps the pkg/kafka retry/DLQ behaviour.
	Transient func(error) bool
	// RetryBase and RetryMax bound the backoff between transient retries.
	RetryBase, RetryMax time.Duration
	// CallTimeout bounds each store call, so a hung OpenSearch does not
	// block forever (0 = no extra bound).
	CallTimeout time.Duration
	Now         func() time.Time
	Log         *slog.Logger

	mu        sync.Mutex
	indices   []string
	expiresAt time.Time
}

var errSchema = kafka.Permanent(errors.New("job event failed schema validation"))

// Handle implements kafka.Handler.
func (p *Projector) Handle(ctx context.Context, env event.Envelope, _ *kgo.Record) error {
	doc, err := json.Marshal(env)
	if err != nil {
		return kafka.Permanent(errors.New("job event not encodable"))
	}
	// Never wrap the validator error: it echoes payload values (PII).
	if err := p.Validate(env.EventType, doc); err != nil {
		return errSchema
	}
	d, err := FromEnvelope(env)
	if err != nil {
		return err
	}
	reresolved := false
	for attempt := 1; ; attempt++ {
		err := p.apply(ctx, d)
		if err == nil {
			p.recordLag(ctx, env)
			return nil
		}
		if !reresolved && p.IndexGone != nil && p.IndexGone(err) {
			reresolved = true
			p.invalidate()
			continue
		}
		if p.Transient == nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err() // shutdown: pkg/kafka neither commits nor dead-letters
		}
		if !p.Transient(err) {
			p.logger().Error("job document rejected by read model", "job_id", d.JobID, "version", d.Version, "error", err.Error())
			return errRejected
		}
		wait := p.backoff(attempt)
		p.logger().Warn("read model unavailable, retrying", "job_id", d.JobID, "attempt", attempt, "retry_in", wait.String(), "error", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		p.invalidate()
	}
}

var errRejected = kafka.Permanent(errors.New("job document rejected by read model"))

func (p *Projector) backoff(attempt int) time.Duration {
	base, ceiling := p.RetryBase, p.RetryMax
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	if ceiling <= 0 {
		ceiling = 10 * time.Second
	}
	d := base
	for i := 1; i < attempt && d < ceiling; i++ {
		d *= 2
	}
	return min(d, ceiling)
}

// callCtx bounds one store call by CallTimeout.
func (p *Projector) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if p.CallTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, p.CallTimeout)
}

// apply writes d to every index behind the (cached) write alias.
func (p *Projector) apply(ctx context.Context, d Document) error {
	indices, err := p.writeIndices(ctx)
	if err != nil {
		return fmt.Errorf("resolve write alias: %w", err)
	}
	for _, idx := range indices {
		cctx, cancel := p.callCtx(ctx)
		applied, err := p.Store.Put(cctx, idx, d)
		cancel()
		if err != nil {
			return fmt.Errorf("index job document: %w", err)
		}
		if !applied {
			p.logger().Debug("stale job event ignored", "index", idx, "job_id", d.JobID, "version", d.Version)
		}
	}
	return nil
}

func (p *Projector) invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.indices = nil
}

func (p *Projector) writeIndices(ctx context.Context) ([]string, error) {
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.indices != nil && now.Before(p.expiresAt) {
		return p.indices, nil
	}
	cctx, cancel := p.callCtx(ctx)
	defer cancel()
	indices, err := p.Store.WriteIndices(cctx)
	if err != nil {
		return nil, err
	}
	if len(indices) == 0 {
		return nil, errors.New("write alias has no index")
	}
	p.indices, p.expiresAt = indices, now.Add(p.AliasTTL)
	return indices, nil
}

func (p *Projector) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Projector) logger() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
