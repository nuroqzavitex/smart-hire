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
	Now       func() time.Time
	Log       *slog.Logger

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
	err = p.apply(ctx, d)
	if err != nil && p.IndexGone != nil && p.IndexGone(err) {
		p.invalidate()
		err = p.apply(ctx, d)
	}
	return err
}

// apply writes d to every index behind the (cached) write alias.
func (p *Projector) apply(ctx context.Context, d Document) error {
	indices, err := p.writeIndices(ctx)
	if err != nil {
		return fmt.Errorf("resolve write alias: %w", err)
	}
	for _, idx := range indices {
		applied, err := p.Store.Put(ctx, idx, d)
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
	indices, err := p.Store.WriteIndices(ctx)
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
