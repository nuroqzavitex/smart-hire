package outbox

import "context"

// Tick runs a single relay iteration (test hook for package outbox_test).
func (r *Relay) Tick(ctx context.Context) (int, error) { return r.tick(ctx) }
