package app_test

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// fakeTx runs fn directly and records whether fn returned nil (i.e. a real
// transaction would have committed). Like a real Begin, it fails on a
// cancelled ctx.
type fakeTx struct{ lastCommitted *bool }

func (f fakeTx) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := ctx.Err(); err != nil {
		if f.lastCommitted != nil {
			*f.lastCommitted = false
		}
		return err
	}
	err := fn(ctx)
	if f.lastCommitted != nil {
		*f.lastCommitted = err == nil
	}
	return err
}

type fakeUsers struct {
	mu      sync.Mutex
	byEmail map[string]domain.User
}

func newFakeUsers() *fakeUsers { return &fakeUsers{byEmail: map[string]domain.User{}} }

func (f *fakeUsers) Create(_ context.Context, u domain.User) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.byEmail[u.Email]; ok {
		return apperr.New(apperr.ErrConflict, "email already registered")
	}
	f.byEmail[u.Email] = u
	return nil
}

func (f *fakeUsers) GetByEmail(_ context.Context, email string) (domain.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.byEmail[email]
	if !ok {
		return domain.User{}, apperr.New(apperr.ErrNotFound, "user not found")
	}
	return u, nil
}

func (f *fakeUsers) GetByID(_ context.Context, id uuid.UUID) (domain.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.byEmail {
		if u.ID == id {
			return u, nil
		}
	}
	return domain.User{}, apperr.New(apperr.ErrNotFound, "user not found")
}

func (f *fakeUsers) setStatus(email string, s domain.UserStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u := f.byEmail[email]
	u.Status = s
	f.byEmail[email] = u
}

type fakeTokens struct {
	mu   sync.Mutex
	list []domain.RefreshToken
}

func (f *fakeTokens) Create(_ context.Context, t domain.RefreshToken) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.list = append(f.list, t)
	return nil
}

func (f *fakeTokens) GetByHashForUpdate(_ context.Context, hash []byte) (domain.RefreshToken, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.list {
		if string(t.Hash) == string(hash) {
			return t, nil
		}
	}
	return domain.RefreshToken{}, apperr.New(apperr.ErrNotFound, "refresh token not found")
}

func (f *fakeTokens) Revoke(_ context.Context, id uuid.UUID, at time.Time, replacedBy *uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.list {
		if f.list[i].ID == id {
			f.list[i].RevokedAt = &at
			f.list[i].ReplacedBy = replacedBy
		}
	}
	return nil
}

func (f *fakeTokens) RevokeFamily(_ context.Context, familyID uuid.UUID, at time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.list {
		if f.list[i].FamilyID == familyID && f.list[i].RevokedAt == nil {
			f.list[i].RevokedAt = &at
		}
	}
	return nil
}

// byHash returns the stored token for a raw token string.
func (f *fakeTokens) byHash(raw string) (domain.RefreshToken, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := sha256Sum(raw)
	for _, t := range f.list {
		if string(t.Hash) == string(h) {
			return t, true
		}
	}
	return domain.RefreshToken{}, false
}

func (f *fakeTokens) all() []domain.RefreshToken {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.RefreshToken(nil), f.list...)
}

type fakeOutbox struct{ events []event.Envelope }

func (f *fakeOutbox) Write(_ context.Context, e event.Envelope, _ string) error {
	f.events = append(f.events, e)
	return nil
}

// fakeHasher hashes as "h:"+pw and records every Verify call.
type fakeHasher struct {
	mu    sync.Mutex
	calls []verifyCall
}

type verifyCall struct{ pw, hash string }

func (f *fakeHasher) Hash(pw string) (string, error) { return "h:" + pw, nil }

func (f *fakeHasher) Verify(pw, hash string) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, verifyCall{pw, hash})
	f.mu.Unlock()
	return hash == "h:"+pw, nil
}

// verifyCallsAgainst counts Verify calls made against the given stored hash.
func (f *fakeHasher) verifyCallsAgainst(hash string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.hash == hash {
			n++
		}
	}
	return n
}

type fakeIssuer struct{}

func (fakeIssuer) IssueAccess(u domain.User, _ time.Time) (string, time.Duration, error) {
	return "access-" + u.ID.String(), 15 * time.Minute, nil
}

func sha256Sum(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}
