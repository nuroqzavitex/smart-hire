package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

func (e *env) seed(user uuid.UUID, n int) []domain.Notification {
	var out []domain.Notification
	for i := range n {
		x := domain.Notification{ID: uuid.New(), UserID: user, Type: domain.TypeCVParsed, Title: "t", Body: "b",
			CreatedAt: t0.Add(time.Duration(i) * time.Minute)}
		e.notes.list = append(e.notes.list, x)
		out = append(out, x)
	}
	return out
}

func TestList_PagesAndUnreadCount(t *testing.T) {
	e := newEnv(t)
	me := uuid.New()
	ns := e.seed(me, 3)
	e.seed(uuid.New(), 2) // someone else's
	ns[0].ReadAt = &t0
	e.notes.list[0].ReadAt = &t0

	page, next, unread, err := e.svc.List(as(me, authz.RoleCandidate), app.ListInput{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, ns[2].ID, page[0].ID, "newest first")
	assert.Equal(t, 2, unread)
	require.NotEmpty(t, next)
	page2, next2, _, err := e.svc.List(as(me, authz.RoleCandidate), app.ListInput{PageSize: 2, PageToken: next})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, ns[0].ID, page2[0].ID)
	assert.Empty(t, next2)

	onlyUnread, _, _, err := e.svc.List(as(me, authz.RoleRecruiter), app.ListInput{UnreadOnly: true})
	require.NoError(t, err)
	assert.Len(t, onlyUnread, 2)

	_, _, _, err = e.svc.List(as(me, authz.RoleCandidate), app.ListInput{PageToken: "bad"})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, _, _, err = e.svc.List(context.Background(), app.ListInput{})
	assert.ErrorIs(t, err, apperr.ErrUnauthenticated)
}

func TestMarkRead_OwnerOnly(t *testing.T) {
	e := newEnv(t)
	me := uuid.New()
	n := e.seed(me, 1)[0]
	e.now = t0.Add(time.Hour)
	got, err := e.svc.MarkRead(as(me, authz.RoleCandidate), n.ID)
	require.NoError(t, err)
	require.NotNil(t, got.ReadAt)
	assert.Equal(t, t0.Add(time.Hour), *got.ReadAt)

	e.now = t0.Add(2 * time.Hour)
	again, err := e.svc.MarkRead(as(me, authz.RoleCandidate), n.ID)
	require.NoError(t, err)
	assert.Equal(t, t0.Add(time.Hour), *again.ReadAt, "first read_at kept")

	_, err = e.svc.MarkRead(as(uuid.New(), authz.RoleCandidate), n.ID)
	assert.ErrorIs(t, err, apperr.ErrNotFound)
}
