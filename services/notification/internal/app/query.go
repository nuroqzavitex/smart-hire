package app

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// ListInput pages the caller's notifications.
type ListInput struct {
	UnreadOnly bool
	PageSize   int
	PageToken  string
}

func me(ctx context.Context) (uuid.UUID, error) {
	if err := authz.RequireRole(ctx); err != nil {
		return uuid.Nil, err
	}
	c, _ := authz.ClaimsFrom(ctx)
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, apperr.New(apperr.ErrUnauthenticated, "invalid subject")
	}
	return id, nil
}

// List returns a page of the caller's notifications (newest first), the next
// page token and the caller's unread count.
func (s *Service) List(ctx context.Context, in ListInput) ([]domain.Notification, string, int, error) {
	user, err := me(ctx)
	if err != nil {
		return nil, "", 0, err
	}
	size := in.PageSize
	switch {
	case size == 0:
		size = defaultPageSize
	case size < 0 || size > maxPageSize:
		return nil, "", 0, apperr.New(apperr.ErrValidation, "page_size must be 1-100")
	}
	var after *Cursor
	if in.PageToken != "" {
		c, err := DecodeCursor(in.PageToken)
		if err != nil {
			return nil, "", 0, err
		}
		after = &c
	}
	ns, err := s.Notifications.List(ctx, user, in.UnreadOnly, after, size+1)
	if err != nil {
		return nil, "", 0, fmt.Errorf("list notifications: %w", err)
	}
	next := ""
	if len(ns) > size {
		ns = ns[:size]
		last := ns[len(ns)-1]
		next = EncodeCursor(Cursor{At: last.CreatedAt, ID: last.ID})
	}
	unread, err := s.Notifications.CountUnread(ctx, user)
	if err != nil {
		return nil, "", 0, fmt.Errorf("count unread: %w", err)
	}
	return ns, next, unread, nil
}

// MarkRead marks one of the caller's notifications read; anyone else's is
// ErrNotFound.
func (s *Service) MarkRead(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	user, err := me(ctx)
	if err != nil {
		return domain.Notification{}, err
	}
	return s.Notifications.MarkRead(ctx, id, user, s.Now().UTC())
}
