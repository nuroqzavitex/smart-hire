// Package grpc adapts the notification use cases to the generated
// NotificationService gRPC API.
package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/notification/v1"
	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

// PublicMethods are the RPCs callable without an access token: none.
var PublicMethods = []string{}

// UseCases is satisfied by *app.Service.
type UseCases interface {
	List(ctx context.Context, in app.ListInput) ([]domain.Notification, string, int, error)
	MarkRead(ctx context.Context, id uuid.UUID) (domain.Notification, error)
}

// Handler implements notificationv1.NotificationServiceServer.
type Handler struct {
	notificationv1.UnimplementedNotificationServiceServer
	uc UseCases
}

// NewHandler returns the NotificationService implementation backed by uc.
func NewHandler(uc UseCases) *Handler { return &Handler{uc: uc} }

// ListNotifications pages the caller's notifications.
func (h *Handler) ListNotifications(ctx context.Context, req *notificationv1.ListNotificationsRequest) (*notificationv1.ListNotificationsResponse, error) {
	ns, next, unread, err := h.uc.List(ctx, app.ListInput{
		UnreadOnly: req.GetUnreadOnly(), PageSize: int(req.GetPageSize()), PageToken: req.GetPageToken(),
	})
	if err != nil {
		return nil, err
	}
	resp := &notificationv1.ListNotificationsResponse{
		Notifications: make([]*notificationv1.Notification, len(ns)), NextPageToken: next,
		UnreadCount: int32(unread), //nolint:gosec // per-user count
	}
	for i, n := range ns {
		resp.Notifications[i] = ToProto(n)
	}
	return resp, nil
}

// MarkNotificationRead marks one of the caller's notifications read.
func (h *Handler) MarkNotificationRead(ctx context.Context, req *notificationv1.MarkNotificationReadRequest) (*notificationv1.Notification, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, apperr.New(apperr.ErrValidation, "id must be a uuid")
	}
	n, err := h.uc.MarkRead(ctx, id)
	if err != nil {
		return nil, err
	}
	return ToProto(n), nil
}

// ToProto maps a notification to its API message.
func ToProto(n domain.Notification) *notificationv1.Notification {
	out := &notificationv1.Notification{
		Id: n.ID.String(), Type: string(n.Type), Title: n.Title, Body: n.Body, Data: n.Data,
		CreatedAt: timestamppb.New(n.CreatedAt),
	}
	if n.ReadAt != nil {
		out.ReadAt = timestamppb.New(*n.ReadAt)
	}
	return out
}
