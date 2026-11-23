package grpc_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	notificationv1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/notification/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	ngrpc "github.com/nuroqzavitex/smarthire/services/notification/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/app"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

type fakeUC struct {
	list   []domain.Notification
	in     app.ListInput
	marked uuid.UUID
}

func (f *fakeUC) List(_ context.Context, in app.ListInput) ([]domain.Notification, string, int, error) {
	f.in = in
	return f.list, "next", 7, nil
}

func (f *fakeUC) MarkRead(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	f.marked = id
	at := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	return domain.Notification{ID: id, Type: domain.TypeWelcome, ReadAt: &at}, nil
}

func setup(t *testing.T) (notificationv1.NotificationServiceClient, *fakeUC, context.Context) {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)
	uc := &fakeUC{}
	srv, _ := grpcx.NewServer(grpcx.ServerConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Verifier: v, PublicMethods: ngrpc.PublicMethods})
	notificationv1.RegisterNotificationServiceServer(srv, ngrpc.NewHandler(uc))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpcx.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	tok := is.Sign(t, uuid.NewString(), authz.RoleCandidate, time.Minute)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+tok)
	return notificationv1.NewNotificationServiceClient(conn), uc, ctx
}

func TestHandler_ListMapsFields(t *testing.T) {
	c, uc, ctx := setup(t)
	created := time.Date(2026, 11, 23, 9, 0, 0, 0, time.UTC)
	n := domain.Notification{ID: uuid.New(), Type: domain.TypeApplicationStatus, Title: "t", Body: "b",
		Data: map[string]string{"job_id": "j"}, CreatedAt: created}
	uc.list = []domain.Notification{n}
	resp, err := c.ListNotifications(ctx, &notificationv1.ListNotificationsRequest{UnreadOnly: true, PageSize: 5, PageToken: "tok"})
	require.NoError(t, err)
	assert.Equal(t, app.ListInput{UnreadOnly: true, PageSize: 5, PageToken: "tok"}, uc.in)
	require.Len(t, resp.GetNotifications(), 1)
	got := resp.GetNotifications()[0]
	assert.Equal(t, n.ID.String(), got.GetId())
	assert.Equal(t, "application_status", got.GetType())
	assert.Equal(t, map[string]string{"job_id": "j"}, got.GetData())
	assert.Nil(t, got.GetReadAt())
	assert.True(t, got.GetCreatedAt().AsTime().Equal(created))
	assert.Equal(t, "next", resp.GetNextPageToken())
	assert.Equal(t, int32(7), resp.GetUnreadCount())
}

func TestHandler_MarkRead(t *testing.T) {
	c, uc, ctx := setup(t)
	id := uuid.New()
	got, err := c.MarkNotificationRead(ctx, &notificationv1.MarkNotificationReadRequest{Id: id.String()})
	require.NoError(t, err)
	assert.Equal(t, id, uc.marked)
	assert.NotNil(t, got.GetReadAt())
	_, err = c.MarkNotificationRead(ctx, &notificationv1.MarkNotificationReadRequest{Id: "x"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestHandler_RequiresToken(t *testing.T) {
	c, _, _ := setup(t)
	_, err := c.ListNotifications(context.Background(), &notificationv1.ListNotificationsRequest{})
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
