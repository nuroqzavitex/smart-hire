package apperr_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

func TestNew(t *testing.T) {
	err := apperr.New(apperr.ErrNotFound, "user not found")
	assert.Equal(t, "user not found", err.Error())
	assert.ErrorIs(t, err, apperr.ErrNotFound)
	assert.NotErrorIs(t, err, apperr.ErrConflict)
}

func TestToGRPC(t *testing.T) {
	cases := []struct {
		in   error
		code codes.Code
		msg  string
	}{
		{apperr.New(apperr.ErrNotFound, "user not found"), codes.NotFound, "user not found"},
		{apperr.New(apperr.ErrConflict, "email taken"), codes.AlreadyExists, "email taken"},
		{apperr.New(apperr.ErrValidation, "bad"), codes.InvalidArgument, "bad"},
		{apperr.New(apperr.ErrForbidden, "no"), codes.PermissionDenied, "no"},
		{apperr.New(apperr.ErrUnauthenticated, "who"), codes.Unauthenticated, "who"},
		{fmt.Errorf("wrap: %w", apperr.New(apperr.ErrNotFound, "x")), codes.NotFound, "x"},
		{context.DeadlineExceeded, codes.DeadlineExceeded, "deadline exceeded"},
		{context.Canceled, codes.Canceled, "canceled"},
		{status.Error(codes.Unavailable, "down"), codes.Unavailable, "down"},
		{errors.New("pq: secret detail"), codes.Internal, "internal error"},
	}
	for _, c := range cases {
		st, _ := status.FromError(apperr.ToGRPC(c.in))
		assert.Equal(t, c.code, st.Code())
		assert.Equal(t, c.msg, st.Message())
	}
	assert.NoError(t, apperr.ToGRPC(nil))
}
