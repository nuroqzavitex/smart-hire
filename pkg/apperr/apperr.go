// Package apperr defines shared domain error kinds and their mapping to gRPC status errors.
package apperr

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Sentinel error kinds. Wrap them with New to attach a message.
var (
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrValidation      = errors.New("validation failed")
	ErrForbidden       = errors.New("forbidden")
	ErrUnauthenticated = errors.New("unauthenticated")
)

type appError struct {
	kind error
	msg  string
}

func (e *appError) Error() string { return e.msg }
func (e *appError) Unwrap() error { return e.kind }

// New returns an error whose Error() is msg and for which errors.Is(err, kind) is true.
func New(kind error, msg string) error {
	return &appError{kind: kind, msg: msg}
}

var kindCodes = []struct {
	kind error
	code codes.Code
}{
	{ErrNotFound, codes.NotFound},
	{ErrConflict, codes.AlreadyExists},
	{ErrValidation, codes.InvalidArgument},
	{ErrForbidden, codes.PermissionDenied},
	{ErrUnauthenticated, codes.Unauthenticated},
}

// ToGRPC converts err to a gRPC status error. Unknown errors become Internal
// with a generic message so internal details are never leaked.
func ToGRPC(err error) error {
	if err == nil {
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return err
	}
	var ae *appError
	if errors.As(err, &ae) {
		for _, kc := range kindCodes {
			if errors.Is(err, kc.kind) {
				return status.Error(kc.code, ae.msg)
			}
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	}
	return status.Error(codes.Internal, "internal error")
}
