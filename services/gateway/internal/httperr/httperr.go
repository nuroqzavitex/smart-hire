// Package httperr renders gRPC errors as the gateway's JSON error envelope.
package httperr

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
)

type body struct {
	Error detail `json:"error"`
}

type detail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   []any  `json:"details"`
	RequestID string `json:"request_id"`
}

func httpStatus(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// Write renders err as the error envelope. Errors that are not gRPC statuses
// are reported as INTERNAL "internal error" so their text never leaks.
func Write(w http.ResponseWriter, r *http.Request, err error) {
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Internal, "internal error")
	}
	code := st.Code()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus(code))
	_ = json.NewEncoder(w).Encode(body{Error: detail{
		Code:      rpccode.Code(code).String(), //nolint:gosec // gRPC codes are 0..16
		Message:   st.Message(),
		Details:   []any{},
		RequestID: logger.RequestIDFrom(r.Context()),
	}})
}

// GatewayErrorHandler adapts Write to grpc-gateway's error handler hook.
func GatewayErrorHandler(_ context.Context, _ *runtime.ServeMux, _ runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	Write(w, r, err)
}
