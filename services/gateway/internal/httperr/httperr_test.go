package httperr_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/assert"

	"github.com/nuroqzavitex/smarthire/pkg/logger"
	"github.com/nuroqzavitex/smarthire/services/gateway/internal/httperr"
)

func req() *http.Request {
	return httptest.NewRequest("GET", "/", nil).WithContext(logger.WithRequestID(context.Background(), "rid"))
}

func TestWrite_MapsCodes(t *testing.T) {
	for code, want := range map[codes.Code]int{codes.InvalidArgument: 400, codes.Unauthenticated: 401,
		codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.ResourceExhausted: 429,
		codes.DeadlineExceeded: 504, codes.Unavailable: 503, codes.Internal: 500, codes.DataLoss: 500} {
		rec := httptest.NewRecorder()
		httperr.Write(rec, req(), status.Error(code, "m"))
		assert.Equal(t, want, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.JSONEq(t, fmt.Sprintf(`{"error":{"code":%q,"message":"m","details":[],"request_id":"rid"}}`,
			rpccode.Code(code).String()), rec.Body.String()) //nolint:gosec // gRPC codes are 0..16
	}
}

func TestWrite_NonStatusErrorDoesNotLeak(t *testing.T) {
	rec := httptest.NewRecorder()
	httperr.Write(rec, req(), errors.New("secret db password"))
	assert.Equal(t, 500, rec.Code)
	assert.NotContains(t, rec.Body.String(), "secret")
	assert.JSONEq(t, `{"error":{"code":"INTERNAL","message":"internal error","details":[],"request_id":"rid"}}`, rec.Body.String())
}
