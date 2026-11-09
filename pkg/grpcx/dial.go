package grpcx

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultTimeout is applied to client calls whose context has no deadline.
const DefaultTimeout = 3 * time.Second

// Dial creates a client connection with insecure credentials (in-cluster
// traffic), OpenTelemetry instrumentation and a default per-call timeout.
// Caller-supplied opts are applied after the defaults and may override them.
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithChainUnaryInterceptor(defaultTimeoutInterceptor),
	}
	return grpc.NewClient(target, append(base, opts...)...)
}

func defaultTimeoutInterceptor(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}
