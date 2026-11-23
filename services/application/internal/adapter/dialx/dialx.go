// Package dialx holds the gRPC client retry policy of the application
// service (spec §14.2): at most 3 attempts with exponential backoff and
// jitter, only for UNAVAILABLE and DEADLINE_EXCEEDED. grpc-go applies it
// from the service config, so no hand-written retry loop is needed.
package dialx

import (
	"fmt"

	"google.golang.org/grpc"
)

// ServiceConfig returns the service config JSON enabling the retry policy
// for every method of the fully qualified gRPC service.
func ServiceConfig(service string) string {
	return fmt.Sprintf(`{"methodConfig":[{"name":[{"service":%q}],"retryPolicy":{`+
		`"maxAttempts":3,"initialBackoff":"0.1s","maxBackoff":"1s","backoffMultiplier":2,`+
		`"retryableStatusCodes":["UNAVAILABLE","DEADLINE_EXCEEDED"]}}]}`, service)
}

// RetryPolicy is the dial option carrying ServiceConfig(service).
func RetryPolicy(service string) grpc.DialOption {
	return grpc.WithDefaultServiceConfig(ServiceConfig(service))
}
