// Package kafka wraps franz-go with trace propagation, retry and a DLQ.
package kafka

// Record is a message to publish.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}
