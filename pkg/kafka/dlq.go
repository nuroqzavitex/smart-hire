package kafka

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const maxDLQErrorRunes = 512

// DLQTopic returns the dead-letter topic of topic.
func DLQTopic(topic string) string { return topic + ".dlq" }

func dlqHeaders(rec *kgo.Record, err error, attempts int) map[string]string {
	msg := []rune(err.Error())
	if len(msg) > maxDLQErrorRunes {
		msg = msg[:maxDLQErrorRunes]
	}
	return map[string]string{
		"dlq-error":              string(msg),
		"dlq-attempts":           strconv.Itoa(attempts),
		"dlq-original-topic":     rec.Topic,
		"dlq-original-partition": strconv.Itoa(int(rec.Partition)),
		"dlq-original-offset":    strconv.FormatInt(rec.Offset, 10),
	}
}
