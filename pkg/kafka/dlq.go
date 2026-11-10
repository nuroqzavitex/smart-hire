package kafka

import (
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// dlqHeaderKeys is the fixed order in which dlq-* headers are appended.
var dlqHeaderKeys = []string{"dlq-attempts", "dlq-error", "dlq-original-offset", "dlq-original-partition", "dlq-original-topic"}

// dlqRecordHeaders returns orig (order kept) without any dlq-* header, followed
// by a fresh dlq-* set, so a re-dead-lettered record carries one set only.
func dlqRecordHeaders(orig []kgo.RecordHeader, rec *kgo.Record, cause error, attempts int) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(orig)+len(dlqHeaderKeys))
	for _, h := range orig {
		if !strings.HasPrefix(h.Key, "dlq-") {
			out = append(out, h)
		}
	}
	m := dlqHeaders(rec, cause, attempts)
	for _, k := range dlqHeaderKeys {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(m[k])})
	}
	return out
}

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
