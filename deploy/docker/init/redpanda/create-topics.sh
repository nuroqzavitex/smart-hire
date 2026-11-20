#!/bin/bash
# Idempotent: creates the M1a + M1b + M2b topics and their DLQs, disables auto-create.
set -euo pipefail
B=redpanda:9092

rpk -X brokers=$B -X admin.hosts=redpanda:9644 cluster config set auto_create_topics_enabled false

for base in user.registered job.created job.updated job.closed job.deleted \
    cv.uploaded cv.parsed cv.parse_failed cv.updated match.completed; do
  for t in "$base" "$base.dlq"; do
    rpk -X brokers=$B topic create "$t" -p 3 -r 1 || rpk -X brokers=$B topic describe "$t" >/dev/null
  done
done
echo "topics ready"
