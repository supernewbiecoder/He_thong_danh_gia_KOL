#!/usr/bin/env bash
# Raw throughput benchmark with Kafka's built-in perf tools (no Python needed).
# Measures the ceiling of the cluster itself; use loadtest/ for realistic JSON events.
#
#   ./scripts/perf-test.sh
#   RECORDS=2000000 RECORD_SIZE=512 ACKS=1 ./scripts/perf-test.sh
#   THROUGHPUT=20000 ./scripts/perf-test.sh          # fixed rate instead of max speed
set -euo pipefail
source "$(dirname "$0")/lib.sh"

TOPIC="${TOPIC:-kol.loadtest}"
RECORDS="${RECORDS:-500000}"
RECORD_SIZE="${RECORD_SIZE:-1024}"
THROUGHPUT="${THROUGHPUT:--1}"
ACKS="${ACKS:-all}"
BS="$(running_bootstrap)"

step "Producer: ${RECORDS} records x ${RECORD_SIZE}B, acks=${ACKS}, throughput=${THROUGHPUT} -> ${TOPIC}"
kexec kafka-producer-perf-test.sh \
  --topic "$TOPIC" \
  --num-records "$RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput "$THROUGHPUT" \
  --producer-props bootstrap.servers="$BS" acks="$ACKS" linger.ms=5 batch.size=131072 compression.type=lz4

step "Consumer: read ${RECORDS} records from ${TOPIC}"
kexec kafka-consumer-perf-test.sh \
  --bootstrap-server "$BS" \
  --topic "$TOPIC" \
  --messages "$RECORDS" \
  --group "perf-consumer-$(date +%s)" \
  --timeout 60000 \
  --show-detailed-stats --reporting-interval 5000

log "Done. Compare with the 'Throughput' and 'Latency' rows in Grafana (http://localhost:3000)."
