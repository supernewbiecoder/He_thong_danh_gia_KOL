#!/usr/bin/env bash
# End-to-end verification script for Module 3 (Stream Processor) on the running Kafka cluster.
#
# Usage:
#   ./scripts/test-module-3.sh
set -euo pipefail
source "$(dirname "$0")/lib.sh"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="python3"
if [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
fi

step "1/5 Running Module 3 unit tests"
"$PYTHON_BIN" -m unittest discover -s processor/tests -v

step "2/5 Starting stream-processor container (Docker Compose)"
docker compose --profile processor up -d --build stream-processor

step "3/5 Producing synthetic events into kol.raw.bluesky & kol.raw.youtube (15 seconds)"
"$PYTHON_BIN" loadtest/loadgen.py produce \
  --bootstrap 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 \
  --topic kol.raw.bluesky \
  --rate 100 \
  --duration 10 \
  --metrics-port 0

"$PYTHON_BIN" loadtest/loadgen.py produce \
  --bootstrap 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 \
  --topic kol.raw.youtube \
  --rate 50 \
  --duration 5 \
  --metrics-port 0

step "4/5 Waiting 6s for stream-processor to flush scores & keywords to Kafka"
sleep 6
docker compose logs --tail=15 stream-processor

step "5/5 Reading live KOL Leaderboard & Trending Keywords from Kafka"
"$PYTHON_BIN" processor/view_leaderboard.py \
  --bootstrap 127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094 \
  --wait 4

log "Module 3 end-to-end test completed! Open http://localhost:8080 to inspect topics in Kafka UI."
