#!/usr/bin/env bash
# Shared helpers for the host-side scripts (Docker Compose environment).
# Works on Linux/macOS and on Windows with Git Bash.

# Git Bash would otherwise rewrite /opt/kafka/... into C:/Program Files/Git/opt/kafka/...
export MSYS_NO_PATHCONV=1

BROKERS=(kafka-1 kafka-2 kafka-3)
BOOTSTRAP_INTERNAL="kafka-1:19092,kafka-2:19092,kafka-3:19092"
KAFKA_BIN=/opt/kafka/bin

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
step() { printf '\n\033[1;32m==== %s ====\033[0m\n' "$*"; }

is_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" == "true" ]]
}

health_of() {
  docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$1" 2>/dev/null || echo "missing"
}

# First broker container that is running; CLI commands are executed inside it.
pick_broker() {
  local b
  for b in "${BROKERS[@]}"; do
    if is_running "$b"; then echo "$b"; return 0; fi
  done
  echo "ERROR: no Kafka broker container is running (docker compose up -d)" >&2
  return 1
}

# Bootstrap list made of running brokers only, so CLI tools do not wait on a dead one.
running_bootstrap() {
  local b out=""
  for b in "${BROKERS[@]}"; do
    if is_running "$b"; then out="${out:+$out,}$b:19092"; fi
  done
  echo "$out"
}

# kexec <kafka-cli.sh> [args...]  - run a Kafka CLI tool inside a running broker
kexec() {
  local tool="$1"; shift
  docker exec -e KAFKA_HEAP_OPTS=-Xmx256m "$(pick_broker)" "${KAFKA_BIN}/${tool}" "$@"
}

wait_healthy() {
  local container="$1" timeout="${2:-180}" start=$SECONDS
  while [[ "$(health_of "$container")" != "healthy" ]]; do
    if (( SECONDS - start > timeout )); then
      warn "$container not healthy after ${timeout}s"; return 1
    fi
    sleep 3
  done
  log "$container is healthy ($((SECONDS - start))s)"
}
