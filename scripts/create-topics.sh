#!/usr/bin/env bash
# Creates / updates Kafka topics from config/topics.conf. Idempotent: safe to run many times.
#   - Creates missing topics
#   - Increases the partition count if topics.conf asks for more
#   - Applies the configs listed in topics.conf
#
# Runs inside a Kafka container (Compose service `kafka-init`, Kubernetes Job `kafka-topics-init`).
# Env:
#   BOOTSTRAP_SERVERS  (default localhost:19092)
#   TOPICS_FILE        (default /config/topics.conf)
#   KAFKA_BIN          (default /opt/kafka/bin)
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-localhost:19092}"
TOPICS_FILE="${TOPICS_FILE:-/config/topics.conf}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-180}"

# Short-lived CLI JVMs: keep them small and never attach the JMX exporter.
export KAFKA_HEAP_OPTS="-Xmx256m"
unset KAFKA_OPTS || true

log() { echo "[create-topics] $*"; }

log "Waiting for Kafka at ${BOOTSTRAP} (timeout ${MAX_WAIT_SECONDS}s)..."
deadline=$((SECONDS + MAX_WAIT_SECONDS))
until "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    log "ERROR: Kafka not reachable after ${MAX_WAIT_SECONDS}s"
    exit 1
  fi
  sleep 3
done
log "Kafka is reachable."

while read -r name partitions rf configs _ || [[ -n "${name:-}" ]]; do
  [[ -z "${name}" || "${name}" == \#* ]] && continue

  log "Topic ${name}: partitions=${partitions} rf=${rf} configs=${configs:--}"
  create_args=(--bootstrap-server "${BOOTSTRAP}" --create --if-not-exists
               --topic "${name}" --partitions "${partitions}" --replication-factor "${rf}")
  if [[ -n "${configs:-}" && "${configs}" != "-" ]]; then
    IFS=',' read -ra kvs <<< "${configs}"
    for kv in "${kvs[@]}"; do
      create_args+=(--config "${kv}")
    done
  fi
  "${KAFKA_BIN}/kafka-topics.sh" "${create_args[@]}"

  current=$("${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${name}" \
    | awk -F'PartitionCount:' 'NF > 1 { split($2, a, " "); print a[1]; exit }')
  if [[ -n "${current}" && "${current}" -lt "${partitions}" ]]; then
    log "  increasing partitions ${current} -> ${partitions}"
    "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --alter \
      --topic "${name}" --partitions "${partitions}"
  elif [[ -n "${current}" && "${current}" -gt "${partitions}" ]]; then
    log "  WARNING: topic has ${current} partitions (> ${partitions}); Kafka cannot shrink partitions"
  fi

  if [[ -n "${configs:-}" && "${configs}" != "-" ]]; then
    "${KAFKA_BIN}/kafka-configs.sh" --bootstrap-server "${BOOTSTRAP}" --alter \
      --entity-type topics --entity-name "${name}" --add-config "${configs}" >/dev/null
  fi
done < <(tr -d '\r' < "${TOPICS_FILE}")   # tolerate CRLF files edited on Windows

log "Done. Current topics:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
