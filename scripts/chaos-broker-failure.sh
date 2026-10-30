#!/usr/bin/env bash
# Fault-tolerance demo: stop broker(s), show that the cluster keeps working, bring them back.
#
#   ./scripts/chaos-broker-failure.sh                 # stop kafka-2 (expected: no data loss, no downtime)
#   ./scripts/chaos-broker-failure.sh kafka-1         # stop the broker of your choice
#   ./scripts/chaos-broker-failure.sh kafka-2 kafka-3 # 2/3 down: quorum lost -> writes fail (expected)
#
# Tip: run a load test at the same time and watch Grafana:
#   docker compose --profile loadtest up -d
set -euo pipefail
source "$(dirname "$0")/lib.sh"

TOPIC="${TOPIC:-kol.loadtest}"
DOWN_SECONDS="${DOWN_SECONDS:-30}"
VICTIMS=("$@")
[[ ${#VICTIMS[@]} -eq 0 ]] && VICTIMS=(kafka-2)

for v in "${VICTIMS[@]}"; do
  [[ " ${BROKERS[*]} " == *" $v "* ]] || { echo "Unknown broker: $v (use: ${BROKERS[*]})"; exit 1; }
done

describe_topic() {
  kexec kafka-topics.sh --bootstrap-server "$(running_bootstrap)" --describe --topic "$TOPIC" \
    | sed 's/^/  /'
}

produce_probe() {
  # 2000 x 1KB messages with acks=all: succeeds only if every partition still has >= min.insync.replicas
  kexec kafka-producer-perf-test.sh --topic "$TOPIC" --num-records 2000 --record-size 1024 --throughput -1 \
    --producer-props bootstrap.servers="$(running_bootstrap)" acks=all request.timeout.ms=10000 delivery.timeout.ms=30000 \
    2>&1 | tail -n 3 | sed 's/^/  /'
}

step "1. Before: leaders and ISR of ${TOPIC}"
describe_topic

step "2. Stopping: ${VICTIMS[*]}"
t_stop=$SECONDS
docker stop "${VICTIMS[@]}" >/dev/null
log "stopped. Waiting 15s for the controller to move partition leaders..."
sleep 15

step "3. During failure: leaders moved, ISR shrunk"
describe_topic || warn "describe failed - cluster may be unavailable"

step "4. Producing 2000 messages with acks=all while ${VICTIMS[*]} is down"
if produce_probe; then
  log "producer finished (check the 'records sent' line above for errors)"
else
  warn "producer failed - expected only when 2+ brokers are down (quorum / min.insync.replicas lost)"
fi

remaining=$(( DOWN_SECONDS - (SECONDS - t_stop) ))
(( remaining > 0 )) && { log "keeping broker(s) down for ${remaining}s more..."; sleep "$remaining"; }

step "5. Restarting: ${VICTIMS[*]}"
t_start=$SECONDS
docker start "${VICTIMS[@]}" >/dev/null
for v in "${VICTIMS[@]}"; do wait_healthy "$v" 240; done

step "6. Waiting for replicas to catch up (ISR back to 3)"
while true; do
  urp=$(kexec kafka-topics.sh --bootstrap-server "$(running_bootstrap)" --describe --under-replicated-partitions | grep -c "Topic:" || true)
  [[ "$urp" -eq 0 ]] && break
  log "  under-replicated partitions: ${urp}"
  sleep 5
done
log "fully replicated again $((SECONDS - t_start))s after restart"

step "7. Preferred leader election (rebalance leaders back to the restarted broker)"
kexec kafka-leader-election.sh --bootstrap-server "$(running_bootstrap)" --election-type PREFERRED --all-topic-partitions \
  2>&1 | tail -n 2 | sed 's/^/  /' || true

step "8. After"
describe_topic
log "Total downtime simulated: $((t_start - t_stop))s. See Grafana 'Cluster health' + 'Broker resources' rows."
