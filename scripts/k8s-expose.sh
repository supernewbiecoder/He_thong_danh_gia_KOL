#!/usr/bin/env bash
# Makes the cluster's services reachable from this machine with kubectl port-forward, for clusters
# whose NodePorts are not on localhost (minikube). Keep the window open; Ctrl+C stops everything.
#
#   ./scripts/k8s-expose.sh                # 127.0.0.1 only
#   TAILSCALE=1 ./scripts/k8s-expose.sh    # also publish the same ports to the team on Tailscale
#
# Kafka advertises <KOL_EXTERNAL_HOST>:30090..30092. For the team, deploy first with
#   KOL_EXTERNAL_HOST=<this machine's Tailscale name> ./scripts/k8s-deploy.sh
# A port-forward dies when its pod restarts (failover, chaos test): each one is restarted here.
set -uo pipefail
export MSYS_NO_PATHCONV=1
source "$(dirname "$0")/lib.sh"
NS=kol-system

FORWARDS=(
  "pod/kafka-0 30090:9094"
  "pod/kafka-1 30091:9094"
  "pod/kafka-2 30092:9094"
  "svc/kol-db-rw 5432:5432"
  "svc/grafana 3000:3000"
  "svc/kafka-ui 8080:8080"
  "svc/prometheus 9090:9090"
)

forward() {
  local target="$1" ports="$2" child=""
  trap '[[ -n "$child" ]] && kill "$child" 2>/dev/null; exit 0' TERM INT
  while true; do
    kubectl -n "$NS" port-forward --address 127.0.0.1 "$target" "$ports" >/dev/null 2>&1 &
    child=$!
    wait "$child"
    sleep 2
  done
}

cleanup() {
  trap - INT TERM EXIT
  kill $(jobs -p) 2>/dev/null
  wait 2>/dev/null
  log "Stopped all port-forwards."
}
trap cleanup INT TERM EXIT

for entry in "${FORWARDS[@]}"; do
  # shellcheck disable=SC2086
  forward $entry &
done

if [[ "${TAILSCALE:-0}" == "1" ]]; then
  for entry in "${FORWARDS[@]}"; do
    port="${entry##* }"; port="${port%%:*}"
    tailscale serve --bg --tcp="$port" "tcp://localhost:$port" >/dev/null
  done
  log "Published on Tailscale (stays until: tailscale serve reset)"
fi

cat <<EOF
Forwarding (Ctrl+C to stop):
  Kafka        127.0.0.1:30090,127.0.0.1:30091,127.0.0.1:30092
  PostgreSQL   127.0.0.1:5432   (primary, database kol)
  Grafana      http://127.0.0.1:3000
  Kafka UI     http://127.0.0.1:8080
  Prometheus   http://127.0.0.1:9090
EOF
wait
