#!/usr/bin/env bash
# Runs the team-shared environment on the host machine.
#
#   ./scripts/shared.sh up       create .env.shared if missing, start everything, publish on Tailscale
#   ./scripts/shared.sh down     stop containers and unpublish (data is kept)
#   ./scripts/shared.sh status   containers, disk usage, Tailscale forwards
#   ./scripts/shared.sh info     connection details to send to the team (contains passwords)
#   ./scripts/shared.sh backup   back up PostgreSQL now
#
# NO_TAILSCALE=1 skips Tailscale (for testing on one machine, clients use 127.0.0.1).
set -euo pipefail
source "$(dirname "$0")/lib.sh"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENV_FILE=.env.shared
PORTS=(9092 9093 9094 5432 8080 3000 9090)

tailscale_cli() {
  if command -v tailscale >/dev/null 2>&1; then
    tailscale "$@"
  elif [[ -x "/c/Program Files/Tailscale/tailscale.exe" ]]; then
    "/c/Program Files/Tailscale/tailscale.exe" "$@"
  else
    echo "ERROR: Tailscale is not installed: https://tailscale.com/download" >&2
    return 1
  fi
}

use_tailscale() { [[ "${NO_TAILSCALE:-0}" != "1" ]]; }

# Full MagicDNS name of this machine, e.g. kol-server.tail1234.ts.net
tailscale_hostname() {
  tailscale_cli status --json | tr -d '\r' \
    | awk '/"Self":/ { self = 1 } self && /"DNSName":/ { gsub(/[",]/, "", $2); sub(/\.$/, "", $2); print $2; exit }'
}

dc() {
  docker compose -f docker-compose.yml -f docker-compose.shared.yml --env-file "$ENV_FILE" "$@"
}

env_value() {
  tr -d '\r' < "$ENV_FILE" | sed -n "s/^$1=//p" | head -n 1
}

create_env_file() {
  local host="127.0.0.1"
  if use_tailscale; then
    host="$(tailscale_hostname)"
    [[ -n "$host" ]] || { echo "ERROR: Tailscale is not connected (run: tailscale up)" >&2; exit 1; }
  fi
  log "Creating ${ENV_FILE} with random passwords (advertised host: ${host})"
  sed -e "s|^KAFKA_ADVERTISED_HOST=.*|KAFKA_ADVERTISED_HOST=${host}|" \
      -e "s|^POSTGRES_ADMIN_PASSWORD=.*|POSTGRES_ADMIN_PASSWORD=$(openssl rand -hex 24)|" \
      -e "s|^POSTGRES_APP_PASSWORD=.*|POSTGRES_APP_PASSWORD=$(openssl rand -hex 24)|" \
      -e "s|^POSTGRES_READONLY_PASSWORD=.*|POSTGRES_READONLY_PASSWORD=$(openssl rand -hex 24)|" \
      -e "s|^GRAFANA_ADMIN_PASSWORD=.*|GRAFANA_ADMIN_PASSWORD=$(openssl rand -hex 12)|" \
      .env.shared.example > "$ENV_FILE"
}

# The shared host runs the collector too, so its credentials must be in .env.shared. Copying them
# from the local .env avoids retyping the YouTube key into a second file.
carry_over_credentials() {
  local key value
  [[ -f .env ]] || return 0
  for key in YOUTUBE_API_KEY YOUTUBE_CHANNEL_IDS YOUTUBE_SEARCH_QUERIES BLUESKY_HANDLES; do
    value="$(tr -d '\r' < .env | sed -n "s/^${key}=//p" | head -n 1)"
    [[ -n "$value" ]] || continue
    sed -i.bak -e "s|^${key}=.*|${key}=${value}|" "$ENV_FILE" && rm -f "${ENV_FILE}.bak"
    log "Copied ${key} from .env"
  done
}

publish() {
  local p
  for p in "${PORTS[@]}"; do
    tailscale_cli serve --bg --tcp="$p" "tcp://localhost:$p" >/dev/null
  done
  log "Published on Tailscale: ${PORTS[*]}"
}

unpublish() {
  local p
  for p in "${PORTS[@]}"; do
    tailscale_cli serve --tcp="$p" off >/dev/null 2>&1 || true
  done
  log "Removed Tailscale forwards"
}

show_info() {
  local host; host="$(env_value KAFKA_ADVERTISED_HOST)"
  cat <<EOF

Connection details (contain passwords: share them privately, do not commit them)

  Kafka       ${host}:9092,${host}:9093,${host}:9094
  PostgreSQL  postgresql://kol_app:$(env_value POSTGRES_APP_PASSWORD)@${host}:5432/kol
  Read-only   postgresql://kol_readonly:$(env_value POSTGRES_READONLY_PASSWORD)@${host}:5432/kol
  Kafka UI    http://${host}:8080
  Grafana     http://${host}:3000  (admin / $(env_value GRAFANA_ADMIN_PASSWORD))
  Prometheus  http://${host}:9090

EOF
  if [[ -z "$(env_value YOUTUBE_API_KEY)" ]]; then
    warn "YOUTUBE_API_KEY is empty in ${ENV_FILE}: only Bluesky is collected. Add the key and run: ./scripts/shared.sh up"
  fi
}

case "${1:-}" in
  up)
    if [[ ! -f "$ENV_FILE" ]]; then
      create_env_file
      carry_over_credentials
    fi
    mkdir -p backups
    dc up -d --build --wait --wait-timeout 300 \
      kafka-1 kafka-2 kafka-3 kafka-ui kafka-exporter prometheus grafana postgres postgres-backup \
      db-writer social-collectors
    dc run --rm kafka-init
    use_tailscale && publish
    show_info
    ;;
  down)
    use_tailscale && unpublish
    dc down
    log "Stopped. Data is kept in the Docker volumes."
    ;;
  status)
    dc ps
    step "Kafka data disk"
    docker exec kafka-1 df -h /var/lib/kafka/data
    step "Backups"
    ls -lh backups 2>/dev/null | tail -n 5
    if use_tailscale; then
      step "Tailscale forwards"
      tailscale_cli serve status
    fi
    ;;
  info)
    show_info
    ;;
  backup)
    dc exec -T postgres-backup bash /scripts/pg-backup.sh --once
    ;;
  *)
    sed -n '2,10p' "$0"
    exit 1
    ;;
esac
