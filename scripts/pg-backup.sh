#!/usr/bin/env bash
# Dumps the database to /backups and deletes old dumps. Runs in the postgres-backup container.
#   pg-backup.sh          loop: back up now, then every BACKUP_INTERVAL_HOURS
#   pg-backup.sh --once   one backup, then exit
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/backups}"
BACKUP_INTERVAL_HOURS="${BACKUP_INTERVAL_HOURS:-24}"
BACKUP_KEEP_DAYS="${BACKUP_KEEP_DAYS:-14}"

backup_once() {
  local target="${BACKUP_DIR}/kol-$(date -u +%Y%m%d-%H%M%S).dump"
  # Write to a temporary name so a failed dump never looks like a valid backup
  pg_dump --format=custom --file="${target}.tmp"
  mv "${target}.tmp" "${target}"
  echo "[pg-backup] wrote ${target} ($(du -h "${target}" | cut -f1))"
  find "${BACKUP_DIR}" -name 'kol-*.dump' -mtime "+${BACKUP_KEEP_DAYS}" -print -delete
}

if [[ "${1:-}" == "--once" ]]; then
  backup_once
  exit 0
fi

while true; do
  # A failed backup must not stop later ones
  backup_once || echo "[pg-backup] ERROR: backup failed, retrying at the next interval" >&2
  sleep "$((BACKUP_INTERVAL_HOURS * 3600))"
done
