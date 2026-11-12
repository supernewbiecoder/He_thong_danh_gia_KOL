#!/usr/bin/env bash
set -Eeuo pipefail

: "${POSTGRES_REPLICATION_PASSWORD:?POSTGRES_REPLICATION_PASSWORD is required}"

psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=replication_password="$POSTGRES_REPLICATION_PASSWORD" \
    <<'SQL'
CREATE ROLE kol_replicator
    WITH REPLICATION
    LOGIN
    PASSWORD :'replication_password';
SQL
