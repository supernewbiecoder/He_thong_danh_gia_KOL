
#!/usr/bin/env bash
set -Eeuo pipefail

DATA_DIR="${PGDATA:-/var/lib/postgresql/data/pgdata}"
PG_HOME="/var/lib/postgresql"
PASSFILE="${PG_HOME}/.pgpass"

PRIMARY_HOST="${PRIMARY_HOST:?PRIMARY_HOST is required}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:?REPLICATION_USER is required}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"

echo "Preparing PostgreSQL standby..."

install -d -o postgres -g postgres -m 0700 "$DATA_DIR"

if [ ! -s "$DATA_DIR/PG_VERSION" ]; then
    echo "Waiting for primary PostgreSQL..."

    until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT"; do
        sleep 3
    done

    echo "Starting base backup..."

    export PGPASSWORD="$REPLICATION_PASSWORD"

    gosu postgres pg_basebackup \
        -h "$PRIMARY_HOST" \
        -p "$PRIMARY_PORT" \
        -U "$REPLICATION_USER" \
        -D "$DATA_DIR" \
        -Fp \
        -Xs \
        -P \
        -R

    unset PGPASSWORD

    echo "Base backup completed."
else
    echo "Existing standby data found. Skipping base backup."
fi

echo "Configuring PostgreSQL password file..."

printf '%s:%s:replication:%s:%s\n' \
    "$PRIMARY_HOST" \
    "$PRIMARY_PORT" \
    "$REPLICATION_USER" \
    "$REPLICATION_PASSWORD" > "$PASSFILE"

chown postgres:postgres "$PASSFILE"
chmod 0600 "$PASSFILE"

unset REPLICATION_PASSWORD

echo "Starting PostgreSQL..."

exec /usr/local/bin/docker-entrypoint.sh "$@"
