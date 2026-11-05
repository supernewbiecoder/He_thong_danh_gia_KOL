#!/usr/bin/env bash
# Creates the application roles and schema. Run by the postgres image only when the data
# volume is empty, so changing a password in .env.shared later has no effect here
# (use ALTER ROLE instead, see docs/shared-infrastructure.md).
#
#   kol_admin     owner of the database, for administration and backups
#   kol_app       creates tables in schema kol and reads/writes them (DB module, writers)
#   kol_readonly  SELECT only (API, dashboard, ad-hoc queries)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_password="$POSTGRES_APP_PASSWORD" \
  -v readonly_password="$POSTGRES_READONLY_PASSWORD" <<'SQL'
CREATE ROLE kol_app LOGIN PASSWORD :'app_password';
CREATE ROLE kol_readonly LOGIN PASSWORD :'readonly_password';

REVOKE ALL ON DATABASE kol FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE kol TO kol_app;
GRANT CONNECT ON DATABASE kol TO kol_readonly;

CREATE SCHEMA kol AUTHORIZATION kol_app;
GRANT USAGE ON SCHEMA kol TO kol_readonly;

-- Tables that kol_app creates later become readable by kol_readonly automatically
ALTER DEFAULT PRIVILEGES FOR ROLE kol_app IN SCHEMA kol GRANT SELECT ON TABLES TO kol_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE kol_app IN SCHEMA kol GRANT SELECT ON SEQUENCES TO kol_readonly;

ALTER ROLE kol_app SET search_path = kol;
ALTER ROLE kol_readonly SET search_path = kol;
SQL
