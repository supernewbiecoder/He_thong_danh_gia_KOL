-- Kubernetes (CloudNativePG) equivalent of 01-roles.sh. Runs once, as superuser, in database `kol`
-- right after the cluster is created (spec.bootstrap.initdb.postInitApplicationSQLRefs).
--
--   kol_app       owner of database kol (created by initdb), creates tables in schema kol
--   kol_readonly  SELECT only; its password is set by spec.managed.roles from a Secret
CREATE ROLE kol_readonly LOGIN;

REVOKE ALL ON DATABASE kol FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE kol TO kol_app;
GRANT CONNECT ON DATABASE kol TO kol_readonly;

CREATE SCHEMA IF NOT EXISTS kol AUTHORIZATION kol_app;
GRANT USAGE ON SCHEMA kol TO kol_readonly;

-- Tables that kol_app creates later become readable by kol_readonly automatically
ALTER DEFAULT PRIVILEGES FOR ROLE kol_app IN SCHEMA kol GRANT SELECT ON TABLES TO kol_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE kol_app IN SCHEMA kol GRANT SELECT ON SEQUENCES TO kol_readonly;

ALTER ROLE kol_app SET search_path = kol;
ALTER ROLE kol_readonly SET search_path = kol;
