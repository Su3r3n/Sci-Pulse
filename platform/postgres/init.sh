#!/usr/bin/env bash
set -euo pipefail

export PGHOST="${PGHOST:-postgres}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${POSTGRES_USER:-postgres}"
export PGPASSWORD="${POSTGRES_PASSWORD:-postgres_admin_dev_pass}"

until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" >/dev/null 2>&1; do
  echo "Waiting for Postgres at $PGHOST:$PGPORT..."
  sleep 2
done

create_role_and_db() {
  local db="$1"
  local user="$2"
  local pass="$3"

  psql -v ON_ERROR_STOP=1 --dbname postgres <<EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${user}') THEN
    EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD %L', '${user}', '${pass}');
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', '${user}', '${pass}');
  END IF;
END
\$\$;
SELECT 'CREATE DATABASE ' || quote_ident('${db}') || ' OWNER ' || quote_ident('${user}')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = '${db}')\gexec
GRANT ALL PRIVILEGES ON DATABASE "${db}" TO "${user}";
EOSQL

  psql -v ON_ERROR_STOP=1 --dbname "${db}" <<EOSQL
ALTER SCHEMA public OWNER TO "${user}";
GRANT ALL ON SCHEMA public TO "${user}";
EOSQL
}

create_role_and_db "${REGISTRY_DB:-registry}" "${REGISTRY_DB_USER:-registry_app}" "${REGISTRY_DB_PASSWORD:-registry_dev_pass}"
create_role_and_db "${ICEBERG_DB:-iceberg_catalog}" "${ICEBERG_DB_USER:-iceberg}" "${ICEBERG_DB_PASSWORD:-iceberg_dev_pass}"
create_role_and_db "${HARVESTER_DB:-harvester}" "${HARVESTER_DB_USER:-harvester_app}" "${HARVESTER_DB_PASSWORD:-harvester_dev_pass}"
create_role_and_db "${AIRFLOW_DB:-airflow}" "${AIRFLOW_DB_USER:-airflow}" "${AIRFLOW_DB_PASSWORD:-airflow_dev_pass}"

# Debezium replication role
DBZ_USER="${DEBEZIUM_DB_USER:-debezium}"
DBZ_PASS="${DEBEZIUM_DB_PASSWORD:-debezium_dev_pass}"
REG_DB="${REGISTRY_DB:-registry}"
REG_USER="${REGISTRY_DB_USER:-registry_app}"

psql -v ON_ERROR_STOP=1 --dbname postgres <<EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DBZ_USER}') THEN
    EXECUTE format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', '${DBZ_USER}', '${DBZ_PASS}');
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', '${DBZ_USER}', '${DBZ_PASS}');
  END IF;
END
\$\$;
EOSQL

psql -v ON_ERROR_STOP=1 --dbname "${REG_DB}" <<EOSQL
GRANT CONNECT ON DATABASE "${REG_DB}" TO "${DBZ_USER}";
GRANT USAGE ON SCHEMA public TO "${DBZ_USER}";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "${DBZ_USER}";
ALTER DEFAULT PRIVILEGES FOR ROLE "${REG_USER}" IN SCHEMA public GRANT SELECT ON TABLES TO "${DBZ_USER}";
EOSQL

echo "pg-init completed successfully."
