#!/usr/bin/env bash
set -euo pipefail

CH_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${CLICKHOUSE_NATIVE_PORT:-9000}"
CH_USER="${CLICKHOUSE_USER:-default}"
CH_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

export CLICKHOUSE_GRAFANA_PASSWORD="${CLICKHOUSE_GRAFANA_PASSWORD:-grafana_ro_dev_pass}"
export CLICKHOUSE_SPARK_PASSWORD="${CLICKHOUSE_SPARK_PASSWORD:-spark_writer_dev_pass}"

ch_exec() {
  local sql_file="$1"
  echo "Applying ClickHouse SQL: $sql_file"
  # Substitute password placeholders safely for 00_users.sql
  sed -e "s|\${CLICKHOUSE_GRAFANA_PASSWORD}|${CLICKHOUSE_GRAFANA_PASSWORD}|g" \
      -e "s|\${CLICKHOUSE_SPARK_PASSWORD}|${CLICKHOUSE_SPARK_PASSWORD}|g" \
      "$sql_file" | clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER" ${CH_PASSWORD:+--password "$CH_PASSWORD"} --multiquery
}

until clickhouse-client --host "$CH_HOST" --port "$CH_PORT" --user "$CH_USER" ${CH_PASSWORD:+--password "$CH_PASSWORD"} --query "SELECT 1" >/dev/null 2>&1; do
  echo "Waiting for ClickHouse at $CH_HOST:$CH_PORT..."
  sleep 2
done

for f in /opt/clickhouse-init/*.sql; do
  ch_exec "$f"
done

echo "ch-init completed successfully."
