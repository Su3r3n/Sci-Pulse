CREATE DATABASE IF NOT EXISTS scipulse;

CREATE USER IF NOT EXISTS grafana_ro
  IDENTIFIED WITH plaintext_password BY '${CLICKHOUSE_GRAFANA_PASSWORD}'
  SETTINGS readonly = 1;

GRANT SELECT ON scipulse.dm_research_trends TO grafana_ro;
GRANT SELECT ON scipulse.dm_hep_software_impact TO grafana_ro;
GRANT SELECT ON scipulse.dm_live_activity TO grafana_ro;
GRANT SELECT ON scipulse.dm_pipeline_health TO grafana_ro;
GRANT SELECT ON scipulse.v_hep_software_impact TO grafana_ro;
GRANT SELECT ON scipulse.v_projects_current TO grafana_ro;
GRANT SELECT ON scipulse.v_repos_current TO grafana_ro;
GRANT SELECT ON scipulse.v_institutions_current TO grafana_ro;
GRANT SELECT ON scipulse.v_project_institutions_current TO grafana_ro;
GRANT SELECT ON scipulse.v_mart_freshness TO grafana_ro;
GRANT SELECT ON scipulse.ops_pipeline_runs TO grafana_ro;
GRANT SELECT ON scipulse.ops_dlq_events TO grafana_ro;
GRANT SELECT ON scipulse.ops_dq_results TO grafana_ro;
GRANT SELECT ON scipulse.ops_reconciliation TO grafana_ro;
GRANT SELECT ON scipulse.live_papers_1m TO grafana_ro;
GRANT SELECT ON scipulse.live_code_1m TO grafana_ro;
GRANT SELECT ON scipulse.bench_events TO grafana_ro;

CREATE USER IF NOT EXISTS spark_writer
  IDENTIFIED WITH plaintext_password BY '${CLICKHOUSE_SPARK_PASSWORD}';

GRANT SELECT, INSERT, ALTER ON scipulse.dm_research_trends TO spark_writer;
GRANT SELECT, INSERT, ALTER, TRUNCATE ON scipulse.dm_research_trends_stage TO spark_writer;
GRANT SELECT, INSERT, ALTER ON scipulse.dm_hep_software_impact TO spark_writer;
GRANT SELECT, INSERT, ALTER, TRUNCATE ON scipulse.dm_hep_software_impact_stage TO spark_writer;
GRANT SELECT, INSERT, ALTER ON scipulse.live_papers_1m TO spark_writer;
GRANT SELECT, INSERT, ALTER ON scipulse.ops_pipeline_runs TO spark_writer;
GRANT SELECT, INSERT, ALTER ON scipulse.ops_dq_results TO spark_writer;
GRANT SELECT, INSERT, ALTER ON scipulse.ops_reconciliation TO spark_writer;
GRANT SELECT ON scipulse.v_projects_current TO spark_writer;
GRANT SELECT ON scipulse.v_repos_current TO spark_writer;
GRANT SELECT ON scipulse.v_institutions_current TO spark_writer;
GRANT SELECT ON scipulse.v_project_institutions_current TO spark_writer;
GRANT SELECT ON scipulse.v_mart_freshness TO spark_writer;
