USE scipulse;

-- =============================================================================
-- 1. Materialized Views (Kafka Queues -> Target Tables)
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_registry_projects TO registry_projects AS
SELECT id, slug, name, description, keywords, topics, priority, status,
       __source_ts_ms AS version, __deleted = 'true' AS is_deleted
FROM registry_projects_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_registry_repos TO registry_repos AS
SELECT id, project_id, github_full_name, is_primary,
       __source_ts_ms AS version, __deleted = 'true' AS is_deleted
FROM registry_repos_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_registry_institutions TO registry_institutions AS
SELECT id, ror_id, name, country,
       __source_ts_ms AS version, __deleted = 'true' AS is_deleted
FROM registry_institutions_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_registry_project_institutions TO registry_project_institutions AS
SELECT project_id, institution_id,
       __source_ts_ms AS version, __deleted = 'true' AS is_deleted
FROM registry_project_institutions_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_live_papers TO live_papers_1m AS
SELECT toStartOfMinute(_timestamp) AS minute,
       _topic AS topic,
       source,
       countState() AS events,
       max(ingested_at) AS last_ingested
FROM papers_queue
GROUP BY minute, topic, source;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_live_code TO live_code_1m AS
SELECT toStartOfMinute(_timestamp) AS minute,
       repo_full_name,
       type,
       countState() AS events,
       max(replayed_at) AS last_ingested
FROM code_events_queue
GROUP BY minute, repo_full_name, type;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dlq_events TO ops_dlq_events AS
SELECT failed_at AS ts,
       original_topic AS topic,
       source,
       reason,
       stage
FROM dlq_queue;

-- =============================================================================
-- 2. Current State Views (Registry)
-- =============================================================================

CREATE OR REPLACE VIEW v_projects_current AS
SELECT id,
       argMax(slug, version)     AS slug,
       argMax(name, version)     AS name,
       argMax(description, version) AS description,
       argMax(keywords, version) AS keywords,
       argMax(topics, version)   AS topics,
       argMax(priority, version) AS priority,
       argMax(status, version)   AS status
FROM registry_projects
GROUP BY id
HAVING argMax(is_deleted, version) = 0;

CREATE OR REPLACE VIEW v_repos_current AS
SELECT id,
       argMax(project_id, version)       AS project_id,
       argMax(github_full_name, version) AS github_full_name,
       argMax(is_primary, version)       AS is_primary
FROM registry_repos
GROUP BY id
HAVING argMax(is_deleted, version) = 0;

CREATE OR REPLACE VIEW v_institutions_current AS
SELECT id,
       argMax(ror_id, version)  AS ror_id,
       argMax(name, version)    AS name,
       argMax(country, version) AS country
FROM registry_institutions
GROUP BY id
HAVING argMax(is_deleted, version) = 0;

CREATE OR REPLACE VIEW v_project_institutions_current AS
SELECT project_id, institution_id
FROM registry_project_institutions
GROUP BY project_id, institution_id
HAVING argMax(is_deleted, version) = 0;

-- =============================================================================
-- 3. Mart & Health Views
-- =============================================================================

CREATE OR REPLACE VIEW dm_live_activity AS
SELECT minute,
       'paper' AS kind,
       source AS label,
       countMerge(events) AS events
FROM live_papers_1m
GROUP BY minute, label
UNION ALL
SELECT minute,
       'code' AS kind,
       concat(repo_full_name, ' · ', type) AS label,
       countMerge(events) AS events
FROM live_code_1m
WHERE repo_full_name IN (SELECT github_full_name FROM v_repos_current)
GROUP BY minute, label;

CREATE OR REPLACE VIEW v_hep_software_impact AS
SELECT i.*,
       p.name AS project_name,
       p.priority,
       p.status
FROM dm_hep_software_impact AS i
LEFT JOIN v_projects_current AS p ON p.slug = i.project_slug;

CREATE OR REPLACE VIEW v_mart_freshness AS
SELECT 'dm_research_trends' AS mart, max(updated_at) AS fresh_at FROM dm_research_trends
UNION ALL
SELECT 'dm_hep_software_impact' AS mart, max(updated_at) AS fresh_at FROM dm_hep_software_impact
UNION ALL
SELECT 'dm_live_activity' AS mart, toDateTime(max(last_ingested)) AS fresh_at FROM live_papers_1m;

CREATE OR REPLACE VIEW dm_pipeline_health AS
SELECT
  toStartOfMinute(minute) AS ts,
  sum(paper_events) AS papers_per_min,
  sum(dlq_events) AS dlq_per_min
FROM (
  SELECT minute, countMerge(events) AS paper_events, toUInt64(0) AS dlq_events
  FROM live_papers_1m
  GROUP BY minute
  UNION ALL
  SELECT toStartOfMinute(toDateTime(ts)) AS minute, toUInt64(0) AS paper_events, count() AS dlq_events
  FROM ops_dlq_events
  GROUP BY minute
)
GROUP BY ts
ORDER BY ts DESC;
