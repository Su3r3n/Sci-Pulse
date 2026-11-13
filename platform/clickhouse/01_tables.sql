USE scipulse;

-- =============================================================================
-- 1. Registry CDC Replicas (ReplacingMergeTree)
-- =============================================================================

CREATE TABLE IF NOT EXISTS registry_projects (
  id          UInt64,
  slug        String,
  name        String,
  description Nullable(String),
  keywords    Array(String),
  topics      Array(String),
  priority    UInt8,
  status      LowCardinality(String),
  version     UInt64,
  is_deleted  UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS registry_repos (
  id               UInt64,
  project_id       UInt64,
  github_full_name String,
  is_primary       UInt8,
  version          UInt64,
  is_deleted       UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS registry_institutions (
  id         UInt64,
  ror_id     Nullable(String),
  name       String,
  country    Nullable(String),
  version    UInt64,
  is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS registry_project_institutions (
  project_id     UInt64,
  institution_id UInt64,
  version        UInt64,
  is_deleted     UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY (project_id, institution_id);

-- =============================================================================
-- 2. Speed Layer (AggregatingMergeTree)
-- =============================================================================

CREATE TABLE IF NOT EXISTS live_papers_1m (
  minute        DateTime,
  topic         LowCardinality(String),
  source        LowCardinality(String),
  events        AggregateFunction(count),
  last_ingested SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, topic, source)
TTL minute + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS live_code_1m (
  minute         DateTime,
  repo_full_name LowCardinality(String),
  type           LowCardinality(String),
  events         AggregateFunction(count),
  last_ingested  SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, repo_full_name, type)
TTL minute + INTERVAL 90 DAY;

-- =============================================================================
-- 3. Gold Data Marts & Stage Tables (P-5: atomic REPLACE PARTITION)
-- =============================================================================

CREATE TABLE IF NOT EXISTS dm_research_trends (
  month            Date,
  topic_l1         LowCardinality(String),
  topic_l2         LowCardinality(String),
  papers           UInt32,
  avg_citations    Float32,
  papers_with_code UInt32,
  updated_at       DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (month, topic_l1, topic_l2);

CREATE TABLE IF NOT EXISTS dm_research_trends_stage AS dm_research_trends;

CREATE TABLE IF NOT EXISTS dm_hep_software_impact (
  project_slug     LowCardinality(String),
  month            Date,
  linked_papers    UInt32,
  cited_total      UInt64,
  releases         UInt16,
  activity_events  UInt32,
  top_institutions Array(String),
  updated_at       DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (project_slug, month);

CREATE TABLE IF NOT EXISTS dm_hep_software_impact_stage AS dm_hep_software_impact;

-- =============================================================================
-- 4. Operational Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS ops_pipeline_runs (
  run_id      String,
  dag_id      LowCardinality(String),
  task_id     LowCardinality(String),
  started_at  DateTime,
  finished_at DateTime,
  duration_s  Float32,
  status      LowCardinality(String),
  rows_out    UInt64
) ENGINE = MergeTree
ORDER BY (dag_id, task_id, started_at);

CREATE TABLE IF NOT EXISTS ops_dlq_events (
  ts     DateTime64(3),
  topic  LowCardinality(String),
  source LowCardinality(String),
  reason LowCardinality(String),
  stage  LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts, reason)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS ops_dq_results (
  run_id      String,
  dag_run_id  String,
  suite       LowCardinality(String),
  expectation String,
  severity    LowCardinality(String),
  success     UInt8,
  observed    String,
  ts          DateTime
) ENGINE = MergeTree
ORDER BY (suite, ts);

CREATE TABLE IF NOT EXISTS ops_reconciliation (
  run_id     String,
  topic      LowCardinality(String),
  hour       DateTime,
  bronze_cnt UInt64,
  ch_cnt     UInt64,
  diff       Int64,
  status     LowCardinality(String),
  checked_at DateTime
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (topic, hour);

-- =============================================================================
-- 5. Benchmark Table (TTL 1 day)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bench_events (
  event_type  LowCardinality(String),
  country     LowCardinality(String),
  latency_ms  UInt32,
  bytes       UInt32,
  produced_at DateTime64(6),
  ingested_at DateTime64(6) DEFAULT now64(6),
  synthetic   UInt8
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(produced_at)
ORDER BY produced_at
TTL toDateTime(produced_at) + INTERVAL 1 DAY;
