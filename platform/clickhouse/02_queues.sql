USE scipulse;

-- =============================================================================
-- Kafka Engine Queues (AvroConfluent per ADR-004 / P-2)
-- Each queue uses a dedicated consumer group to prevent rebalance interference.
-- =============================================================================

CREATE TABLE IF NOT EXISTS registry_projects_queue (
  id             UInt64,
  slug           String,
  name           String,
  description    Nullable(String),
  keywords       Array(String),
  topics         Array(String),
  priority       UInt8,
  status         String,
  __op           String,
  __source_ts_ms UInt64,
  __deleted      String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'cdc.registry.public.tracked_projects',
         kafka_group_name  = 'ch_registry_projects',
         kafka_flush_interval_ms = 500,
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS registry_repos_queue (
  id               UInt64,
  project_id       UInt64,
  github_full_name String,
  is_primary       UInt8,
  __op             String,
  __source_ts_ms   UInt64,
  __deleted        String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'cdc.registry.public.project_repos',
         kafka_group_name  = 'ch_registry_repos',
         kafka_flush_interval_ms = 500,
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS registry_institutions_queue (
  id             UInt64,
  ror_id         Nullable(String),
  name           String,
  country        Nullable(String),
  __op           String,
  __source_ts_ms UInt64,
  __deleted      String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'cdc.registry.public.institutions',
         kafka_group_name  = 'ch_registry_institutions',
         kafka_flush_interval_ms = 500,
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS registry_project_institutions_queue (
  project_id     UInt64,
  institution_id UInt64,
  __op           String,
  __source_ts_ms UInt64,
  __deleted      String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'cdc.registry.public.project_institutions',
         kafka_group_name  = 'ch_registry_project_institutions',
         kafka_flush_interval_ms = 500,
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS papers_queue (
  source       String,
  source_id    String,
  doi          Nullable(String),
  title        String,
  published_at DateTime64(3),
  ingested_at  DateTime64(3)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'raw.papers.openalex,raw.papers.arxiv,raw.papers.inspire',
         kafka_group_name  = 'ch_live_papers',
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS code_events_queue (
  event_id       String,
  type           String,
  repo_full_name String,
  actor_login    Nullable(String),
  created_at     DateTime64(3),
  replayed_at    DateTime64(3),
  source_hour    String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'raw.code.events',
         kafka_group_name  = 'ch_live_code',
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE IF NOT EXISTS dlq_queue (
  reason         String,
  stage          String,
  source         String,
  original_topic String,
  original_key   Nullable(String),
  error          String,
  failed_at      DateTime64(3)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'dlq.papers,dlq.code',
         kafka_group_name  = 'ch_dlq',
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';
