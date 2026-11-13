"""Shared SparkSession, Iceberg DDL, watermark, and ClickHouse JDBC/HTTP helpers."""

import os
from typing import Optional
import requests
from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """Create a SparkSession configured for Iceberg JDBC catalog and MinIO S3A."""
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    iceberg_db = os.getenv("ICEBERG_DB", "iceberg_catalog")
    iceberg_user = os.getenv("ICEBERG_DB_USER", "iceberg")
    iceberg_pass = os.getenv("ICEBERG_DB_PASSWORD", "iceberg_dev_pass")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_dev_pass")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3a://warehouse/")

    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "jdbc")
        .config(
            "spark.sql.catalog.lake.uri",
            f"jdbc:postgresql://{pg_host}:{pg_port}/{iceberg_db}",
        )
        .config("spark.sql.catalog.lake.jdbc.user", iceberg_user)
        .config("spark.sql.catalog.lake.jdbc.password", iceberg_pass)
        .config("spark.sql.catalog.lake.warehouse", warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    """Create Iceberg namespaces and core tables if they do not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.ops")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lake.bronze.raw_events (
          topic       string,
          partition   int,
          offset      bigint,
          kafka_ts    timestamp,
          key         binary,
          value       binary,
          ingest_date date
        ) USING iceberg
        PARTITIONED BY (ingest_date)
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lake.silver.papers (
          paper_key      string,
          doi            string,
          arxiv_id       string,
          sources        array<string>,
          source_ids     map<string, string>,
          title          string,
          abstract       string,
          comments       string,
          journal_ref    string,
          authors        array<struct<name: string, orcid: string, affiliations: array<string>>>,
          topics         array<string>,
          topic_l1       string,
          topic_l2       string,
          published_at   timestamp,
          updated_at     timestamp,
          cited_by_count int,
          links          array<struct<url: string, kind: string>>,
          ingested_at    timestamp,
          project_slugs  array<string>,
          repo_links     array<string>
        ) USING iceberg
        PARTITIONED BY (months(published_at))
        """
    )

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lake.ops.job_watermarks (
          job          string,
          source_table string,
          snapshot_id  bigint,
          updated_at   timestamp
        ) USING iceberg
        """
    )


def get_watermark(spark: SparkSession, job: str, source_table: str) -> Optional[int]:
    """Return the last processed Iceberg snapshot_id for (job, source_table)."""
    rows = (
        spark.table("lake.ops.job_watermarks")
        .filter(f"job = '{job}' AND source_table = '{source_table}'")
        .select("snapshot_id")
        .collect()
    )
    if not rows:
        return None
    return int(rows[0]["snapshot_id"])


def set_watermark(spark: SparkSession, job: str, source_table: str, snapshot_id: int) -> None:
    """Upsert the processed Iceberg snapshot_id watermark."""
    spark.sql(
        f"""
        MERGE INTO lake.ops.job_watermarks AS t
        USING (
          SELECT '{job}' AS job,
                 '{source_table}' AS source_table,
                 CAST({snapshot_id} AS BIGINT) AS snapshot_id,
                 current_timestamp() AS updated_at
        ) AS s
        ON t.job = s.job AND t.source_table = s.source_table
        WHEN MATCHED THEN UPDATE SET
          t.snapshot_id = s.snapshot_id,
          t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def get_latest_snapshot_id(spark: SparkSession, table: str) -> Optional[int]:
    """Return the current snapshot_id of an Iceberg table, or None if empty."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    if not rows:
        return None
    return int(rows[0]["snapshot_id"])


def ch_jdbc_url() -> str:
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    ch_db = os.getenv("CLICKHOUSE_DB", "scipulse")
    return f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}"


def ch_jdbc_props() -> dict:
    return {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": os.getenv("CLICKHOUSE_SPARK_USER", "spark_writer"),
        "password": os.getenv("CLICKHOUSE_SPARK_PASSWORD", "spark_writer_dev_pass"),
    }


def ch_http_exec(sql: str) -> None:
    """Execute a DDL/DML statement against ClickHouse HTTP interface."""
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    ch_db = os.getenv("CLICKHOUSE_DB", "scipulse")
    user = os.getenv("CLICKHOUSE_SPARK_USER", "spark_writer")
    password = os.getenv("CLICKHOUSE_SPARK_PASSWORD", "spark_writer_dev_pass")

    resp = requests.post(
        f"http://{ch_host}:{ch_port}/",
        params={"database": ch_db},
        data=sql.encode("utf-8"),
        auth=(user, password),
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"ClickHouse HTTP error {resp.status_code}: {resp.text}")
