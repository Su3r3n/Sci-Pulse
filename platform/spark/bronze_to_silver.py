"""Incremental Spark job: Iceberg lake.bronze.raw_events -> lake.silver.papers."""

import json
import os
from pathlib import Path
from pyspark.sql import Window, functions as F
from pyspark.sql.avro.functions import from_avro
from common import (
    ensure_tables,
    get_latest_snapshot_id,
    get_spark,
    get_watermark,
    set_watermark,
)

PAPER_V1_SCHEMA_FALLBACK = {
    "type": "record",
    "name": "Paper",
    "namespace": "org.scipulse.v1",
    "fields": [
        {
            "name": "source",
            "type": {"type": "enum", "name": "Source", "symbols": ["openalex", "arxiv", "inspire"]},
        },
        {"name": "source_id", "type": "string"},
        {"name": "doi", "type": ["null", "string"], "default": None},
        {"name": "arxiv_id", "type": ["null", "string"], "default": None},
        {"name": "title", "type": "string"},
        {"name": "abstract", "type": ["null", "string"], "default": None},
        {"name": "comments", "type": ["null", "string"], "default": None},
        {"name": "journal_ref", "type": ["null", "string"], "default": None},
        {
            "name": "authors",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Author",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "orcid", "type": ["null", "string"], "default": None},
                        {"name": "affiliations", "type": {"type": "array", "items": "string"}, "default": []},
                    ],
                },
            },
        },
        {"name": "topics", "type": {"type": "array", "items": "string"}, "default": []},
        {"name": "published_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "updated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "cited_by_count", "type": ["null", "int"], "default": None},
        {
            "name": "links",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Link",
                    "fields": [
                        {"name": "url", "type": "string"},
                        {"name": "kind", "type": "string"},
                    ],
                },
            },
            "default": [],
        },
        {"name": "ingested_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
}


def load_paper_schema() -> str:
    contract_path = Path(os.getenv("PAPER_SCHEMA_PATH", "/opt/spark/contracts/paper.v1.avsc"))
    if contract_path.exists():
        return contract_path.read_text(encoding="utf-8")
    return json.dumps(PAPER_V1_SCHEMA_FALLBACK)


def main() -> None:
    spark = get_spark("scipulse-bronze-to-silver")
    ensure_tables(spark)

    latest_snap = get_latest_snapshot_id(spark, "lake.bronze.raw_events")
    if latest_snap is None:
        print("bronze.raw_events has no snapshots yet; nothing to process.")
        spark.stop()
        return

    last_snap = get_watermark(spark, "bronze_to_silver", "lake.bronze.raw_events")
    if last_snap is not None and last_snap == latest_snap:
        print(f"Already at latest snapshot {latest_snap}; nothing to process.")
        spark.stop()
        return

    reader = spark.read
    if last_snap is not None:
        reader = reader.option("start-snapshot-id", str(last_snap)).option(
            "end-snapshot-id", str(latest_snap)
        )
    else:
        reader = reader.option("snapshot-id", str(latest_snap))

    bronze_df = reader.table("lake.bronze.raw_events").filter(
        F.col("topic").startswith("raw.papers.") & (F.length(F.col("value")) > 5)
    )

    if bronze_df.rdd.isEmpty():
        set_watermark(spark, "bronze_to_silver", "lake.bronze.raw_events", latest_snap)
        print(f"No new paper events up to snapshot {latest_snap}.")
        spark.stop()
        return

    schema_json = load_paper_schema()
    decoded = (
        bronze_df.withColumn(
            "avro_payload", F.expr("substring(value, 6, length(value) - 5)")
        )
        .withColumn("p", from_avro(F.col("avro_payload"), schema_json))
        .select("p.*")
    )

    # Normalize DOI and arXiv ID (§6.5)
    norm = (
        decoded.withColumn(
            "doi_clean",
            F.lower(
                F.regexp_replace(
                    F.trim(F.col("doi")),
                    r"^(https?://(dx\.)?doi\.org/|doi:)",
                    "",
                )
            ),
        )
        .withColumn("doi_clean", F.when(F.col("doi_clean") == "", None).otherwise(F.col("doi_clean")))
        .withColumn(
            "arxiv_from_doi",
            F.when(
                F.col("doi_clean").startswith("10.48550/arxiv."),
                F.regexp_replace(F.col("doi_clean"), r"^10\.48550/arxiv\.", ""),
            ),
        )
        .withColumn(
            "arxiv_clean",
            F.regexp_replace(
                F.trim(F.coalesce(F.col("arxiv_id"), F.col("arxiv_from_doi"))),
                r"v\d+$",
                "",
            ),
        )
        .withColumn(
            "arxiv_clean",
            F.when(F.col("arxiv_clean") == "", None).otherwise(F.col("arxiv_clean")),
        )
        .withColumn(
            "paper_key",
            F.coalesce(
                F.col("doi_clean"),
                F.col("arxiv_clean"),
                F.concat(F.col("source"), F.lit(":"), F.col("source_id")),
            ),
        )
    )

    # Map topics -> topic_l1, topic_l2 using reference/topics_map.csv
    topics_csv = os.getenv("TOPICS_MAP_PATH", "/opt/spark/reference/topics_map.csv")
    if Path(topics_csv).exists():
        tmap = spark.read.option("header", "true").csv(topics_csv)
        exploded = norm.select(
            "paper_key", F.posexplode_outer("topics").alias("t_pos", "raw_topic")
        )
        joined = (
            exploded.join(F.broadcast(tmap), on="raw_topic", how="left")
            .filter(F.col("topic_l1").isNotNull())
        )
        w_topic = Window.partitionBy("paper_key").orderBy(F.col("t_pos").asc())
        mapped_topics = (
            joined.withColumn("rn", F.row_number().over(w_topic))
            .filter(F.col("rn") == 1)
            .select("paper_key", "topic_l1", "topic_l2")
        )
        norm = norm.join(mapped_topics, on="paper_key", how="left")
    else:
        norm = norm.withColumn("topic_l1", F.lit(None).cast("string")).withColumn(
            "topic_l2", F.lit(None).cast("string")
        )

    norm = norm.withColumn("topic_l1", F.coalesce(F.col("topic_l1"), F.lit("Other"))).withColumn(
        "topic_l2", F.coalesce(F.col("topic_l2"), F.lit("General"))
    )

    # Deduplicate within batch by paper_key, keeping latest updated_at
    w_dedup = Window.partitionBy("paper_key").orderBy(
        F.col("updated_at").desc(), F.col("ingested_at").desc()
    )
    updates = (
        norm.withColumn("rn", F.row_number().over(w_dedup))
        .filter(F.col("rn") == 1)
        .select(
            F.col("paper_key"),
            F.col("doi_clean").alias("doi"),
            F.col("arxiv_clean").alias("arxiv_id"),
            F.array(F.col("source")).alias("sources"),
            F.map_from_arrays(F.array(F.col("source")), F.array(F.col("source_id"))).alias(
                "source_ids"
            ),
            F.col("title"),
            F.col("abstract"),
            F.col("comments"),
            F.col("journal_ref"),
            F.col("authors"),
            F.col("topics"),
            F.col("topic_l1"),
            F.col("topic_l2"),
            F.col("published_at"),
            F.col("updated_at"),
            F.col("cited_by_count"),
            F.col("links"),
            F.col("ingested_at"),
            F.array().cast("array<string>").alias("project_slugs"),
            F.array().cast("array<string>").alias("repo_links"),
        )
    )

    updates.createOrReplaceTempView("paper_updates")

    spark.sql(
        """
        MERGE INTO lake.silver.papers AS t
        USING paper_updates AS s
        ON t.paper_key = s.paper_key
           OR (t.doi IS NOT NULL AND s.doi IS NOT NULL AND t.doi = s.doi)
           OR (t.arxiv_id IS NOT NULL AND s.arxiv_id IS NOT NULL AND t.arxiv_id = s.arxiv_id)
        WHEN MATCHED THEN UPDATE SET
          t.doi            = coalesce(s.doi, t.doi),
          t.arxiv_id       = coalesce(s.arxiv_id, t.arxiv_id),
          t.sources        = array_distinct(concat(t.sources, s.sources)),
          t.source_ids     = map_concat(t.source_ids, s.source_ids),
          t.title          = CASE WHEN s.updated_at >= t.updated_at THEN s.title ELSE t.title END,
          t.abstract       = CASE WHEN s.updated_at >= t.updated_at THEN coalesce(s.abstract, t.abstract) ELSE t.abstract END,
          t.comments       = coalesce(s.comments, t.comments),
          t.journal_ref    = coalesce(s.journal_ref, t.journal_ref),
          t.authors        = CASE WHEN s.updated_at >= t.updated_at AND size(s.authors) > 0 THEN s.authors ELSE t.authors END,
          t.topics         = array_distinct(concat(coalesce(t.topics, array()), coalesce(s.topics, array()))),
          t.topic_l1       = CASE WHEN t.topic_l1 = 'Other' AND s.topic_l1 != 'Other' THEN s.topic_l1 ELSE t.topic_l1 END,
          t.topic_l2       = CASE WHEN t.topic_l2 = 'General' AND s.topic_l2 != 'General' THEN s.topic_l2 ELSE t.topic_l2 END,
          t.updated_at     = greatest(t.updated_at, s.updated_at),
          t.cited_by_count = greatest(coalesce(t.cited_by_count, 0), coalesce(s.cited_by_count, 0)),
          t.links          = CASE WHEN size(s.links) > 0 THEN s.links ELSE t.links END,
          t.ingested_at    = greatest(t.ingested_at, s.ingested_at)
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    set_watermark(spark, "bronze_to_silver", "lake.bronze.raw_events", latest_snap)
    print(f"Merged paper updates up to snapshot {latest_snap}.")
    spark.stop()


if __name__ == "__main__":
    main()
