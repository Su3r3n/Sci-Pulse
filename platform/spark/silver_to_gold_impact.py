"""Spark job: lake.silver.papers -> ClickHouse dm_hep_software_impact (v0) via REPLACE PARTITION (P-5)."""

import argparse
from pyspark.sql import functions as F
from common import ch_http_exec, ch_jdbc_props, ch_jdbc_url, ensure_tables, get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dm_hep_software_impact gold mart (v0)")
    parser.add_argument(
        "--months",
        default="all",
        help="Comma-separated YYYY-MM months to recompute, or 'all'",
    )
    args = parser.parse_args()

    spark = get_spark("scipulse-silver-to-gold-impact")
    ensure_tables(spark)

    papers = spark.table("lake.silver.papers").filter(
        F.col("published_at").isNotNull() & (F.size(F.coalesce(F.col("project_slugs"), F.array())) > 0)
    )
    if args.months != "all":
        month_list = [m.strip() for m in args.months.split(",") if m.strip()]
        papers = papers.filter(F.date_format("published_at", "yyyy-MM").isin(month_list))

    agg = (
        papers.withColumn("project_slug", F.explode(F.col("project_slugs")))
        .withColumn("month", F.to_date(F.date_trunc("month", F.col("published_at"))))
        .groupBy("project_slug", "month")
        .agg(
            F.count("*").cast("long").alias("linked_papers"),
            F.sum(F.coalesce(F.col("cited_by_count"), F.lit(0))).cast("long").alias("cited_total"),
            F.lit(0).cast("int").alias("releases"),
            F.lit(0).cast("long").alias("activity_events"),
            F.array().cast("array<string>").alias("top_institutions"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )

    if agg.rdd.isEmpty():
        print("No rows to write to dm_hep_software_impact.")
        spark.stop()
        return

    partitions = [
        r["ym"]
        for r in agg.select(F.date_format("month", "yyyyMM").alias("ym")).distinct().collect()
    ]

    ch_http_exec("TRUNCATE TABLE IF EXISTS scipulse.dm_hep_software_impact_stage")
    agg.write.jdbc(
        url=ch_jdbc_url(),
        table="dm_hep_software_impact_stage",
        mode="append",
        properties=ch_jdbc_props(),
    )

    for ym in partitions:
        ch_http_exec(
            f"ALTER TABLE scipulse.dm_hep_software_impact REPLACE PARTITION '{ym}' FROM scipulse.dm_hep_software_impact_stage"
        )

    ch_http_exec("TRUNCATE TABLE IF EXISTS scipulse.dm_hep_software_impact_stage")
    print(f"Replaced {len(partitions)} partition(s) in dm_hep_software_impact: {partitions}")
    spark.stop()


if __name__ == "__main__":
    main()
