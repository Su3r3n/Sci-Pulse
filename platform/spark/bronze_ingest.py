"""Spark Structured Streaming job: Kafka raw.* -> Iceberg lake.bronze.raw_events."""

import argparse
import os
from pyspark.sql import functions as F
from common import ensure_tables, get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw.* Kafka topics into bronze.raw_events")
    parser.add_argument(
        "--trigger",
        choices=["availableNow", "continuous"],
        default="availableNow",
        help="Streaming trigger mode (availableNow for batch, continuous for daemon)",
    )
    parser.add_argument(
        "--topic-pattern",
        default=r"raw\..*",
        help="Kafka subscribePattern regex",
    )
    args = parser.parse_args()

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    checkpoint = os.getenv("BRONZE_CHECKPOINT", "s3a://warehouse/_checkpoints/bronze")

    spark = get_spark("scipulse-bronze-ingest")
    ensure_tables(spark)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribePattern", args.topic_pattern)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("topic").cast("string").alias("topic"),
            F.col("partition").cast("int").alias("partition"),
            F.col("offset").cast("long").alias("offset"),
            F.col("timestamp").cast("timestamp").alias("kafka_ts"),
            F.col("key").cast("binary").alias("key"),
            F.col("value").cast("binary").alias("value"),
            F.to_date(F.col("timestamp")).alias("ingest_date"),
        )
    )

    writer = (
        raw_stream.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
    )

    if args.trigger == "availableNow":
        query = writer.trigger(availableNow=True).toTable("lake.bronze.raw_events")
    else:
        query = writer.trigger(processingTime="10 seconds").toTable("lake.bronze.raw_events")

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
