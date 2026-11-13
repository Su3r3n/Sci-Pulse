"""
Wave 1 Cross-Branch Topology & Spec Compliance Verification Suite (§2.2–§2.3, §4, §5, §6, §7, §9.4–§9.5).

Inspects the artifacts produced in `coder-1` (`main`) and `coder_2-2` (`main-2`)
to verify spec compliance and surface integration mismatches.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict

CODER1_ROOT = Path(r"C:\Users\beish\orca\workspaces\Sci-Pulse\main")
CODER2_ROOT = Path(r"C:\Users\beish\orca\workspaces\Sci-Pulse\main-2")


EXPECTED_KAFKA_TOPICS: Dict[str, int] = {
    "raw.papers.openalex": 3,
    "raw.papers.arxiv": 3,
    "raw.papers.inspire": 3,
    "raw.code.github": 3,
    "raw.code.events": 3,
    "raw.code.zenodo": 1,
    "cdc.registry.public.tracked_projects": 1,
    "cdc.registry.public.project_repos": 1,
    "cdc.registry.public.institutions": 1,
    "cdc.registry.public.project_institutions": 1,
    "dlq.papers": 1,
    "dlq.code": 1,
    "load.synthetic": 6,
}


def test_kafka_init_topics_matches_section_2_2() -> None:
    init_topics = (CODER2_ROOT / "platform" / "kafka" / "init-topics.sh").read_text(encoding="utf-8")
    for topic, partitions in EXPECTED_KAFKA_TOPICS.items():
        assert topic in init_topics, f"Topic {topic} missing from platform/kafka/init-topics.sh"
        pattern = rf'create_(?:delete|compact)_topic\s+"{re.escape(topic)}"\s+{partitions}\b'
        assert re.search(pattern, init_topics), (
            f"Topic {topic} partition count does not match {partitions} in init-topics.sh"
        )


def test_debezium_registry_config_matches_section_4_4() -> None:
    cfg_path = CODER1_ROOT / "platform" / "debezium" / "registry.json"
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert data["name"] == "registry-postgres"
    cfg = data["config"]
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["topic.prefix"] == "cdc.registry"
    assert cfg["publication.name"] == "dbz_registry"
    assert cfg["transforms.unwrap.delete.handling.mode"] == "rewrite"
    assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert cfg["value.converter"] == "io.confluent.connect.avro.AvroConverter"


def test_clickhouse_queues_consumer_groups_match_section_2_3() -> None:
    queues_sql = (CODER2_ROOT / "platform" / "clickhouse" / "02_queues.sql").read_text(encoding="utf-8")
    for expected_group in (
        "ch_registry",
        "ch_live_papers",
        "ch_live_code",
        "ch_dlq",
        "ch_synthetic",
    ):
        assert expected_group in queues_sql, f"Missing consumer group {expected_group} in 02_queues.sql"


def test_spark_jobs_exist_and_cover_medallion_pipeline() -> None:
    spark_dir = CODER2_ROOT / "platform" / "spark"
    for script in (
        "common.py",
        "bronze_ingest.py",
        "bronze_to_silver.py",
        "enrich_projects.py",
        "silver_to_gold_trends.py",
        "silver_to_gold_impact.py",
    ):
        assert (spark_dir / script).is_file(), f"Missing Spark script {script}"
