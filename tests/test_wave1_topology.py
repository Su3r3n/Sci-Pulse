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

# Оба кодера влиты в main, поэтому и coder-1, и coder-2 артефакты лежат в корне репозитория.
REPO_ROOT = Path(__file__).resolve().parent.parent
CODER1_ROOT = REPO_ROOT
CODER2_ROOT = REPO_ROOT


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
    # Допускаются оба формата: обёртка {name, config} (для POST /connectors) и плоская карта
    # (для PUT /connectors/registry-postgres/config, см. L1 в docs/reviews/wave1-live-check.md).
    if isinstance(data.get("config"), dict):
        assert data["name"] == "registry-postgres"
        cfg = data["config"]
    else:
        cfg = data
        assert cfg.get("name", "registry-postgres") == "registry-postgres"
    assert cfg["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert cfg["plugin.name"] == "pgoutput"
    assert cfg["topic.prefix"] == "cdc.registry"
    assert cfg["publication.name"] == "dbz_registry"
    # §4.4 (примечание) и ADR-004: в Debezium >= 2.5 пара delete.handling.mode=rewrite +
    # drop.tombstones=true заменена одним ключом delete.tombstone.handling.mode=rewrite
    # (строка удаления с __deleted=true, tombstone не пишется). Образ — 2.7.3.Final, поэтому
    # ожидаем новый ключ; старые ключи deprecated и вместе с новым не допускаются.
    assert cfg.get("transforms.unwrap.delete.tombstone.handling.mode") == "rewrite"
    for deprecated in ("transforms.unwrap.delete.handling.mode", "transforms.unwrap.drop.tombstones"):
        assert deprecated not in cfg, f"{deprecated} is deprecated in Debezium 2.7 and conflicts with the new key"
    assert cfg["transforms.unwrap.add.fields"] == "op,source.ts_ms"
    # Р-2 / ADR-004 (ред. после ревью m4): ключ и значение — AvroConfluent, оба со ссылкой на SR.
    assert cfg["key.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert cfg["value.converter"] == "io.confluent.connect.avro.AvroConverter"
    assert cfg["key.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert cfg["value.converter.schema.registry.url"] == "http://schema-registry:8081"
    assert cfg["database.user"] == "${file:/secrets/debezium.properties:user}"
    assert cfg["database.password"] == "${file:/secrets/debezium.properties:password}"


def test_clickhouse_queues_consumer_groups_match_section_2_3() -> None:
    # ch_synthetic (synthetic_queue) — объект спринта S5 (§7.1), в волне 1 не проверяется.
    queues_sql = (CODER2_ROOT / "platform" / "clickhouse" / "02_queues.sql").read_text(encoding="utf-8")
    groups = set(re.findall(r"kafka_group_name\s*=\s*'([^']+)'", queues_sql))
    for expected_group in ("ch_live_papers", "ch_live_code", "ch_dlq"):
        assert expected_group in groups, f"Missing consumer group {expected_group} in 02_queues.sql"
    # §2.3 называет группу реестра `ch_registry`; coder-2 использует по группе на очередь
    # (ch_registry_projects, ...). Функционально эквивалентно, поэтому проверяем префикс.
    registry_groups = {g for g in groups if g.startswith("ch_registry")}
    assert len(registry_groups) >= 2, f"Registry queues need consumer groups, got {sorted(groups)}"


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
