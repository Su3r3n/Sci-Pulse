"""
Волна 1, проверка на живом стенде (docs/reviews/wave1-live-check.md).

Статические инварианты для дефектов, которые проявились только при запуске стека
(`docker compose --profile core up`). Docker для этих тестов не нужен. L1–L3 и L5
исправлены и подтверждены на стенде (docs/reviews/wave1-final-check.md); тесты держат
регрессию.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = REPO_ROOT / "docker-compose.yml"
REGISTRY_JSON = REPO_ROOT / "platform" / "debezium" / "registry.json"
QUEUES_SQL = REPO_ROOT / "platform" / "clickhouse" / "02_queues.sql"


def _service(name: str) -> dict:
    svc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"].get(name)
    assert svc is not None, f"service {name} missing from docker-compose.yml"
    return svc


def _env(svc: dict) -> dict:
    env = svc.get("environment") or {}
    if isinstance(env, list):
        return dict(e.split("=", 1) for e in env)
    return {k: str(v) for k, v in env.items()}


def test_connect_init_payload_matches_connect_rest_endpoint() -> None:
    """L1: `PUT /connectors/{name}/config` принимает плоскую карту конфигурации.

    Обёртка {"name": ..., "config": {...}} подходит только для `POST /connectors`. На стенде
    connect-init отправлял обёрнутый registry.json в PUT и получал HTTP 500
    `Cannot deserialize value of type java.lang.String from Object value ... ["config"]`,
    curl -f завершался с кодом 22, а harvester (depends_on connect-init) не стартовал.
    """
    script = " ".join(_service("connect-init").get("entrypoint") or []) + " " + str(
        _service("connect-init").get("command") or ""
    )
    data = json.loads(REGISTRY_JSON.read_text(encoding="utf-8"))
    uses_put_config = bool(re.search(r"PUT.*?/connectors/[\w.-]+/config", script, re.S))
    sends_registry_json = "registry.json" in script
    if uses_put_config and sends_registry_json:
        assert "config" not in data or not isinstance(data["config"], dict), (
            "connect-init PUTs registry.json to /connectors/<name>/config, which expects a flat config "
            "map, but registry.json is wrapped in {name, config}; flatten registry.json or extract .config"
        )
        assert data.get("connector.class"), "flat registry.json must carry connector.class"


def test_kafka_kraft_listeners_have_a_routable_controller_address() -> None:
    """L2: Kafka 3.9 (KRaft, broker+controller) выводит advertised-адрес контроллера из
    KAFKA_LISTENERS, если CONTROLLER не указан в KAFKA_ADVERTISED_LISTENERS. Адрес 0.0.0.0
    отвергается при форматировании хранилища:
    `advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`, kafka в рестарт-цикле.
    """
    env = _env(_service("kafka"))
    listeners = dict(
        item.split("://", 1) for item in env["KAFKA_LISTENERS"].split(",") if "://" in item
    )
    advertised = dict(
        item.split("://", 1) for item in env.get("KAFKA_ADVERTISED_LISTENERS", "").split(",") if "://" in item
    )
    for name in env.get("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER").split(","):
        if name in advertised:
            assert not advertised[name].startswith("0.0.0.0:"), advertised[name]
            continue
        assert name in listeners, f"controller listener {name} missing from KAFKA_LISTENERS"
        assert not listeners[name].startswith("0.0.0.0:"), (
            f"{name}://{listeners[name]} is derived into advertised.listeners and Kafka 3.9 rejects 0.0.0.0; "
            f"use {name}://:{listeners[name].rsplit(':', 1)[1]} or add {name}://kafka:<port> to "
            "KAFKA_ADVERTISED_LISTENERS"
        )


def test_registry_cdc_queues_flush_fast_enough_for_section_4_6_slo() -> None:
    """L3: целевая задержка CDC p95 < 2 с (ТЗ §4.6, S2-8).

    Kafka engine ClickHouse по умолчанию сбрасывает блок раз в stream_flush_interval_ms = 7500 мс.
    На стенде `measure_cdc_latency.py` дал p95 = 6.04 с (Debezium публикует событие за ~0.4–0.5 с).
    С `kafka_flush_interval_ms = 500` на той же очереди p95 = 1.53 с.
    """
    sql = QUEUES_SQL.read_text(encoding="utf-8")
    for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (registry_\w+_queue)\s*\(.*?\)\s*ENGINE\s*=\s*Kafka(.*?);", sql, re.S):
        name, settings = m.group(1), m.group(2)
        flush = re.search(r"kafka_flush_interval_ms\s*=\s*(\d+)", settings)
        assert flush and int(flush.group(1)) <= 1000, (
            f"{name}: kafka_flush_interval_ms is not set (default 7500 ms); CDC p95 cannot meet 2 s"
        )


def _mem_mib(value: object) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([kmg]?)b?", str(value).strip().lower())
    assert m, f"unparseable mem_limit {value!r}"
    num, unit = float(m.group(1)), m.group(2)
    return num * {"": 1 / 1024 / 1024, "k": 1 / 1024, "m": 1, "g": 1024}[unit]


@pytest.mark.parametrize(
    ("service", "observed_mib"),
    [
        # docker stats на стенде (wave1-live-check / wave1-final-check): пиковое
        # наблюдаемое потребление. Нужен запас не менее 25 % до mem_limit.
        ("connect", 653),
        ("schema-registry", 340),
    ],
)
def test_jvm_services_have_memory_headroom(service: str, observed_mib: int) -> None:
    """L5: connect и schema-registry не должны работать у самого mem_limit."""
    limit = _mem_mib(_service(service).get("mem_limit"))
    assert observed_mib <= 0.75 * limit, (
        f"{service}: mem_limit {limit:.0f} MiB, observed {observed_mib} MiB "
        f"(>{observed_mib / limit:.0%}); raise mem_limit or lower the heap"
    )
