"""
Волна 1, повторная проверка после исправлений (docs/reviews/wave1-recheck.md).

Статические инварианты, которые закрывают замечания B1–B4, M1–M9 и minor из
docs/reviews/wave1-spec-review.md, и проверки согласованности между зонами coder-1 и coder-2
(compose ↔ код сервисов ↔ образ Connect). Docker для этих тестов не нужен.

Тесты, помеченные xfail(strict=True), описывают известные minor-замечания: когда владелец
исправит код, xfail станет XPASS и упадёт, и пометку нужно будет снять.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE = REPO_ROOT / "docker-compose.yml"
DEBEZIUM_DIR = REPO_ROOT / "platform" / "debezium"
SPARK_DIR = REPO_ROOT / "platform" / "spark"
CH_DIR = REPO_ROOT / "platform" / "clickhouse"


def _compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _service(name: str) -> dict:
    svc = _compose()["services"].get(name)
    assert svc is not None, f"service {name} missing from docker-compose.yml"
    return svc


def _env_keys(svc: dict) -> set:
    env = svc.get("environment") or {}
    if isinstance(env, list):
        return {e.split("=", 1)[0] for e in env}
    return set(env)


def _go_env_tags(module_dir: Path) -> set:
    """Имена переменных окружения, которые читает модуль (теги env:"..." и os.Getenv)."""
    names = set()
    for path in module_dir.rglob("*.go"):
        if path.name.endswith("_test.go"):
            continue
        text = path.read_text(encoding="utf-8")
        names.update(re.findall(r'env:"([A-Z0-9_]+)"', text))
        names.update(re.findall(r'os\.(?:Getenv|LookupEnv)\("([A-Z0-9_]+)"\)', text))
    return names


def _volume_targets(svc: dict) -> dict:
    """target -> read_only для коротких и длинных форм volumes."""
    out = {}
    for vol in svc.get("volumes") or []:
        if isinstance(vol, str):
            parts = vol.split(":")
            target = parts[1] if len(parts) > 1 else parts[0]
            out[target] = len(parts) > 2 and "ro" in parts[2].split(",")
        else:
            out[vol.get("target")] = bool(vol.get("read_only"))
    return out


# --------------------------------------------------------------------------------------
# B2 / B3: образ Connect и секрет Debezium
# --------------------------------------------------------------------------------------


def test_connect_debezium_secret_has_exactly_one_working_source() -> None:
    """B3 после правок: файл /secrets/debezium.properties должен создаваться ровно одним способом.

    Вариант 1 (entrypoint coder-1): скрипт пишет файл из DEBEZIUM_DB_USER/PASSWORD, значит compose
    обязан передать обе переменные и НЕ монтировать /secrets read-only (иначе `set -eu` + запись в RO
    mount = выход с ошибкой и рестарт-цикл connect).
    Вариант 2 (make secrets coder-2): файл монтируется с хоста, значит entrypoint его не пишет.
    """
    entrypoint = (DEBEZIUM_DIR / "scipulse-connect-entrypoint.sh").read_text(encoding="utf-8")
    dockerfile = (DEBEZIUM_DIR / "Dockerfile").read_text(encoding="utf-8")
    connect = _service("connect")
    env = _env_keys(connect)
    mounts = _volume_targets(connect)

    entrypoint_used = "scipulse-connect-entrypoint.sh" in dockerfile and "ENTRYPOINT" in dockerfile
    entrypoint_writes = bool(re.search(r">\s*/secrets/debezium\.properties", entrypoint))

    if entrypoint_used and entrypoint_writes:
        assert {"DEBEZIUM_DB_USER", "DEBEZIUM_DB_PASSWORD"} <= env, (
            "entrypoint requires DEBEZIUM_DB_USER/DEBEZIUM_DB_PASSWORD (`:?` with set -eu), "
            f"but compose connect passes only {sorted(env)}"
        )
        assert mounts.get("/secrets") is not True, (
            "compose mounts ./secrets:/secrets:ro while the entrypoint writes /secrets/debezium.properties: "
            "the write fails on a read-only mount and connect exits"
        )
    else:
        assert "/secrets" in mounts, "nobody provides /secrets/debezium.properties to connect"


def test_connect_avro_converter_is_copied_from_a_path_that_exists() -> None:
    """B2: в cp-kafka-connect 7.7.x Avro-конвертер лежит в /usr/share/java/kafka-serde-tools
    (пакет confluent-schema-registry). Каталог /usr/share/confluent-hub-components в базовом образе
    создаётся пустым (kafka-images v7.7.1, kafka-connect-base/Dockerfile.ubi8), плагин туда попадает
    только после `confluent-hub install`. Без этого `RUN test -d ...` в Dockerfile валит сборку.
    """
    dockerfile = (DEBEZIUM_DIR / "Dockerfile").read_text(encoding="utf-8")
    copies_hub_dir = "/usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter" in dockerfile
    if copies_hub_dir:
        assert "confluent-hub install" in dockerfile, (
            "Dockerfile copies confluentinc-kafka-connect-avro-converter from confluent-hub-components, "
            "but cp-kafka-connect does not ship it there; add `confluent-hub install "
            "confluentinc/kafka-connect-avro-converter:<ver>` in the confluent stage or copy "
            "/usr/share/java/kafka-serde-tools"
        )
    else:
        assert "kafka-serde-tools" in dockerfile or "kafka-connect-avro-converter" in dockerfile, (
            "no Confluent Avro converter is added to the Connect image (Р-2)"
        )


def test_connect_image_is_built_from_platform_debezium() -> None:
    connect = _service("connect")
    build = connect.get("build") or {}
    assert build, "connect must be built from platform/debezium (stock image has no AvroConverter)"
    ctx = REPO_ROOT / build.get("context", ".")
    assert (ctx / build.get("dockerfile", "Dockerfile")).is_file()


# --------------------------------------------------------------------------------------
# B1, M1, m3: имена переменных окружения между compose и кодом
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "service, module",
    [
        ("registry", "services/registry-go"),
        ("harvester", "services/harvester-go"),
        pytest.param(
            "loadgen",
            "services/loadgen-go",
            marks=pytest.mark.xfail(
                strict=True,
                reason="m3: compose loadgen задаёт PORT, а loadgen-go читает HTTP_ADDR (работает на дефолте :8084)",
            ),
        ),
    ],
)
def test_compose_env_is_read_by_service(service: str, module: str) -> None:
    passed = _env_keys(_service(service))
    read = _go_env_tags(REPO_ROOT / module)
    unread = sorted(passed - read)
    assert not unread, f"compose passes {unread} to {service}, but {module} never reads them"


def test_harvester_has_state_database_in_compose() -> None:
    """M1 + новое правило main.go: вне --offline harvester без DSN завершается с ошибкой."""
    env = _service("harvester").get("environment") or {}
    dsn = env.get("HARVESTER_DB_DSN") or env.get("DATABASE_URL")
    assert dsn, "harvester needs HARVESTER_DB_DSN (it refuses to start without a DB outside --offline)"
    assert dsn.startswith("postgres://")
    assert re.search(r"/(\$\{HARVESTER_DB:-harvester\}|harvester)(\?|$)", dsn), dsn


def test_loadgen_buildable_and_wired() -> None:
    """B1: Dockerfile loadgen существует, модуль в go.work, порт healthcheck = EXPOSE = дефолт кода."""
    svc = _service("loadgen")
    dockerfile = REPO_ROOT / svc["build"]["context"] / svc["build"]["dockerfile"]
    assert dockerfile.is_file(), f"{dockerfile} missing"
    assert "./services/loadgen-go" in (REPO_ROOT / "go.work").read_text(encoding="utf-8")
    expose = re.search(r"^EXPOSE\s+(\d+)", dockerfile.read_text(encoding="utf-8"), re.M)
    main_go = (REPO_ROOT / "services/loadgen-go/cmd/loadgen/main.go").read_text(encoding="utf-8")
    default = re.search(r'env:"HTTP_ADDR"\s+default:":(\d+)"', main_go)
    health = " ".join(svc["healthcheck"]["test"])
    assert expose and default
    assert expose.group(1) == default.group(1) and f"localhost:{default.group(1)}/healthz" in health


def test_loadgen_control_api_routes_match_section_5_6() -> None:
    api = (REPO_ROOT / "services/loadgen-go/internal/api/api.go").read_text(encoding="utf-8")
    for route in ("POST /api/v1/stress-test", "GET /api/v1/status", "DELETE /api/v1/stress-test"):
        assert f'"{route}"' in api, f"§5.6 route {route} not registered"


@pytest.mark.xfail(strict=True, reason="m13: make test не запускает services/loadgen-go и pytest tests/")
def test_make_test_covers_all_go_work_modules() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    modules = re.findall(r"^\s*\./(services/[\w-]+)", (REPO_ROOT / "go.work").read_text(encoding="utf-8"), re.M)
    missing = [m for m in modules if m not in makefile]
    assert not missing, f"make test skips {missing}"


# --------------------------------------------------------------------------------------
# M3: права ClickHouse на базовые таблицы View (SQL SECURITY INVOKER)
# --------------------------------------------------------------------------------------


def _view_sources() -> dict:
    sql = (CH_DIR / "03_views.sql").read_text(encoding="utf-8")
    views = {}
    for m in re.finditer(r"CREATE (?:OR REPLACE )?VIEW\s+(\w+)\s+AS(.*?);", sql, re.S):
        name, body = m.group(1), m.group(2)
        views[name] = set(re.findall(r"(?:FROM|JOIN)\s+(\w+)", body))
    return views


def _base_tables(view: str, views: dict, seen=None) -> set:
    seen = seen or set()
    out = set()
    for src in views.get(view, ()):
        if src in views and src not in seen:
            seen.add(src)
            out |= _base_tables(src, views, seen)
        elif src not in views:
            out.add(src)
    return out


@pytest.mark.parametrize("user", ["grafana_ro", "spark_writer"])
def test_clickhouse_view_grants_cover_base_tables(user: str) -> None:
    users_sql = (CH_DIR / "00_users.sql").read_text(encoding="utf-8")
    granted = set(re.findall(rf"GRANT\s+[A-Z, ]*SELECT[A-Z, ]*\s+ON\s+scipulse\.(\w+)\s+TO\s+{user}\b", users_sql))
    views = _view_sources()
    missing = {}
    for view in granted & set(views):
        lacking = sorted(_base_tables(view, views) - granted)
        if lacking:
            missing[view] = lacking
    assert not missing, f"{user}: views granted without their base tables (INVOKER): {missing}"


# --------------------------------------------------------------------------------------
# M4, M5, M9: Spark enrich и seed
# --------------------------------------------------------------------------------------


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_seed_registry_matches_section_4_3() -> None:
    seed = _load(REPO_ROOT / "reference" / "seed_registry.py", "seed_registry_recheck")
    got = {p["slug"]: p["repos"] for p in seed.PROJECTS}
    assert got == {
        "root": [("root-project/root", True)],
        "rucio": [("rucio/rucio", True)],
        "xrootd": [("xrootd/xrootd", True)],
    }
    assert "seed_registry.py" in (REPO_ROOT / "Makefile").read_text(encoding="utf-8")


def test_repo_normalization_keeps_seed_repos_linkable() -> None:
    """M5: ссылки на репозитории seed из статьи совпадают с github_full_name реестра."""
    repo_names = _load(SPARK_DIR / "repo_names.py", "repo_names_recheck")
    text = (
        "See https://github.com/root-project/root.git, github.com/rucio/rucio/ "
        "and https://github.com/xrootd/xrootd."
    )
    assert repo_names.extract_repos(text) == ["root-project/root", "rucio/rucio", "xrootd/xrootd"]
    enrich = (SPARK_DIR / "enrich_projects.py").read_text(encoding="utf-8")
    assert ".rstrip(\".git" not in enrich and ".rstrip('.git" not in enrich
    assert "normalize_repo" in enrich


def test_enrich_projects_does_not_read_array_columns_over_jdbc() -> None:
    """M4: Array(String) из v_projects_current сворачивается в строку на стороне ClickHouse."""
    enrich = (SPARK_DIR / "enrich_projects.py").read_text(encoding="utf-8")
    assert "arrayStringConcat(keywords" in enrich and "arrayStringConcat(topics" in enrich
    assert 'spark.read.jdbc(jdbc_url, "v_projects_current"' not in enrich


# --------------------------------------------------------------------------------------
# M6, M7, M8: Makefile, MinIO, гигиена
# --------------------------------------------------------------------------------------


def test_demo_slice_enables_core_and_spark_profiles() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    runs = re.findall(r"run --rm spark", makefile)
    assert runs, "demo-slice must run the spark service"
    assert "--profile core --profile spark run --rm spark" in makefile


def test_minio_readiness_does_not_depend_on_curl() -> None:
    minio = _service("minio")
    hc = " ".join((minio.get("healthcheck") or {}).get("test", []))
    assert "curl" not in hc
    init = (REPO_ROOT / "platform" / "minio" / "init.sh").read_text(encoding="utf-8")
    assert "mc ready" in init


def test_no_binaries_tracked_in_git() -> None:
    import subprocess

    out = subprocess.run(
        ["git", "ls-files", "*.exe", "*.dll", "*.so", "*.test"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        pytest.skip("git unavailable")
    assert out.stdout.strip() == "", f"binaries tracked in git: {out.stdout.split()}"
