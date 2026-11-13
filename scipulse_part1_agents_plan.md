# План выполнения SciPulse (Часть 1) для 3 агентов: `coder-1`, `coder-2`, `tester-verifier`

Этот файл кладётся в корень репозитория как `PLAN.md` рядом с полным текстом ТЗ (`docs/tz-part1.md`).
Само ТЗ (`docs/tz-part1.md`) менять **не нужно** — агенты используют его как точный справочник схем, SQL и контрактов.

---

## 0. Зафиксированные решения (§11.3 ТЗ) — ОБЯЗАТЕЛЬНО ДЛЯ ВСЕХ АГЕНТОВ

Чтобы `coder-1` и `coder-2` не написали несовместимый код, все развилки из §11.3 зафиксированы заранее:

1. **Р-1 (ADR-003, запись bronze):** используем **Spark Structured Streaming** (`bronze_ingest.py` с `availableNow`).
2. **Р-2 (ADR-004, формат CDC):** **`AvroConfluent`** через Schema Registry (единый формат со всеми остальными топиками).
3. **Р-3 (ADR-005, инструмент DQ):** **Great Expectations** поверх Spark.
4. **Р-4 (Запуск Spark из Airflow):** свой образ `scipulse/airflow` с `spark-submit --master local[*]`, Iceberg JAR-ами и бинарником `harvester` (без монтирования Docker socket).
5. **Р-5 (Атомарность записи gold в ClickHouse):** запись во временную/stage-таблицу `*_stage` и атомарный `ALTER TABLE ... REPLACE PARTITION ... FROM *_stage`.
6. **Р-6 (Лимит WAL в Postgres):** `max_slot_wal_keep_size = '2GB'` в конфиге Postgres + алерт на отставание слота.
7. **Р-7 (Хранение разметки связей):** в репозитории по пути `evals/g1/links_labeled.csv`.
8. **Р-8 (Профили Airflow):** `profiles: [full, ml]`.
9. **Р-9 (Пороги алертов и допуск сверки):** стартовые из §9.2, `reconcile.tolerance_pct = 0` для закрытого окна.
10. **Р-10 (Правило R3):** базовый `confidence = 0.5`, записи с `< 0.7` пишутся в `silver.paper_repo_links_review`.

---

## 1. Жёсткое разделение владения папками (Zero File Collision)

Ни один файл не редактируется двумя кодерами одновременно:

| Агент | Рекомендуемая модель | Зона ответственности (Эксклюзивное владение папками) | Что НЕ трогает |
| :--- | :--- | :--- | :--- |
| **`coder-1`** *(Go & Streaming Core)* | `Gemini 4 Hydrogen (Medium/High)` | `contracts/`, `services/common-go/`, `services/registry-go/`, `services/harvester-go/`, `services/loadgen-go/`, `platform/debezium/`, `scripts/init_registry.sh` | Не трогает `spark/`, `airflow/`, `clickhouse/`, `grafana/` |
| **`coder-2`** *(Data Platform, Lakehouse & Infra)* | `Gemini 4 Hydrogen (Medium/High)` | `docker-compose.yml`, `.env.example`, `Makefile`, `platform/postgres/`, `platform/kafka/`, `platform/minio/`, `platform/clickhouse/`, `platform/spark/`, `platform/dq/`, `platform/airflow/`, `platform/etl/steps.sh`, `platform/prometheus/`, `platform/grafana/`, `reference/` | Не трогает `services/*-go/` и `contracts/*.avsc` |
| **`tester-verifier`** *(QA, E2E, Chaos & Sprint Review)* | Тесты: `Hydrogen (Medium)` / Финальное ревью волны: `Argon (Medium/High)` | `tests/`, `evals/g1/`, `scripts/bench.py`, `scripts/chaos.py`, `scripts/measure_cdc_latency.py`, `docs/adr/`, `docs/runbook.md`, `docs/benchmarks/`, `.github/workflows/` | Не правит бизнес-код в `services/` и `platform/` (при багах пишет репорт для `coder-1` или `coder-2`) |

---

## 2. Разбивка на 3 волны (под спринты S0–S6)

### Волна 1: Фундамент, Контракты, Go-ядро и Базовая Инфраструктура (Спринты S0 + S1 + S2)

#### Задача для `coder-1` (Ветка/Worktree: `wave1-go-core`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§1–§5, §10.3–§10.5). Твоя зона — только `contracts/`, `services/`, `platform/debezium/`, `scripts/init_registry.sh`.
> 1. Создай все Avro-контракты в `contracts/` (`paper.v1` с `comments` и `journal_ref`, `dlq_envelope.v1`, `repo_snapshot.v1`, `release.v1`, `code_event.v1`, `software_record.v1`, `synthetic_event.v1`) и скрипт `scripts/init_registry.sh` с проверкой `BACKWARD`.
> 2. Реализуй библиотеку `services/common-go/` (`kafka` на franz-go с `acks=all`, `avro` с Confluent wire format, `config`, `obs` со slog и `/healthz`, `/metrics`).
> 3. Реализуй `services/registry-go/` по §4: миграции `000001_init` (4 таблицы включая `project_institutions`) и `000002_cdc`, REST API на `chi` + `pgx`, `X-API-Key`, `openapi.yaml`, `Dockerfile`, плюс `platform/debezium/registry.json`.
> 4. Реализуй `services/harvester-go/` (каркас, состояния в базе `harvester`, источники OpenAlex и arXiv, конвейер `map -> validate -> dlq`, режимы `run`, `--once`, `--offline`) и юнит-тесты `go test ./...`.

#### Задача для `coder-2` (Ветка/Worktree: `wave1-infra-lakehouse`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§1, §2, §6, §7, §9). Твоя зона — `docker-compose.yml`, `Makefile`, `.env.example`, `platform/postgres/`, `platform/kafka/`, `platform/minio/`, `platform/clickhouse/`, `platform/spark/`, `platform/prometheus/`, `platform/grafana/`.
> 1. Собери `docker-compose.yml` со всеми сервисами, портами (`127.0.0.1`), `mem_limit` и healthcheck по §9.4–§9.5 и `Makefile` (`up`, `up-full`, `down`, `nuke`, `topics`, `seed`).
> 2. Напиши init-скрипты: `pg-init` (4 базы и роли), `kafka-init` (все 13 топиков из §2.2 с нужным retention/compact), `minio-init`.
> 3. Напиши DDL ClickHouse в `platform/clickhouse/` строго в порядке §9.5: целевые таблицы (`registry_*`, `live_papers_1m`, `dm_research_trends`, `dm_hep_software_impact`, `ops_*`), затем очереди Kafka Engine (`AvroConfluent`), затем MV и View (`v_*_current`, `dm_live_activity`, `v_mart_freshness`), пользователи `grafana_ro` и `spark_writer`.
> 4. Реализуй первые PySpark-джобы в `platform/spark/`: `bronze_ingest.py`, `bronze_to_silver.py` (v0 для `silver.papers`), `enrich_projects.py`, `silver_to_gold_trends.py` и `silver_to_gold_impact.py` (через `REPLACE PARTITION` по Р-5).

#### Задача для `tester-verifier` (Ветка/Worktree: `wave1-verify`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§3.5, §4.6, §11.1).
> 1. Зафиксируй ADR-001…ADR-005 и ADR-110 в `docs/adr/`.
> 2. Подготовь офлайн-фикстуры в `data/cache/` для режима `harvester --offline` (валидные и битые записи OpenAlex/arXiv для проверки DLQ).
> 3. Напиши тесты совместимости схем `contracts/*.avsc`, интеграционные тесты `registry-go` и `e2e` тест CDC (`measure_cdc_latency.py`: `INSERT/UPDATE/DELETE` в Postgres -> `v_projects_current` в ClickHouse).
> 4. Проведи ревью веток `coder-1` и `coder-2`: проверь соответствие имён топиков, колонок и consumer groups таблице §2.2–§2.3.

---

### Волна 2: Полный сбор источников, Airflow, DQ, Сверка и Наблюдаемость (Спринты S3 + S4)

#### Задача для `coder-1` (Ветка/Worktree: `wave2-harvester-full`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§5, §10.4, §10.9).
> 1. Допиши в `services/harvester-go/`: чтение снимка реестра из `cdc.registry.*` (группа `harvester-registry`), планировщик по `priority` с пропуском `paused`, `pg_try_advisory_lock` и флаги `--sources` и `--until`.
> 2. Добавь оставшиеся источники: INSPIRE-HEP, GitHub (с `ETag` в `http_etags`), GH Archive (replay `--speed 60`) и Zenodo.
> 3. Реализуй команду `harvester replay-dlq --reason --since [--dry-run]` с именованными правилами исправления в `internal/replay/`.
> 4. Реализуй сервис `services/loadgen-go/` по §5.6 (HTTP API `/api/v1/stress-test`, token bucket, генерация `synthetic_event.v1` в `load.synthetic`, метрики Prometheus).

#### Задача для `coder-2` (Ветка/Worktree: `wave2-airflow-dq-gold`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§6.4–§6.6, §8, §9.2–§9.3).
> 1. Допиши PySpark-джобы: расширь `bronze_to_silver.py` на таблицы кода (`repos`, `releases`, `code_events`, `software_records`), реализуй `link_entities.py` (правила R1–R3), `backfill_openalex.py`, `reconcile.py` (включая `--repair --hour`) и `maintenance.py` (с сохранением тегов снапшотов).
> 2. Реализуй `platform/dq/dq_run.py` и `thresholds.yaml` (наборы `silver_papers`, `silver_links`, `silver_code`, `gold_post`).
> 3. Напиши единый `platform/etl/steps.sh` (для `make demo-slice` и Airflow) и собери `platform/airflow/` (образ Airflow 3.x, DAG `scipulse_etl` на 11 задач с пулом `spark`, `iceberg_maintenance`, `backfill_openalex`, `dlq_replay`, колбэки в `ops_pipeline_runs`).
> 4. Добавь провижининг 5 дашбордов и 10 правил оповещений Grafana в `platform/grafana/` по §9.2–§9.3, плюс таблицы бенчмарка `synthetic_queue -> bench_events` в ClickHouse.

#### Задача для `tester-verifier` (Ветка/Worktree: `wave2-verify`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§8.3, §8.4, §11.1).
> 1. Создай чистую и испорченную фикстуры для проверки DQ-гейта (`dq_silver` должен падать на 3 нарушениях и блокировать gold) и стартовую разметку `evals/g1/links_labeled.csv` (~100 пар для оценки precision правил R1–R3).
> 2. Напиши `pytest`-тесты для импорта всех Airflow DAGs (отсутствие циклов, корректность пула `spark`) и юнит-тесты PySpark (нормализация DOI/arXiv, дедупликация, правила R1–R3).
> 3. Проверь идемпотентность: двойной прогон `make demo-slice` и сверку ` cityHash64` контрольной суммы `dm_research_trends`, а также тест `reconcile.py` с удалением 5 минут из `live_papers_1m` и восстановлением через `--repair`.

---

### Волна 3: Нагрузочное тестирование, Chaos Engineering и Релиз v1.0 (Спринты S5 + S6)

#### Задача для `coder-1` и `coder-2` (Фикс багов по репортам `tester-verifier`)
> В третьей волне `coder-1` и `coder-2` исправляют только замечания, найденные на интеграции Волны 2, и доводят `Dockerfile` / `docker-compose.yml` до полной проходимости `make nuke && make up-full`.

#### Задача для `tester-verifier` (Ветка/Worktree: `wave3-chaos-release`)
> **Промпт:**
> Прочитай `PLAN.md` и `docs/tz-part1.md` (§8.6, §11.2).
> 1. Реализуй `scripts/bench.py` (`make bench`: ступени от 1 000 до 25 000 событий/с через `loadgen`, проверка `0` строк с `synthetic=true` в gold-витринах).
> 2. Реализуй `scripts/chaos.py` (`make chaos`: сценарии C1–C5 с записью `docs/benchmarks/chaos.md` и `docs/benchmarks/chaos_log.csv` с колонками `scenario,fault_start,fault_end,recovered_at`).
> 3. Напиши `docs/runbook.md` (3 сценария из §8.6), `docs/contracts.md`, финальный `README.md` и CI-пайплайн `.github/workflows/ci.yml`.
> 4. Выполни финальное ревью всего репозитория (на модели **Gemini 4 Argon**) по критериям приёмки Части 1.
