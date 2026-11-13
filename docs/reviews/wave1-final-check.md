# Волна 1 — финальная проверка на живом стенде (tester-verifier)

- **Дата:** 2026-11-13
- **Ревизия:** `063e222` (L2, L5) поверх `67c43ae` (L3) и `a56fd50` (L1). Предыдущий отчёт: `docs/reviews/wave1-live-check.md`.
- **Окружение:** то же, что в прошлом прогоне: Docker 29.9.2 в контейнере, 15.7 ГБ RAM. У контейнеров и `docker build` нет выхода в интернет, `quay.io` и `api.openalex.org` закрыты прокси песочницы.
- **Как поднимался стенд:** `docker compose --profile core up -d --no-build <сервисы>` из репозитория, **без каких-либо override-файлов**. Подняты все сервисы профиля core, кроме `minio`, `minio-init` (L4, образы недоступны) и `grafana` (L6, нет сети для плагина). Образы `scipulse-{registry,harvester,loadgen}` взяты из кэша прошлого прогона: они собраны на хосте, потому что `apk add` в Dockerfile без сети не работает. С тех пор Go-код не менялся. Образ `scipulse/debezium-connect:2.7.3-avro` тоже из кэша, собран штатно через `docker compose build connect`.

## Вердикт

**Принять.** L1, L2, L3 и L5 подтверждены на стенде. Kafka стартует без override и не перезапускается. `connect-init` и `make init-cdc` возвращают 2xx, коннектор в `RUNNING`, `harvester` стартует после `connect-init`. CDC p95 = **1.525 с** при target < 2 с. У connect и schema-registry около 36 % запаса памяти. `pytest tests`: 0 failed, Go-тесты во всех модулях PASS. Открытые пункты (L4, L6, R4, m8, m9, M4/m10) не блокируют волну 1 и перечислены ниже.

## Таблица L1/L2/L3/L5

| # | Статус | Команда | Вывод (последние строки) |
|---|---|---|---|
| **L2** Kafka `CONTROLLER://:9093` | **pass** | `docker compose --profile core up -d --no-build --wait kafka` без override; затем `docker inspect`, логи | `Container scipulse-kafka-1 Healthy`<br>`running health=healthy restarts=0`<br>`listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://:9093`<br>`INFO [KafkaRaftServer nodeId=1] Kafka Server started`<br>После всех проверок, примерно через 6 минут: `kafka OOMKilled=false restarts=0` |
| **L1** плоский `registry.json` | **pass** | `up … connect-init harvester`; `make init-cdc` (повторно); явный `curl -X PUT --data @platform/debezium/registry.json …/connectors/registry-postgres/config` | `connect-init  exited  Exited (0)`<br>`connect-init exit=0 fin=…11:17:33.43Z`<br>`harvester start=…11:17:33.83Z status=running restarts=0` (стартовал после `connect-init`)<br>`make init-cdc exit=0`<br>`PUT http=200`<br>`{"name":"registry-postgres","connector":{"state":"RUNNING",…},"tasks":[{"id":0,"state":"RUNNING",…}]}` |
| **L3** `kafka_flush_interval_ms = 500` | **pass** | `SELECT … FROM system.tables WHERE name LIKE 'registry_%_queue'`; `python3 scripts/measure_cdc_latency.py --iterations 20` | Все 4 очереди: `kafka_flush_interval_ms = 500`<br>`"samples": 60, "p50_seconds": 0.5265, "p95_seconds": 1.5248, "slo_p95_seconds": 2.0, "passed": true` (в прошлом прогоне до исправления было p95 = 6.038 с) |
| **L5** `mem_limit` connect 1024m, schema-registry 512m | **pass** | `docker stats --no-stream` после прогона задержки (loadgen и harvester работают) | `scipulse-connect-1  643.9MiB / 1GiB  62.88%`<br>`scipulse-schema-registry-1  326.3MiB / 512MiB  63.73%`<br>`connect OOMKilled=false restarts=0`, `schema-registry OOMKilled=false restarts=0`. Запас около 380 и 186 MiB соответственно. Для сравнения: kafka 555 MiB / 1 GiB, clickhouse 392 MiB / 1.5 GiB |

## Прочие живые проверки

| Шаг | Результат |
|---|---|
| Состояние сервисов | healthy: postgres, kafka, schema-registry, connect, clickhouse, registry, loadgen, prometheus, harvester. kafka-exporter в состоянии running. `pg-init`, `kafka-init`, `registry-init`, `ch-init`, `connect-init` завершились с кодом 0 |
| `make seed` | `root`, `rucio`, `xrootd` и их primary-репозитории созданы |
| INSERT/UPDATE/DELETE через REST → `v_projects_current` | `POST id=5` → `5 l-final-check2 Final Check 2`; `PATCH 200` → `5 l-final-check2 Final Check v2 1`; `DELETE 204` → `rows=0`. В витрине остаются только 3 проекта из seed |
| harvester (daemon) | `harvester starting … mode=daemon`, затем `source pass failed … dial tcp: lookup api.openalex.org / export.arxiv.org`. Это ожидаемо: в песочнице нет сети. Процесс работает, рестартов 0 |
| `python3 -m pytest tests -q` | `42 passed, 1 skipped` (с новым тестом L5; L1–L3 больше не падают) |
| `RUN_LIVE_REGISTRY_TESTS=1 pytest tests/test_registry_integration.py` | `3 passed` |
| `go vet ./... && go test -count=1 ./...` в `services/{common,registry,harvester,loadgen}-go` | все четыре модуля: vet чисто, тесты PASS (exit 0) |
| `tests/`: `GOWORK=off go vet ./... && go test ./...` | `ok github.com/scipulse/scipulse/tests` |
| `gofmt -l services tests` | чисто |
| Разбор стенда | `docker compose --profile core --profile full --profile spark down -v --remove-orphans`: 0 контейнеров, 0 томов `scipulse_*` |

## Не проверено из-за ограничений песочницы

- `make up` целиком (`--build --wait`): `minio`/`minio-init` (L4, образ не скачивается) и `grafana` (L6, нет сети для `GF_INSTALL_PLUGINS`). Без сети в `docker build` не работает и `apk add` в Dockerfile Go-сервисов, поэтому использованы образы, собранные на хосте.
- Spark-срез (`make demo-slice`, M4, m9, m10): нет MinIO.
- R4 (OpenAlex `sort=updated_date:asc`): `api.openalex.org` закрыт.

## Остаток замечаний

| # | Severity | Владелец | Что осталось |
|---|---|---|---|
| L4 | major (для стенда) | coder-2 | Образы MinIO `minio/minio:RELEASE.2024-10-13T13-34-11Z` и `minio/mc:…` недоступны. Нужно проверить у владельца на машине с сетью (возможно, дело в песочнице) или закрепить доступный источник |
| L6 | minor | coder-2 | Grafana без интернета уходит в рестарт-цикл из-за `GF_INSTALL_PLUGINS`. Для офлайн-стенда нужен вшитый плагин |
| R4 | risk | coder-1 | Проверить OpenAlex `sort=updated_date:asc` на стенде с выходом в интернет |
| m8, m9 | minor | coder-2 | `bronze_to_silver.py`: `from_avro` с фиксированной схемой и `MERGE … OR …`. Срок S2-6 |
| M4, m10 | — | coder-2 + tester | Spark-срез после L4: `make e2e` |

## Изменения tester-verifier в этом прогоне

- `tests/test_wave1_live_findings.py`: обновлён docstring (L1–L3 исправлены). Добавлен `test_jvm_services_have_memory_headroom[connect|schema-registry]` (L5): наблюдаемое на стенде потребление (653 и 340 MiB) должно быть не больше 75 % от `mem_limit`. На старых лимитах 768m/384m тест падает, на текущих проходит.
- Override-файлы не использовались. Ничего не закоммичено.
