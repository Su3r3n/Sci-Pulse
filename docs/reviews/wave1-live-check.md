# Волна 1 — проверка на живом стенде (tester-verifier, роль «Argon»)

- **Дата:** 2026-11-13
- **Ревизия:** `a016ae0` (coder-1, round 2) поверх `357bcf2` (coder-2, round 2). Предыдущий отчёт: `docs/reviews/wave1-recheck.md` (R1–R7, m3, m8–m13).
- **Окружение:** Docker daemon в контейнере (Compose v5.6.1), 15 ГБ RAM, 4 ядра. Сеть: образы Docker Hub тянутся (с периодическими 429), у контейнеров и у `docker build` выхода в интернет нет (DNS не резолвится), `api.openalex.org` и `quay.io` закрыты прокси песочницы.

## Вердикт

**Вернуть на доработку (короткую).** Все замечания round 2 закрыты, и это подтверждено на стенде: образ Connect собирается, AvroConverter виден, секрет пишется entrypoint'ом, коннектор `registry-postgres` в `RUNNING`, CDC доходит до `v_projects_current` и `v_repos_current`, harvester offline выдаёт ровно столько записей, сколько в кэше. Но `make up` + `make init-cdc` с текущим репозиторием не проходят из-за трёх дефектов, которые видны только вживую:

1. **L2 (blocker, coder-2).** Kafka не стартует: `CONTROLLER://0.0.0.0:9093` в `KAFKA_LISTENERS` отвергается Kafka 3.9.
2. **L1 (blocker, coder-1).** `connect-init` (то же `make init-cdc`) падает с кодом 22: `registry.json` в формате `{name, config}` отправляется в `PUT /connectors/…/config`, а этот endpoint ждёт плоскую карту. Из-за `depends_on` не стартует и `harvester`.
3. **L3 (major, coder-2).** Целевая задержка CDC не выполняется: p95 = 6.04 с при target < 2 с (ТЗ §365, S2-8). Причина — флаш Kafka engine ClickHouse по умолчанию (7.5 с). С `kafka_flush_interval_ms = 500` получилось p95 = 1.53 с.

Кроме того, образы MinIO по умолчанию не скачиваются (L4), поэтому `minio`, `minio-init` и весь Spark-срез (`make demo-slice`, M4, m10) на стенде не проверены.

Все правки однострочные (см. «Остаток замечаний»). После них нужен повторный `make nuke && make up && make init-cdc && python3 scripts/measure_cdc_latency.py`.

## Статическая проверка

| Проверка | Результат |
|---|---|
| `python3 -m pytest tests -q` (до моих правок) | 35 passed, 1 skipped, **2 failed = XPASS(strict)** m3 и m13, то есть исправлены |
| `python3 -m pytest tests -q` (после правок, с новыми тестами L1–L3) | **37 passed, 3 failed (L1, L2, L3 — ожидаемые находки), 1 skipped** |
| `RUN_LIVE_REGISTRY_TESTS=1 pytest tests/test_registry_integration.py` (на стенде) | 3 passed |
| `go vet ./... && go test -count=1 ./...` в `services/{common,registry,harvester,loadgen}-go` | vet чисто, все пакеты PASS |
| `go test -race` `internal/runner`, `internal/source` (harvester) | PASS |
| `tests/`: `GOWORK=off go vet ./... && go test ./...` | PASS |
| `gofmt -l services tests` | чисто |

## Таблица статусов

| # | Статус | Доказательство |
|---|---|---|
| **R1** секрет Debezium | **fixed** (live) | compose передаёт `DEBEZIUM_DB_USER/PASSWORD`, RO-mount и `make secrets` убраны. В контейнере `/secrets` = `drwx------ kafka`, `debezium.properties` = `-rw------- kafka`, 41 байт. `${file:…}` резолвится, коннектор `RUNNING` |
| **R2** AvroConverter | **fixed** (live) | `docker compose build connect` проходит, `RUN test -f …/kafka-connect-avro-converter-7.7.1.jar` OK, в `/kafka/connect/confluent-avro-converter` 178 jar. `GET /connector-plugins?connectorsOnly=false` содержит `io.confluent.connect.avro.AvroConverter`. CDC-топики пишутся в Avro, в SR появились `cdc.registry.public.{tracked_projects,project_repos}-{key,value}` |
| **R3** offline arXiv × наборы | **fixed** (live) | `harvester --once --offline`: `raw.papers.arxiv` +2, `raw.papers.openalex` +3, `dlq.papers` +11. Это ровно содержимое кэша: arXiv 2 валидных + 4 невалидных + 1 битая страница, OpenAlex 3 + 5 + 1. Утроения нет |
| **R4** `sort=updated_date:asc` в OpenAlex | **not verified** | `curl https://api.openalex.org/…` → `CONNECT tunnel failed, response 403` (хост закрыт прокси). coder-1 добавил в лог и в ошибку тело ответа 4xx (`openalex.go:208-219`), так что при отказе API причина будет видна. Риск остаётся |
| **R5** `CONNECT_*_CONVERTER_SCHEMA_REGISTRY_URL` | **fixed** (live) | в `/kafka/config/connect-distributed.properties` worker'а есть `key/value.converter.schema.registry.url=http://schema-registry:8081` |
| **R6** offline-курсоры | **fixed** (live) | после offline-прохода с DSN `SELECT count(*) FROM harvest_cursors` = 0 (`runner.go:307`, `Config.Offline`) |
| **R7** `export` всего `.env` в Makefile | **fixed** | `export` убран. Seed получает `SEED_URL`/`REGISTRY_API_KEY` явно. `-include .env` остался только для чтения этих двух значений |
| **m3** `HTTP_ADDR`/`PORT` loadgen | **fixed** (live) | `listenAddr` в `loadgen/main.go`: HTTP_ADDR → `:PORT` → дефолт. Compose задаёт оба (`:8084`/`8084`). Лог: `loadgen HTTP server listening addr=:8084`, контейнер `healthy`. xfail снят |
| **m8** `from_avro` с фиксированной схемой | **not fixed** | `bronze_to_silver.py:117` без изменений. Владелец coder-2, срок S2-6 |
| **m9** arXiv-DOI | **partially** | обнуление DOI `10.48550/arXiv.*` на месте (`bronze_to_silver.py:141-146`), `MERGE … ON … OR …` (стр. 233-237) не менялся. На стенде не проверено (нет Spark/MinIO) |
| **m13** `make test` | **fixed** | цикл по 4 модулям включая `services/loadgen-go`, `pytest -q platform/spark tests`. xfail снят, тест теперь проверяет именно рецепт `test:` |
| M3 гранты CH | **fixed** (live) | `grafana_ro` и `spark_writer` читают все `v_*_current` (`v_projects_current`, `v_repos_current`, `v_institutions_current`, `v_project_institutions_current`, `v_mart_freshness`). `spark_writer` не читает `dm_*` и `v_hep_software_impact`; для волны 1 это не требуется |
| M9 seed | **fixed** (live) | `make seed`: созданы `root`, `rucio`, `xrootd` и primary-репозитории. Повторный запуск печатает `exists`. В `v_repos_current` 3 строки с `is_primary=1` |

## Живой стенд: что поднималось и как

Команда `docker compose --profile core up -d --build --wait` (то же, что `make up`) не проходит в этом окружении и в этом репозитории. Причины:

| Что мешает | Чьё | Обход для проверки |
|---|---|---|
| `minio/minio:RELEASE.2024-10-13T13-34-11Z` и `minio/mc:RELEASE.2024-10-08T09-37-26Z`: `pull access denied … repository does not exist`, Hub API отвечает `object not found`. `quay.io` закрыт прокси | репозиторий (L4) + окружение | `minio` и `minio-init` не поднимались |
| `apk add` в Dockerfile registry/harvester/loadgen: `DNS lookup error` для `dl-cdn.alpinelinux.org` (у сборки нет сети) | окружение | бинарники собраны на хосте (`GOWORK=off CGO_ENABLED=0 go build`, как в Dockerfile) и упакованы в `alpine:3.21` под теми же именами образов (`scipulse-registry` и т.д.) |
| Kafka в рестарт-цикле (L2) | coder-2 | временный override `KAFKA_LISTENERS: …,CONTROLLER://:9093` (файл в scratchpad, не в репозитории) |
| Grafana в рестарт-цикле: `GF_INSTALL_PLUGINS` качает плагин с `grafana.com`, DNS не резолвится | окружение (см. L6) | Grafana не нужна для проверок |

Образ `connect` собран штатно через `docker compose build connect`.

**Поднято и healthy:** postgres, kafka, schema-registry, connect, clickhouse, registry, loadgen, prometheus, kafka-exporter. Одноразовые `pg-init`, `kafka-init`, `registry-init`, `ch-init` завершились с кодом 0. `connect-init` упал с кодом 22 (L1). Не подняты: minio, minio-init (L4), grafana (сеть), harvester как сервис (ждёт `connect-init`), spark (профиль `spark`, без MinIO смысла нет).

Память под нагрузкой (`docker stats`): connect 653/768 MiB (85 %), schema-registry 340/384 MiB (88 %), kafka 528/1024 MiB, clickhouse 403/1536 MiB. Профиль core в 15 ГБ помещается с запасом, но у connect и schema-registry мало запаса до `mem_limit` (L5).

## Результаты живых проверок

| Шаг | Результат |
|---|---|
| `make topics` (повторно) | `All 13 Kafka topics verified/created.` Есть все `cdc.registry.public.*`, `raw.*`, `dlq.*`, `load.synthetic` |
| `init_registry.sh` (через `registry-init`, повторно) | `All 10 Schema Registry subjects initialized with BACKWARD compatibility.` `GET /config` → `BACKWARD`, `GET /config/raw.papers.arxiv-value` → `BACKWARD` |
| `make init-cdc` | **FAIL (L1)**: `HTTP 500 Cannot deserialize value of type java.lang.String from Object value … ["config"]`. После `PUT` плоского `config` из того же `registry.json`: HTTP 201, `connector RUNNING`, `task 0 RUNNING` |
| `make seed` ×2 | OK, идемпотентно |
| INSERT/UPDATE/DELETE через REST (`X-API-Key`) | события в `cdc.registry.public.tracked_projects` (176 сообщений за прогон), `project_repos` (3). В ClickHouse: `v_projects_current` отражает insert, update (`name`, `priority`) и delete (`HAVING argMax(is_deleted, version) = 0`) |
| Задержка Debezium (REST → запись в Kafka) | 0.42–0.48 с (по `CreateTime` в `kafka-console-consumer`) |
| `measure_cdc_latency.py --iterations 20` (как есть в репозитории) | **FAIL (L3)**: p50 = 6.007 с, **p95 = 6.038 с**, все 60 замеров 5.97–6.06 с (кроме первого insert 2.98 с) |
| То же после пересоздания `registry_projects_queue` с `kafka_flush_interval_ms = 500` (эксперимент на стенде) | p50 = 0.525 с, **p95 = 1.531 с**, PASS |
| `harvester --once --offline` | exit 0. 2 + 3 записи в raw, 11 в `dlq.papers`. В ClickHouse `ops_dlq_events` 11 строк. Причины: arXiv — `bad_doi`, `future_published_at`, `missing_title`, `no_authors`, `parse_error`. OpenAlex — то же плюс `missing_published_at`. `live_papers_1m` заполняется. Курсоры не записаны (R6) |
| Гранты M3 | см. таблицу статусов |
| R4 OpenAlex | не проверено, хост закрыт |
| `make demo-slice`, `enrich_projects` (M4), запись `Array(String)` через JDBC (m10) | не проверено: нет MinIO (L4) |

## Остаток замечаний

| # | Severity | Владелец | Где | Симптом и лог | Как исправить |
|---|---|---|---|---|---|
| **L1** | blocker | coder-1 (`registry.json`), согласовать с coder-2 (`connect-init`) | `platform/debezium/registry.json:1-3,24-25`; `docker-compose.yml:251-253` | `connect-init` exit 22, `make init-cdc` падает. `PUT /connectors/registry-postgres/config` → `{"error_code":500,"message":"Cannot deserialize value of type java.lang.String from Object value (token JsonToken.START_OBJECT) … (through reference chain: java.util.LinkedHashMap[\"config\"])"}`. `harvester` остаётся в `Created` | сделать `registry.json` плоской картой: содержимое `config` на верхнем уровне, `"name": "registry-postgres"` можно оставить внутри. PUT идемпотентен, compose менять не нужно. Тест `test_connect_init_payload_matches_connect_rest_endpoint`; `test_debezium_registry_config_matches_section_4_4` принимает оба формата |
| **L2** | blocker | coder-2 | `docker-compose.yml:90` | kafka в рестарт-цикле, `make up` → `dependency failed to start: container scipulse-kafka-1 is unhealthy`. Лог: `IllegalArgumentException: requirement failed: advertised.listeners cannot use the nonroutable meta-address 0.0.0.0` (`StorageTool`) | `CONTROLLER://:9093` вместо `CONTROLLER://0.0.0.0:9093` (проверено на стенде) или добавить `CONTROLLER://kafka:9093` в `KAFKA_ADVERTISED_LISTENERS`. Тест `test_kafka_kraft_listeners_have_a_routable_controller_address` |
| **L3** | major | coder-2 | `platform/clickhouse/02_queues.sql:21-25` и три другие `registry_*_queue` | CDC p95 = 6.04 с при target < 2 с. Debezium публикует за ~0.45 с, остальное — ожидание флаша Kafka engine (по умолчанию `stream_flush_interval_ms` = 7500 мс) | добавить `kafka_flush_interval_ms = 500` в SETTINGS всех `registry_*_queue`. Проверено на `registry_projects_queue`: p95 = 1.53 с. Тест `test_registry_cdc_queues_flush_fast_enough_for_section_4_6_slo` |
| **L4** | major (для окружения проверки — blocker профиля core) | coder-2 | `docker-compose.yml:259,277`, `.env.example:12-13` | `docker pull minio/minio:RELEASE.2024-10-13T13-34-11Z` → `pull access denied for minio/minio, repository does not exist`, то же для `minio/mc`. `GET hub.docker.com/v2/repositories/minio/minio/` → `object not found`. `make up --build` прерывается на пулле | закрепить доступный источник образов MinIO (зеркало или реестр, доступный стенду) или заменить на совместимое S3-хранилище; проверить `docker compose pull minio minio-init`. Не исключено, что это особенность сети песочницы, поэтому нужна проверка у владельца |
| **L5** | minor | coder-2 | `docker-compose.yml` `connect.mem_limit 768m`, `schema-registry.mem_limit 384m` | на простое connect 653/768 MiB, schema-registry 340/384 MiB. Под нагрузкой (волна 2, loadgen) возможен OOM-kill | поднять лимиты до 1024m и 512m или уменьшить heap; учесть в бюджете §9.4–§9.5 |
| **L6** | minor | coder-2 | `docker-compose.yml:489` `GF_INSTALL_PLUGINS` | без интернета Grafana в рестарт-цикле: `Get "https://grafana.com/api/plugins/grafana-clickhouse-datasource/versions": … no such host` | для офлайн-стенда: собрать образ Grafana с плагином или монтировать плагин в `/var/lib/grafana/plugins` |
| **R4** | risk | coder-1 | `openalex.go:382` | не проверено: хост закрыт | проверить на стенде с выходом в интернет |
| **m8** | minor | coder-2 | `bronze_to_silver.py:117` | без изменений | к S2-6 |
| **m9** | minor | coder-2 | `bronze_to_silver.py:233-237` | риск кардинальности `MERGE … OR …` | к S2-6, проверить на стенде со Spark |
| M4, m10 | — | coder-2 | Spark | не проверены (нет MinIO) | после L4 |

## Изменения tester-verifier в этом прогоне

- `tests/test_wave1_recheck.py`: сняты xfail у m3 (`test_compose_env_is_read_by_service[loadgen]`) и m13. Тест m13 теперь проверяет рецепт `test:` (все модули `go.work` и `pytest … tests`). `test_loadgen_buildable_and_wired` вычисляет реальный порт по правилу `listenAddr` (HTTP_ADDR → PORT → дефолт), проверяет согласованность `PORT` и `HTTP_ADDR` и сверяет порт с `EXPOSE` и healthcheck.
- `tests/test_wave1_live_findings.py` (новый): статические тесты L1, L2, L3. Сейчас падают, потому что это открытые находки.
- `tests/test_wave1_topology.py`: `test_debezium_registry_config_matches_section_4_4` принимает и обёрнутый, и плоский `registry.json`, чтобы не мешать исправлению L1.
- `docs/adr/ADR-004-cdc-serialization-format.md`: путь конвертера `/usr/share/java/kafka-serde-tools` → `/kafka/connect/confluent-avro-converter`, отдельный classloader, проверка jar при сборке.
- `scripts/measure_cdc_latency.py`: убран несуществующий столбец `updated_at` из запроса к `v_projects_current` (иначе каждый опрос падал бы с ошибкой ClickHouse); ключ по умолчанию = `REGISTRY_API_KEY` из `.env.example` (был `dev-registry-key`, давал 401); имя MV в docstring исправлено на `mv_registry_projects`.
- Стенд разобран: `docker compose --profile core --profile full --profile spark down -v --remove-orphans`. Ничего не закоммичено.

## Порядок доработки

1. **coder-2:** L2 (одна строка), L3 (четыре строки SETTINGS), L4, затем L5, L6.
2. **coder-1:** L1 (сделать `registry.json` плоским). R4 проверить там, где есть доступ к `api.openalex.org`.
3. **tester-verifier:** `make nuke && make up && make topics && make init-cdc && make seed`, `python3 scripts/measure_cdc_latency.py` (ожидается p95 < 2 с), `make e2e` со Spark-срезом (M4, m9, m10), `pytest tests` (ожидается 0 failed).
