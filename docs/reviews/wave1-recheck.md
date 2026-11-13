# Волна 1 — повторная проверка после исправлений (tester-verifier, роль «Argon»)

- **Дата:** 2026-11-13
- **Ревизия:** `20ae1e1` (coder-1: курсор, ошибки, loadgen, образ Debezium) поверх `4119b97` (coder-2: compose, секреты, гранты CH, Spark, seed). Сравнение с исходным ревью: `git diff 3310ad3..20ae1e1`.
- **Исходные замечания:** `docs/reviews/wave1-spec-review.md` (B1–B4, M1–M9, m1–m13).
- **Окружение:** Go 1.26.0, Python 3.13, Docker CLI и Compose v5.6.1 есть, демона нет (`/var/run/docker.sock` отсутствует). Поэтому образы не собирались и стек не поднимался. Поведение образов Debezium и Confluent проверено по их исходным Dockerfile и entrypoint (`debezium/container-images` 2.7, `confluentinc/kafka-images` v7.7.1).

## Вердикт

**Вернуть на доработку.** Исправления пошли по существу: правило курсора, ошибки источников, гранты, `enrich_projects`, seed и `make demo-slice` теперь работают. Но профиль `core` всё ещё не поднимется:

1. **B3 → регрессия.** Подходы coder-1 и coder-2 к секрету Debezium конфликтуют, и контейнер `connect` завершается на первой строке entrypoint.
2. **B2 не исправлен.** `platform/debezium/Dockerfile` копирует Avro-конвертер из каталога, которого нет в `cp-kafka-connect:7.7.1`, поэтому сборка образа падает.
3. **Новая major-регрессия.** В `--offline` arXiv после `SplitJobs` отдаёт каждую запись кэша столько раз, сколько настроено наборов (по умолчанию 3). `make e2e` утраивает данные и DLQ.

Все три правки короткие (см. «Оставшиеся замечания»). После них нужен прогон на стенде: `make nuke && make up`, `make seed`, `scripts/measure_cdc_latency.py`, `make e2e`.

## Что запускалось

| Проверка | Команда | Результат |
|---|---|---|
| Go, 4 модуля | `go vet ./... && go test -count=1 ./...` в `services/{common,registry,harvester,loadgen}-go` | vet чисто, все пакеты PASS |
| Гонки | `go test -race ./internal/runner ./internal/source` (harvester) | PASS |
| Сборка как в Dockerfile (без `go.work`) | `GOWORK=off go build ./...` для harvester, registry, loadgen; `go mod verify` | OK |
| Go `tests/` | `cd tests && GOWORK=off go vet ./... && go test ./...` | PASS |
| gofmt | `gofmt -l services tests` | чисто (в `tests/wave1_verification_test.go` формат поправлен мной) |
| pytest | `python3 -m pytest tests -q -rxXfs` | **32 passed, 3 failed, 2 xfailed, 1 skipped.** Падения ожидаемые, это находки R1, R2, R3. xfail — m3 и m13. skip — живой тест registry |
| pytest Spark | `python3 -m pytest -q platform/spark` | 10 passed |
| Синтаксис | `py_compile` для `platform/spark/*.py`, `reference/seed_registry.py`, `scripts/*.py`; `bash -n` для всех `*.sh` | OK |
| Compose | `docker compose --env-file .env.example --profile core --profile spark --profile full config -q` | OK. Итоговый env и volumes разобраны через `config --format json` |
| YAML | `yaml.safe_load` для `prometheus.yml` и `docker-compose.yml` | OK |

## Таблица статусов

| # | Статус | Доказательство |
|---|---|---|
| **B1** loadgen ломает `make up` | **fixed** | Есть `services/loadgen-go/` (Dockerfile, `/healthz`, `/metrics`, маршруты §5.6). Модуль в `go.work`, `GOWORK=off go build` проходит, тесты PASS. Тесты `test_loadgen_buildable_and_wired` и `test_loadgen_control_api_routes_match_section_5_6`. Остаток — m3 (имя env) |
| **B2** нет AvroConverter в Connect | **not fixed** (сборка падает) | `platform/debezium/Dockerfile:12-13,19`: `COPY --from=confluent /usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter`. В `cp-kafka-connect:7.7.1` этот каталог создаётся пустым (`kafka-connect-base/Dockerfile.ubi8:76`). Конвертер приходит пакетом `confluent-schema-registry` в `/usr/share/java/kafka-serde-tools` (стр. 66-67), `kafka-connect/Dockerfile.ubi8` ничего не доустанавливает. Значит, `RUN test -d … \|\| exit 1` валит `docker compose build connect`. Тест `test_connect_avro_converter_is_copied_from_a_path_that_exists` — FAIL |
| **B3** нет `/secrets/debezium.properties` | **regression** | Подробности в разделе «Конфликт секретов Debezium». Тест `test_connect_debezium_secret_has_exactly_one_working_source` — FAIL |
| **B4** правило курсора §5.3 | **fixed** (в коде) / **risk** на живом API | `runner.go:188-317`: ordered-источник коммитит курсор после каждой подтверждённой пачки, unordered — только после всей задачи. При `Err` или отмене курсор unordered не двигается. `openalex.go:382` `sort=updated_date:asc`. `arxiv.go:118-139` `SplitJobs`: scope = OAI-набор. 7 юнит-тестов coder-1 и мои пробы на реальных источниках против httptest: `TestProbeOpenAlex5xxKeepsCursorAtAckedPrefix`, `TestProbeArxivFailingSetDoesNotMoveOtherScopes` — PASS. Риск R4: сортировку по `updated_date` нужно проверить на живом OpenAlex |
| **M1** harvester без БД в compose | **fixed** | compose `harvester.environment.HARVESTER_DB_DSN` → база `harvester`. Код принимает и алиас `DATABASE_URL` (`main.go:36,93`). Вне `--offline` без DSN — ошибка (`main.go:107-110`, тест `TestRunRequiresStateDatabaseOutsideOffline`). Совместимость — ниже |
| **M2** ошибки источников глушатся | **fixed** | `source.go:44-49` `RawRecord.Err`. `sendFatal` на ошибку запроса, не-200, бюджет, повтор cursor/token (`openalex.go:171-232`, `arxiv.go:186-232`). `runSourcePass` собирает ошибки через `errors.Join`, поэтому `--once` → код 1. Тесты `*IsTerminal*` и проба OpenAlex 5xx |
| **M3** гранты CH | **fixed** | `00_users.sql:24-29,47-51`. Обобщённый тест `test_clickhouse_view_grants_cover_base_tables[grafana_ro|spark_writer]` проходит по графу FROM/JOIN всех View и сверяет базовые таблицы с грантами — PASS |
| **M4** Array через JDBC | **fixed** (статически) | `enrich_projects.py:15-41`: `arrayStringConcat(…, char(31))` в подзапросе, затем `split` в Spark. `v_repos_current` массивов не содержит. Проверить на стенде |
| **M5** `rstrip(".git/")` | **fixed** | `platform/spark/repo_names.py` + `test_repo_names.py` (8 случаев). Мой тест `test_repo_normalization_keeps_seed_repos_linkable` проверяет, что ссылки на три репозитория seed сводятся к `github_full_name` |
| **M6** `make demo-slice` без профиля | **fixed** | `Makefile:46`: `--profile core --profile spark run --rm spark` |
| **M7** healthcheck MinIO через curl | **fixed** | healthcheck убран, у `minio-init` условие `service_started` и цикл `mc alias set && mc ready local` (`platform/minio/init.sh:10-11`) |
| **M8** `.exe` в git | **fixed** | `git ls-files '*.exe'` пусто (тест `test_no_binaries_tracked_in_git`). Локально файл остался неотслеживаемым (игнорируется), в истории blob есть |
| **M9** seed без репозиториев | **fixed** (статически) | `reference/seed_registry.py`: идемпотентно, 409 → поиск по slug, `POST /projects/{id}/repos` с `is_primary=true`. Поля сверены с `model/domain.go:114-121`. Тест `test_seed_registry_matches_section_4_3`. Живой прогон — на стенде |
| m1 ошибки SR → DLQ | **fixed** | `sink.go:80-114`: ошибки SR и сети валят пачку, в DLQ уходят только ошибки `EncodeWithSchema`. Клиент кэширует схему (`avro.go:163-227`). Тест `TestKafkaSinkRegistryOutageFailsBatchInsteadOfDLQ` |
| m2 ключ DLQ при `parse_error` | **fixed** | `pipeline.go:84-97` `rawRecordKey`. Тест `TestParseErrorDLQKeyMatchesMappedSourceKey` |
| m3 `HTTP_ADDR`/`PORT` | **partially** | registry: compose `PORT`, код читает `PORT` (+ алиас `HTTP_ADDR`). harvester: `HARVESTER_METRICS_ADDR`. **loadgen:** compose `PORT: "8084"`, код читает `HTTP_ADDR` (`loadgen/main.go:25`) — работает на дефолте. xfail `test_compose_env_is_read_by_service[loadgen]` |
| m4 ключ `StringConverter` | **fixed** | `registry.json:16,24`: `AvroConverter` + `key.converter.schema.registry.url`. ADR-004 обновлён |
| m5 параметры удаления | **fixed** | `registry.json:21` `delete.tombstone.handling.mode=rewrite`, deprecated-пара удалена. Это корректно для Debezium 2.7.3 (ключ появился в 2.5, `rewrite` = строка с `__deleted=true` без tombstone) и соответствует оговорке ТЗ §4.4. Тест `test_debezium_registry_config_matches_section_4_4` переписан под новый ключ и запрещает смешивание. ADR-004 обновлён |
| m6 heap JVM | **fixed** | `KAFKA_HEAP_OPTS=-Xms256m -Xmx512m` для connect. Entrypoint Debezium 2.7 переопределяет его только при заданном `HEAP_OPTS`, а он не задан. `SCHEMA_REGISTRY_HEAP_OPTS=-Xms128m -Xmx256m` |
| m7 scrape `connect:9404` | **fixed** | джоба закомментирована (`prometheus.yml:30-34`), YAML валиден |
| m8 `from_avro` с фиксированной схемой | **not fixed** | `bronze_to_silver.py:117` без изменений. Владелец coder-2, к S2-6 |
| m9 arXiv-DOI | **partially** | `bronze_to_silver.py:141-146`: `doi` обнуляется, `paper_key` идёт от `arxiv_id`. `MERGE … ON … OR …` (стр. 233-237) не изменён, риск кардинальности остаётся |
| m10 `Array(String)` через JDBC при записи | **not verified** | нужен стенд |
| m11 профиль `spark` | **fixed** | комментарий в compose. Решение дописано мной в ADR-110 |
| m12 gofmt | **fixed** | `gofmt -l services` пусто |
| m13 `make test`/`make e2e` | **partially** | цели есть. `make test` не запускает `services/loadgen-go` и `pytest tests/` (xfail `test_make_test_covers_all_go_work_modules`). `make e2e` упирается в R1 и R3 |

## Конфликт секретов Debezium (B3)

Что получилось после двух коммитов:
- **coder-2:** `docker-compose.yml:202-204` (volume на стр. 204) монтирует `./secrets:/secrets:ro`, а `DEBEZIUM_DB_USER`/`DEBEZIUM_DB_PASSWORD` из `environment` у `connect` **удалены**. `make secrets` пишет `secrets/debezium.properties` с `umask 077`, то есть права 600 и владелец — пользователь хоста.
- **coder-1:** `platform/debezium/Dockerfile:24-29` ставит entrypoint `scipulse-connect-entrypoint.sh`. Тот в режиме `set -eu` требует обе переменные (`: "${DEBEZIUM_DB_USER:?…}"`, стр. 7-8) и **пишет** `/secrets/debezium.properties` (стр. 15-19).

Что произойдёт при `make up` (уже после исправления B2):
1. В контейнере нет `DEBEZIUM_DB_USER`, что видно в `docker compose config`. Entrypoint падает на строке 7 с сообщением `DEBEZIUM_DB_USER must be set`, код 1. Из-за `restart: unless-stopped` начинается цикл рестартов. `connect` не становится healthy, `connect-init` и `harvester` (зависит от `connect-init`) не стартуют, `make up --wait` и `make e2e` падают.
2. Если вернуть переменные, падение будет на записи в read-only bind mount: `Read-only file system`, и `set -e` снова завершает скрипт.
3. Если вместо этого убрать запись из entrypoint и оставить монтирование, файл с хоста с правами 600 не прочитает пользователь `kafka` (uid 1001 в образе Debezium), если uid хоста не 1001. Тогда `FileConfigProvider` не резолвит `${file:…}`, и `PUT /connectors/registry-postgres/config` в `connect-init` падает.

**Решение: оставить подход coder-1 (entrypoint пишет файл из env внутри контейнера).** Причины: файл на хосте не нужен, нет проблем с uid и правами, работает обычный `docker compose up` без `make secrets`, а `/secrets` в образе уже принадлежит `kafka` с правами 700. Пароль и так лежит в `.env`, так что env в compose не ухудшает модель угроз dev-стенда. ADR-004 обновлён под это решение.

**Что сделать (coder-2):**
- в `connect` вернуть `DEBEZIUM_DB_USER: ${DEBEZIUM_DB_USER:-debezium}` и `DEBEZIUM_DB_PASSWORD: ${DEBEZIUM_DB_PASSWORD:-debezium_dev_pass}`;
- удалить `volumes: ./secrets:/secrets:ro`;
- убрать цель `secrets` и зависимости от неё в `up`, `up-full`, `init-cdc`, `e2e`, строку `secrets/` в `.gitignore` и комментарий про `make secrets` в `.env.example:59`.

`platform/debezium/debezium.properties.example` (coder-1) уже описывает правильную схему.

**Согласованность имён env** (compose ↔ `.env.example` ↔ код):

| Переменная | compose | `.env.example` | код | Итог |
|---|---|---|---|---|
| `DEBEZIUM_DB_USER/PASSWORD` | у `connect` нет (только в `pg-init`) | есть | entrypoint требует | **R1** |
| `HARVESTER_DB_DSN` | `harvester` | собирается из `HARVESTER_DB_*` (комментарий стр. 55-57) | `main.go:35` (+ алиас `DATABASE_URL`) | ок |
| `DATABASE_URL` | `registry` | из `REGISTRY_DB_*` | `registry main.go:30` | ок |
| `PORT` | registry `"8080"`, loadgen `"8084"` | в комментарии только для registry | registry читает. **loadgen читает `HTTP_ADDR`** | m3 (loadgen) |
| `HARVESTER_METRICS_ADDR` | `":8082"` | в комментарии | `main.go:37` | ок |
| `OPENALEX_EMAIL` | есть | есть | `env:"OPENALEX_EMAIL"` | ок |
| `KEY_/VALUE_CONVERTER_SCHEMA_REGISTRY_URL` | у `connect` | — | entrypoint Debezium переносит в worker только `CONNECT_*`, эти игнорируются | R5 (minor: на уровне коннектора URL задан) |

## Регрессии и новые замечания

| # | Severity | Владелец | Где | Суть | Как исправить |
|---|---|---|---|---|---|
| **R1** | blocker | coder-2 (+coder-1 согласовать) | `docker-compose.yml:202-227`, `Makefile:7-20`, `platform/debezium/scipulse-connect-entrypoint.sh` | конфликт секретов Debezium, `connect` не стартует (см. выше) | вернуть env, убрать RO-mount и `make secrets` |
| **R2** | blocker | coder-1 | `platform/debezium/Dockerfile:12-13,19` | копируется несуществующий каталог, сборка падает (B2) | `COPY --from=confluent /usr/share/java/kafka-serde-tools /kafka/connect/kafka-serde-tools` (отдельный каталог плагина изолирует classloader) **или** в стадии `confluent`: `RUN confluent-hub install --no-prompt --component-dir /usr/share/confluent-hub-components confluentinc/kafka-connect-avro-converter:7.7.1` (нужен доступ к Confluent Hub при сборке). Проверка на стенде: `GET /connector-plugins`, коннектор `RUNNING` |
| **R3** | major | coder-1 | `harvester-go/internal/source/arxiv.go:118-139` (`SplitJobs`), `fetchOffline` (стр. 154-174) игнорирует набор | в `--offline` каждая задача-набор заново проигрывает весь `data/cache/arxiv`. По умолчанию `ARXIV_SETS` = 3 набора, поэтому 7 записей превращаются в 21 (включая DLQ-кейсы). Ломает подсчёты `make e2e` и сверку §11.1. Проба `TestProbeOfflineArxivIsNotMultipliedBySets` — FAIL | при `cfg.Offline` в `SplitJobs` возвращать `[]Job{job}` (одна задача, scope `default`) |
| **R4** | risk (major при подтверждении) | coder-1 | `openalex.go:382` | правило курсора OpenAlex держится на `sort=updated_date:asc`. В документации OpenAlex сортируемые поля перечислены как `display_name`, `cited_by_count`, `works_count`, `publication_date`, `relevance_score`. Если API ответит 400 на `updated_date`, то с M2 каждый онлайн-проход OpenAlex падает. Из песочницы API недоступен, не проверено | на стенде: `curl 'https://api.openalex.org/works?per_page=1&sort=updated_date:asc&cursor=*'`. Если 400 — объявить OpenAlex unordered (`OrderedCursor()=false`): курсор коммитится после полного прохода, дубли допустимы |
| **R5** | minor | coder-2 | `docker-compose.yml` `connect.environment` | `KEY_CONVERTER_SCHEMA_REGISTRY_URL`/`VALUE_CONVERTER_SCHEMA_REGISTRY_URL` без префикса `CONNECT_` entrypoint Debezium не переносит в worker. Сейчас спасает конфиг коннектора | переименовать в `CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL` и `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL` |
| **R6** | minor | coder-1 | `runner.go:188-317` | `--offline` при заданном DSN (compose, `make e2e`) коммитит курсоры фикстур (`datestamp` 2024-06-01) в боевую `harvest_cursors`. Следующий онлайн-проход стартует от дат фикстур. OpenAlex offline к тому же объявлен ordered, хотя кэш не отсортирован | в `--offline` не вызывать `commitCursor` (флаг в `runner.Config`) |
| **R7** | minor | coder-2 | `Makefile:7-8` | `-include .env` + `export` экспортирует весь `.env` во все рецепты. Синтаксис `.env` для make и compose различается (кавычки, `$`) | после R1 строки не нужны, кроме `REGISTRY_API_KEY` для seed: экспортировать точечно |

**Совместимость «harvester падает без БД вне `--offline`»:**
- compose: DSN передаётся, ок.
- `make e2e`: идёт с `--offline`, ок, но см. R6.
- `make test`: `TestRunRequiresStateDatabaseOutsideOffline` сам обнуляет env, ок.
- Airflow (волна 2, ADR-110/Р-4): бинарник `harvester` внутри образа `scipulse/airflow`. Задачи DAG, которые вызывают его без `--offline`, обязаны получать `HARVESTER_DB_DSN`. Это требование к coder-2 на волну 2, сейчас Airflow в репозитории нет.

## Изменения tester-verifier в этом прогоне

- `tests/test_wave1_topology.py` (`test_debezium_registry_config_matches_section_4_4`): теперь ожидается `delete.tombstone.handling.mode=rewrite`, deprecated-ключи запрещены, ключ и значение — `AvroConverter` с SR URL, `FileConfigProvider` для user/password.
- `tests/test_wave1_recheck.py` (новый): R1, R2, имена env compose ↔ код, M1/B1/loadgen §5.6, обобщённая проверка грантов CH (M3), M4, M5, M6, M7, M8, M9. Плюс xfail(strict) на m3 (loadgen) и m13.
- `tests/test_harvester_probes.py` + `tests/go_probes/runner_probe_test.go.tmpl` (новые): пробы runner на реальных источниках OpenAlex и arXiv против httptest. Код подставляется в пакет `internal/runner` через `go test -overlay`, в `services/` ничего не пишется. Пробы: курсор OpenAlex при 5xx, изоляция scope arXiv, offline-дубли (R3).
- `tests/wave1_verification_test.go`: `gofmt`.
- `docs/adr/ADR-004-cdc-serialization-format.md`: ключ Avro, `delete.tombstone.handling.mode`, способ доставки секрета, путь конвертера в `cp-kafka-connect`.
- `docs/adr/ADR-110-airflow-profiles-and-spark-execution.md`: дополнение о профиле `spark` (m11, M6).
- `scripts/measure_cdc_latency.py` не менялся: формат ключа и параметры удаления его не затрагивают (`--help` и `py_compile` OK).

Примечание: в ветке появился коммит `ee14f9b "Wave 1 recheck: interim tester updates"` с моей правкой `tests/test_wave1_topology.py`. Этот агент его не создавал, остальные изменения прогона не закоммичены.

## Порядок доработки

1. **coder-2:** R1, затем R5, R7, m3 (`HTTP_ADDR: ":8084"` у loadgen), m13 (добавить `services/loadgen-go` и `pytest tests` в `make test`).
2. **coder-1:** R2, R3, затем R6. R4 проверить на стенде вместе с tester-verifier.
3. **tester-verifier:** после правок — `pytest tests` (ожидается 0 failed; xfail m3 и m13 станут XPASS, пометки снять) и прогон на стенде: `make nuke && make up`, `GET :8083/connector-plugins`, `make seed`, `scripts/measure_cdc_latency.py`, `make e2e`, проверка `v_projects_current`, `v_repos_current`, `enrich_projects` (M4, M9, m10).
