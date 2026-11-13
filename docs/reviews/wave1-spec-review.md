# Волна 1 — ревью соответствия ТЗ (tester-verifier, роль «Argon»)

- **Дата:** 2026-11-13
- **Ревизия:** `main` @ `c9fe50a` (merge PR #3; код coder-1 — `9d6ac8d`/`fa18e78`, coder-2 — `864b844`)
- **Режим:** только чтение. Код не правился. Отчёт о прогоне тестов ведётся отдельно в `docs/reviews/wave1-test-report.md`.
- **Источники:** `SciPulse Platform -part1_document.md` (далее «ТЗ»), `scipulse_part1_agents_plan.md` (решения Р-1…Р-10, владение папками).
- **Что запускалось:** `go vet ./...` для `registry-go` и `harvester-go` (чисто), `gofmt -l services/` (3 файла не отформатированы). Docker-стек не поднимался, поэтому пункты о поведении образов помечены «проверить на стенде».

## Вердикт

**Вернуть на доработку.** Контракты, DDL реестра, топики и DDL ClickHouse в основном совпадают с ТЗ. При этом:

1. `make up` в текущем виде не поднимет профиль `core`: в compose есть сервис без Dockerfile.
2. CDC-путь (Р-2, §4.4) не заработает: нет Avro-конвертера в образе Connect и нет файла с секретами для `FileConfigProvider`.
3. Harvester нарушает правило курсора §5.3 (возможна потеря данных) и в compose работает без базы `harvester`.

Ниже 4 blocker, 9 major и 13 minor.

Для каждого пункта указаны место, раздел ТЗ, владелец и способ исправления.

---

## Сводная таблица

| # | Severity | Файл:строка | § ТЗ | Владелец | Кратко |
|---|---|---|---|---|---|
| B1 | blocker | `docker-compose.yml:401-424` | §9.4, §9.5 | coder-2 | сервис `loadgen` в профиле `core` собирается из несуществующего `services/loadgen-go/Dockerfile`, поэтому `make up` падает на сборке |
| B2 | blocker | `docker-compose.yml:186-222`, `platform/debezium/registry.json:17-18` | §4.4, Р-2 | coder-2 / coder-1 | в образе `debezium/connect:2.7.3.Final` нет `io.confluent.connect.avro.AvroConverter` |
| B3 | blocker | `platform/debezium/registry.json:9-10`, `docker-compose.yml:186-222` | §4.4 | coder-2 / coder-1 | файл `/secrets/debezium.properties` никто не создаёт и не монтирует, поэтому коннектор не регистрируется |
| B4 | blocker | `services/harvester-go/internal/source/openalex.go:149-216, 361-380`; `internal/runner/runner.go:235-237`; `internal/source/arxiv.go:144-158` | §5.3, §5.4, §11.2 C5 | coder-1 | курсор = max(`updated_date`/`datestamp`) по несортированной выдаче и сохраняется после каждой пачки; у arXiv один курсор на все наборы. Возможна потеря записей |
| M1 | major | `docker-compose.yml:387` ↔ `services/harvester-go/cmd/harvester/main.go:35` | §5.4, §1.3 | coder-2 / coder-1 | compose передаёт `DATABASE_URL`, harvester читает `HARVESTER_DB_DSN`. В compose состояние хранится в памяти и не переживает рестарт |
| M2 | major | `openalex.go:150-198`, `arxiv.go:186-238`, `runner.go:171-251` | §5.3 (`--once` → код 1) | coder-1 | ошибки HTTP, 429/5xx и обрыв пагинации глушатся (`return` без ошибки). `--once` завершается с кодом 0, курсор и `last_success` двигаются |
| M3 | major | `platform/clickhouse/00_users.sql:7-40` | §7.6 | coder-2 | у `grafana_ro`/`spark_writer` есть `SELECT` на `v_*_current`, но нет прав на `registry_*`. Обычные View в CH 24.8 исполняются с правами вызывающего (`SQL SECURITY INVOKER`) |
| M4 | major | `platform/spark/enrich_projects.py:16-19` | §6.4 (`enrich_projects`) | coder-2 | `spark.read.jdbc` по `v_projects_current` с колонками `Array(String)` (`keywords`, `topics`): generic JDBC в Spark 3.5 не мапит `ARRAY` |
| M5 | major | `platform/spark/enrich_projects.py:50,56` | §6.4, §4.3 seed | coder-2 | `m.rstrip(".git/")` срезает символы, а не суффикс: `root-project/root` превращается в `root-project/roo`. ROOT из seed не связывается |
| M6 | major | `Makefile:38-43`, `docker-compose.yml:332` | §9.5, §2.4 | coder-2 | `docker compose run --rm spark` без `--profile core`: зависимости `pg-init`, `minio-init`, `ch-init` из неактивного профиля, поэтому `make demo-slice` не стартует |
| M7 | major | `docker-compose.yml:262` | §9.4 | coder-2 | healthcheck MinIO через `curl`, которого, по всей видимости, нет в образах MinIO 2024 (проверить на стенде) |
| M8 | major | `services/harvester-go/cmd_harvester_test.exe` | план §1, гигиена репо | coder-1 | в git лежит Windows-бинарник PE32+ на 20 МБ |
| M9 | major | `Makefile:23-36` | §4.3 (seed) | coder-2 | `make seed` не создаёт репозитории `root-project/root`, `rucio/rucio`, `xrootd/xrootd` |
| m1 | minor | `services/harvester-go/internal/sink/sink.go:92-127` | §3.3 | coder-1 | любая ошибка `codec.Encode` (в том числе сетевая ошибка Schema Registry) классифицируется как `schema_error/serialize` |
| m2 | minor | `services/harvester-go/internal/pipeline/pipeline.go:28,55` | §3.4 | coder-1 | при `parse_error` ключ DLQ = `source:raw.ID`, где `raw.ID` — полный URL OpenAlex или OAI-идентификатор. Он не совпадает с ключом `source:source_id`, и replay уходит в другую партицию |
| m3 | minor | `docker-compose.yml:179,392` ↔ `registry-go/cmd/registry/main.go:144`, `harvester-go/cmd/harvester/main.go:36` | §9.4 | coder-2 / coder-1 | `HTTP_ADDR` никем не читается (сервисы читают `PORT` и `HARVESTER_METRICS_ADDR`). Работает только за счёт совпадения дефолтов |
| m4 | minor | `platform/debezium/registry.json:16` | Р-2, §3.4 | coder-1 | `key.converter=StringConverter` для ключа-Struct даёт строку вида `Struct{id=1}`. Это отступление от «единого Avro» Р-2 |
| m5 | minor | `platform/debezium/registry.json:21-22` | §4.4 (примечание) | coder-1 | `delete.handling.mode`/`drop.tombstones` в Debezium 2.7 deprecated, нужно сверить с `delete.tombstone.handling.mode` |
| m6 | minor | `docker-compose.yml:122-142, 186-222` | §9.4 | coder-2 | heap JVM не согласован с `mem_limit`: у Connect по умолчанию `-Xmx2G` при лимите 768m, у Schema Registry около 512M при лимите 384m |
| m7 | minor | `platform/prometheus/prometheus.yml:42-44`; compose `connect` | §9.1 | coder-2 | scrape `connect:9404` настроен, а JMX-экспортёр в Connect не подключён (target всегда down) |
| m8 | minor | `platform/spark/bronze_to_silver.py:113-118` | §3.5, §6.5 | coder-2 | `from_avro` с фиксированной схемой из файла игнорирует ID схемы в заголовке. На v1+N полях или битой записи (FAILFAST) джоб упадёт |
| m9 | minor | `platform/spark/bronze_to_silver.py:153-160, 225-251` | §6.5 | coder-2 | DOI `10.48550/arXiv.*` остаётся в `doi` и становится `paper_key`, а должен превращаться в `arxiv_id`. `MERGE ... ON ... OR ...` не переносит строку на новый ключ |
| m10 | minor | `platform/spark/silver_to_gold_impact.py:172, 188-193` | §7.4 | coder-2 | запись `array<string>` (`top_institutions`) через Spark JDBC в ClickHouse не проверена (`createArrayOf("text")`). Проверить в e2e |
| m11 | minor | `docker-compose.yml:328-336` | §9.4 | coder-2 | у `spark` профиль `spark`, а не `core` (ТЗ: core, разовый `run --rm`). Допустимо, но нужно отразить в ADR или README |
| m12 | minor | `gofmt -l`: `harvester-go/internal/source/{arxiv,openalex}.go`, `common-go/config/config_test.go`; `openalex.go:171` (`Limiter.Wait(ctx, )`) | — | coder-1 | код не прогнан через `gofmt` |
| m13 | minor | `Makefile:45-49` | §9.5 (`make test`/`make e2e`) | coder-2 | `test` и `e2e` — заглушки `echo` |

---

## Blocker

### B1. `loadgen` ломает `make up`
- **Где:** `docker-compose.yml:401-424`: `build: dockerfile: services/loadgen-go/Dockerfile`, `profiles: ["core"]`. Каталога `services/loadgen-go/` в репозитории нет (сервис запланирован на волну 2 и S5).
- **ТЗ:** §9.5. `make up` = `docker compose --profile core up -d --wait` должен поднимать `core`.
- **Последствие:** compose при `up` собирает все build-сервисы активного профиля. Сборка падает, и стек не поднимается.
- **Исправить (coder-2):** до волны 2 вынести `loadgen` в отдельный профиль (например `profiles: ["bench"]`) или закомментировать его. Тогда Prometheus-джоба `loadgen` будет просто down.

### B2. В образе Connect нет Avro-конвертера Confluent (Р-2)
- **Где:** `docker-compose.yml:187, 210-213` (`debezium/connect:2.7.3.Final`, `KEY/VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter`), `platform/debezium/registry.json:17-18`.
- **ТЗ:** §4.4, Р-2 (AvroConfluent для CDC), §7.2 (`kafka_format='AvroConfluent'` у `registry_*_queue`).
- **Последствие:** начиная с Debezium 2.x образы не содержат jar-ов `kafka-connect-avro-converter` от Confluent. Worker с таким `VALUE_CONVERTER` не стартует (ClassNotFound), healthcheck `connect` не проходит. За ним по цепочке падают `connect-init` и `harvester` (он зависит от `connect-init`), а `v_projects_current` остаётся пустым.
- **Исправить (coder-2, файл `platform/debezium/Dockerfile` владеет coder-1, согласовать):** собрать свой образ `FROM debezium/connect:2.7.3.Final` и положить в `/kafka/connect/` jar-ы `kafka-connect-avro-converter`, `kafka-avro-serializer`, `kafka-schema-registry-client`, `avro`, `common-*` (версия 7.7.x). Другой вариант — `confluent-hub install` в образе `cp-kafka-connect` вместе с Debezium-плагином. Проверить на стенде: `GET /connector-plugins` и статус `RUNNING` у `registry-postgres`.

### B3. `FileConfigProvider` ссылается на несуществующий файл
- **Где:** `platform/debezium/registry.json:9-10` (`${file:/secrets/debezium.properties:user|password}`). В `docker-compose.yml:186-222` нет volume `/secrets`, и никакой init-шаг этот файл не пишет. Переменные `DEBEZIUM_DB_USER`/`DEBEZIUM_DB_PASSWORD` (строки 216-217) передаются в контейнер, но коннектор их не использует.
- **ТЗ:** §4.4: пароль читается через `FileConfigProvider` и не лежит в репозитории.
- **Последствие:** `PUT /connectors/registry-postgres/config` вернёт ошибку при резолве конфига, и `connect-init` упадёт.
- **Исправить (coder-2):** добавить в `connect` entrypoint-обёртку, которая перед стартом пишет `/secrets/debezium.properties` из env (`user=${DEBEZIUM_DB_USER}`, `password=${DEBEZIUM_DB_PASSWORD}`). Другой вариант — монтировать `./secrets/debezium.properties` (в `.gitignore`), сгенерированный `make`-целью из `.env`.

### B4. Правило курсора harvester (§5.3) нарушено, возможна потеря данных
- **Где:**
  - `services/harvester-go/internal/runner/runner.go:235-237`: `latestCursor` = лексикографический максимум `rec.Cursor` по всем записям прохода. Сохраняется в `flushBatch` после **каждой** пачки (строки 205-219 файла).
  - `internal/source/openalex.go:361-380` (`buildWorksURL`): нет `sort=updated_date:asc`, поэтому порядок выдачи OpenAlex по `updated_date` не гарантирован. Курсор записи = `updated_date` (строки 280-283).
  - `internal/source/arxiv.go:144-158`: все наборы (`cs`, `physics:hep-ex`, …) обходятся в одном `Job` со scope `default` (`runner.go:157`), курсор — максимальный `datestamp` по всем наборам.
- **ТЗ:** §5.3 «Курсор сдвигается только после того, как продюсер подтвердил все записи пачки. При падении пачка собирается снова: дубли допустимы, потери нет». §5.4 `scope` = набор arXiv, тема или slug. §11.2 C5.
- **Сценарий потери:**
  1. Первая пачка из 100 записей OpenAlex содержит работу с `updated_date` = сегодня. После ack курсор становится «сегодня».
  2. Процесс убит (C5) или страница 3 вернула 5xx (см. M2).
  3. Следующий запуск идёт с `from_updated_date:<сегодня>`, и все несобранные страницы со старшими датами пропадают.
  4. С arXiv то же самое: набор A догнан до сегодняшнего дня, обрыв в наборе B — B больше никогда не догоняется.
- **Исправить (coder-1):**
  1. OpenAlex: добавить `sort=updated_date:asc` или сохранять промежуточный cursor пагинации отдельно. Курсор `from_updated_date` коммитить только по завершении полного прохода.
  2. arXiv: свой `Job`/scope на каждый набор (`scope = set`), курсор = `datestamp` после полного прохода набора (OAI-PMH не гарантирует порядок внутри `resumptionToken`).
  3. Добавить юнит-тест: несортированная выдача + обрыв на второй странице → курсор не превышает минимальный несобранный.

---

## Major

### M1. Harvester в compose без базы состояния
- `docker-compose.yml:387` задаёт `DATABASE_URL=...harvester`, а `services/harvester-go/cmd/harvester/main.go:35` читает `HARVESTER_DB_DSN`. При пустом DSN включается `state.NewMemoryStore()` (строки 116-121), с одним warn в логе.
- **ТЗ:** §5.4 (курсоры, ETag и бюджет живут вне контейнера), §1.3.
- **Исправить:** договориться об имени переменной. Либо coder-2 меняет compose на `HARVESTER_DB_DSN`, либо coder-1 принимает `DATABASE_URL` как алиас. Вне `--offline` при отсутствии DSN лучше падать, а не молча работать в памяти.

### M2. Ошибки источников не доходят до runner, `--once` всегда завершается с кодом 0
- `openalex.go:183, 192-198` (ошибка запроса, 429/5xx, неожиданный статус) и `arxiv.go:204-238` (ошибка или исчерпание ретраев 503): горутина делает `return` и закрывает канал. `runner.runJob` (`runner.go:171-251`) не отличает это от нормального конца выдачи. Курсор сохраняется (см. B4), `harvester_last_success_timestamp_seconds` обновляется, `RunOnce` возвращает `nil`.
- **ТЗ:** §5.3 (`--once` → выход с кодом 0 **или 1**), §9.2 `SourceStale` (не должен «зеленеть» при сбое).
- **Исправить (coder-1):** передавать ошибку из `Fetch` (канал ошибок или финальная запись с `Err`). При ошибке не двигать курсор, вызвать `RecordError` и вернуть ошибку из `runJob`. Тогда `--once` завершится с кодом 1.

### M3. Права ClickHouse: View без прав на базовые таблицы
- `platform/clickhouse/00_users.sql:12-15, 36-39`: `GRANT SELECT ON v_projects_current|v_repos_current|v_institutions_current|v_project_institutions_current`, `v_hep_software_impact`, `dm_live_activity`. При этом `SELECT ON registry_projects|registry_repos|registry_institutions|registry_project_institutions` не выдан ни `grafana_ro`, ни `spark_writer`.
- В ClickHouse 24.8 (`.env.example:11`) обычные View по умолчанию `SQL SECURITY INVOKER`, то есть права на базовые таблицы проверяются у вызывающего пользователя. Ожидаемый результат: `ACCESS_DENIED` у Grafana (панели HEP impact и Live activity) и у `enrich_projects`.
- **ТЗ:** §7.6 (`grafana_ro` — `SELECT` на `dm_*`, `v_*`, `ops_*`; `spark_writer` — `SELECT` на `v_*`), §4.5 (Spark читает реплику в CH).
- **Исправить (coder-2):** создавать View с `SQL SECURITY DEFINER DEFINER = default` (в 24.8 поддерживается) или выдать `SELECT` на `registry_*`. Проверить на стенде: `clickhouse-client -u grafana_ro -q 'SELECT * FROM scipulse.v_hep_software_impact'`.

### M4. `enrich_projects` читает `Array(String)` через generic JDBC
- `platform/spark/enrich_projects.py:16-19`: `spark.read.jdbc(..., "v_projects_current")`. Колонки `keywords Array(String)` и `topics Array(String)` драйвер отдаёт как `java.sql.Types.ARRAY`. В Spark 3.5 для диалекта без поддержки массивов это `Unrecognized SQL type ... ARRAY`, и джоб падает на чтении схемы.
- **ТЗ:** §6.4, §4.5.
- **Исправить (coder-2):** читать через `query` с `arrayStringConcat(keywords, '\x1f') AS keywords_s` и затем `split` в Spark, либо через ClickHouse Spark connector. Добавить джоб в e2e.

### M5. Ошибка `rstrip(".git/")` в связывании репозиториев
- `platform/spark/enrich_projects.py:50, 56`. Проверено: `'root-project/root'.rstrip('.git/')` → `'root-project/roo'`, `'foo/bit'` → `'foo/b'`. Ссылки на репозиторий ROOT из seed (§4.3) никогда не совпадут с `repo_to_slug`.
- **Исправить (coder-2):** `re.sub(r'(\.git)?/*$', '', m)` или `m.removesuffix('/').removesuffix('.git')`. Добавить юнит-тест.

### M6. `make demo-slice` не стартует
- `Makefile:39-43` вызывает `docker compose run --rm spark ...` без профилей, а у `spark` (`docker-compose.yml:332`) профиль `spark` и `depends_on` на `pg-init`, `minio-init`, `ch-init` из профиля `core`. Compose v2 отвечает ошибкой «service ... was pulled in as a dependency ... but is not enabled by the active profiles».
- **ТЗ:** §2.4 и §9.5 (`make demo-slice` — пакетный путь в `core`).
- **Исправить (coder-2):** `$(COMPOSE) --profile core --profile spark run --rm spark ...`. В будущем перевести на `platform/etl/steps.sh` (волна 2).

### M7. Healthcheck MinIO через `curl` (проверить на стенде)
- `docker-compose.yml:262`: `curl -sf http://localhost:9000/minio/health/live`. В образах `minio/minio` выпусков конца 2023–2024 `curl` убран. Если это подтвердится, `minio` всегда unhealthy, `minio-init` и `spark` не стартуют, `make up --wait` падает.
- **Исправить (coder-2):** `test: ["CMD", "mc", "ready", "local"]` (mc есть в образе minio) или проверка через `/dev/tcp`.

### M8. Бинарник в git
- `services/harvester-go/cmd_harvester_test.exe`: PE32+ x86-64 Windows, 20 226 560 байт, добавлен коммитом `9d6ac8d` (coder-1). `.gitignore` с `*.exe` появился позже (`864b844`) и на уже отслеживаемый файл не действует.
- **Исправить (coder-1):** `git rm --cached services/harvester-go/cmd_harvester_test.exe`. Чтобы файл не остался в истории, можно переписать историю, если команда согласна, или хотя бы не тащить его в релизную ветку.

### M9. Seed без репозиториев
- `Makefile:23-36` создаёт только проекты `root`, `rucio`, `xrootd`. ТЗ §4.3: «Seed (`make seed`): ROOT — `root-project/root`, Rucio — `rucio/rucio`, XRootD — `xrootd/xrootd`». Без репозиториев `v_repos_current` пуст, и проверки `enrich_projects` и `dm_live_activity` (код) на демо-данных невозможны.
- **Исправить (coder-2):** после создания проекта `POST /api/v1/projects/{id}/repos` с `is_primary=true` (id брать из ответа или `GET /api/v1/projects?…`).

---

## Minor (кратко)

- **m1** `sink.go:92-127`. Сетевые ошибки Schema Registry надо отличать от несовпадения со схемой. В DLQ `schema_error` отправлять только ошибки сериализации (`EncodeWithSchema`), остальные возвращать как ошибку пачки, не двигая курсор.
- **m2** `pipeline.go:28,55`. Для `parse_error` нормализовать `raw.ID`: обрезать `https://openalex.org/` и `oai:arXiv.org:`, чтобы ключ совпадал с `source:source_id` (§3.4: «DLQ наследует исходный ключ»).
- **m3** Выровнять имена переменных: compose `HTTP_ADDR` против `PORT` и `HARVESTER_METRICS_ADDR` в коде.
- **m4** `registry.json:16`. Либо ключ в Avro (по Р-2), либо явно зафиксировать `StringConverter` в ADR-004. Нынешний формат `Struct{id=…}` неудобен harvester-у для снимка реестра (волна 2, группа `harvester-registry`).
- **m5** Сверить с Debezium 2.7: `delete.tombstone.handling.mode=rewrite` вместо пары `delete.handling.mode`/`drop.tombstones` (ТЗ §4.4 само об этом предупреждает).
- **m6** Задать `HEAP_OPTS=-Xms256m -Xmx512m` для `connect` и `SCHEMA_REGISTRY_HEAP_OPTS=-Xmx256m` для `schema-registry`.
- **m7** Подключить JMX-экспортёр (javaagent, :9404) к Connect или убрать джобу до волны 2 (алерт `CdcDisconnected` §9.2 на него опирается).
- **m8** `bronze_to_silver.py:113-118`. Брать writer-схему по ID из заголовка (Schema Registry) или хотя бы `mode=PERMISSIVE` с отбраковкой битых строк. Иначе эволюция `paper.v1` (S2-6) ломает джоб.
- **m9** `bronze_to_silver.py`. Реализовать правило §6.5: `10.48550/arXiv.<id>` → `arxiv_id`, `doi=null`. При совпадении по `doi`/`arxiv_id` обновлять `paper_key`. Учесть, что `MERGE` с `OR` может дать несколько совпадений источника на одну строку цели (ошибка кардинальности Iceberg).
- **m10** `silver_to_gold_impact.py`. Прогнать запись `top_institutions Array(String)` в e2e. Если `createArrayOf("text")` не поддержан драйвером, писать через `INSERT ... FORMAT` по HTTP.
- **m11** Профиль `spark` вместо `core` у `spark` — зафиксировать в ADR-110 или README.
- **m12** Прогнать `gofmt -w` по трём файлам.
- **m13** `make test` → `go test ./services/...` и `pytest tests/`. `make e2e` — сквозной сценарий на `--offline`.

---

## Что проверено и соответствует ТЗ

| Область | Проверка | Результат |
|---|---|---|
| Топики §2.2 | `platform/kafka/init-topics.sh:44-62`: 13 топиков. raw: 3/3/3/3/3/1 партиции, 7 дней. CDC: 1 партиция, `cleanup.policy=compact`. DLQ: 1 партиция, 30 дней. `load.synthetic`: 6 партиций, 1 день. `--if-not-exists` | соответствует |
| Автосоздание топиков | `docker-compose.yml:100` `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` | соответствует |
| Consumer groups §2.3, §7.2 | `02_queues.sql`: `ch_registry_projects`, `ch_registry_repos`, `ch_registry_institutions`, `ch_registry_project_institutions` (отдельные, §7.2), `ch_live_papers` (3 raw.papers-топика), `ch_live_code`, `ch_dlq` (`dlq.papers,dlq.code`). Все `AvroConfluent` + `schema-registry:8081`. `ch_synthetic`, `harvester-registry`, `dlq-replayer` — волны 2–3 | соответствует |
| ClickHouse DDL §7, порядок §9.5 | `00_users` → `01_tables` (целевые и stage-таблицы) → `02_queues` → `03_views` (MV, затем View). `init.sh` применяет файлы по алфавиту. Колонки `registry_*`, `live_papers_1m`, `live_code_1m`, `dm_research_trends`, `dm_hep_software_impact`, `ops_*`, `ops_dq_results` (§8.3), `ops_reconciliation` (§8.4), `v_*_current`, `dm_live_activity`, `v_hep_software_impact`, `v_mart_freshness` совпадают с текстом ТЗ | соответствует (кроме M3) |
| Р-5 | `dm_*_stage AS dm_*` (`01_tables.sql:131,146`). `silver_to_gold_{trends,impact}.py`: `TRUNCATE stage` → JDBC append → `ALTER TABLE … REPLACE PARTITION … FROM *_stage` по каждому пересчитанному месяцу | соответствует |
| Р-1 | `bronze_ingest.py`: Structured Streaming, `subscribePattern raw\..*`, `availableNow`, чекпойнт `s3a://warehouse/_checkpoints/bronze`, колонки по §6.3 | соответствует |
| §6.4 `bronze_to_silver` | инкремент по `snapshot_id` из `ops.job_watermarks`, дедупликация окном по `updated_at`, `MERGE INTO`, нормализация DOI и arXiv, `topics_map.csv` | в основном соответствует (m8, m9) |
| Р-6 / §4.3 Postgres | `docker-compose.yml:30-39`: `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`, `max_slot_wal_keep_size=2GB`. Алерт на отставание слота — волна 2 (§9.2) | соответствует |
| pg-init §1.3 | 4 базы и роли-владельцы, `debezium` с `LOGIN REPLICATION`, `GRANT SELECT` и `ALTER DEFAULT PRIVILEGES FOR ROLE registry_app` | соответствует |
| Compose §9.4 | все порты на `127.0.0.1`, `mem_limit` совпадают с таблицей §9.4 (postgres 512m, kafka 1024m с heap 512m, SR 384m, connect 768m, CH 1536m, minio 384m, spark 2560m, registry 64m, harvester 128m, prometheus 384m, grafana 256m, exporter 64m). Healthcheck по §9.4. Нестандартные порты MinIO 9100/9101, Kafka EXTERNAL 9094 | соответствует (кроме B1, M7, m6) |
| Контракты §3 | `paper.v1` побайтно совпадает с §3.2, включая `comments`/`journal_ref` (у `authors` добавлен `default: []` — безвредно). `dlq_envelope.v1`: все поля §3.3, `stage` enum `fetch…produce`, `attempt` default 0, `original_key`/`rule_applied` nullable + `default null`. `repo_snapshot`, `release`, `code_event` (enum из 6 типов), `software_record`, `synthetic_event` (`timestamp-micros`, `synthetic` default true) по §3.1. Копии в `harvester-go/internal/sink/schemas/` идентичны `contracts/` (проверено `diff`) | соответствует |
| Subject-стратегии §3.1 | `scripts/init_registry.sh`: `<topic>-value` для `raw.papers.{openalex,arxiv,inspire}`, `raw.code.events`, `raw.code.zenodo`, `dlq.{papers,code}`, `load.synthetic`; `raw.code.github-org.scipulse.v1.RepoSnapshot` и `…Release` (TopicRecordNameStrategy). Глобальный и per-subject `BACKWARD`, проверка `POST /compatibility/.../versions/latest` перед регистрацией | соответствует |
| registry-go §4.3 | `000001_init.up.sql` построчно совпадает с DDL ТЗ (enum, 4 таблицы, CHECK на slug и `github_full_name`, частичный уникальный индекс primary, триггеры `updated_at`). `000002_cdc.up.sql`: `REPLICA IDENTITY FULL` ×4 и `CREATE PUBLICATION dbz_registry` на 4 таблицы. Down-миграции есть | соответствует |
| registry-go §4.1–4.2 | все маршруты из §4.2 (`router.go:51-84`). Запись под `APIKeyAuth`. `X-API-Key` сравнивается через `crypto/subtle.ConstantTimeCompare` (`middleware.go:239`), пустой ключ отклоняется. `application/problem+json` с непустым `errors[]` (`problem.go`, `handlers.go:384`). `limit` 1..100 → 400. Курсор по `id`. 409 по `23505`. Смена primary в одной транзакции с `FOR UPDATE`. Таймаут 5 с. Recoverer в формате problem+json. Метрики `http_requests_total{route,code}`, `http_request_duration_seconds`, коллектор пула pgx. Миграции при старте. Graceful shutdown. Multi-stage Dockerfile, non-root | соответствует |
| Debezium §4.4 | `registry.json`: `pgoutput`, `topic.prefix=cdc.registry`, публикация и слот `dbz_registry`, `autocreate=disabled`, 4 таблицы, `ExtractNewRecordState` с `rewrite`, `drop.tombstones=true`, `add.fields=op,source.ts_ms`. `connect-init` идемпотентен (`PUT …/config`) | конфиг соответствует, но B2/B3 мешают запуску |
| harvester §5.1, §5.3 | режимы `run` (по умолчанию), `--once`, `--offline`, `--sources`. SIGTERM: `signal.NotifyContext` → новые запросы прекращаются → `flushBatch` через `context.WithoutCancel` → `Flush` → сохранение курсора → `Shutdown` продюсера. Есть тест `TestGracefulDrainOnSIGTERM…`. Продюсер franz-go `acks=all` с идемпотентностью и `RecordDeliveryTimeout` 150 с (C1). arXiv: пауза ≥ 3 с и `Retry-After` на 503. OpenAlex: бюджет в `source_budget` и `/rate-limit` | соответствует, кроме B4, M1, M2 |
| DLQ-коды §3.3 | `pipeline/types.go:7-16`: `parse_error`, `missing_source_id`, `missing_title`, `missing_published_at`, `bad_doi` (regex `^10\.\d{4,9}/\S+$`), `future_published_at` (допуск +24 ч), `no_authors`, `schema_error` (стадия `serialize`). `http_404` — для GitHub (S4) | соответствует |
| БД harvester §5.4 | `migrations/000001_init.up.sql` и `state/postgres.go:13-38`: `harvest_cursors`, `http_etags`, `source_budget` по DDL ТЗ (`IF NOT EXISTS`) | соответствует (DDL продублирован в двух местах, можно оставить один источник) |
| Метрики §5.5/§9.1 | все 9 метрик с префиксом `harvester_` и нужными метками. Имя `harvester_last_success_timestamp_seconds` взято из §9.1/§9.2, а не `…_timestamp` из §5.5: в ТЗ здесь противоречие, выбран вариант, на который опирается алерт `SourceStale`. `harvester_registry_projects` зарегистрирована, заполнится в волне 2 | соответствует |
| Ключи §3.4 | `Paper.RecordKey()`: нормализованный DOI, иначе `source:source_id`, строкой UTF-8 | соответствует (m2 — только для DLQ при `parse_error`) |
| arXiv `comments`/`journal_ref` | `mapper.go:335-345, 405-416`: поля заполняются, GitHub-ссылки извлекаются с `kind=code` | соответствует |

## Границы владения (план §1)

| Нарушение или замечание | Кто | Оценка |
|---|---|---|
| `services/harvester-go/cmd_harvester_test.exe` — бинарник в зоне coder-1 | coder-1 | major (M8) |
| `go.work`, `go.work.sum` в корне — вне явных зон; нужны для `common-go`, конфликтов нет | coder-1 | ок, зафиксировать владельца |
| `.gitignore` — нет в списке coder-2, но с другими агентами не пересекается | coder-2 | ок |
| `scipulse_part1_agents_plan.md` добавлен обоими коммитами с одинаковым содержимым (blob `b5f5822`); при merge конфликта не было | оба | ок |
| `docker-compose.yml` (coder-2) ссылается на артефакты coder-1 (`services/loadgen-go`, имена env harvester и registry) без согласования | coder-2 / coder-1 | B1, M1, m3: нужен общий список env-переменных, например таблица в `.env.example` или README |
| Чужих правок в `services/` от coder-2 и в `platform/{clickhouse,spark,…}` от coder-1 не найдено (по `git show --stat`) | — | ок |

## Рекомендуемый порядок исправлений

1. **coder-2:** B1, B3, M6, M7, M3, M9, затем M4, M5.
2. **coder-1 + coder-2:** B2 (образ Connect с Avro-конвертером), M1 и m3 (имена env).
3. **coder-1:** B4 и M2 (курсор и ошибки источников, с тестами), M8, затем m1, m2, m12.
4. **tester-verifier:** после исправлений повторить проверку на стенде: `make nuke && make up`, `make seed`, `scripts/measure_cdc_latency.py`, `harvester --once --offline`, `make demo-slice`.
