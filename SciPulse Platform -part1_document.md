# SciPulse Platform — детальное ТЗ части 1

Nov 9, 2026 · @Murat

Документ детализирует часть 1 единого ТЗ до уровня таблиц, топиков и пакетов кода: по нему можно писать миграции, DDL и каркасы сервисов без дополнительных решений. Имена файлов, таблиц и задач (S1-3, S2-4…) совпадают с плейбуком; то, чего в плейбуке не было, помечено «новое в этом ТЗ».

## 1. Архитектура и границы сервисов

Часть 1 — три своих сервиса на Go и десять инфраструктурных компонентов. Сервисы не вызывают друг друга напрямую, а обмениваются событиями через Kafka, поэтому каждый можно перезапустить, не останавливая остальные.

&#91;embedded content: архитектура части 1 · 4 модуля Go, два пути обработки\]

harvester и loadgen пишут в Kafka напрямую, реестр попадает туда через Postgres и Debezium, а harvester читает реестр обратно из `cdc.registry.*`.

### 1.1 Компоненты и их данные

| Компонент | Тип | Технология | Владеет | Пишет в | Читает из | Профиль |
| --- | --- | --- | --- | --- | --- | --- |
| `registry-go` | свой сервис | Go, chi, pgx, golang-migrate | база Postgres `registry` | Postgres (дальше CDC) | — | `core` |
| `harvester-go` | свой сервис | Go, franz-go | база `harvester` (курсоры, ETag), кэш `data/cache/` | `raw.*`, `dlq.*` | внешние API, `cdc.registry.*` | `core` |
| `loadgen-go` | свой сервис | Go | — | `load.synthetic` | — | `core`, включается для бенчмарка |
| `common-go` | библиотека | Go | — | — | — | — |
| Debezium Connect | инфраструктура | Kafka Connect + Postgres connector | смещения в Kafka | `cdc.registry.*` | WAL базы `registry` | `core` |
| Kafka + Schema Registry | инфраструктура | KRaft, один брокер; Avro | топики и схемы | — | — | `core` |
| Spark | джобы | PySpark, local-режим | — | Iceberg, ClickHouse (JDBC) | Kafka, Iceberg, ClickHouse | `core` |
| MinIO + Iceberg | хранилище | S3 API; каталог JDBC в базе `iceberg_catalog` | bronze, silver, ops | — | — | `core` |
| ClickHouse | слой выдачи | Kafka engine, MV, MergeTree-семейство | реплика реестра, витрины, ops | — | Kafka | `core` |
| Airflow | оркестратор | LocalExecutor; метаданные в базе `airflow` | DAG и история запусков | `ops_pipeline_runs` | — | `full` |
| Prometheus + Grafana | наблюдаемость | + kafka-exporter | метрики, дашборды, алерты | — | `/metrics` всех, ClickHouse | `core` |

### 1.2 Правила границ

1. **У каждого хранилища один писатель.** Базу `registry` пишет только registry-go, silver — только Spark, витрины — только Spark-джобы или Materialized Views.
2. **Сервисы не ходят в чужие базы.** Harvester узнаёт о проектах из топика `cdc.registry.*`; REST реестра — только запасной путь на старте.
3. **Все события — Avro со схемой в Schema Registry.** Исключение возможно только для CDC, если ADR-004 выберет JSON для Connect.
4. **At-least-once везде, дубли снимает запись.** Продюсеры с `acks=all` и идемпотентностью; silver — `MERGE INTO` по ключу, ClickHouse — `ReplacingMergeTree` или перезапись партиции.
5. **Синтетика не покидает песочницу.** `load.synthetic` читают только таблицы бенчмарка; CI проверяет 0 строк с `synthetic=true` в gold.
6. **Наружу ничего не открыто.** Все порты публикуются на `127.0.0.1`, секреты только в `.env`.

### 1.3 Базы в одном экземпляре Postgres

Один контейнер Postgres держит несколько баз с разными владельцами: так дешевле по памяти, а границы остаются на уровне ролей.

| База | Роль-владелец | Что внутри | Появляется |
| --- | --- | --- | --- |
| `registry` | `registry_app`; чтение WAL — `debezium` | проекты, репозитории, институты | S2-1 |
| `iceberg_catalog` | `iceberg` | метаданные таблиц Iceberg (JDBC-каталог) | S1-6 |
| `harvester` | `harvester_app` | курсоры инкремента, ETag GitHub — новое в этом ТЗ | S1-4 |
| `airflow` | `airflow` | метаданные DAG и запусков | S3-3, профиль `full` |

## 2. Как связаны Kafka, Spark, Iceberg, ClickHouse и Airflow

Kafka — единственная точка входа данных. Из неё независимо читают три потребителя: Spark пишет в bronze, Kafka engine ClickHouse кормит быстрый путь, harvester держит снимок реестра. Airflow из Kafka ничего не читает: он только запускает пакетные шаги по расписанию.

&#91;embedded content: топики → consumer groups → хранилища · 8 групп\]

У каждого потребителя своя группа и свои смещения, поэтому остановка ClickHouse в chaos C3 не задерживает запись bronze и наоборот.

### 2.1 Путь одной статьи

1. **Harvester** получает работу из OpenAlex, кладёт сырой ответ в кэш, переводит в `paper.v1` и проверяет. Невалидная запись уходит в `dlq.papers`.
2. **Продюсер** пишет в `raw.papers.openalex` с ключом `doi` (или `source:source_id`), `acks=all`, идемпотентно.
3. **Быстрый путь, секунды.** Kafka engine `papers_queue` читает сообщение группой `ch_live_papers`, Materialized View добавляет его в поминутный счётчик `live_papers_1m`.
4. **Пакетный путь, раз в сутки.** DAG `scipulse_etl` запускает `bronze_ingest` в режиме `availableNow`: сообщение ложится в `bronze.raw_events` сырыми байтами вместе со смещением.
5. **`bronze_to_silver`** декодирует Avro, нормализует DOI и темы, дедуплицирует и делает `MERGE INTO silver.papers`.
6. **`dq_silver`** проверяет silver; при провале DAG останавливается и gold не трогает.
7. **`silver_to_gold_*`** пишут витрины в ClickHouse по JDBC, перезаписывая партиции пересчитанных месяцев.
8. **`reconcile`** сверяет число сообщений по топику и часу в bronze и в `live_papers_1m` за закрытое окно.
9. **Grafana** читает витрины и представления ClickHouse.

### 2.2 Карта топиков

Имена и параметры raw-топиков — из S1-1; число партиций и retention для CDC, DLQ и синтетики — новое в этом ТЗ. Все топики создаёт идемпотентный `make topics`, автосоздание в брокере выключено.

| Топик | Продюсер | Потребители | Ключ | Схема | Партиции · хранение | Спринт |
| --- | --- | --- | --- | --- | --- | --- |
| `raw.papers.openalex` | harvester | Spark `bronze_ingest`; CH `papers_queue` | `doi` или `source:source_id` | `paper.v1` | 3 · 7 дней | S1 |
| `raw.papers.arxiv` | harvester | те же | тот же | `paper.v1` | 3 · 7 дней | S1 |
| `raw.papers.inspire` | harvester | те же | тот же | `paper.v1` | 3 · 7 дней | S4 |
| `raw.code.github` | harvester | Spark | `repo_full_name` | `repo_snapshot.v1`, `release.v1` | 3 · 7 дней | S4 |
| `raw.code.events` | harvester, replay GH Archive | Spark; CH `code_events_queue` | `event_id` | `code_event.v1` | 3 · 7 дней | S4, Should |
| `raw.code.zenodo` | harvester | Spark | `record_doi` | `software_record.v1` | 1 · 7 дней | S4, Should |
| `cdc.registry.public.tracked_projects` | Debezium | CH `registry_projects_queue`; harvester | `id` | строка после `ExtractNewRecordState` | 1 · compact | S2 |
| `cdc.registry.public.project_repos` | Debezium | CH `registry_repos_queue`; harvester | `id` | то же | 1 · compact | S2 |
| `cdc.registry.public.institutions` | Debezium | Spark (правило R3) | `id` | то же | 1 · compact | S2 |
| `cdc.registry.public.project_institutions` | Debezium | Spark (правило R3) | `project_id`, `institution_id` | то же | 1 · compact | S2, новое |
| `dlq.papers` | harvester, replay | CH `dlq_queue`; `harvester replay-dlq` | исходный ключ | `dlq_envelope.v1` | 1 · 30 дней | S1 |
| `dlq.code` | harvester | CH `dlq_queue` | исходный ключ | `dlq_envelope.v1` | 1 · 30 дней | S4 |
| `load.synthetic` | loadgen | CH `synthetic_queue` | без ключа | `synthetic_event.v1` | 6 · 1 день | S5 |

CDC-топики — с `cleanup.policy=compact`: harvester строит снимок реестра чтением с начала, и без compaction старые версии исчезли бы по retention. Шесть партиций у `load.synthetic` нужны, чтобы в S5-5 менять `kafka_num_consumers` в ClickHouse.

### 2.3 Потребители и смещения

| Потребитель | Читает | Где смещение | Что снимает дубли |
| --- | --- | --- | --- |
| Spark `bronze_ingest` | `raw\..*` по шаблону | чекпойнт `s3a://warehouse/_checkpoints/bronze`, не consumer group | чекпойнт + ключ `(topic, partition, offset)` в bronze |
| CH `papers_queue` | `raw.papers.*` | группа `ch_live_papers` | дубли возможны при падении между вставкой и коммитом; ловит `reconcile` |
| CH `code_events_queue` | `raw.code.events` | `ch_live_code` | то же |
| CH `registry_projects_queue`, `registry_repos_queue` | `cdc.registry.public.*` | `ch_registry` | `ReplacingMergeTree(version)` |
| CH `dlq_queue` | `dlq.*` | `ch_dlq` | для доли DLQ точности до единиц не нужно |
| CH `synthetic_queue` | `load.synthetic` | `ch_synthetic` | не нужно: только бенчмарк |
| harvester, снимок реестра | CDC проектов и репозиториев | `harvester-registry`, на старте с earliest | снимок — map по `id`, последняя версия побеждает |
| `harvester replay-dlq` | `dlq.papers` | `dlq-replayer` | коммит после публикации; silver снимает повторы `MERGE` |

### 2.4 Кто кого запускает

- **Постоянно работают** в `core`: Debezium, Kafka engine ClickHouse, registry-go, harvester в режиме демона с планировщиком по приоритетам.
- **По расписанию** в `full`: Airflow запускает harvester `--once`, Spark-джобы, DQ и reconcile. В `core` те же шаги запускает `make demo-slice`.
- **Как Airflow запускает Spark** решается в S3-3. Рекомендация этого ТЗ: свой образ Airflow с `spark-submit --master local[*]`, Iceberg-джарами и бинарником harvester. Вариант с `docker compose run` проще, но требует монтировать Docker-сокет, а это права root на хосте.

## 3. Контракты событий

Все события описаны Avro-схемами в `contracts/`, зарегистрированы в Schema Registry с режимом `BACKWARD` и проверяются в CI. Новое поле допускается только nullable с default; всё остальное — новая версия контракта.

### 3.1 Список контрактов

| Контракт | Топик | Subject в Schema Registry | Поля | Спринт |
| --- | --- | --- | --- | --- |
| `paper.v1` | `raw.papers.*` | `raw.papers.<source>-value` | см. 3.2 | S1-1 |
| `repo_snapshot.v1` | `raw.code.github` | `raw.code.github-org.scipulse.v1.RepoSnapshot` | `repo_full_name`, `stars`, `forks`, `open_issues`, `archived`, `pushed_at`, `snapshot_at` | S4-2 |
| `release.v1` | `raw.code.github` | `raw.code.github-org.scipulse.v1.Release` | `repo_full_name`, `tag`, `name`?, `published_at`, `prerelease`, `fetched_at` | S4-2 |
| `code_event.v1` | `raw.code.events` | `raw.code.events-value` | `event_id`, `type` (Push, Release, Watch, Issues, PullRequest, Fork), `repo_full_name`, `actor_login`?, `created_at`, `replayed_at`, `source_hour` | S4-3 |
| `software_record.v1` | `raw.code.zenodo` | `raw.code.zenodo-value` | `record_doi`, `concept_doi`?, `version`?, `title`, `published_at`, `related_identifiers[]` (relation, scheme, identifier), `repo_full_name`? | S4-4 |
| `dlq_envelope.v1` | `dlq.*` | `dlq.<домен>-value` | см. 3.3 | S1-5 |
| `synthetic_event.v1` | `load.synthetic` | `load.synthetic-value` | `event_type`, `country`, `latency_ms`, `bytes`, `produced_at` (timestamp-micros), `synthetic` = true | S5-1 |
| строка CDC | `cdc.registry.public.*` | создаёт Debezium | колонки таблицы + `__op`, `__source_ts_ms`, `__deleted` | S2-3 |

Знак `?` — nullable с default `null`. Два контракта в одном топике `raw.code.github` требуют `TopicRecordNameStrategy`; остальные топики — `TopicNameStrategy`.

### 3.2 paper.v1

Поля — из S1-1. Новое в этом ТЗ: `comments` и `journal_ref`. Правило связывания R1 ищет ссылки на GitHub именно в этих полях arXiv, и добавить их нужно до первой регистрации схемы — то есть сейчас, в S1-1.

```json
{
  "type": "record", "name": "Paper", "namespace": "org.scipulse.v1",
  "fields": [
    {"name": "source", "type": {"type": "enum", "name": "Source", "symbols": ["openalex", "arxiv", "inspire"]}},
    {"name": "source_id", "type": "string"},
    {"name": "doi", "type": ["null", "string"], "default": null},
    {"name": "arxiv_id", "type": ["null", "string"], "default": null},
    {"name": "title", "type": "string"},
    {"name": "abstract", "type": ["null", "string"], "default": null},
    {"name": "comments", "type": ["null", "string"], "default": null},
    {"name": "journal_ref", "type": ["null", "string"], "default": null},
    {"name": "authors", "type": {"type": "array", "items": {
      "type": "record", "name": "Author", "fields": [
        {"name": "name", "type": "string"},
        {"name": "orcid", "type": ["null", "string"], "default": null},
        {"name": "affiliations", "type": {"type": "array", "items": "string"}, "default": []}
      ]}}},
    {"name": "topics", "type": {"type": "array", "items": "string"}, "default": []},
    {"name": "published_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "updated_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "cited_by_count", "type": ["null", "int"], "default": null},
    {"name": "links", "type": {"type": "array", "items": {
      "type": "record", "name": "Link", "fields": [
        {"name": "url", "type": "string"},
        {"name": "kind", "type": "string"}
      ]}}, "default": []},
    {"name": "ingested_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

`links.kind`: `landing`, `pdf`, `code` (ссылка на GitHub, извлечённая harvester), `other`. DOI хранится нормализованным: нижний регистр, без префикса `https://doi.org/`.

### 3.3 DLQ

`dlq_envelope.v1`: `reason` (код из таблицы ниже), `stage` (`fetch`, `parse`, `map`, `validate`, `serialize`, `produce`), `source`, `original_topic`, `original_key`?, `payload` (bytes, сырая запись источника — достаточно для replay), `error`, `failed_at`, `attempt` (default 0), `rule_applied`? (заполняет replay).

| Код | Стадия | Когда |
| --- | --- | --- |
| `parse_error` | parse | обрезанный XML, пустой или битый JSON |
| `missing_source_id`, `missing_title`, `missing_published_at` | validate | нет обязательного поля |
| `bad_doi` | validate | DOI не проходит регулярное выражение |
| `future_published_at` | validate | дата публикации в будущем |
| `no_authors` | validate | пустой список авторов |
| `schema_error` | serialize | запись не ложится в Avro-схему |
| `http_404` | fetch | репозиторий переименован или удалён (GitHub) |

### 3.4 Ключи

- **Ключ — обычная строка UTF-8, не Avro**: её проще читать в ClickHouse и Spark.
- **Работа:** нормализованный `doi`, если есть, иначе `source:source_id`. Все версии одной работы из одного источника попадают в одну партицию; склейка разных источников — задача silver.
- **Код:** `repo_full_name` для снапшотов и релизов (порядок в рамках репозитория), `event_id` для событий, `record_doi` для Zenodo.
- **DLQ** наследует исходный ключ, чтобы replay вернул запись в ту же партицию.

### 3.5 Правила эволюции

| Изменение | Можно в v1 | Почему |
| --- | --- | --- |
| Новое nullable-поле с default | да | новый читатель читает старые данные |
| Новое значение в конце enum | да | старые данные его не содержат |
| Удалить обязательное поле, сменить тип, переименовать без alias | нет | CI краснеет (S3-1) |
| Ломающее изменение | через v2 | `paper.v2` и новый subject; v1 живёт, пока потребители не переедут |

CI для каждого `contracts/*.avsc` вызывает `POST /compatibility/subjects/<subject>/versions/latest`; описание каждого поля — в `docs/contracts.md`.

## 4. Сервис registry-go

Реестр — маленький CRUD-сервис с одной задачей: быть источником истины о том, что отслеживается. Остальные компоненты узнают об изменениях из CDC, а не из его API, поэтому падение реестра не останавливает сбор данных.

&#91;embedded content: ER-схема базы registry · 4 таблицы, 3 связи, путь в CDC\]

`project_institutions` — таблица-связка без собственного `id`, поэтому в ClickHouse её реплика упорядочена по `(project_id, institution_id)`.

### 4.1 Устройство

```
services/registry-go/
├── cmd/registry/main.go   # конфиг из env, slog JSON, graceful shutdown по SIGTERM
├── internal/
│   ├── http/              # chi-роутер, handlers, middleware, problem+json
│   ├── service/           # правила: формат slug, один primary-репозиторий
│   ├── repository/        # pgx, SQL, транзакции
│   └── model/             # структуры домена
├── migrations/            # 000001_init, 000002_cdc (up и down)
├── openapi.yaml
└── Dockerfile             # multi-stage, non-root
```

Путь запроса на запись:

1. **Middleware:** request id, лог slog, recover, таймаут 5 с, проверка `X-API-Key` с `REGISTRY_API_KEY` сравнением за постоянное время.
2. **Handler** разбирает JSON и проверяет поля; ошибки — `application/problem+json` с массивом `errors[]`.
3. **Service** применяет правила домена: смена primary-репозитория снимает флаг со старого в одной транзакции.
4. **Repository** пишет в Postgres; конфликт уникальности превращается в 409.
5. **WAL → Debezium → Kafka.** Сервис ничего не публикует сам: событие появляется только после коммита транзакции, поэтому outbox не нужен.

### 4.2 API

| Метод и путь | Ключ | Что делает | Ответы |
| --- | --- | --- | --- |
| `GET /healthz`, `GET /metrics`, `GET /docs` | — | живость, метрики Prometheus, Swagger UI | 200 |
| `GET /api/v1/projects?status=&limit=&cursor=` | — | список; курсор по `id`, `limit` ≤ 100 | 200, 400 |
| `POST /api/v1/projects` | да | создать проект | 201, 400, 401, 409 |
| `GET /api/v1/projects/{id}` | — | проект с репозиториями и институтами | 200, 404 |
| `PATCH /api/v1/projects/{id}` | да | частичное обновление: `priority`, `status`, `keywords`… | 200, 400, 401, 404, 409 |
| `DELETE /api/v1/projects/{id}` | да | удалить; репозитории и связи удаляются каскадом | 204, 401, 404 |
| `GET`, `POST /api/v1/projects/{id}/repos` | POST да | репозитории проекта | 200, 201, 400, 401, 404, 409 |
| `DELETE /api/v1/projects/{id}/repos/{repoId}` | да | отвязать репозиторий | 204, 401, 404 |
| `GET`, `POST /api/v1/institutions` | POST да | справочник институтов; `ror_id` уникален | 200, 201, 400, 401, 409 |
| `PUT`, `DELETE /api/v1/projects/{id}/institutions/{institutionId}` | да | привязать или отвязать институт — новое в этом ТЗ | 204, 401, 404 |

### 4.3 Схема базы registry

Три таблицы — из S2-1. Четвёртая, `project_institutions`, — новое в этом ТЗ: без неё правилу связывания R3 («название проекта + институт-контрибьютор») не с чем сравнивать аффилиации авторов.

| Таблица | Ключ | Основные поля | Ограничения |
| --- | --- | --- | --- |
| `tracked_projects` | `id` | `slug`, `name`, `keywords text[]`, `topics text[]`, `priority`, `status` | `slug` уникален и в нижнем регистре; `priority` 1–5, 1 — собирать первым |
| `project_repos` | `id` | `project_id`, `github_full_name`, `is_primary` | репозиторий привязан к одному проекту; один primary на проект |
| `institutions` | `id` | `ror_id`, `name`, `country` | `ror_id` уникален |
| `project_institutions` | `(project_id, institution_id)` | — | каскадное удаление с обеих сторон |

```sql
-- migrations/000001_init.up.sql
CREATE TYPE project_status AS ENUM ('active', 'paused');

CREATE TABLE institutions (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ror_id      text UNIQUE,
  name        text NOT NULL,
  country     char(2),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tracked_projects (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  slug        text NOT NULL UNIQUE CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
  name        text NOT NULL,
  description text,
  keywords    text[] NOT NULL DEFAULT '{}',
  topics      text[] NOT NULL DEFAULT '{}',
  priority    smallint NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
  status      project_status NOT NULL DEFAULT 'active',
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE project_repos (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id       bigint NOT NULL REFERENCES tracked_projects(id) ON DELETE CASCADE,
  github_full_name text NOT NULL UNIQUE
                   CHECK (github_full_name ~ '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'),
  is_primary       boolean NOT NULL DEFAULT false,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX project_repos_one_primary ON project_repos (project_id) WHERE is_primary;
CREATE INDEX project_repos_by_project ON project_repos (project_id);

CREATE TABLE project_institutions (
  project_id     bigint NOT NULL REFERENCES tracked_projects(id) ON DELETE CASCADE,
  institution_id bigint NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (project_id, institution_id)
);

CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END $$;

CREATE TRIGGER trg_projects_upd     BEFORE UPDATE ON tracked_projects     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_repos_upd        BEFORE UPDATE ON project_repos        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_institutions_upd BEFORE UPDATE ON institutions         FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_proj_inst_upd    BEFORE UPDATE ON project_institutions FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- migrations/000002_cdc.up.sql
ALTER TABLE tracked_projects     REPLICA IDENTITY FULL;
ALTER TABLE project_repos        REPLICA IDENTITY FULL;
ALTER TABLE institutions         REPLICA IDENTITY FULL;
ALTER TABLE project_institutions REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_registry
  FOR TABLE tracked_projects, project_repos, institutions, project_institutions;
```

Роли `registry_app` и `debezium` (`LOGIN REPLICATION`) создаёт init-скрипт контейнера Postgres, а не миграция: пароли берутся из `.env`. Postgres стартует с `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4`. Seed (`make seed`): ROOT — `root-project/root`, Rucio — `rucio/rucio`, XRootD — `xrootd/xrootd`.

### 4.4 Debezium

```json
{
  "name": "registry-postgres",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres",
    "database.dbname": "registry",
    "database.user": "${file:/secrets/debezium.properties:user}",
    "database.password": "${file:/secrets/debezium.properties:password}",
    "topic.prefix": "cdc.registry",
    "publication.name": "dbz_registry",
    "publication.autocreate.mode": "disabled",
    "slot.name": "dbz_registry",
    "table.include.list": "public.tracked_projects,public.project_repos,public.institutions,public.project_institutions",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.add.fields": "op,source.ts_ms"
  }
}
```

- После `unwrap` сообщение — плоская строка таблицы плюс `__op` (`c`, `u`, `d`, `r`), `__source_ts_ms` и `__deleted`.
- `drop.tombstones=true`: в compacted-топике остаётся запись с `__deleted=true`, а Kafka engine ClickHouse не спотыкается на пустые сообщения.
- В новых версиях Debezium параметры удаления переименованы в `delete.tombstone.handling.mode`; сверить с версией образа в S2-3.
- Конвертеры ключа и значения (Avro или JSON) задаёт ADR-004; пароль читается через `FileConfigProvider` и не лежит в репозитории.
- `make init-cdc` регистрирует коннектор идемпотентно: `PUT /connectors/registry-postgres/config`.

### 4.5 Кто читает реестр и как

- **ClickHouse** держит реплику всех четырёх таблиц в `ReplacingMergeTree(version)` и читается только через представления `v_*_current` (раздел 7).
- **Harvester** держит снимок проектов и репозиториев в памяти из CDC-топиков (раздел 5).
- **Spark** для связывания и витрин читает реплику в ClickHouse по JDBC, а не Postgres: так соблюдается правило «не ходить в чужие базы».

### 4.6 Проверки

- Интеграционные тесты на testcontainers-go: CRUD, ответы 400, 401, 404, 409, каскадное удаление, один primary-репозиторий.
- `migrate up` и `migrate down` на чистой базе; тест триггера `updated_at`.
- e2e CDC (S2-8): `INSERT`, `UPDATE`, `DELETE` в Postgres → строка в `v_projects_current`; задержка p95 < 2 с (target).
- Метрики: `http_requests_total{route,code}`, `http_request_duration_seconds`, пул соединений pgx.

## 5. Сервис harvester-go

Harvester — конвейер «источник → перевод в контракт → проверка → Kafka» с одним интерфейсом для шести источников. Он работает демоном с планировщиком по приоритетам реестра или одним проходом по команде Airflow.

&#91;embedded content: конвейер harvester-go · пакеты internal/ и путь пачки\]

Если процесс падает между отправкой и ack, курсор остаётся на месте и пачка собирается снова; дубли схлопывает `MERGE` в silver.

### 5.1 Устройство

```
services/harvester-go/
├── cmd/harvester/main.go  # режимы run, --once, --offline, replay-dlq
└── internal/
    ├── source/     # openalex, arxiv, inspire, github, gharchive, zenodo
    ├── registry/   # снимок проектов и репозиториев из cdc.registry.*
    ├── scheduler/  # очередь задач по priority, пропуск paused
    ├── ratelimit/  # token bucket на источник, дневной бюджет OpenAlex
    ├── cache/      # data/cache/<source>/<sha256>.json.gz
    ├── pipeline/   # map → validate → route (raw или dlq)
    ├── sink/       # Kafka-продюсер из common-go
    ├── state/      # курсоры, ETag, бюджет в базе harvester
    └── replay/     # replay-dlq и именованные правила исправления
```

Интерфейс источника из S1-2 расширен задачей `Job`: с S2-5 сбор идёт по проектам реестра, а не только по дате.

```go
type Source interface {
    Name() string
    Fetch(ctx context.Context, job Job) (<-chan RawRecord, error)
}

type Job struct {
    Project  string    // slug из реестра; пусто для общих тем из topics.yaml
    Topics   []string
    Keywords []string
    Repos    []string  // для github, gharchive, zenodo
    Since    string    // курсор из state
}

type RawRecord struct {
    Source  string
    ID      string
    Body    []byte    // сырая запись; идёт в payload DLQ
    Fetched time.Time
}
```

### 5.2 Источники

| Источник | Как ходит | Инкремент | Лимиты и вежливость | Топик | Спринт |
| --- | --- | --- | --- | --- | --- |
| OpenAlex | REST, курсорная пагинация, `per_page=100`, `select` | дата обновления из `harvest_cursors` | API-ключ; дневной бюджет из `/rate-limit`, остановка с понятным логом | `raw.papers.openalex` | S1-3 |
| arXiv | OAI-PMH `ListRecords`, наборы из конфига | `from` = дата последней успешной выгрузки | ≥ 3 с между запросами; `resumptionToken`; 503 + `Retry-After` | `raw.papers.arxiv` | S1-4 |
| INSPIRE-HEP | REST `/api/literature`, `q` из `keywords` проекта, `fields` | дата обновления | задержки из `docs/sources.md` | `raw.papers.inspire` | S4-1 |
| GitHub | REST `/repos/{owner}/{repo}` и релизы | раз в сутки по `project_repos` | токен на чтение; `ETag` и `If-None-Match`, 304 не тратит лимит | `raw.code.github` | S4-2 |
| GH Archive | почасовые файлы `json.gz` | последний обработанный час | кэш на диск; replay `--speed 60` | `raw.code.events` | S4-3 |
| Zenodo | REST записей, `related_identifiers` | дата публикации | вежливый темп | `raw.code.zenodo` | S4-4 |

### 5.3 Режимы и гарантии

- **`run`, демон.** Снимок реестра → планировщик строит задачи по `priority` (1 — первым), `paused` пропускает → один воркер на источник, чтобы не превысить лимиты → конвейер → Kafka. Новый проект попадает в сбор ≤ 1 минуты без перезапуска.
- **`--once`.** Один проход по всем задачам и выход с кодом 0 или 1 — для задачи `harvest` в Airflow.
- **`--offline`.** Читает только кэш: e2e-тесты в CI работают без сети.
- **`replay-dlq --reason --since [--dry-run]`.** Читает `dlq.papers` группой `dlq-replayer`, применяет именованное правило, заново проверяет и пишет в `original_topic` с исходным ключом.
- **Остановка по SIGTERM.** Новые запросы прекращаются → продюсер досылает буфер → курсоры сохраняются → выход. Потерь нет (тест S1-2, chaos C5).
- **Правило курсора.** Курсор сдвигается только после того, как продюсер подтвердил все записи пачки. При падении пачка собирается снова: дубли допустимы, потери нет.

### 5.4 База harvester

Новое в этом ТЗ. `--once` из Airflow запускается в свежем контейнере, поэтому дата `from` для arXiv, ETag GitHub и потраченный бюджет OpenAlex должны жить вне контейнера.

```sql
CREATE TABLE harvest_cursors (
  source          text NOT NULL,   -- openalex, arxiv, inspire, gharchive, zenodo
  scope           text NOT NULL,   -- набор arXiv, тема или slug проекта
  cursor_value    text,            -- дата from, курсор OpenAlex, последний час GH Archive
  last_success_at timestamptz,
  last_error      text,
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, scope)
);

CREATE TABLE http_etags (
  url         text PRIMARY KEY,
  etag        text NOT NULL,
  last_status smallint NOT NULL,
  fetched_at  timestamptz NOT NULL
);

CREATE TABLE source_budget (
  source  text NOT NULL,
  day     date NOT NULL,
  spent   integer NOT NULL DEFAULT 0,
  budget  integer,
  PRIMARY KEY (source, day)
);
```

### 5.5 Метрики harvester

Имена из S1-2 с префиксом `harvester_`, чтобы не пересекаться с другими экспортерами.

| Метрика | Тип | Метки | Зачем |
| --- | --- | --- | --- |
| `harvester_fetched_total`, `harvester_valid_total` | counter | `source` | пропускная способность и доля валидных |
| `harvester_dlq_total` | counter | `source`, `reason` | доля DLQ по причинам |
| `harvester_http_requests_total` | counter | `source`, `code` | 429, 5xx, 304 |
| `harvester_budget_remaining` | gauge | `source` | дневной бюджет OpenAlex |
| `harvester_cache_hits_total` | counter | `source` | работа кэша |
| `harvester_produce_latency_seconds` | histogram | `topic` | задержка подтверждения Kafka |
| `harvester_last_success_timestamp` | gauge | `source`, `scope` | алерт «нет данных» |
| `harvester_registry_projects` | gauge | `status` | снимок реестра жив |

### 5.6 loadgen-go

- Управление по HTTP: `POST /api/v1/stress-test?rate=25000&duration=60s&profile=steady|burst`, `GET /api/v1/status`, `DELETE /api/v1/stress-test`.
- Воркеры-горутины с пейсером на token bucket; размер батча и linger продюсера настраиваются.
- По умолчанию 30–50 событий в секунду, нагрузка только по команде; метрики `loadgen_offered_rate`, `loadgen_achieved_rate`, ошибки продюсера.
- Пишет только в `load.synthetic` с `synthetic=true`; `produced_at` в микросекундах нужен для замера e2e-задержки в ClickHouse.

### 5.7 common-go

| Пакет | Что даёт |
| --- | --- |
| `kafka` | обёртка franz-go: идемпотентный продюсер `acks=all`; консьюмер с ручным коммитом после обработки |
| `avro` | формат Schema Registry (магический байт + 4 байта ID схемы), регистрация и кэш ID, тест round-trip |
| `config` | чтение и проверка переменных окружения |
| `obs` | slog в JSON, реестр Prometheus, сервер `/healthz` и `/metrics` |

## 6. Lakehouse: bronze, silver и gold

Bronze хранит байты из Kafka без изменений, silver — одну строку на сущность после декодирования, дедупликации и связывания. Gold живёт не в Iceberg, а в ClickHouse: Spark считает витрины из silver и пишет их по JDBC.

&#91;embedded content: medallion · слои bronze, silver, gold, ops и джобы между ними\]

Bronze только дописывается, silver обновляется `MERGE INTO` по ключу, gold в ClickHouse перезаписывается целыми месяцами (способ — решение Р-5 в §11.3).

### 6.1 Каталог и хранилище

Каталог Iceberg — JDBC на Postgres (база `iceberg_catalog`), данные — в MinIO, доступ через S3A, как и чекпойнты Structured Streaming. Имена пространств: `bronze`, `silver`, `ops`.

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=jdbc
spark.sql.catalog.lake.uri=jdbc:postgresql://postgres:5432/iceberg_catalog
spark.sql.catalog.lake.warehouse=s3a://warehouse/
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
# ключи MinIO и пароль каталога — из .env
```

### 6.2 Таблицы

| Таблица | Ключ | Партиции | Пишет | Читает | Спринт |
| --- | --- | --- | --- | --- | --- |
| `bronze.raw_events` | `(topic, partition, offset)` | `ingest_date` | `bronze_ingest` | `bronze_to_silver`, `reconcile` | S1-6 |
| `silver.papers` | `paper_key` | `months(published_at)` | `bronze_to_silver`, `backfill_openalex`, `enrich_projects` | `link_entities`, gold-джобы, DQ | S1-7, S2-6, S4-6 |
| `silver.repos` | `(repo_full_name, snapshot_date)` | `months(snapshot_date)` | `bronze_to_silver` | `silver_to_gold_impact` | S4-2 |
| `silver.releases` | `(repo_full_name, tag)` | — | `bronze_to_silver` | `silver_to_gold_impact` | S4-2 |
| `silver.code_events` | `event_id` | `days(created_at)` | `bronze_to_silver` | `silver_to_gold_impact` | S4-3 |
| `silver.software_records` | `record_doi` | — | `bronze_to_silver` | `link_entities` (R2) | S4-4 |
| `silver.paper_repo_links` | `(paper_key, repo_full_name, rule)` | — | `link_entities` | `silver_to_gold_impact` | S4-5 |
| `silver.paper_repo_links_review` | тот же | — | `link_entities` | ручная проверка | S4-5 |
| `ops.dq_results` | `(run_id, expectation)` | `days(ts)` | `dq_run` | Grafana через копию в ClickHouse | S3-2 |
| `ops.reconciliation` | `(run_id, topic, hour)` | — | `reconcile` | Grafana через копию в ClickHouse | S3-5 |
| `ops.job_watermarks` | `(job, source_table)` | — | все инкрементальные джобы | те же | S1-7, новое |

### 6.3 DDL ключевых таблиц

```sql
CREATE TABLE lake.bronze.raw_events (
  topic       string,
  partition   int,
  offset      bigint,
  kafka_ts    timestamp,
  key         binary,
  value       binary,        -- заголовок Schema Registry (5 байт) + Avro, как пришло
  ingest_date date
) USING iceberg
PARTITIONED BY (ingest_date);

CREATE TABLE lake.silver.papers (
  paper_key      string,     -- coalesce(doi, arxiv_id, source || ':' || source_id)
  doi            string,     -- нижний регистр, без https://doi.org/
  arxiv_id       string,     -- без версии: 2401.01234
  sources        array<string>,        -- новое: из каких источников склеена строка
  source_ids     map<string, string>,  -- новое: openalex → W…, inspire → control_number
  title          string,
  abstract       string,
  comments       string,
  journal_ref    string,
  authors        array<struct<name: string, orcid: string, affiliations: array<string>>>,
  topics         array<string>,
  topic_l1       string,
  topic_l2       string,
  published_at   timestamp,
  updated_at     timestamp,
  cited_by_count int,
  links          array<struct<url: string, kind: string>>,
  ingested_at    timestamp,
  project_slugs  array<string>,        -- S2-6, ALTER TABLE … ADD COLUMNS
  repo_links     array<string>         -- S2-6
) USING iceberg
PARTITIONED BY (months(published_at));

CREATE TABLE lake.silver.paper_repo_links (
  paper_key      string,
  repo_full_name string,
  rule           string,     -- R1, R2, R3
  confidence     double,
  evidence       string,     -- фрагмент текста или related identifier
  linked_at      timestamp
) USING iceberg;
```

| Таблица | Колонки |
| --- | --- |
| `silver.repos` | `repo_full_name`, `project_slug`, `stars`, `forks`, `open_issues`, `archived`, `pushed_at`, `snapshot_date` |
| `silver.releases` | `repo_full_name`, `tag`, `name`, `published_at`, `prerelease`, `fetched_at` |
| `silver.code_events` | `event_id`, `type`, `repo_full_name`, `actor_login`, `created_at`, `replayed_at` |
| `silver.software_records` | `record_doi`, `concept_doi`, `version`, `title`, `published_at`, `related_identifiers` (relation, scheme, identifier), `repo_full_name` |
| `silver.paper_repo_links_review` | колонки `paper_repo_links` + `review_status` |
| `ops.dq_results` | `run_id`, `suite`, `expectation`, `success`, `observed`, `ts` |
| `ops.reconciliation` | `run_id`, `window_start`, `window_end`, `topic`, `hour`, `bronze_count`, `ch_count`, `diff`, `within_tolerance` |
| `ops.job_watermarks` | `job`, `source_table`, `snapshot_id`, `updated_at` |

### 6.4 Spark-джобы

| Джоб | Вход | Выход | Идемпотентность | Спринт |
| --- | --- | --- | --- | --- |
| `bronze_ingest.py` | Kafka `raw\..*` | `bronze.raw_events` | чекпойнт Structured Streaming; `availableNow` для батча, непрерывно по флагу | S1-6 |
| `bronze_to_silver.py` | новые снапшоты bronze | `silver.papers`; в S4 — таблицы кода | инкрементальное чтение по `snapshot_id` из `ops.job_watermarks`; `MERGE INTO`, побеждает максимальный `updated_at` | S1-7 |
| `enrich_projects.py` | `silver.papers`, CH `v_projects_current` | `project_slugs`, `repo_links` | `MERGE` | S2-6 |
| `link_entities.py` | `silver.papers`, `silver.software_records`, реплика реестра в CH | `paper_repo_links`, `_review` | `MERGE` по `(paper_key, repo_full_name, rule)` | S4-5 |
| `backfill_openalex.py` | CC0-снапшот, JSON Lines gz | `silver.papers` | `MERGE`; запись API с более поздним `updated_at` побеждает | S4-6 |
| `silver_to_gold_trends.py` | `silver.papers` | CH `dm_research_trends` | `DROP PARTITION` пересчитанных месяцев, затем `INSERT` | S1-8 |
| `silver_to_gold_impact.py` | `silver.papers`, связи, репозитории, релизы, события | CH `dm_hep_software_impact` | то же | S2-7, S4-7 |
| `reconcile.py` | `bronze.raw_events`, CH `live_papers_1m` | `ops.reconciliation`, CH `ops_reconciliation` | перезапись окна | S3-5 |
| `maintenance.py` | все таблицы Iceberg | — | `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files` | S2-6, S3 |

### 6.5 Правила silver

- **Нормализация.** DOI в нижний регистр без префикса; `arxiv_id` без версии (`2401.01234v2` → `2401.01234`); темы → `topic_l1` и `topic_l2` по `reference/topics_map.csv`.
- **Ключ.** DOI вида `10.48550/arXiv.<id>` сначала превращается в `arxiv_id` (S4-5). `MERGE` ищет существующую строку и по `doi`, и по `arxiv_id`, поэтому препринт, получивший журнальный DOI, переезжает на новый ключ, а не дублируется.
- **Склейка источников.** `sources` — объединение; заголовок и аннотация — из записи с максимальным `updated_at`; `cited_by_count` — максимум по источникам; авторы — из источника с ORCID, обычно OpenAlex; `comments` и `journal_ref` — из arXiv.
- **Дубли внутри пачки** снимает оконная функция по ключу с максимальным `updated_at`.

### 6.6 Обслуживание и time travel

- Раз в неделю: `rewrite_data_files` (сжатие мелких файлов от стриминга) и `expire_snapshots` старше 14 дней; раз в месяц — `remove_orphan_files`.
- Снапшоты, на которых будут обучаться модели части 2, помечаются тегом: `ALTER TABLE lake.silver.papers CREATE TAG train_2027_02 AS OF VERSION <id> RETAIN 365 DAYS`. `expire_snapshots` теги не трогает — это и есть передача H-3.
- Time travel для демо S2-6: `SELECT count(*) FROM lake.silver.papers VERSION AS OF <snapshot_id>` возвращает строки до `ALTER TABLE`.

## 7. ClickHouse: схема таблиц и витрины

В ClickHouse три слоя объектов: очереди на Kafka engine (только транзит), целевые таблицы, куда их переносят Materialized Views, и витрины с представлениями для Grafana. Читать очереди напрямую нельзя: `SELECT` из Kafka engine сдвигает смещение и крадёт сообщения у MV.

&#91;embedded content: объекты ClickHouse · 5 очередей, 8 таблиц, 4 группы представлений\]

Grafana читает только правую колонку и целевые таблицы; реплика реестра подмешивается к витринам при чтении, поэтому переименование проекта не требует пересчёта.

### 7.1 Объекты

| Объект | Движок | Откуда данные | Ключ сортировки | Спринт |
| --- | --- | --- | --- | --- |
| `registry_*_queue` (4 шт.) | Kafka | `cdc.registry.public.*` | — | S2-4 |
| `registry_projects`, `registry_repos`, `registry_institutions`, `registry_project_institutions` | ReplacingMergeTree(version) | MV из очередей | `id` или `(project_id, institution_id)` | S2-4 |
| `v_projects_current`, `v_repos_current`, `v_institutions_current`, `v_project_institutions_current` | View | `argMax` по `version`, без удалённых | — | S2-4 |
| `papers_queue` → `live_papers_1m` | Kafka → AggregatingMergeTree | `raw.papers.*` | `(minute, topic, source)` | S2-7 |
| `code_events_queue` → `live_code_1m` | Kafka → AggregatingMergeTree | `raw.code.events` | `(minute, repo_full_name, type)` | S4-7 |
| `dm_live_activity` | View | `live_papers_1m` + `live_code_1m` | — | S2-7, S4-7 |
| `dm_research_trends` | MergeTree | Spark `silver_to_gold_trends` | `(month, topic_l1, topic_l2)` | S1-8 |
| `dm_hep_software_impact` → `v_hep_software_impact` | MergeTree → View | Spark `silver_to_gold_impact` + реплика реестра | `(project_slug, month)` | S2-7, S4-7 |
| `dlq_queue` → `ops_dlq_events` | Kafka → MergeTree | `dlq.*` | `(ts, reason)` | S3-6 |
| `ops_pipeline_runs`, `ops_dq_results`, `ops_reconciliation` | MergeTree | Airflow, `dq_run`, `reconcile` | `(dag_id, task_id, started_at)` и т. п. | S3 |
| `dm_pipeline_health` | набор View | ops-таблицы + `live_papers_1m` | — | S3-6 |
| `synthetic_queue` → `bench_events` | Kafka → MergeTree, TTL 1 день | `load.synthetic` | `produced_at` | S5 |

### 7.2 Реплика реестра

У каждой очереди своя consumer group (`ch_registry_projects`, `ch_registry_repos`…): если несколько Kafka-таблиц с разными топиками делят одну группу, они мешают друг другу ребалансировками.

```sql
CREATE TABLE registry_projects_queue (
  id UInt64, slug String, name String, description Nullable(String),
  keywords Array(String), topics Array(String), priority UInt8, status String,
  __op String, __source_ts_ms UInt64, __deleted String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'cdc.registry.public.tracked_projects',
         kafka_group_name  = 'ch_registry_projects',
         kafka_format      = 'AvroConfluent',           -- или JSONEachRow по ADR-004
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE registry_projects (
  id UInt64, slug String, name String, description Nullable(String),
  keywords Array(String), topics Array(String), priority UInt8,
  status LowCardinality(String), version UInt64, is_deleted UInt8
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

CREATE MATERIALIZED VIEW mv_registry_projects TO registry_projects AS
SELECT id, slug, name, description, keywords, topics, priority, status,
       __source_ts_ms AS version, __deleted = 'true' AS is_deleted
FROM registry_projects_queue;

CREATE VIEW v_projects_current AS
SELECT id,
       argMax(slug, version)     AS slug,
       argMax(name, version)     AS name,
       argMax(keywords, version) AS keywords,
       argMax(topics, version)   AS topics,
       argMax(priority, version) AS priority,
       argMax(status, version)   AS status
FROM registry_projects
GROUP BY id
HAVING argMax(is_deleted, version) = 0;
```

Остальные три таблицы реестра устроены так же. Правило из S2-4: реплика читается только через `v_*_current`, потому что `ReplacingMergeTree` схлопывает дубли лишь при фоновом слиянии.

### 7.3 Быстрый путь

Минута считается по времени сообщения Kafka (`_timestamp`), а не по `ingested_at`: bronze хранит тот же `kafka_ts`, и reconciliation сравнивает одно и то же время.

```sql
CREATE TABLE papers_queue (
  source String, source_id String, doi Nullable(String), title String,
  published_at DateTime64(3), ingested_at DateTime64(3)
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list  = 'raw.papers.openalex,raw.papers.arxiv,raw.papers.inspire',
         kafka_group_name  = 'ch_live_papers',
         kafka_format      = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE TABLE live_papers_1m (
  minute        DateTime,
  topic         LowCardinality(String),
  source        LowCardinality(String),
  events        AggregateFunction(count),
  last_ingested SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, topic, source)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW mv_live_papers TO live_papers_1m AS
SELECT toStartOfMinute(_timestamp) AS minute, _topic AS topic, source,
       countState() AS events, max(ingested_at) AS last_ingested
FROM papers_queue
GROUP BY minute, topic, source;

CREATE VIEW dm_live_activity AS
SELECT minute, 'paper' AS kind, source AS label, countMerge(events) AS events
FROM live_papers_1m GROUP BY minute, label
UNION ALL
SELECT minute, 'code' AS kind, concat(repo_full_name, ' · ', type) AS label, countMerge(events)
FROM live_code_1m
WHERE repo_full_name IN (SELECT github_full_name FROM v_repos_current)
GROUP BY minute, label;
```

`live_code_1m` устроена как `live_papers_1m` с ключом `(minute, repo_full_name, type)`. Без GH Archive в `dm_live_activity` остаются только строки `paper`.

### 7.4 Витрины, которые пишет Spark

```sql
CREATE TABLE dm_research_trends (
  month            Date,
  topic_l1         LowCardinality(String),
  topic_l2         LowCardinality(String),
  papers           UInt32,
  avg_citations    Float32,
  papers_with_code UInt32,      -- v1, после связывания в S4
  updated_at       DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (month, topic_l1, topic_l2);

CREATE TABLE dm_hep_software_impact (
  project_slug     LowCardinality(String),
  month            Date,
  linked_papers    UInt32,      -- связи с confidence ≥ 0,7
  cited_total      UInt64,
  releases         UInt16,
  activity_events  UInt32,
  top_institutions Array(String),
  updated_at       DateTime
) ENGINE = MergeTree
PARTITION BY toYYYYMM(month)
ORDER BY (project_slug, month);

CREATE VIEW v_hep_software_impact AS
SELECT i.*, p.name AS project_name, p.priority, p.status
FROM dm_hep_software_impact AS i
LEFT JOIN v_projects_current AS p ON p.slug = i.project_slug;
```

Атрибуты проекта подтягиваются при чтении, поэтому переименование проекта в реестре видно на панели без пересчёта (S2-7). Для идемпотентности Spark сначала делает `ALTER TABLE … DROP PARTITION` для пересчитанных месяцев, затем `INSERT`.

### 7.5 Операционные таблицы

```sql
CREATE TABLE ops_pipeline_runs (
  run_id String, dag_id LowCardinality(String), task_id LowCardinality(String),
  started_at DateTime, finished_at DateTime, duration_s Float32,
  status LowCardinality(String), rows_out UInt64
) ENGINE = MergeTree ORDER BY (dag_id, task_id, started_at);

CREATE TABLE ops_dlq_events (
  ts DateTime64(3), topic LowCardinality(String), source LowCardinality(String),
  reason LowCardinality(String), stage LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts) ORDER BY (ts, reason)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE VIEW v_mart_freshness AS
SELECT 'dm_research_trends' AS mart, max(updated_at) AS fresh_at FROM dm_research_trends
UNION ALL SELECT 'dm_hep_software_impact', max(updated_at) FROM dm_hep_software_impact
UNION ALL SELECT 'dm_live_activity', toDateTime(max(last_ingested)) FROM live_papers_1m;
```

`dm_pipeline_health` — набор представлений: пропускная способность по минутам, доля DLQ за час, длительность джоб, `v_mart_freshness`, последний статус DQ и reconciliation. Consumer lag в ClickHouse не хранится: его даёт kafka-exporter в Prometheus, и дашборд собирает оба источника.

### 7.6 Пользователи

| Пользователь | Права | Кто |
| --- | --- | --- |
| `grafana_ro` | `readonly=1`, `SELECT` на `dm_*`, `v_*`, `ops_*` | Grafana |
| `spark_writer` | `INSERT` и `ALTER … DROP PARTITION` на `dm_*` и `ops_*`; `SELECT` на `v_*` | Spark-джобы, `dq_run`, Airflow |
| `default` | только DDL из `platform/clickhouse/*.sql` | `make up` |

## 8. Airflow, качество данных и сверка

Airflow управляет только пакетным путём. Быстрый путь (Kafka → ClickHouse) и CDC работают без него, поэтому остановка Airflow не останавливает живые панели. В профиле `core` Airflow нет, и те же шаги в том же порядке выполняет `make demo-slice`.

Новое в этом ТЗ. Команды задач лежат в одном файле `platform/etl/steps.sh` (функции `step_harvest`, `step_bronze_ingest` и т. д.). DAG и `make demo-slice` вызывают одни и те же функции, чтобы два способа запуска не разошлись.

### 8.1 Как Airflow запускает работу

- **Образ.** `scipulse/airflow:<версия>` на базе официального образа Airflow 3.x с фиксированной версией. Внутри Java 17, PySpark и Iceberg runtime тех же версий, что в контейнере `spark`, бинарник harvester и `clickhouse-client`.
- **Компоненты.** В Airflow 3 это `api-server` (UI и API), `scheduler` и `dag-processor`. С LocalExecutor задачи выполняются внутри контейнера `scheduler`, поэтому у него самый большой лимит памяти (§9.4).
- **Оператор.** Все задачи — `BashOperator` из пакета `apache-airflow-providers-standard`. Spark работает в режиме `local[*]` с `spark.driver.memory=2g`. Docker socket не монтируется (это root на хосте), а `SparkSubmitOperator` к кластеру не нужен, потому что в `core` кластера нет. Выбор записывается в ADR в задаче S3-3.
- **Параллельность.** `parallelism=4`. Пул `spark` с одним слотом: две Spark-задачи никогда не идут одновременно, ни по памяти в 16 ГБ, ни по конфликтам коммитов Iceberg (`MERGE` и обслуживание одной таблицы).
- **Метаданные.** База `airflow` в общем Postgres. Подключения задаются переменными окружения `AIRFLOW_CONN_*` из `.env`, в UI ничего не настраивается.
- **Учёт запусков.** Колбэки `on_success_callback` и `on_failure_callback` каждой задачи пишут строку в ClickHouse `ops_pipeline_runs` (длительность, статус, `rows_out` из XCom).

### 8.2 DAG `scipulse_etl`

| Параметр | Значение |
| --- | --- |
| Расписание | `0 3 * * *`, раз в сутки в 03:00 UTC |
| `catchup` | `False`; прошлые интервалы — только через `airflow backfill create` |
| `max_active_runs` | 1 |
| Ретраи по умолчанию | 2, задержка 5 мин с экспоненциальным ростом |
| `trigger_rule` по умолчанию | `all_success` |
| Таймаут всего запуска | `dagrun_timeout` 3 ч |

&#91;embedded content: граф scipulse\_etl · 11 задач, DQ-гейт, пул spark\]

Из 11 задач вне пула `spark` только `check_sources` и `harvest`; остальные стоят в очереди на один слот, поэтому параллельные ветки на схеме при выполнении идут по очереди.

| Задача | Команда | Таймаут | Ретраи | Пул | Спринт |
| --- | --- | --- | --- | --- | --- |
| `check_sources` | Python: один дешёвый запрос к каждому API, проверка Kafka и Schema Registry | 5 мин | 2 | — | S3-3 |
| `harvest` | `harvester --once --until {{ data_interval_end }} --sources {{ ti.xcom_pull(task_ids='check_sources') \| join(',') }}` | 45 мин | 2 | — | S3-3 |
| `bronze_ingest` | `spark-submit bronze_ingest.py --trigger availableNow` | 30 мин | 2 | `spark` | S3-3 |
| `bronze_to_silver` | `spark-submit bronze_to_silver.py` | 30 мин | 2 | `spark` | S3-3 |
| `enrich_projects` | `spark-submit enrich_projects.py` | 15 мин | 2 | `spark` | S2-6 |
| `link_entities` | `spark-submit link_entities.py` | 20 мин | 2 | `spark` | S4-5 |
| `dq_silver` | `dq_run.py --suites silver_papers,silver_links,silver_code --fail-on error` | 20 мин | **0** | `spark` | S3-2 |
| `gold_trends` | `spark-submit silver_to_gold_trends.py` | 20 мин | 2 | `spark` | S3-3 |
| `gold_impact` | `spark-submit silver_to_gold_impact.py` | 20 мин | 2 | `spark` | S4-7 |
| `dq_gold` | `dq_run.py --suites gold_post --fail-on never` | 10 мин | 1 | `spark` | S3-2 |
| `reconcile` | `spark-submit reconcile.py --window {{ data_interval_start }}..{{ data_interval_end }}` | 15 мин | 1 | `spark` | S3-5 |

Правила графа:

1. **Проверка источников не роняет запуск из-за одного API.** `check_sources` кладёт в XCom список доступных источников, недоступный пропускается с предупреждением в `ops_pipeline_runs`. Задача падает, только если недоступны Kafka, Schema Registry или все источники сразу.
2. **`harvest` — барьер, а не второй сборщик.** Демон harvester работает и в `full`. Задача `harvest` догоняет курсор каждого источника до `data_interval_end`, если демон ещё не дошёл, и сразу завершается, если дошёл. Так `bronze_ingest` стартует, когда интервал точно собран. Чтобы демон и задача не собирали одну пачку одновременно, оба берут `pg_try_advisory_lock` на источник в базе `harvester` перед каждой пачкой и читают курсор уже под локом. Новое в этом ТЗ: флаги `--sources` и `--until`, advisory lock.
3. **DQ-гейт без ретраев.** Нарушенная проверка — не временный сбой, повтор её не исправит. При провале `dq_silver` задачи `gold_trends` и `gold_impact` получают статус `upstream_failed`, витрины остаются вчерашними.
4. **Сверка — отдельная ветка.** `reconcile` зависит только от `bronze_ingest`. В плейбуке она стоит последней; здесь её вынесли, чтобы провал DQ не скрывал расхождение быстрого пути и наоборот.
5. **Идемпотентность по `data_interval`.** Повтор того же интервала безопасен: дубли в Kafka и bronze схлопывает `MERGE` в silver, gold перезаписывается по месяцам (`DROP PARTITION` + `INSERT`), сверка перезаписывает своё окно.

Проверка идемпотентности для S3-3: снять контрольную сумму витрины, дважды выполнить `airflow backfill create --dag-id scipulse_etl --from-date 2026-12-10 --to-date 2026-12-12 --reprocess-behavior completed` и сравнить сумму после каждого прогона:

```sql
SELECT count(), groupBitXor(cityHash64(month, topic_l1, topic_l2, papers, avg_citations, papers_with_code))
FROM dm_research_trends;
```

### 8.3 DQ-гейты

Инструмент по ADR-005 — Great Expectations с движком Spark. Наборы и пороги лежат в `platform/dq/`, порог изменения числа строк — в `platform/dq/thresholds.yaml`, а не в коде. Уникальность и ссылочная целостность проверяются по всей таблице, остальное — по строкам, изменённым в этом запуске (по `snapshot_id` из `ops.job_watermarks`).

| Набор | Проверки | Уровень | Спринт |
| --- | --- | --- | --- |
| `silver_papers` | `source_id`, `title`, `published_at` не NULL; `paper_key` уникален; `published_at` от 1990-01-01 до завтра; DOI по шаблону `^10\.\d{4,9}/\S+$`; `cited_by_count >= 0`; число строк за запуск отличается от прошлого не больше порога | error | S3-2 |
| `silver_links` | `confidence` в \[0, 1\]; `rule` из списка правил; каждый `paper_key` есть в `silver.papers`; `repo_full_name` по шаблону `owner/name` | error | S4-5 |
| `silver_code` | `full_name` репозитория уникален; у релиза есть `tag`; дата релиза не в будущем | error | S4-2 |
| `gold_post` | за последний закрытый месяц `papers > 0`; сумма `papers` за месяц равна числу работ в silver за тот же месяц | warn | S3-2 |

Уровень `error` роняет задачу и блокирует gold. Уровень `warn` только записывается и поднимает alert в Grafana: к моменту `dq_gold` витрина уже записана.

Каждый прогон пишет результаты в Iceberg `ops.dq_results` и копию в ClickHouse:

```sql
CREATE TABLE ops_dq_results (
  run_id String, dag_run_id String, suite LowCardinality(String),
  expectation String, severity LowCardinality(String),
  success UInt8, observed String, ts DateTime
) ENGINE = MergeTree ORDER BY (suite, ts);
```

Фикстуры для тестов: чистая и испорченная (дубликат ключа, дата в будущем, NULL в заголовке). На испорченной гейт падает и перечисляет все три нарушения.

### 8.4 Сверка быстрого и пакетного путей

1. **Окно.** Закрытые часы от начала `data_interval` до `max(kafka_ts)` в bronze минус 15 минут, с округлением вниз до часа. Последние минуты исключаются, потому что ClickHouse и bronze дочитывают их в разное время.
2. **Что сравнивается.** По каждой паре (топик, час): `count(*)` в `bronze.raw_events` по `kafka_ts` против `countMerge(events)` в `live_papers_1m` по `minute`. Обе стороны считают сообщения, а не уникальные работы, поэтому дубли at-least-once на результат не влияют, пока обе стороны их видят.
3. **Знак расхождения.** `ch < bronze` — потеря в быстром пути. `ch > bronze` — ClickHouse вставил блок дважды после рестарта (вставка прошла, коммит смещения — нет); Spark-sink в Iceberg такого не делает благодаря чекпойнту. Оба случая поднимают alert.
4. **Допуск.** `reconcile.tolerance_pct` в конфиге, для закрытого окна по умолчанию 0.
5. **Результат.** Iceberg `ops.reconciliation` и ClickHouse `ops_reconciliation`.
6. **Тест S3-5.** `ALTER TABLE live_papers_1m DELETE WHERE minute IN (…5 минут…)` — сверка находит отрицательное расхождение в этом часе, Grafana поднимает alert.

```sql
CREATE TABLE ops_reconciliation (
  run_id String, topic LowCardinality(String), hour DateTime,
  bronze_cnt UInt64, ch_cnt UInt64, diff Int64,
  status LowCardinality(String),        -- ok | loss | duplicate
  checked_at DateTime
) ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (topic, hour);
```

Новое в этом ТЗ: режим `reconcile.py --repair --hour <час>`. Он удаляет агрегаты этого часа из `live_papers_1m` и пересчитывает их из bronze по JDBC. Так расхождение исправляется без сброса смещений consumer group, который дал бы двойной счёт.

### 8.5 Остальные DAG

| DAG | Расписание | Задачи | Спринт |
| --- | --- | --- | --- |
| `iceberg_maintenance` | `0 4 * * 0`, раз в неделю | `rewrite_data_files` (цель 128 МБ) → `expire_snapshots` (старше 7 дней, не меньше 20 последних, теги не трогаются) → `remove_orphan_files` (старше 3 дней); пул `spark` не даёт пересечься с `scipulse_etl` | S3 |
| `backfill_openalex` | вручную, параметры: путь к снапшоту, фильтр | `backfill_openalex` → `dq_silver` → `gold_trends --months all` | S4-6 |
| `dlq_replay` | вручную, параметры: `reason`, `since`, `dry_run` | `harvester replay-dlq …`; основной путь остаётся CLI, DAG — удобная кнопка | S3-4 |

Should в S3-7: провайдер OpenLineage для Airflow и `OpenLineageSparkListener` для Spark, транспорт HTTP в `marquez:5000`. Один запуск DAG рисует граф raw → bronze → silver → gold.

### 8.6 Runbook

Три сценария для `docs/runbook.md` из DoD Sprint 3:

1. **Провал DQ.** Панель «DQ» на дашборде красная. Найти нарушения: `SELECT * FROM ops_dq_results WHERE success = 0 AND ts > now() - INTERVAL 1 DAY`. Исправить маппинг или правило, затем `airflow tasks clear scipulse_etl -t dq_silver -s <дата> --downstream`.
2. **Replay из DLQ.** Найти причину в `ops_dlq_events` по `reason`. Добавить именованное правило в `internal/replay/` с тестом. Выполнить `harvester replay-dlq --reason <код> --since <время> --dry-run`, проверить вывод, повторить без `--dry-run`. Следующий запуск DAG доведёт записи до silver.
3. **Расхождение сверки.** Посмотреть `system.kafka_consumers` (исключения, отставание) и lag группы `ch_live_papers` в Prometheus. Устранить причину, затем `reconcile.py --repair --hour <час>` и повторная сверка окна.

## 9. Наблюдаемость и инфраструктура

Наблюдаемость стоит на двух источниках: Prometheus собирает метрики процессов (lag, скорости, ошибки), а ClickHouse хранит результаты джоб (`ops_*`). Grafana читает оба и держит все правила оповещений в одном месте.

### 9.1 Метрики

| Компонент | Откуда берёт Prometheus | Ключевые метрики |
| --- | --- | --- |
| harvester-go | `/metrics` на :8082 | `harvester_fetched_total{source}`, `harvester_valid_total{source}`, `harvester_dlq_total{source,reason}`, `harvester_http_requests_total{source,code}`, `harvester_budget_remaining{source}`, `harvester_last_success_timestamp_seconds{source}` |
| registry-go | `/metrics` на :8080 | `http_requests_total{route,code}`, `http_request_duration_seconds`, статистика пула pgx |
| loadgen-go | `/metrics` на :8084 | `loadgen_offered_rate`, `loadgen_achieved_rate`, `loadgen_produce_errors_total` |
| Kafka | kafka-exporter :9308 | `kafka_consumergroup_lag` по группам из §2, `kafka_topic_partition_current_offset` |
| ClickHouse | встроенный endpoint :9363 | вставленные строки, число частей, память; ошибки Kafka engine — из `system.kafka_consumers` запросом из Grafana |
| Debezium Connect | JMX-экспортёр (javaagent) :9404 | `Connected`, `MilliSecondsBehindSource`, число событий |
| Spark | PrometheusServlet на UI драйвера :4040, только пока джоб идёт | нужна для замеров S5-3; длительность и число строк каждого запуска надёжно живут в `ops_pipeline_runs` |

Джоб Spark живёт минуты, и Prometheus может не успеть его опросить. Поэтому панели про Spark строятся по `ops_pipeline_runs`, а не по метрикам драйвера.

### 9.2 Оповещения

Все правила — Grafana-managed alerts, лежат в `platform/grafana/provisioning/alerting/` и загружаются при старте. По умолчанию статус виден только в UI; контакт-пойнт (например Telegram-бот) включается переменными из `.env`. Пороги ниже — стартовые, после замеров S5-3 их заменяют измеренными.

| Оповещение | Условие | Длится | Источник | Спринт |
| --- | --- | --- | --- | --- |
| `HarvesterDown` | `up{job="harvester"} == 0` | 5 мин | Prometheus | S3-6 |
| `SourceStale` | `time() - harvester_last_success_timestamp_seconds > 7200` | 10 мин | Prometheus | S3-6 |
| `NoNewPapers` | сумма `increase(kafka_topic_partition_current_offset{topic=~"raw\\.papers\\..*"}[6h])` равна 0 | — | Prometheus | S3-6 |
| `ConsumerLagHigh` | `kafka_consumergroup_lag` по группе `ch_*` > 10 000 | 10 мин | Prometheus | S3-6 |
| `DlqRateHigh` | доля `harvester_dlq_total` от `harvester_fetched_total` за 15 мин > 5 % | 15 мин | Prometheus | S3-6 |
| `CdcDisconnected` | JMX-метрика `Connected` коннектора `registry` равна 0 | 5 мин | Prometheus | S3-6 |
| `MartStale` | в `v_mart_freshness`: витрины Spark старше 26 ч, `dm_live_activity` старше 15 мин | — | ClickHouse | S3-6 |
| `DqFailed` | в последнем прогоне есть строка `ops_dq_results` с `success = 0` | — | ClickHouse | S3-2 |
| `ReconMismatch` | за сутки в `ops_reconciliation` есть `status != 'ok'` | — | ClickHouse | S3-5 |
| `EtlRunFailed` | в `ops_pipeline_runs` задача последнего запуска со статусом `failed` | — | ClickHouse | S3-3 |

`SourceStale` ловит зависший источник даже тогда, когда новых работ нет по уважительной причине (у arXiv нет объявлений в выходные). Приёмка S3-6 «остановка harvester даёт alert» проверяется на `HarvesterDown`.

### 9.3 Дашборды

| Дашборд | Источники | Спринт |
| --- | --- | --- |
| Research trends | `dm_research_trends` | S1-8 |
| HEP software impact | `v_hep_software_impact` | S2-7, S4-7 |
| Live activity | `dm_live_activity`, `v_repos_current` | S2-7, S4-7 |
| Pipeline health | Prometheus + `dm_pipeline_health`, `ops_*` | S3-6 |
| Benchmark | `bench_events`, метрики loadgen и контейнеров | S5-3 |

JSON дашбордов — в `platform/grafana/dashboards/`, datasource Prometheus и ClickHouse (плагин ClickHouse, пользователь `grafana_ro`) задаются провижинингом. Ручных настроек в UI нет: `make nuke && make up` восстанавливает все панели.

### 9.4 Docker Compose: сервисы, порты, память

Все порты публикуются только на `127.0.0.1` (NFR-9), сеть одна — `scipulse`. Версии образов зафиксированы в `.env.example`. Лимиты памяти — предложение этого ТЗ; замер S0-4 их уточняет.

| Сервис | Профиль | Порт на хосте | `mem_limit` | Healthcheck |
| --- | --- | --- | --- | --- |
| `kafka` (KRaft, 1 узел) | core | 9094 (внешний listener) | 1 ГБ, heap 512 МБ | `kafka-broker-api-versions` |
| `schema-registry` | core | 8081 | 384 МБ | `GET /subjects` |
| `postgres` | core | 5432 | 512 МБ | `pg_isready` |
| `connect` (Debezium) | core | 8083 | 768 МБ | `GET /connectors` |
| `clickhouse` | core | 8123 (HTTP), 9000 (native) | 1,5 ГБ | `SELECT 1` |
| `minio` | core | 9100 (API), 9101 (консоль) | 384 МБ | `/minio/health/live` |
| `spark` (разовый, `run --rm`) | core | 4040 на время джоба | 2,5 ГБ | — |
| `registry` | core | 8080 | 64 МБ | `/healthz` |
| `harvester` | core | 8082 | 128 МБ | `/healthz` |
| `loadgen` | core | 8084 | 128 МБ | `/healthz` |
| `prometheus` | core | 9090 | 384 МБ | `/-/ready` |
| `grafana` | core | 3000 | 256 МБ | `/api/health` |
| `kafka-exporter` | core | нет (внутри 9308) | 64 МБ | — |
| `airflow-scheduler` | full | нет | 3,5 ГБ (включая Spark задач) | `airflow jobs check` |
| `airflow-api-server` | full | 8089 | 768 МБ | `/api/v2/monitor/health` |
| `airflow-dag-processor` | full | нет | 384 МБ | `airflow jobs check` |
| `marquez`, `marquez-web` (Should) | full | 5000, 3001 | 512 + 128 МБ | `/healthcheck` |

Итог по лимитам: `core` ≈ 8 ГБ с учётом разового `spark`, `full` ≈ 13,3 ГБ. В `full` джобы идут внутри `airflow-scheduler`, а не в контейнере `spark`, поэтому два пика не складываются. Сервисы Airflow объявлены с `profiles: [full, ml]`, чтобы профиль `ml` из части 2 мог их включить (ADR-110).

Почему нестандартные порты: MinIO и ClickHouse по умолчанию оба слушают 9000, Airflow и registry-go — 8080, Marquez-web и Grafana — 3000. Внутри сети сервисы остаются на своих портах (`minio:9000`), меняется только публикация на хост. Kafka объявляет два listener'а: `INTERNAL://kafka:9092` для контейнеров и `EXTERNAL://localhost:9094` для инструментов на хосте; контроллер KRaft — на 9093 внутри сети.

### 9.5 Порядок старта

`make up` = `docker compose --profile core up -d --wait`. Зависимости заданы через `depends_on: condition: service_healthy` и `service_completed_successfully` для разовых init-контейнеров:

1. `postgres` → `pg-init` создаёт базы `registry`, `harvester`, `iceberg_catalog`, `airflow` и роли.
2. `kafka` → `kafka-init` создаёт топики с партициями и retention из §2 (автосоздание топиков выключено).
3. `schema-registry` → `registry-init` (`scripts/init_registry.sh`) регистрирует схемы и ставит `BACKWARD`.
4. `registry` применяет миграции при старте → `connect` → `connect-init` регистрирует коннектор Debezium из `platform/debezium/registry.json`.
5. `minio` → `minio-init` создаёт бакет `warehouse`.
6. `clickhouse` → `ch-init` применяет `platform/clickhouse/*.sql` по номерам: сначала целевые таблицы, потом очереди, потом MV.
7. `harvester`, `loadgen`, `prometheus`, `grafana`, `kafka-exporter`.

Порядок в шаге 6 важен: MV, созданная раньше целевой таблицы, не создастся, а очередь без MV ничего не читает. Все init-скрипты идемпотентны (`IF NOT EXISTS`, проверка коннектора перед `POST`), поэтому повторный `make up` безопасен.

| Цель Makefile | Что делает |
| --- | --- |
| `make up` / `make up-full` | поднимает `core` или `core` + `full` и ждёт healthcheck |
| `make down` / `make nuke` | останавливает; `nuke` удаляет ещё и тома |
| `make demo-slice` | пакетный путь без Airflow через `platform/etl/steps.sh` |
| `make test` / `make e2e` | юнит-тесты; сквозной тест на кэше harvester `--offline` |
| `make bench` / `make chaos` | замеры S5-3 и сценарии C1–C5 |

## 10. План по сервисам

Раздел раскладывает задачи плейбука по компонентам: в каком спринте и в каком порядке строится каждый сервис и по какому результату его принимают. Оценки в часах и полные критерии приёмки остаются в плейбуке.

### 10.1 Карта «компонент × спринт»

&#91;embedded content: дорожки компонентов × спринты S0–S6, критический путь подсвечен\]

C2, C4, C5 — chaos-сценарии задачи S5-4, которые проверяют конкретный компонент (§11.2).

| Компонент | S0 · 2–8 ноя | S1 · 9–22 ноя | S2 · 23 ноя – 6 дек | S3 · 7–20 дек | S4 · 21 дек – 3 янв | S5 · 4–10 янв | S6 · 11–17 янв |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Платформа, compose, CI | S0-1…S0-4 | S1-9 (e2e в CI) | S2-8 | S3-1 | — | S5-4 (`make chaos`) | S6-4 |
| common-go и контракты | S0-6 (ADR-004) | S1-1 | — | S3-1 | S4-2, S4-3, S4-4 | S5-1 | — |
| harvester-go | S0-5 | S1-2…S1-5, S1-9 | S2-5 | S3-3, S3-4 | S4-1…S4-4 | S5-4 (C5) | — |
| registry-go + Debezium | — | — | S2-1…S2-3, S2-8 | — | — | S5-4 (C2) | — |
| Spark + Iceberg | S0-6 (ADR-003) | S1-6…S1-8 | S2-6, S2-7 | S3-5 | S4-5…S4-7 | S5-3, S5-4 (C4) | — |
| ClickHouse | S0-4 | S1-8 | S2-4, S2-7 | S3-5, S3-6 | S4-7 | S5-3…S5-5 (C3) | — |
| Airflow и DQ | — | — | — | S3-2…S3-4, S3-6, S3-7 | S4-5, S4-6 | — | — |
| Prometheus и Grafana | S0-4 | S1-8 | S2-7 | S3-5, S3-6 | S4-7 | S5-3 | — |
| loadgen-go | — | — | — | — | — | S5-1…S5-5 | — |
| Документация и релиз | S0-6, S0-8 | S1-9 | S2-8 | S3-8 | S4-8 | S5-2, S5-6 | S6-1…S6-3, S6-5, S6-7 |

### 10.2 Критический путь

S0-4 → S1-1 → S1-2 → S1-3 → S1-5 → S1-6 → S1-7 → S1-8 → S2-1 → S2-3 → S2-4 → S2-7 → S3-2 → S3-3 → S3-5 → S3-6 → S4-1 → S4-5 → S4-7 → S5-1 → S5-3 → S5-4 → S6-4 → S6-5 → S6-7.

Задержка любой задачи из этой цепочки сдвигает релиз. Всё остальное имеет запас: arXiv (S1-4), эволюция схемы (S2-6), lineage (S3-7), GH Archive и Zenodo (S4-3, S4-4), ноутбук (S4-8) и тюнинг (S5-5) можно перенести или срезать по правилам из §10.10. Самый нагруженный участок — S4: 38 ч при ёмкости 20–25 ч.

### 10.3 common-go

1. **S1-1.** Пакеты `avro` (клиент Schema Registry, сериализация в формате Confluent), `kafka` (продюсер franz-go с `acks=all` и идемпотентностью, консьюмер с коммитом после обработки), `config` (переменные окружения), `obs` (структурные логи, `/healthz`, `/metrics`). Готово, когда `paper.v1` зарегистрирован и тест «записал → прочитал» через реестр зелёный.
2. **S3-1.** Проверка совместимости всех `contracts/*.avsc` в CI. Готово, когда удаление обязательного поля красит CI, а новое nullable-поле с default проходит.
3. **S4-2…S4-4, S5-1.** Контракты `repo_snapshot.v1`, `release.v1`, `code_event.v1`, `software_record.v1`, `synthetic_event.v1` по правилам §3.

### 10.4 harvester-go

1. **S1-2 каркас.** `cmd/harvester`, пакеты из common-go, остановка по SIGTERM, режим `--once`. Готово: тест SIGTERM посреди пачки без потерь.
2. **S1-3 OpenAlex.** Курсорная пагинация, ключ API, дневной бюджет в `source_budget`, кэш ответов. Готово: работы в `raw.papers.openalex`, бюджет считается в метриках.
3. **S1-4 arXiv.** OAI-PMH `ListRecords` с `resumptionToken`, дата `from` в `harvest_cursors`. Первой переносится в S2, если спринт не укладывается.
4. **S1-5 конвейер.** map → validate → route, конверт `dlq_envelope.v1` с кодом причины и исходным payload. Готово: невалидная запись видна в `dlq.papers`.
5. **S1-9 `--offline`.** Чтение только из кэша, чтобы e2e-тест в CI работал без сети.
6. **S2-5 управление реестром.** Снимок из `cdc.registry.*` (группа `harvester-registry`), планировщик по `priority`, пропуск `paused`. Готово: новый проект попадает в сбор за минуту без перезапуска.
7. **S3-3 барьер для Airflow.** Флаги `--sources` и `--until`, advisory lock на источник (§8.2).
8. **S3-4 `replay-dlq`.** Именованные правила исправления, `--dry-run`. Готово: повторный replay не создаёт дублей в silver.
9. **S4-1…S4-4 новые источники.** INSPIRE-HEP; GitHub с `ETag` в `http_etags` (ответ 304 не тратит лимит); GH Archive и Zenodo как Should.
10. **S5-4 chaos C5.** Процесс убит посреди сбора: после рестарта пачка собирается снова, потерь нет.

### 10.5 registry-go и CDC

1. **S2-1 модель.** Миграции `000001` (таблицы, триггеры) и `000002_cdc` (`REPLICA IDENTITY FULL`, публикация `dbz_registry`), seed-набор проектов. Готово: миграции проходят вверх и вниз.
2. **S2-2 REST API.** Маршруты из §4, `X-API-Key` на запись, тесты на Testcontainers. Готово: интеграционные тесты зелёные в CI.
3. **S2-3 Debezium.** Коннектор из `platform/debezium/registry.json`, топики `cdc.registry.public.*` с compaction. Готово: INSERT, UPDATE и DELETE видны в топиках с `__op`.
4. **S2-8 e2e и замер.** Автотест «UPDATE и DELETE в Postgres отражаются в `v_projects_current`», `measure_cdc_latency.py`. Готово: свежесть p95 измерена (target < 2 с).
5. **S5-4 chaos C2.** Остановка Connect: после старта коннектор дочитывает слот репликации, правки не теряются.

### 10.6 Spark и Iceberg

1. **S1-6 `bronze_ingest`.** Способ записи выбран в ADR-003 по замеру памяти. Готово: сырые байты и смещения в `bronze.raw_events`, перезапуск с чекпойнта не даёт дублей.
2. **S1-7 `bronze_to_silver` v0.** `MERGE INTO`, одна строка на статью, водяной знак в `ops.job_watermarks`.
3. **S1-8 `silver_to_gold_trends` v0.** Запись в ClickHouse по JDBC (`DROP PARTITION` + `INSERT`).
4. **S2-6 эволюция и обогащение.** Новое поле в silver без перезаписи, time travel проверен, `enrich_projects` и черновик `maintenance`.
5. **S2-7 `silver_to_gold_impact` v0.**
6. **S3-5 `reconcile`.** Режимы проверки и `--repair` (§8.4). Готово: потеря пяти минут агрегатов ловится.
7. **S4-5 `link_entities`.** Правила R1–R3, порог 0,7, разметка около 100 пар. Готово: precision по каждому правилу записана. Эта задача не режется.
8. **S4-6 `backfill_openalex`.** Подмножество снапшота, `MERGE` с приоритетом более свежего `updated_at`, замеры в `docs/benchmarks/backfill-openalex.md`.
9. **S4-7 витрины v1.** Выборочная сверка пяти пар «проект-месяц» с ручным подсчётом.
10. **S5-3, S5-4.** Время `bronze_to_silver` на трёх объёмах; chaos C4 — джоб убит посредине, незавершённый коммит Iceberg не виден, повтор идемпотентен.

### 10.7 ClickHouse

1. **S0-4.** Контейнер, база `scipulse`, пользователи из §7.6, `ch-init`.
2. **S1-8.** `dm_research_trends` v0 и первая панель Grafana.
3. **S2-4.** Реплика реестра и `v_*_current`. Готово: удалённый проект исчезает из представления до фонового слияния.
4. **S2-7.** `papers_queue` → `live_papers_1m`, каркас `dm_live_activity`, `dm_hep_software_impact` v0 и `v_hep_software_impact`.
5. **S3-5, S3-6.** Таблицы `ops_*`, `dlq_queue` → `ops_dlq_events`, `v_mart_freshness`, `dm_pipeline_health`.
6. **S4-7.** `code_events_queue` → `live_code_1m`, `dm_live_activity` v1.
7. **S5-1…S5-5.** `synthetic_queue` → `bench_events` с задержкой через `now64()`; тюнинг `kafka_num_consumers` и `kafka_max_block_size`; chaos C3 — после рестарта Kafka engine дочитывает отставание, сверка показывает, были ли дубли.

### 10.8 Airflow и DQ

1. **S3-2.** Набор `silver_papers`, `dq_run.py`, `ops.dq_results`, две фикстуры.
2. **S3-3.** Образ, `scipulse_etl`, пул `spark`, тесты импорта DAG в CI, проверка бэкфилла контрольной суммой.
3. **S3-4, S3-6.** DAG `dlq_replay`, колбэки в `ops_pipeline_runs`, `iceberg_maintenance`.
4. **S3-7 (Should).** OpenLineage и Marquez.
5. **S4-2, S4-5, S4-6.** Задача `link_entities`, наборы `silver_links` и `silver_code`, DAG `backfill_openalex`.

### 10.9 loadgen-go

1. **S5-1.** HTTP-управление, пейсер на token bucket, `synthetic_event.v1` в `load.synthetic`, метрики `offered` и `achieved`. По умолчанию 30–50 событий в секунду.
2. **S5-2, S5-3.** Методика и `scripts/bench.py` по точкам от 1 до 25 тыс. событий в секунду.
3. **S5-5.** Одно изменение параметра и сравнение «до/после».

Изоляция синтетики проверяется в CI: в gold-витринах 0 строк с `synthetic=true` (NFR-8).

### 10.10 Что резать, если отстаём

| Спринт | Порядок среза | Не режется |
| --- | --- | --- |
| S0 | эксперимент для ADR-003 → в S1-6 | ADR-004 (нужен к S1-1) |
| S1 | S1-4 arXiv → S2; S1-8 до двух панелей | сквозной срез на OpenAlex |
| S2 | S2-6 → S3; `dm_live_activity` до одной панели | CDC до ClickHouse |
| S3 | S3-7 → Could; S3-6 до lag, доли DLQ и свежести | DQ-гейт и сверка |
| S4 | S4-3 → S4-4 → S4-8 в S6 → S4-6 до одного месяца | S4-5 связывание |
| S5 | S5-5 до одного замера; chaos до C1, C3, C4 | измеренные числа вместо target |
| S6 | видео до одного сценария, отчёт до двух страниц | README и проверка на чистой машине |

## 11. Тесты, chaos и открытые решения

### 11.1 Матрица тестов

| Уровень | Что проверяет | Инструмент | Где | Спринт |
| --- | --- | --- | --- | --- |
| Юнит Go | маппинг каждого источника на фикстурах, валидатор, коды DLQ, правила replay, планировщик | `go test`, golden-файлы | CI, каждый PR | S1+ |
| Юнит PySpark | bronze → silver, дедупликация, правила R1–R3 | `pytest` с локальной SparkSession | CI | S1-7 |
| Контракты | совместимость `BACKWARD` всех схем | Schema Registry в CI | CI | S3-1 |
| Интеграция Go | API реестра с Postgres; harvester → Kafka → Schema Registry | Testcontainers | CI | S2-2 |
| DAG | импорт, зависимости, отсутствие циклов, `airflow dags list-import-errors` | `pytest` | CI | S3-3 |
| DQ | чистая и испорченная фикстуры | Great Expectations | CI | S3-2 |
| e2e срез | кэш `--offline` → Kafka → bronze → silver → `dm_research_trends` | `make e2e` на урезанном compose | CI | S1-9 |
| e2e CDC | INSERT, UPDATE, DELETE → `v_projects_current` | `make e2e` | CI | S2-8 |
| Изоляция синтетики | 0 строк `synthetic=true` в gold | SQL-проверка | CI | S5-1 |
| Идемпотентность | двойной бэкфилл не меняет контрольную сумму витрины | скрипт + SQL | локально | S3-3 |
| Сверка | искусственная потеря пяти минут ловится и поднимает alert | `reconcile.py` | локально | S3-5 |
| Нагрузка и отказы | пропускная способность, задержка, C1–C5 | `make bench`, `make chaos` | локально, не в CI | S5 |

e2e в CI поднимает только Kafka, Schema Registry, Postgres, Debezium, MinIO, ClickHouse и контейнер Spark, без Grafana, Prometheus и Airflow, чтобы уложиться в память раннера.

### 11.2 Chaos: чего ждём от каждого компонента

Все сценарии идут под фоновой нагрузкой loadgen. Критерий общий: «отправлено» против «сохранено» после восстановления и время возврата к нормальной задержке.

| Сценарий | Что должно произойти | Как проверяем | Слабое место |
| --- | --- | --- | --- |
| C1 · брокер Kafka остановлен на 60 с | продюсеры держат буфер и повторяют; harvester не двигает курсор; Kafka engine и консьюмеры переподключаются | `loadgen` sent против строк в `bench_events`; `harvester_valid_total` против смещений топика | таймаут доставки продюсера должен быть больше сбоя (задать явно, например 2 мин), иначе события уходят в ошибки |
| C2 · Debezium Connect остановлен | Postgres копит WAL в слоте репликации; после старта коннектор продолжает с сохранённого смещения; harvester работает на последнем снимке реестра | правки, сделанные во время сбоя, появляются в `v_projects_current` | долгий сбой раздувает WAL; нужен лимит `max_slot_wal_keep_size` |
| C3 · ClickHouse остановлен | lag групп `ch_*` растёт, после старта дочитывается; запись gold из Spark падает, Airflow повторяет | сверка на окне сбоя | возможен дубль блока (вставка есть, коммита нет) — сверка покажет `duplicate`, лечится `--repair` |
| C4 · Spark-джоб убит посредине | незавершённый коммит Iceberg не виден; чекпойнт повторяет батч; повтор `MERGE` идемпотентен | число строк silver и контрольная сумма витрины до и после | джоб, убитый между `DROP PARTITION` и `INSERT`, оставляет месяц пустым до перезапуска (решение Р-5 ниже) |
| C5 · harvester убит посреди сбора | буфер продюсера теряется, но курсор не сдвинут; пачка собирается снова | все ожидаемые `source_id` есть в silver; дубли видны только в bronze | нет, если соблюдено правило курсора из §5 |

`make chaos` пишет результаты в `docs/benchmarks/chaos.md` и журнал `docs/benchmarks/chaos_log.csv` с колонками `scenario`, `fault_start`, `fault_end`, `recovered_at` в UTC. Времена начала и конца сбоя нужны части 2 как разметка для модели аномалий.

### 11.3 Открытые решения

| № | Вопрос | Варианты | Рекомендация этого ТЗ | Решить до |
| --- | --- | --- | --- | --- |
| Р-1 | Запись bronze (ADR-003) | Spark Structured Streaming или Kafka Connect Iceberg sink | Spark: один движок для bronze и silver, чекпойнт уже есть | S1-6 |
| Р-2 | Формат CDC-сообщений для ClickHouse (ADR-004) | `AvroConfluent` или `JSONEachRow` | Avro, как и для остальных топиков | S2-3 |
| Р-3 | Инструмент DQ (ADR-005) | Great Expectations или Soda Core | Great Expectations | S3-2 |
| Р-4 | Как Airflow запускает Spark | свой образ с `spark-submit` или `docker compose run` | свой образ, без Docker socket | S3-3 |
| Р-5 | Атомарность записи gold | `DROP PARTITION` + `INSERT` или запись в `*_stage` и `ALTER TABLE … REPLACE PARTITION … FROM *_stage` | `REPLACE PARTITION`: месяц не бывает пустым даже при C4 | S3-3 |
| Р-6 | Лимит WAL для слота Debezium | без лимита или `max_slot_wal_keep_size` | лимит 2 ГБ и alert на отставание слота | S2-3 |
| Р-7 | Где хранить разметку связей | `data/labels/` (в `.gitignore`) или в репозитории | `evals/g1/links_labeled.csv` в репозитории: на ней считается precision и её использует часть 2 | S4-5 |
| Р-8 | Airflow для профиля `ml` (ADR-110) | только `full` или `full` и `ml` | `profiles: [full, ml]` | до начала части 2 |
| Р-9 | Пороги оповещений и допуск сверки | стартовые из §9.2 или измеренные | заменить по результатам бенчмарка | S5-3 |
| Р-10 | Уверенность правила R3 | стартовые 0,5 или калиброванная | отключить или оставить в review при precision < 0,5 | S4-5 |

Каждое решение закрывается ADR в `docs/adr/`. После принятия Р-5 нужно поправить столбец «Идемпотентность» в §6 и правило из §7.4.
