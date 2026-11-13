# ADR-001: Выбор брокера сообщений и режима кластера (Apache Kafka KRaft)

- **Статус:** Принято (Accepted)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0 / Волна 1 (§1.1, §2.2, §9.4 ТЗ)

## Контекст и проблема

Платформа SciPulse требует единую шину событий для развязки сервисов сбора (`harvester-go`, `loadgen-go`), CDC-потока из базы реестра (`registry` → Debezium) и трёх независимых контуров потребления:
1. Быстрый путь (Speed Layer) в ClickHouse через Kafka Engine;
2. Пакетный путь (Batch Layer) в Apache Iceberg (`bronze.raw_events`) через Spark Structured Streaming;
3. Поддержание актуального снимка реестра проектов в памяти `harvester-go` из compacted CDC-топиков.

При этом весь профиль `core` должен помещаться в лимит памяти ~8 ГБ ОЗУ на одной машине без лишних координационных сервисов.

## Рассмотренные варианты

1. **Apache Kafka в режиме KRaft (1 узел `broker,controller`)** — без ZooKeeper, с поддержкой log compaction (`cleanup.policy=compact`), совместимостью с Confluent Schema Registry, Kafka Connect (Debezium) и ClickHouse Kafka Engine.
2. **Apache Kafka + ZooKeeper** — классическая связка, но требует отдельного контейнера ZooKeeper (+256–512 МБ ОЗУ) и устарела в Kafka 3.x/4.x.
3. **Redpanda** — совместима с Kafka API и не требует JVM, но в рамках учебного и промышленного стека ТЗ требуется штатная связка Apache Kafka + Confluent Schema Registry + Debezium Connect + `kafka-exporter`.

## Решение

Выбран **Apache Kafka (KRaft, один узел `broker,controller`)** с ограничением памяти контейнера `1024m` (`KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"`):
- Два слушателя: `INTERNAL://kafka:9092` (внутри Docker-сети `scipulse`) и `EXTERNAL://localhost:9094` (публикация только на `127.0.0.1:9094`).
- Автосоздание топиков **выключено** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).
- Все 13 топиков (§2.2 ТЗ) создаются идемпотентным скриптом `platform/kafka/init-topics.sh` (`make topics`):
  - `raw.papers.{openalex,arxiv,inspire}`, `raw.code.{github,events}` — 3 партиции, retention 7 дней;
  - `raw.code.zenodo` — 1 партиция, retention 7 дней;
  - `cdc.registry.public.{tracked_projects,project_repos,institutions,project_institutions}` — 1 партиция, `cleanup.policy=compact`;
  - `dlq.{papers,code}` — 1 партиция, retention 30 дней;
  - `load.synthetic` — 6 партиций, retention 1 день.
- Продюсеры (`common-go/kafka`) работают с `acks=all`, включённой идемпотентностью и `DeliveryTimeout >= 2m` для прохождения сценария отказа брокера (Chaos C1).

## Последствия

- **Плюсы:** Отсутствие ZooKeeper экономит память и ускоряет старт `make up`; полная поддержка `compact`-топиков для CDC и строгий контроль топологии топиков.
- **Минусы:** Одиночный брокер (`replication-factor=1`) не обеспечивает отказоустойчивость диска узла, однако при перезапуске контейнера (Chaos C1) буферы продюсеров и смещения потребителей сохраняют гарантию at-least-once без потерь.
