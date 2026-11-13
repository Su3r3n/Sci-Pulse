# ADR-004: Формат сообщений CDC Debezium (`AvroConfluent` через Schema Registry) и защита WAL

- **Статус:** Принято (Accepted — Решения Р-2 и Р-6 из §11.3 ТЗ)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0–S2 / Волна 1 (§1.2, §4.4, §7.2, §11.3 Р-2, Р-6 ТЗ)

## Контекст и проблема

Изменения из 4 таблиц базы `registry` (`tracked_projects`, `project_repos`, `institutions`, `project_institutions`) передаются через логическую репликацию Postgres (`pgoutput`, публикация `dbz_registry`) и Debezium в compacted-топики `cdc.registry.public.*`. Эти топики читают:
1. ClickHouse Kafka Engine (`registry_*_queue` → `mv_registry_*` → `registry_*`);
2. `harvester-go` (группа `harvester-registry`, построение снимка проектов и репозиториев в памяти).

В §11.3 открыты два связанных вопроса:
- **Р-2:** Использовать ли для CDC формат `AvroConfluent` через Schema Registry или `JSONEachRow`?
- **Р-6:** Как защитить диск Postgres от переполнения WAL при длительной остановке Debezium Connect (сценарий Chaos C2)?

## Решение

1. **Формат CDC (Решение Р-2): `AvroConfluent` через Confluent Schema Registry (`http://schema-registry:8081`)**.
   - В коннекторе `platform/debezium/registry.json` и контейнере `connect` используется `io.confluent.connect.avro.AvroConverter` для значений (и строковый/Avro ключ) с регистрацией схем в Schema Registry.
   - Применяется SMT `io.debezium.transforms.ExtractNewRecordState` (`unwrap`) с параметрами:
     - `delete.handling.mode = "rewrite"` (добавляет поле `__deleted`);
     - `drop.tombstones = "true"` (исключает пустые tombstone-сообщения, на которых спотыкается Kafka Engine в ClickHouse);
     - `add.fields = "op,source.ts_ms"` (добавляет `__op` и `__source_ts_ms`).
   - В ClickHouse очереди `registry_*_queue` используют `kafka_format = 'AvroConfluent'` и `format_avro_schema_registry_url = 'http://schema-registry:8081'`.
2. **Безопасность учётных данных:**
   - Коннектор читает логин и пароль роли `debezium` через `FileConfigProvider` (`${file:/secrets/debezium.properties:user}` и `password`), чтобы секреты не хранились в git.
3. **Защита WAL в Postgres (Решение Р-6):**
   - В параметрах запуска контейнера `postgres` фиксируются `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4` и **`max_slot_wal_keep_size=2GB`**, а в Grafana настраивается алерт `CdcDisconnected`.

## Последствия

- **Плюсы:** Полное единообразие сериализации во всех топиках платформы (правило §1.2 п.3 без исключений); строгая типизация колонок между Postgres, Debezium и ClickHouse; защита диска хоста от раздувания WAL.
- **Минусы:** Контейнер `connect` должен иметь плагин `confluentinc/kafka-connect-avro-converter` (или встроенные библиотеки Confluent Avro Converter) и доступ к файлу `/secrets/debezium.properties`.
