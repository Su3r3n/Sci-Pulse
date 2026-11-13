# ADR-004: Формат сообщений CDC Debezium (`AvroConfluent` через Schema Registry) и защита WAL

- **Статус:** Принято (Accepted — Решения Р-2 и Р-6 из §11.3 ТЗ)
- **Дата:** 2026-11-09 (уточнено 2026-11-13 по итогам повторной проверки волны 1)
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
   - В коннекторе `platform/debezium/registry.json` и контейнере `connect` используется `io.confluent.connect.avro.AvroConverter` и для ключа, и для значения (`key.converter.schema.registry.url` и `value.converter.schema.registry.url` = `http://schema-registry:8081`). Строковый ключ (`StringConverter`) не используется: для ключа-Struct он даёт строку вида `Struct{id=1}` (ревью волны 1, m4).
   - Применяется SMT `io.debezium.transforms.ExtractNewRecordState` (`unwrap`) с параметрами:
     - `delete.tombstone.handling.mode = "rewrite"`: удаление превращается в строку с `__deleted=true`, tombstone не пишется, поэтому Kafka Engine ClickHouse не получает пустых сообщений. В Debezium 2.5+ этот ключ заменил пару `delete.handling.mode=rewrite` + `drop.tombstones=true` из текста ТЗ §4.4 (там же оговорено: «сверить с версией образа»). Образ — `debezium/connect:2.7.3.Final`, старые ключи в нём deprecated и одновременно с новым не задаются;
     - `add.fields = "op,source.ts_ms"` (добавляет `__op` и `__source_ts_ms`).
   - В ClickHouse очереди `registry_*_queue` используют `kafka_format = 'AvroConfluent'` и `format_avro_schema_registry_url = 'http://schema-registry:8081'`.
2. **Безопасность учётных данных:**
   - Коннектор читает логин и пароль роли `debezium` через `FileConfigProvider` (`${file:/secrets/debezium.properties:user}` и `password`), чтобы секреты не хранились в git.
   - Файл создаёт entrypoint образа `platform/debezium` (`scipulse-connect-entrypoint.sh`) из переменных `DEBEZIUM_DB_USER`/`DEBEZIUM_DB_PASSWORD`, которые compose передаёт в `connect` из `.env`. Каталог `/secrets` живёт внутри контейнера и не монтируется с хоста (ревью волны 1, повторная проверка: монтирование `./secrets:/secrets:ro` ломает запись файла, а файл с хоста с правами 600 не читается пользователем `kafka` контейнера).
3. **Защита WAL в Postgres (Решение Р-6):**
   - В параметрах запуска контейнера `postgres` фиксируются `wal_level=logical`, `max_replication_slots=4`, `max_wal_senders=4` и **`max_slot_wal_keep_size=2GB`**, а в Grafana настраивается алерт `CdcDisconnected`.

## Последствия

- **Плюсы:** Полное единообразие сериализации во всех топиках платформы (правило §1.2 п.3 без исключений); строгая типизация колонок между Postgres, Debezium и ClickHouse; защита диска хоста от раздувания WAL.
- **Минусы:** Контейнер `connect` должен иметь плагин `confluentinc/kafka-connect-avro-converter` (или встроенные библиотеки Confluent Avro Converter) и доступ к файлу `/secrets/debezium.properties`. Поэтому `connect` собирается из `platform/debezium/Dockerfile`, а не берётся как стоковый образ Debezium. В `cp-kafka-connect:7.7.x` конвертер лежит в `/usr/share/java/kafka-serde-tools`; каталог `/usr/share/confluent-hub-components` там пустой, пока не выполнен `confluent-hub install`.
