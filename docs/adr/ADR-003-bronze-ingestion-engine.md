# ADR-003: Запись слоя Bronze (Spark Structured Streaming с `availableNow`)

- **Статус:** Принято (Accepted — Решение Р-1 из §11.3 ТЗ)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0–S1 / Волна 1 (§2.1, §6.1–§6.4, §11.3 Р-1 ТЗ)

## Контекст и проблема

Необходимо надёжно переносить сырые события из всех топиков `raw.*` в таблицу Apache Iceberg `lake.bronze.raw_events` в объектном хранилище MinIO (`s3a://warehouse/`) с сохранением исходных байтов сообщения (5-байтный заголовок Confluent Schema Registry + Avro payload), ключа, партиции, смещения и времени сообщения Kafka (`kafka_ts`).

В §11.3 (Р-1) открыта развилка между двумя способами записи bronze:
1. **Spark Structured Streaming** (`bronze_ingest.py` с триггером `availableNow` для пакетного запуска и опциональным непрерывным режимом);
2. **Kafka Connect Iceberg Sink** — отдельный коннектор внутри контейнера Kafka Connect.

## Рассмотренные варианты

### Вариант 1: Spark Structured Streaming (`bronze_ingest.py`, `availableNow`)
- Читает топики по регулярному выражению `raw\..*` (`subscribePattern`).
- Пишет напрямую в `lake.bronze.raw_events` (`PARTITIONED BY (ingest_date)`).
- Хранит смещения в чекпойнте S3A (`s3a://warehouse/_checkpoints/bronze`), не требуя отдельной consumer group в Kafka.
- Использует тот же образ и рантайм PySpark + Iceberg, что и последующие шаги `bronze_to_silver`, `enrich_projects`, `reconcile`.

### Вариант 2: Kafka Connect Iceberg Sink
- Постоянно работает внутри контейнера `connect` вместе с Debezium.
- Увеличивает потребление heap-памяти контейнера `connect` (ограниченного `768m` в §9.4) и создаёт большое количество мелких файлов в MinIO при непрерывной записи.

## Решение

Принят **Вариант 1 (Решение Р-1): Spark Structured Streaming (`platform/spark/bronze_ingest.py`) с триггером `availableNow` по умолчанию**.

- В пакетном контуре (`make demo-slice` и задача `bronze_ingest` в DAG `scipulse_etl`) джоб запускается с `--trigger availableNow`, вычитывает все накопленные сообщения из `raw\..*`, атомарно коммитит снапшот Iceberg и чекпойнт в `s3a://warehouse/_checkpoints/bronze` и завершает работу, освобождая память.
- Поддерживается флаг `--trigger continuous` (`processingTime`) для длительных тестов.

## Последствия

- **Плюсы:** Единый стек PySpark + Iceberg для всех слоёв Lakehouse; отсутствие конкуренции за память с Debezium в контейнере `connect`; точная идемпотентность на уровне чекпойнта и ключа `(topic, partition, offset)`.
- **Минусы:** Пакетный запуск требует старта JVM драйвера Spark (компенсируется выполнением раз в сутки по расписанию или по команде `make demo-slice`).
