# ADR-110: Запуск задач Spark из Airflow и профили Docker Compose (`full`, `ml`)

- **Статус:** Принято (Accepted — Решения Р-4 и Р-8 из §11.3 ТЗ)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0–S3 / Волна 1–2 (§2.4, §8.1, §9.4, §11.3 Р-4, Р-8 ТЗ)

## Контекст и проблема

Необходимо определить:
1. **Р-4:** Как оркестратор Airflow запускает PySpark-джобы и бинарник `harvester --once` в локальном окружении Docker Compose без отдельного кластера Spark Standalone/YARN?
2. **Р-8:** В каких профилях Docker Compose должны быть объявлены сервисы Airflow (`airflow-scheduler`, `airflow-api-server`, `airflow-dag-processor`), чтобы часть 2 платформы (ML-контур в профиле `ml`) могла переиспользовать оркестратор?

## Рассмотренные варианты для Р-4

1. **Монтирование `/var/run/docker.sock` в Airflow и вызов `docker compose run --rm spark ...`** — проще в сборке образа, но даёт контейнеру Airflow права `root` на хост-машине и приводит к двойному пику памяти (`airflow-scheduler` + отдельный контейнер `spark`).
2. **Собственный образ `scipulse/airflow` на базе Airflow 3.x** с установленными внутри Java 17, PySpark, Iceberg/Kafka/S3A/JDBC JAR-файлами, бинарником `harvester` и `clickhouse-client`, запускающий задачи через `BashOperator` и единый скрипт `platform/etl/steps.sh` (`spark-submit --master local[*]`).

## Решение

1. **Решение Р-4:** Выбран **собственный образ `scipulse/airflow` (Airflow 3.x, `LocalExecutor`) без монтирования Docker socket**.
   - Все команды ETL вынесены в единый скрипт `platform/etl/steps.sh`, который вызывается как из `make demo-slice` (профиль `core`), так и из `BashOperator` в DAG `scipulse_etl` (профиль `full`).
   - В Airflow создаётся пул `spark` размером **1 слот** (`slots=1`), к которому привязаны все задачи Spark и DQ. Это гарантирует отсутствие конкурентных коммитов в таблицы Iceberg и удержание памяти `airflow-scheduler` в пределах `mem_limit: 3.5g` (`spark.driver.memory=2g`).
   - Для синхронизации фонового демона `harvester` и барьерной задачи `harvest` (`harvester --once --until ... --sources ...`) используется `pg_try_advisory_lock` в базе `harvester`.
2. **Решение Р-8:** Все сервисы Airflow в `docker-compose.yml` объявляются с **`profiles: ["full", "ml"]`**, что позволяет запускать их как в полном контуре части 1 (`make up-full`), так и в ML-контуре части 2 без дублирования описания сервисов.

## Последствия

- **Плюсы:** Безопасность контейнеров (нет доступа к Docker socket хоста), предсказуемый бюджет памяти (не более одной JVM Spark одновременно), идентичность команд между `make demo-slice` и Airflow DAG, бесшовная интеграция с частью 2 (`ml`).
- **Минусы:** Более крупный Docker-образ `scipulse/airflow` (содержит JDK 17, PySpark и JAR-зависимости).
