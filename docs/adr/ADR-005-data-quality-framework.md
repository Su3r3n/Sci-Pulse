# ADR-005: Инструмент контроля качества данных (Great Expectations поверх Spark)

- **Статус:** Принято (Accepted — Решение Р-3 из §11.3 ТЗ)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0–S3 / Волна 1–2 (§8.3, §11.3 Р-3 ТЗ)

## Контекст и проблема

В пакетном пайплайне `scipulse_etl` перед расчётом витрин `gold_trends` и `gold_impact` необходим автоматический гейт качества данных (`dq_silver`), который проверяет таблицы слоя `silver` и блокирует обновление витрин при обнаружении критических ошибок (`--fail-on error`, `retries=0`), а после расчёта витрин выполняет сверку `dq_gold` (`--fail-on never`, уровень `warn`).

В §11.3 (Р-3) рассматривались два инструмента:
1. **Great Expectations (GX)** с исполнением поверх PySpark DataFrame;
2. **Soda Core** (SodaCL).

## Решение

Принят **Great Expectations поверх PySpark (`platform/dq/dq_run.py`)** с вынесением порогов в конфигурационный файл `platform/dq/thresholds.yaml` (Решение Р-3):

1. **Наборы проверок (Suites):**
   - `silver_papers` (`error`): `source_id`, `title`, `published_at` NOT NULL; уникальность `paper_key` по всей таблице; диапазон `published_at` от `1990-01-01` до `now + 1 day`; формат DOI `^10\.\d{4,9}/\S+$`; `cited_by_count >= 0`; отклонение числа строк за запуск в пределах порога из `thresholds.yaml`.
   - `silver_links` (`error`): `confidence` в `[0, 1]`; `rule IN ('R1', 'R2', 'R3')`; ссылочная целостность `paper_key` относительно `silver.papers`; формат `repo_full_name` (`owner/name`).
   - `silver_code` (`error`): уникальность `repo_full_name`; непустой `tag` у релизов; дата релиза не в будущем.
   - `gold_post` (`warn`): `papers > 0` за последний закрытый месяц; равенство суммы `papers` в `dm_research_trends` и числа работ в `silver.papers` за тот же месяц.
2. **Хранение результатов:**
   - Каждая проверка записывает результат в таблицу Iceberg `lake.ops.dq_results` и дублирует его в ClickHouse `scipulse.ops_dq_results` для дашборда Pipeline Health и алерта `DqFailed`.

## Последствия

- **Плюсы:** Проверки выполняются непосредственно на Spark DataFrame поверх таблиц Iceberg без выгрузки данных во внешнюю БД; на испорченной фикстуре гейт фиксирует все нарушения за один проход и возвращает ненулевой код возврата, останавливая ветку `gold_*` со статусом `upstream_failed`.
- **Минусы:** Зависимость от пакета `great_expectations` в образе Spark/Airflow.
