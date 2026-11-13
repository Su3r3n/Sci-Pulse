# ADR-002: Архитектура слоя выдачи, витрин Gold и быстрого пути в ClickHouse

- **Статус:** Принято (Accepted)
- **Дата:** 2026-11-09
- **Контекст:** Спринт S0–S2 / Волна 1 (§6, §7, §11.3 Р-5 ТЗ)

## Контекст и проблема

Платформе необходим аналитический слой выдачи (Serving Layer) для Grafana, который одновременно решает четыре задачи:
1. Хранение и отдача пересчитываемых пакетных витрин (`dm_research_trends`, `dm_hep_software_impact`);
2. Поминутная агрегация живого потока событий из Kafka с секундной задержкой (`live_papers_1m`, `live_code_1m` → `dm_live_activity`);
3. Хранение реплики таблиц реестра из CDC без прямых обращений Spark и Grafana в OLTP-базу Postgres (`registry_*` → `v_*_current`);
4. Хранение операционной телеметрии пайплайнов (`ops_pipeline_runs`, `ops_dlq_events`, `ops_dq_results`, `ops_reconciliation`) и данных нагрузочного тестирования (`bench_events`).

Кроме того, при перезаписи месячных партиций gold-витрин из Spark необходимо исключить состояние, когда партиция пуста при падении джоба посреди записи (сценарий Chaos C4, решение **Р-5**).

## Решение

1. **Единая аналитическая СУБД выдачи — ClickHouse** (база `scipulse`, лимит памяти `1.5 ГБ`):
   - Три строго разделённых слоя объектов: транзитные очереди `ENGINE = Kafka` (`*_queue`), целевые таблицы семейства `*MergeTree` и аналитические представления `VIEW` (`v_*`, `dm_live_activity`, `dm_pipeline_health`).
   - Порядок инициализации в `ch-init` (§9.5): сначала целевые таблицы, затем очереди Kafka Engine, затем Materialized Views и обычные Views, затем пользователи и привилегии.
2. **Изоляция consumer groups для очередей Kafka Engine (§7.2):**
   - Каждая таблица `ENGINE = Kafka` использует собственную consumer group (`ch_registry_projects`, `ch_registry_repos`, `ch_registry_institutions`, `ch_registry_project_institutions`, `ch_live_papers`, `ch_live_code`, `ch_dlq`, `ch_synthetic`), чтобы избежать взаимных ребалансировок.
3. **Реплика реестра на `ReplacingMergeTree(version)` (§7.2):**
   - Таблицы `registry_*` используют `__source_ts_ms` как `version` и `__deleted = 'true'` как `is_deleted`.
   - Чтение выполняется **только** через представления `v_*_current` с `GROUP BY ... HAVING argMax(is_deleted, version) = 0`, что гарантирует консистентный ответ до фонового слияния кусков.
4. **Атомарная перезапись партиций Gold через stage-таблицы (Решение Р-5, §11.3):**
   - Вместо неатомарной пары `ALTER TABLE ... DROP PARTITION` + `INSERT` джобы `silver_to_gold_trends.py` и `silver_to_gold_impact.py` очищают и заполняют `*_stage`-таблицу (`dm_research_trends_stage`, `dm_hep_software_impact_stage`), после чего для каждого затронутого месяца выполняют атомарную команду:
     ```sql
     ALTER TABLE scipulse.<mart> REPLACE PARTITION '<YYYYMM>' FROM scipulse.<mart>_stage
     ```
5. **Разделение прав доступа (§7.6):**
   - `grafana_ro` (`readonly=1`) — только `SELECT` на витрины, представления и `ops_*`;
   - `spark_writer` — `SELECT`, `INSERT`, `ALTER`, `TRUNCATE` на витрины, `*_stage` и `ops_*`, `SELECT` на `v_*_current`.

## Последствия

- **Плюсы:** Переименование проекта в реестре мгновенно отображается в `v_hep_software_impact` за счёт `LEFT JOIN v_projects_current` при чтении; при убийстве Spark-джоба (Chaos C4) партиция в ClickHouse никогда не остаётся пустой.
- **Минусы:** Требуется следить за тем, чтобы никто не выполнял прямой `SELECT` из таблиц `*_queue` (это сдвигает смещения Kafka Engine).
