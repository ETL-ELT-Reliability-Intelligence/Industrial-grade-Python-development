# Хранилище состояний и контракты роли №2 (этап 1)

Этап 1 включает: контракты событий, метрик и записей состояния, схему
PostgreSQL с миграциями Alembic и слой доступа к данным (репозитории).
Код не читает системные часы и не содержит бизнес-логики обнаружения аномалий.

## Структура

| Путь | Назначение |
| --- | --- |
| `contracts/events.py` | Kafka-события: типы, валидация, JSON-сериализация |
| `contracts/metrics.py` | `MetricPoint` — статистика датасета за область (batch/окно) |
| `contracts/state.py` | `Batch`, `PipelineRun`, `Anomaly`, `UserAction` |
| `contracts/base.py` | общий Pydantic v2 `Contract` и ограничения полей |
| `storage/repositories.py` | Protocol-интерфейсы репозиториев |
| `storage/serialization.py` | JSON-представление результатов проверок для JSONB |
| `storage/postgres/` | `connect()`, реализации репозиториев, Alembic (`alembic.ini`, `migrations/`) |
| `infra/docker-compose.yml` | PostgreSQL для разработки и тестов |
| `scripts/storage_setup.*`, `scripts/storage_test.*` | установка и тесты (`.sh` и `.ps1`) |

## События (Kafka — только транспорт)

Типы: `dataset.ingested`, `pipeline.started`, `pipeline.finished`,
`pipeline.failed`, `schema.changed`, `data.quality.failed`, `anomaly.detected`.
События `incident.*` принадлежат Incident Engine и пока не описаны.

К полям из примера ARCHITECTURE.md §5.2 добавлены `event_id` (для
идемпотентности, вопрос 17) и `contract_version` (сейчас 1). Все времена —
timezone-aware, в JSON — ISO 8601. `event_id` — UUID, по умолчанию создаётся UUID4.
Лишние и отсутствующие обязательные поля отклоняются.
Источник истины — PostgreSQL, события лишь доставляют изменения.

Кодирование: `event.model_dump_json()` (при необходимости `.encode("utf-8")`),
декодирование: `parse_event(bytes | str) -> Event`. Сохранены классы с суффиксом
`Event`, `.topic`, `.key` и `raw_uri` из main. Для изменения схемы используются
`batch_id`, `previous_version`, `new_version`, `detected_at`. Времена пайплайна —
`started_at`, `finished_at`, `failed_at`; длительность вычисляется в Python.

Метрики и записи состояния также наследуют `Contract`: именованные аргументы,
строгие типы, неизменяемые поля, запрет лишних полей и проверка defaults.
Счётчики остаются целыми числами, `None` означает отсутствие измерения.
Для JSON используется `model_validate_json`, для Python — `model_validate`.
JSONB результатов качества сохраняет прежнюю структуру, включая длительность
`max_age_seconds`, поэтому миграция ранее записанных результатов не требуется.

## Таблицы

| Группа | Таблицы |
| --- | --- |
| Каталог | `datasets`, `dataset_schemas`, `batches` |
| История запусков | `pipeline_runs` |
| Статистики | `dataset_metrics` |
| Качество | `quality_check_results`, `quality_decisions` |
| Аномалии | `anomalies` |
| Инциденты | `incidents`, `incident_affected_datasets`, `incident_observations`, `incident_root_causes`, `incident_timeline`, `incident_impact` |
| Lineage | `lineage_nodes`, `lineage_edges` |
| ML и аудит | `ml_model_metadata`, `user_actions` |

Таблицы инцидентов покрывают все поля модели `Incident` из
`services/backend/models.py` (наблюдения, кандидаты причин, хронология,
зона влияния). Репозиторий инцидентов, lineage и ML-метаданных — этап 2 и позже.
Все времена хранятся как `timestamptz`; ограничения (`CHECK`) дублируют
валидацию контрактов, например `score` в диапазоне 0..1.

## Семантика записи

| Метод | При повторной записи |
| --- | --- |
| `upsert_batch` | обновляет поля; пустой `object_key` не стирает сохранённый |
| `upsert_pipeline_run` | завершённый запуск не перезаписывается запоздавшим событием |
| `save_schema` | версия схемы неизменна; другой набор полей — `ValueError` |
| `save_metric` | ключ: датасет, область, метрика, колонка, `stats_version`; значение обновляется |
| `save_check_result` | ключ: `check_id`, датасет, область, колонка; побеждает последний результат |
| `save_quality_decision` | решение области заменяется |
| `save_anomaly`, `record_action` | уже сохранённый идентификатор не меняется |

Соединение создаёт `storage.postgres.connect(dsn)` (autocommit, UTC). Каждый
вызов атомарен, составные вызовы открывают собственную транзакцию.

## Запуск

Linux/macOS: `bash scripts/storage_setup.sh`, затем `bash scripts/storage_test.sh`.
Windows: `powershell -File scripts/storage_setup.ps1`, затем
`powershell -File scripts/storage_test.ps1`.

Интеграционные тесты пропускаются, если не задан `TEST_DATABASE_URL` или не
установлены зависимости. Тестовая БД `reliability_test` создаётся только при
первом создании тома; если том уже существовал, выполните
`docker compose -f infra/docker-compose.yml down -v` и повторите setup.

## Допущения, требующие подтверждения

1. Контракты событий согласованы с main и потребителями Ingestion/Airflow; старые dataclass-классы и функции encode/decode заменены Pydantic API.
2. Для результатов проверок хранится только последний результат на ключ; истории пересчётов нет. Если нужна история, схема меняется.
3. `data.quality.failed` не содержит решения policy (WARN/BLOCK): оно читается из `quality_decisions`.
4. Зависимости в `storage/requirements.txt` заданы диапазонами; после первой установки их стоит зафиксировать через `pip freeze`, как в `services/backend/requirements.txt`.
