# Хранилище состояний

Хранилище включает контракты событий, метрик и записей состояния, схему
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
| `docker-compose.yml` | Единая инфраструктура платформы, включая PostgreSQL для разработки и тестов |
| `infra/postgres/init-test-db.sql` | Создание отдельной тестовой БД при первичной инициализации PostgreSQL |

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
Миграция `0002` добавляет историю оценок. Миграция `0003` удаляет прежние результаты,
решения и оценки качества без совместимости, удаляет метрику свежести и добавляет
`dataset_profiles`: версионируемые сводки загрузок с периодом и допуском в эталон.
Raw и batches сохраняются. Старые загрузки можно проверить повторной подачей событий.

## Таблицы

| Группа | Таблицы |
| --- | --- |
| Каталог | `datasets`, `dataset_schemas`, `batches` |
| История запусков | `pipeline_runs` |
| Статистики | `dataset_metrics` |
| Качество | `quality_evaluations`, `quality_executions`, `quality_check_results`, `quality_decisions` |
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

Выполните команды из корня репозитория. Перед применением миграций задайте
переменную `DATABASE_URL` с адресом базы приложения:

```sh
docker compose -f docker-compose.yml up -d --wait postgres
python -m pip install -e ".[dev,storage]"
python -m alembic -c storage/postgres/alembic.ini upgrade head
```

Интеграционные тесты пропускаются, если не задан `TEST_DATABASE_URL` или не
установлены зависимости. `reliability` предназначена для приложения,
`reliability_test` — для тестов, которые очищают таблицы перед каждым запуском.
Не направляйте `TEST_DATABASE_URL` на базу приложения.

## История качества

Миграция `0002` добавляет две таблицы для хранения оценок и их выполнения:

- `quality_evaluations` хранит идентификатор оценки, датасет, область и порядок
  создания. Здесь же сохраняются план, сведения о подготовке и доставке,
  а также итоговое решение;
- `quality_executions` содержит отдельную строку для каждой пары
  `(evaluation_id, check_id)`: наблюдение, ограничение, состояние, результат
  и историю попыток проверки.

Благодаря отдельным строкам обновление одной проверки не требует перезаписи
всего плана. Индекс по датасету, области и порядку создания помогает найти
последнюю оценку. После завершения результаты не меняются; обновляться могут
только сведения о доставке. Для пары `dataset_id`/`scope_id` используется
блокировка PostgreSQL (advisory lock), исключающая одновременную обработку.

Таблицы `quality_check_results` и `quality_decisions` сохраняют роль текущего
представления для backend. При создании новой оценки оно очищается, а после
завершения результаты и решение записываются в одной транзакции.
Повтор старой оценки не заменяет более новую.

Историю можно прочитать через методы репозитория `QualityRepository.list_evaluations`
и `get_evaluation`. Отдельного HTTP API для неё пока нет.

Порядок повторов и запуска новой оценки описан в [документации процессора](stream_processor.md).
