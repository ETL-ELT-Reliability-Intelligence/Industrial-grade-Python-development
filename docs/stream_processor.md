# Потоковый процессор (этап 2: приём событий, статистики и проверки качества)

`services.stream_processor` замыкает контур
`ingestion / Airflow -> Kafka -> stream processor -> PostgreSQL -> backend`.
Сервис подписывается на события загрузки батчей и запусков пайплайна, вычисляет
статистики по сырым данным, запускает детерминированные проверки качества из
`data_quality`, сохраняет результаты и решение policy в PostgreSQL и при сбое
проверки публикует `data.quality.failed`.

## Структура пакета

| Путь | Назначение |
| --- | --- |
| `services/stream_processor/raw.py` | Чтение JSONL-батчей по URI `file://` и `s3://` (MinIO) |
| `services/stream_processor/statistics.py` | Расчёт `row_count`, плоской схемы, `null_rate` и `distinct_count` |
| `services/stream_processor/policy.py` | Пороги проверок (`QualityThresholds`) и построение `Constraint` |
| `services/stream_processor/publisher.py` | Публикация `data.quality.failed` с детерминированным `event_id` |
| `services/stream_processor/handlers.py` | Идемпотентная обработка `dataset.ingested` и `pipeline.*` |
| `services/stream_processor/consumer.py` | Конфигурация и цикл опроса Kafka с ручным коммитом offset |
| `services/stream_processor/__main__.py` | CLI-точка входа `python -m services.stream_processor` |

## Обрабатываемые события

Подписка по умолчанию включает четыре топика:

- `dataset.ingested` — регистрация датасета, чтение raw-файла (если указан
  `raw_uri`), сохранение схемы в `dataset_schemas` и батча в `batches`,
  запись метрик в `dataset_metrics`, запуск проверок `schema`, `freshness`,
  `null_rate` и `row_count`, сохранение результатов в `quality_check_results`
  и итогового решения в `quality_decisions`.
- `pipeline.started`, `pipeline.finished`, `pipeline.failed` — запись и
  обновление статуса запуска в `pipeline_runs` (завершённый запуск не
  перезаписывается запоздавшим событием `started`).

Если в `dataset.ingested` поле `raw_uri` равно `None`, батч сохраняется в
`batches`, а расчёт статистик и проверки качества пропускаются.

## Правила проверок и идемпотентность

- **Схема (`schema`).** Первая версия схемы сохраняется без проверки; при
  появлении новой версии поля батча сравниваются с предыдущей сохранённой
  версией через `check_schema(..., allow_extra_fields=False)`. Новая версия
  также сохраняется в `dataset_schemas`.
- **Свежесть (`freshness`).** Сравнивается возраст батча относительно времени
  оценки с порогом `QUALITY_MAX_FRESHNESS_SECONDS` (по умолчанию `3600`).
- **Доля пропусков (`null_rate`).** Для каждой колонки проверяется порог
  `QUALITY_MAX_NULL_RATE` (по умолчанию `0.05`).
- **Объём батча (`row_count`).** Если в истории `dataset_metrics` накоплено не
  менее `QUALITY_ROW_COUNT_MIN_HISTORY` (по умолчанию `3`) предыдущих точек в
  окне `QUALITY_ROW_COUNT_HISTORY_WINDOW` (по умолчанию `10`), строится
  коридор `[median * min_ratio, median * max_ratio]` (по умолчанию `0.5..2.0`).
- **Идемпотентность.** Все вызовы репозиториев идемпотентны; при повторной
  доставке события записи обновляются без дублей, а `DataQualityFailedEvent`
  получает детерминированный `UUID5` от `(dataset_id, scope_id, check_id)`.
  Offset в Kafka коммитится только после успешной обработки события.

## Переменные окружения и запуск

| Переменная | Назначение | По умолчанию |
| --- | --- | --- |
| `DATABASE_URL` | Строка подключения к PostgreSQL | обязательна |
| `KAFKA_BOOTSTRAP_SERVERS` | Адрес брокера Kafka | `localhost:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `stream-processor` |
| `QUALITY_MAX_FRESHNESS_SECONDS` | Максимальный возраст батча, сек | `3600` |
| `QUALITY_MAX_NULL_RATE` | Максимальная доля `null` в колонке | `0.05` |
| `QUALITY_ROW_COUNT_HISTORY_WINDOW` | Размер окна истории `row_count` | `10` |
| `QUALITY_ROW_COUNT_MIN_HISTORY` | Минимум точек истории для проверки объёма | `3` |
| `QUALITY_ROW_COUNT_MIN_RATIO` | Нижняя граница от медианы `row_count` | `0.5` |
| `QUALITY_ROW_COUNT_MAX_RATIO` | Верхняя граница от медианы `row_count` | `2.0` |

Запуск процессора:

```sh
python -m services.stream_processor --kafka localhost:9092 --max-messages 20
```

Запуск тестов:

```sh
pytest tests/stream_processor -q
```
