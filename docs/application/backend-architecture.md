# Backend: чтение datasets, качества и запусков pipelines

```text
HTTP /api/v1
    → api.py: параметры, зависимости и типизированные ответы
    → queries.py: существование dataset, ограниченные списки
    → repository.py: ReadRepository (интерфейс приложения)
    → postgres.py: SQL чтения; evaluations.py: чтение истории и адаптер общего storage
    → pipelines.py: ограниченное чтение каталога pipelines и истории запусков
    → существующий PostgreSQL
```

`main.py` собирает приложение; factory репозитория подменяется в unit/HTTP-тестах.
Production-factory читает только PostgreSQL. Test double находится в tests,
историческое demo — в `services/demo`; runtime-fallback на них отсутствует.

## HTTP-контракт

Все пути данных имеют префикс `/api/v1`.

| GET | Ответ |
| --- | --- |
| `/datasets` | Page[Dataset] |
| `/datasets/{dataset_id}` | `{dataset_id, created_at}` |
| `/datasets/{dataset_id}/batches` | Page[Batch] из общего контракта |
| `/datasets/{dataset_id}/quality-scopes` | Page[{scope_id, last_saved_at}] |
| `/datasets/{dataset_id}/quality?scope_id=...` | `{dataset_id, scope_id, checks, decision, decided_at}` |
| `/datasets/{dataset_id}/evaluations?scope_id=...` | Page[EvaluationSummary] |
| `/datasets/{dataset_id}/evaluations/{evaluation_id}` | EvaluationDetail |
| `/pipelines` | Page[{pipeline_id}] |
| `/pipeline-runs?pipeline_id=...` | Page[PipelineRun] из общего контракта |

`Page` содержит `items`, `limit`, `offset`, `has_more`. Параметры: limit 1–200
(по умолчанию 50), offset ≥ 0. SQL выбирает limit+1 записей; дополнительная запись
нужна только для has_more. Общий total не вычисляется. Каталог сортируется по
dataset_id ASC, batches по received_at DESC / batch_id DESC, scopes по
last_saved_at DESC / scope_id ASC, история расчётов по sequence DESC. Пагинация offset не гарантирует стабильный
снимок между разными HTTP-запросами при параллельной записи upstream.

`checks` — список `{result: CheckResult, recorded_at}`. `decision` — исходный
`QualityDecision` или null. Используется канонический десериализатор storage,
поддерживающий статистические проверки и правила значений. API не возвращает
внутренний JSONB напрямую. Эталон исторической проверки доступен в details результата.

`quality-scopes` объединяет scopes из quality_check_results, quality_decisions
и quality_evaluations, включая области с начатым расчётом без итоговых результатов. Он не выводится из batches.
Scope с отсутствующими результатами даёт 200, checks=[], decision=null,
decided_at=null; существование такого scope отдельно не утверждается.
Неизвестный dataset даёт 404 во всех дочерних маршрутах.

Неверные параметры — 422. Отсутствие настройки/соединения/таблиц/прав чтения и
таймауты — 503 с `{"detail":"State store unavailable"}`. Неожиданные ошибки
программирования не маскируются под пустую БД. DSN и сообщения драйвера не
возвращаются клиенту и не выводятся нашим обработчиком в лог.

## Соединения и согласованность

На запрос открывается отдельное синхронное соединение общей factory storage;
обработчики `def` выполняются вне event loop. Connect timeout — 5 секунд,
statement timeout — 5 секунд, lock timeout — 2 секунды. Соединение закрывается
при успехе и исключении. Все чтения запроса выполняются в REPEATABLE READ,
READ ONLY транзакции. Нет общей connection на параллельные запросы.

Подробности расчёта объединяют его план и отдельно сохранённые исполнения
через общий PostgresQualityRepository.get_evaluation в одном снимке БД. Решение
относится к этому evaluation_id. Прежний маршрут /quality читает итоговую
проекцию scope; независимые записи legacy-проверок и решения не доказывают
общность ревизии. Приложение не исправляет и не пересчитывает сохранённые факты.

## Время и смысл данных

- `received_at` — получение batch согласно контракту upstream.
- `recorded_at` — техническое время сохранения проверки, а не время наблюдения.
- `decided_at` — время решения, явно заданное producer.
- `last_saved_at` — максимум recorded_at, decided_at, created_at и updated_at расчётов для scope. Это временный
  порядок отображения по имеющимся полям, а не гарантия свежести данных.
- Все даты timezone-aware; UI отображает их в часовом поясе браузера.
- None не заменяется нулём. UNKNOWN не заменяется PASS; отсутствие policy
  не превращается в вычисленное приложением решение.

## Временная граница ownership

Общие storage-репозитории пока не предлагают каталог datasets, список scopes
и offset-пагинацию. SQL для них находится в application read-адаптере; чтение
решения и десериализация проверок переиспользуют storage. Это локальное решение,
а не согласованный новый интерфейс роли Processing & Storage.
Миграции и контракты коллег не изменены. `seed.py` — отдельная явная dev-утилита,
не часть HTTP/runtime-пути. Detail URL пока ожидают dataset_id без slash;
допустимые ID и маршрутизацию необходимо согласовать с владельцами контракта.

## История расчётов

Параметр scope_id обязателен и должен содержать непробельный символ. Для
существующего dataset неизвестный scope даёт пустую страницу истории.
Неизвестный расчёт или расчёт другого dataset возвращает 404 с
`{"detail":"Evaluation not found"}`. Детальный маршрут ограничен dataset;
соответствие выбранному scope дополнительно проверяет frontend.

EvaluationSummary: evaluation_id, dataset_id, scope_id, state, evaluated_at,
created_at, updated_at, config_hash, decision_status (может быть null).
Список выбирает только эти поля SQL-запросом, без чтения payload всех проверок.
EvaluationDetail: dataset_id, scope_id, created_at, updated_at и evaluation
из общего контракта Evaluation. Исполнения внутри evaluation.checks содержат
spec, состояние, наблюдение, ограничение, результат, диагностику, attempts и baseline.
Состояния и решение передаются как сохранены, без вывода общего статуса из проверок.

/ready проверяет доступность quality_evaluations и quality_executions наряду
с прежними таблицами. Нужны миграции до 0003 из общего storage. Миграция 0003
удаляет прежние результаты качества: не применять автоматически к рабочей БД.

Отдельные save_execution не обновляют updated_at родительского расчёта.
Поэтому last_saved_at и updated_at не являются временем последнего изменения
каждой проверки. evaluated_at задаётся при создании плана, не означает время
завершения. Это ограничение текущего producer/storage, а не расчёт приложения.

Зависимости разделены по задачам: EvaluationQueries проверяет существование
сущностей и формирует страницы; PostgresEvaluationReader читает историю;
PostgresReadRepository объединяет операции чтения. Контракты и миграции коллег
не изменяются. Непагинированный общий list_evaluations для HTTP-списков не используется.

## История запусков pipelines

Каталог выводит различные `pipeline_id` из существующей таблицы `pipeline_runs`
в порядке `pipeline_id ASC`. История сортируется по `started_at DESC, run_id DESC`.
Оба списка ограничены `limit+1` с offset; общий total и полный список не загружаются.
`pipeline_id` передаётся обязательным непустым query-параметром, поэтому slash,
пробелы внутри ID и другие специальные символы не требуют нового URL-контракта.
Неизвестный ID даёт 200 и пустую страницу; отсутствие записей не подтверждает
существование или отсутствие pipeline в Airflow.

Чтение возвращает исходный `PipelineRun`: `running/success/failed`, времена и
nullable error. Статус качества из него не выводится. Связи с dataset нет.
Каталог и offset принадлежат application read-адаптеру; общий контракт и методы
записи storage остаются прежними. Все запросы используют существующую READ ONLY
транзакцию и параметризованный SQL. `/ready` дополнительно проверяет доступность
колонок `pipeline_runs`; недоступность возвращается как обычный 503.
