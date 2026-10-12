# Backend: чтение datasets и качества

```text
HTTP /api/v1
    → api.py: параметры, зависимости и типизированные ответы
    → queries.py: существование dataset, ограниченные списки
    → repository.py: ReadRepository (интерфейс приложения)
    → postgres.py: SQL чтения и адаптеры общего storage
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

`Page` содержит `items`, `limit`, `offset`, `has_more`. Параметры: limit 1–200
(по умолчанию 50), offset ≥ 0. SQL выбирает limit+1 записей; дополнительная запись
нужна только для has_more. Общий total не вычисляется. Каталог сортируется по
dataset_id ASC, batches по received_at DESC / batch_id DESC, scopes по
last_saved_at DESC / scope_id ASC. Пагинация offset не гарантирует стабильный
снимок между разными HTTP-запросами при параллельной записи upstream.

`checks` — список `{result: CheckResult, recorded_at}`. `decision` — исходный
`QualityDecision` или null. Используется канонический десериализатор storage,
включая JSONB freshness; HTTP-сериализация общего Pydantic-контракта передаёт
timedelta как ISO 8601 duration. API не возвращает внутренний JSONB напрямую.

`quality-scopes` объединяет scopes из quality_check_results и quality_decisions,
включая проверки без решения и решение без проверок. Он не выводится из batches.
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

Это гарантирует один снимок БД внутри ответа, но **не доказывает**, что проверки
и policy относятся к одной ревизии вычисления: upstream может записывать их
отдельными транзакциями. Для этого потребуется договорённость с владельцами
quality и storage. Приложение не исправляет и не пересчитывает сохранённые факты.

## Время и смысл данных

- `received_at` — получение batch согласно контракту upstream.
- `recorded_at` — техническое время сохранения проверки, а не время наблюдения.
- `decided_at` — время решения, явно заданное producer.
- `last_saved_at` — максимум recorded_at и decided_at для scope. Это временный
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
