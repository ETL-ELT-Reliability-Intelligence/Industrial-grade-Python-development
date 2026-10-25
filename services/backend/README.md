# Backend и dashboard: проверка замечаний

Скоуп приложения: `services/backend`, `services/frontend`, `services/demo`
и `tests/application`. Компоненты загрузки, хранения и качества используются
через существующие интерфейсы; их реализация здесь не меняется.

## Пример с происхождением данных

Зависимости приложения берутся из корневых `pyproject.toml` и `uv.lock`.
Перед запуском подготовьте окружение из корня репозитория:

```powershell
uv sync --locked --group dev --extra storage --group browser
```

`dev` и `browser` — dependency groups: тесты/Ruff и браузерные тесты соответственно.
`storage` — существующий extra для миграций. Группа `dev` включается uv по умолчанию;
для одного backend без инструментов разработки используйте `uv sync --locked --no-dev`.
Команды `uv run --no-sync` ниже используют уже подготовленное окружение;
после изменения зависимостей повторите синхронизацию с нужными группами и extras.

Обычный `uv run --no-sync python -m services.backend.seed` по-прежнему создаёт заранее заданные
измерения для проверки состояний интерфейса.

Чтобы проверить настоящую загрузку исходных записей и сохранённое качество,
в development-БД с применёнными миграциями выполните из корня репозитория:

```powershell
$env:DATABASE_URL = 'postgresql://reliability:reliability@127.0.0.1:5432/reliability'
uv run --no-sync python -m services.backend.ingested_sample
uv run --no-sync python -m uvicorn services.backend.main:app --host 127.0.0.1 --port 8000
```

Утилита читает две примерные записи из `services/backend/samples/orders.csv`
через существующий `IngestionService`, сохраняет JSONL в `data/app-sample`,
загружает строки существующим загрузчиком в `raw."ingested-app-orders"`
и передаёт события существующему `StreamEventHandler`. Измерения и решение
вычисляются из исходного файла, а не задаются вручную. Запись загрузки и качества
в PostgreSQL выполняется в одной транзакции. Это локальный сценарий с передачей
событий в памяти; Kafka, Airflow и dbt не запускаются.

Откройте напечатанный путь dashboard на `http://127.0.0.1:8000`.
В таблице загрузок видны batch ID, число строк и URI исходного JSONL.
Scope качества в этом сценарии совпадает с batch ID. API только читает
сохранённые проверки, решение и время сохранения.

Повтор с тем же `--run-id` (по умолчанию `first-load`) и `--raw-root` использует
сохранённый batch без дублирования строк. Для новой загрузки задайте другой
`--run-id`. Сохраняйте raw-каталог для повторной обработки. При повторной проверке
старого batch оценка свежести может измениться. У первого batch ещё нет истории
для проверки объёма и предыдущей схемы для сравнения.

## Стиль внутри скоупа

- Комментарии и docstring — на английском, пользовательские подписи — на русском.
  Комментарии объясняют причины и ограничения, а не пересказывают код.
- Docstring начинается кратким предложением с точкой; подробности отделяются
  пустой строкой. Не добавляем шаблонные описания к очевидным методам.
- В Python backend/demo параметры и результаты функций аннотированы,
  включая обработчики API и вложенные функции. Используем `list[T]`,
  `dict[K, V]`, `T | None`, `Protocol` и `collections.abc`.
- `Any` допускается для динамических SQL/JSON-данных на границе адаптера.
  Ruff проверяет наличие аннотаций, а не совместимость типов.
- Тестовые фикстуры и двойники могут не иметь аннотаций; стиль форматирования
  у них тот же. Frontend сохраняет существующий JavaScript без перехода на TypeScript.
- Формат Python — Ruff с текущими настройками проекта. Проверки ниже ограничены
  скоупом приложения и не требуют изменения общего CI или конфигурации.

```powershell
uv run --no-sync python -m ruff check services/backend services/demo tests/application
uv run --no-sync python -m ruff check services/backend services/demo --select I,ANN,D200,D205,D209,D210,D211,D212,D415 --ignore ANN401
uv run --no-sync python -m ruff format --check services/backend services/demo tests/application
```

## Тесты

`TEST_DATABASE_URL` должен указывать на отдельную одноразовую БД: существующий
test harness применяет миграции и очищает таблицы. Браузерные тесты требуют
Playwright и браузер (в Windows по умолчанию Edge).

```powershell
$env:TEST_DATABASE_URL = 'postgresql://reliability:reliability@127.0.0.1:5432/reliability_test'
uv run --no-sync python -m pytest tests/application -q
```

`test_ingested_sample_quality_is_read_through_api` связывает реальную загрузку,
сохранение качества и read-маршрут API. `test_pagination.py` проверяет `limit + 1`
и SQL `LIMIT/OFFSET`. Существующий тест согласованного снимка транзакции сохранён.
