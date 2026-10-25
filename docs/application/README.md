# Уровень приложения

Основной backend читает datasets, batches и сохранённое качество из PostgreSQL.
Контракты и схема принадлежат соседним компонентам; приложение не запускает
quality checks и не вычисляет policy, аномалии или инциденты.

Документы: [архитектура backend](backend-architecture.md), [план и границы](plan.md).

## Установка и запуск

Python 3.11+. Команды из корня репозитория; ниже PowerShell.
Зависимости объявлены в корневом `pyproject.toml`, версии зафиксированы в `uv.lock`.
Отдельные списки зависимостей для backend не поддерживаются.
Инструменты тестирования находятся в dependency groups `dev` и `browser`;
`storage` остаётся extra для миграций. Группа `dev` включена uv по умолчанию.

```powershell
uv sync --locked --group dev --extra storage --group browser
docker compose -f docker-compose.yml up -d --wait postgres
$env:DATABASE_URL = 'postgresql://reliability:reliability@127.0.0.1:5432/reliability'
uv run --no-sync python -m alembic -c storage/postgres/alembic.ini upgrade head
uv run --no-sync python -m uvicorn services.backend.main:app --host 127.0.0.1 --port 8000
```

Можно использовать существующий PostgreSQL вместо Docker: задать его `DATABASE_URL`
и применить миграции коллег. Backend сам миграции не применяет.
Команды `uv run --no-sync` ниже используют окружение, подготовленное `uv sync`.
После изменения зависимостей повторите синхронизацию с нужными группами и extras.
Для запуска backend без инструментов разработки достаточно `uv sync --locked --no-dev`;
для миграций и CSV-примера добавьте `--extra storage`.
В Linux/macOS команды uv те же, переменные задаются через `export`.

Адрес приложения: <http://127.0.0.1:8000/>. API: <http://127.0.0.1:8000/docs>.
Экран datasets добавляется отдельным frontend-коммитом; backend без frontend
отвечает на `/` сообщением со ссылкой на `/docs`.

- `/health` — процесс жив; подключение к БД не требуется.
- `/ready` — соединение и чтение необходимых таблиц/колонок доступны.
- Без `DATABASE_URL` запросы данных и readiness возвращают 503; статические файлы
  и документация доступны. После изменения переменной перезапустить процесс.
- Приложение не подставляет demo при пустой или недоступной БД.

Это локальная итерация без авторизации; сервер по умолчанию запускается на loopback.

## Явное тестовое наполнение

После миграций, в отдельном терминале с той же `DATABASE_URL`:

```powershell
uv run --no-sync python -m services.backend.seed
```

Команда атомарно создаёт **тестовые** `sample-app-orders` и `sample-app-empty`.
В первом наборе две загрузки и scope `sample-window-1`, отличающийся от batch ID:
нулевой row count, неизвестная доля null и решение BLOCK. Второй dataset пустой.
При существовании любого из этих ID команда отказывается от записи: существующие
данные не изменяются. Нет автоматического наполнения или очистки при старте.

Этот seed проверяет storage → API → UI на заранее заданных измерениях.
Для примера с загрузкой CSV и вычислением качества используйте
[инструкцию backend](../../services/backend/README.md#пример-с-происхождением-данных).

## Отдельное историческое demo

```powershell
uv run --no-sync python -m uvicorn services.demo.backend.main:app --host 127.0.0.1 --port 8001
```

Адрес <http://127.0.0.1:8001/#inc-042>. Это старый пример инцидентов на JSON,
со своими API, frontend и тестами; PostgreSQL не нужен. Он не является
контрактом будущего Incident Engine и не подключается к основному приложению.

## Проверки

`TEST_DATABASE_URL` должен указывать **только на выделенную одноразовую тестовую БД**:
существующий storage harness применяет миграции и очищает таблицы перед тестами.
Не подставлять development/production URL.

```powershell
$env:TEST_DATABASE_URL = 'postgresql://reliability:reliability@127.0.0.1:5432/reliability_test'
uv run --no-sync python -m pytest tests/application/test_api.py tests/application/test_postgres.py tests/application/demo -q
uv run --no-sync python -m pytest -q
```

Корневой `docker-compose.yml` создаёт `reliability_test` при первичной инициализации
PostgreSQL на пустом томе.

Windows-браузерные тесты используют установленный Edge в headless-режиме.
Linux/macOS: `uv run --no-sync python -m playwright install chromium`.
`PLAYWRIGHT_EXECUTABLE_PATH` позволяет указать другой Chromium-браузер.
Без Playwright браузерные тесты пропускаются; без `TEST_DATABASE_URL`
пропускаются интеграционные тесты. Такие пропуски не подтверждают интеграцию.
