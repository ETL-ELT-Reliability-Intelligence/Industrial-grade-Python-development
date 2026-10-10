# Уровень приложения: backend и frontend

Документация и запуск части проекта в `services/backend` и `services/frontend`.

- [План и границы ответственности](./plan.md) — последовательность итераций и зависимости от команды.
- [Архитектура backend](./backend-architecture.md) — текущие границы модулей и подключение реальных источников в следующих итерациях.
- Ниже — запуск и возможности первой реализации.

## Запуск первой итерации

Требуется Python 3.11+; можно использовать `uv` или стандартный `pip`. Все команды выполняются из корня репозитория. Зависимости этой части находятся в `services/backend/requirements*.txt`; общий корневой `pyproject.toml` пока не входит в изменения для коммита.

```powershell
uv venv .venv
uv pip install --python .venv/Scripts/python.exe -r services/backend/requirements-dev.txt
.venv/Scripts/python.exe -m uvicorn services.backend.main:app --reload --host 127.0.0.1 --port 8000
```

Dashboard: <http://127.0.0.1:8000/>. Swagger/OpenAPI: <http://127.0.0.1:8000/docs>.

Без `uv` можно использовать обычное виртуальное окружение:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r services/backend/requirements-dev.txt
.venv/Scripts/python.exe -m uvicorn services.backend.main:app --host 127.0.0.1 --port 8000
```

## Реализованный сценарий

1. Открыть обзор: два активных инцидента, один критический, четыре затронутых dataset и один закрытый инцидент. Счётчики относятся ко всему набору данных, фильтр меняет только список.
2. Открыть `inc-042`: успешная загрузка orders сопровождается снижением row_count с 4,5 млн до 1,8 млн строк.
3. Сравнить наблюдения в orders, fct_orders и revenue, посмотреть хронологию и гипотезу причины.
4. Посмотреть потенциальный impact: он может включать dataset без подтверждённой аномалии, например customer_ltv. Оценка RCA — score ранжирования, а не доказанная причинность.
5. Переключить фильтр на закрытые инциденты и открыть `inc-040`.

URL карточки сохраняется в hash (`/#inc-042`); доступны загрузка, пустой список, ошибки API и повтор запроса. Интерфейс адаптируется к узкому экрану. Время показывается в часовом поясе браузера; API отдаёт timestamps с UTC offset.

## Контракт первой версии

| Метод и путь | Назначение |
| --- | --- |
| `GET /health` | Жив ли процесс; это не readiness проверки БД |
| `GET /api/v1/overview` | Агрегаты готовых инцидентов и время снимка |
| `GET /api/v1/incidents?status=open` | Список; status: open/investigating/resolved; без фильтра — все |
| `GET /api/v1/incidents/{id}` | Детали, observations, root_cause_candidates, timeline и potential impact |

Список возвращает `items`, `total`, `data_mode`, `snapshot_at`. Неизвестный ID — HTTP 404 с `{"detail":"Incident not found"}`; неподдерживаемый status — стандартный FastAPI HTTP 422. Точные схемы доступны в `/openapi.json`. Пагинация, изменение статуса, dataset API и investigation ещё не реализованы.

## Где менять

| Путь | Ответственность |
| --- | --- |
| `services/backend/main.py` | Сборка приложения, выбор адаптера и раздача frontend |
| `services/backend/api.py` | HTTP endpoints, валидация запросов и HTTP-ошибки |
| `services/backend/queries.py` | Чтение инцидентов, фильтр и сборка обзора |
| `services/backend/models.py` | Типизированные публичные ответы |
| `services/backend/repository.py` | Граница чтения данных; следующая итерация добавит PostgreSQL adapter |
| `services/backend/demo.json` | Единственный источник демонстрационного сценария |
| `services/frontend/` | Экран, стили и API client; расчёта RCA/impact здесь нет |
| `tests/application/test_api.py` | Связность ответов, фильтр, ошибки и доступность frontend |
| `tests/application/test_dashboard.py` | Проверки пользовательского сценария в браузере |

Данные синтетические, фиксированные, загружаются при старте backend. PostgreSQL, Kafka, Agent и авторизация не подключены. Контракт — начальное предложение уровня приложения; интеграцию согласуем с владельцами источников данных. Интерфейс пока оформлен простым CSS в духе Windows XP и веб-страниц начала 2000-х; приоритет — архитектура backend и работа сценариев.

## Проверка

```powershell
.venv/Scripts/python.exe -m pytest tests/application/test_api.py -q
node --check services/frontend/app.js
```

`node` нужен только для необязательной проверки синтаксиса JavaScript; frontend работает без Node.js и сборки.

Для браузерных проверок:

```powershell
uv pip install --python .venv/Scripts/python.exe -r services/backend/requirements-browser.txt
.venv/Scripts/python.exe -m pytest tests/application -q
```

На Windows тесты используют установленный Microsoft Edge в headless-режиме. На других ОС путь к Python в командах — `.venv/bin/python`; сначала выполнить `.venv/bin/python -m playwright install chromium`. Другой Chromium-браузер можно указать через `PLAYWRIGHT_EXECUTABLE_PATH`. Без Playwright браузерные тесты пропускаются. Все API-запросы браузерных тестов обслуживаются локальным ASGI-приложением, отдельный сервер не нужен.

Подход опирается на официальные руководства FastAPI: [раздача статических файлов](https://fastapi.tiangolo.com/tutorial/static-files/) и [тестирование API](https://fastapi.tiangolo.com/tutorial/testing/).
