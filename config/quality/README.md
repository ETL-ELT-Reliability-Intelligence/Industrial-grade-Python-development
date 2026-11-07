# Настройка качества

Конфигурация объединяется в порядке defaults.yaml → profiles/<profile>.yaml → datasets/<dataset>.yaml.
checks добавляет правила, overrides меняет унаследованные. Неизвестные ключи, неверные типы,
дубли YAML и несогласованные границы отклоняются при старте. После изменения конфигурации
перезапустите stream processor. Повышайте version при изменении правил.

Профиль standard проверяет схему и исторический объём. Правила колонок настроены отдельно
для Olist, mock_orders и ingested-app-orders. Проверок ID и уникальности нет.

## Проверки

| kind | Основные параметры |
| --- | --- |
| row_count | mode: historical/fixed; для истории relative_tolerance, absolute_tolerance, mad_multiplier; для fixed min_count, max_count |
| null_rate | mode: historical/fixed; рост: min_increase в долях, min_samples; fixed: max_null_rate; условная выборка: where_column и where_value вместе |
| cardinality | min_samples, volume_tolerance, count_tolerance, ratio_tolerance |
| distribution | data_type: numeric/categorical, min_samples, max_distance; для numeric обязательны строго возрастающие конечные bins |
| value_rule | rule: non_negative/datetime/date_order/aware_datetime/not_future, allow_null, max_invalid_rate; для date_order обязательна other_column |
| schema | fields, allow_extra_fields; без fields используется сохранённая схема |

Все исторические проверки имеют history_window (по умолчанию 10) и min_history (3).
Для null_rate, cardinality, distribution и value_rule обязательна column.
date_order требует, чтобы значение column было не раньше other_column.
Для mock_orders `aware_datetime` требует дату со временем и часовым поясом,
а `not_future` запрещает даты позже сохранённого `received_at` партии.
allow_null: true исключает пустые значения/пары из знаменателя предметного правила.
Если ни одного значения проверить нельзя, получается UNKNOWN, а не PASS.

Пример правила:

```yaml
dataset_id: order_items
version: 2
profile: standard
checks:
  price.non_negative:
    kind: value_rule
    column: price
    params:
      rule: non_negative
    on_fail: BLOCK
  price.null_growth:
    kind: null_rate
    column: price
    params:
      mode: historical
      min_increase: 0.05
      min_samples: 30
    on_fail: WARN
```

Числа Olist преобразуются явно для числовых проверок; raw не меняется. Ошибочные числа,
NaN/Infinity и bool не считаются допустимыми денежными значениями. Пустые CSV-ячейки
преобразуются в null. Почтовые коды не приводятся к числам.

## История и реакции

Сопоставляются источник, гранулярность, версия схемы, статистики и хеш конфигурации.
Выбираются только предыдущие периоды, без будущих поступлений относительно проверяемой
загрузки. Последняя доставка периода заменяет предыдущую даже при нарушениях качества.
Повторная доставка события не добавляет профиль. Профиль с FAIL, UNKNOWN, ERROR или
неполным окном не допускается в эталон. Начальные пропуски insufficient_history и
missing_schema допускаются, если остальные проверки прошли. Это позволяет накопить эталон.
Для кардинальности дополнительный отбор по числу непустых значений выполняется до
ограничения history_window.

Изменение конфигурации начинает новую сопоставимую историю. Чтобы использовать старые raw,
их нужно переоценить с новой конфигурацией в порядке периодов. stats_version сейчас v2.
Поиск профилей пока читает историю одного dataset целиком; для больших историй потребуется
перенос фильтров/выборки в SQL. Исходные файлы истории повторно не читаются.

В поставляемых настройках статистический FAIL → WARN, нарушение предметного правила или
схемы → BLOCK. UNKNOWN → WARN. Нехватка истории, малая выборка и неполное окно →
on_insufficient_history (WARN по умолчанию). Отсутствие обязательной колонки и ошибка
числового разбора в distribution → BLOCK для обязательной проверки.

Пороги — стартовые настройки, требующие калибровки на реальной истории. Три месяца демо
не дают сезонную модель. Месяцы сравниваются без нормализации на число дней.
Срез Olist считается полным по контракту источника; полноту оригинального CSV по строкам
установить нельзя. complete=false в событии исключает окно из исторических сравнений.

Повторы задаются через retry.max_attempts и retry.delays_seconds и применяются только
к временным техническим ошибкам. FAIL и недостаток истории не повторяются автоматически.
План фиксирует конфигурацию, наблюдения, пороги и состав эталона. Явная переоценка:

```sh
python -m services.stream_processor.reevaluate EVALUATION_ID --request-id review-2
```

## Обновление существующей базы

Миграция 0003 **удаляет старые оценки, результаты и решения качества**, а также метрику
freshness_seconds. Совместимость со старым форматом не поддерживается. Raw, batches,
схемы и остальные скалярные метрики сохраняются; прежние метрики не используются как новый
эталон. Добавляется таблица dataset_profiles. Откат требует восстановления резервной копии.
Перед запуском новой версии выполните:

```sh
alembic -c storage/postgres/alembic.ini upgrade head
```

Старые EVALUATION_ID после миграции отсутствуют: для них нужно повторно подать события
загрузки, затем применять обычную переоценку. Старые mock-события с dataset_id=orders
не следует воспроизводить: mock теперь использует mock_orders.

BLOCK сохраняет результат мониторинга; DAG пока не ждёт решения перед dbt.
