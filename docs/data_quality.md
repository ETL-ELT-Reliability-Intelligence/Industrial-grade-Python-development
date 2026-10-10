# Детерминированные проверки качества

`data_quality` — общая библиотека для batch- и stream-компонентов.
Она принимает модели из `contracts.quality`, не обращается к источникам,
не читает системные часы и не сохраняет результаты. Контракты используют Pydantic v2.
Модели создаются с именованными аргументами; они неизменяемы, строго проверяют типы,
запрещают лишние поля и проверяют значения по умолчанию. Ошибки контракта вызывают
`pydantic.ValidationError` (подкласс `ValueError`).

## Проверки

Все функции принимают `observation`, `constraint`, обязательный именованный
`check_id` и необязательный `context`. Возвращается `CheckResult` с исходными
наблюдением, ограничением и контекстом.

| Функция | Условие PASS | Причины FAIL |
| --- | --- | --- |
| `check_freshness` | Возраст на момент `evaluated_at` не превышает `max_age` | `threshold_exceeded` |
| `check_null_rate` | Доля null не превышает `max_null_rate` | `threshold_exceeded` |
| `check_row_count` | Количество находится внутри включительных min/max | `below_minimum`, `above_maximum` |
| `check_schema` | Ожидаемые поля присутствуют, типы совпадают, дополнительные поля разрешены либо отсутствуют | `schema_mismatch` |

Успешная проверка возвращает код `requirement_met`. Отсутствующее наблюдение
(`None`) даёт `UNKNOWN` с кодом `missing_observation`. Ноль строк, нулевая
доля null и пустая схема — доступные наблюдения, которые проверяются обычно.
Для пустой выборки вызывающий компонент передаёт null rate как `None`, если
доля не определена. Ошибки входных значений отклоняются валидацией контрактов.

Freshness использует разницу UTC-моментов переданных дат. Все границы
включительны. Схема плоская, порядок полей не важен, имена и типы сравниваются
точно, с учётом регистра. Нормализация типов — обязанность вызывающего кода.
Все расхождения схемы возвращаются в `details`, отсортированных по имени поля:
`missing_field`, `unexpected_field`, `type_mismatch`.

## Policy

`evaluate_quality(results, rules=())` принимает iterable результатов и настроек
`PolicyRule`. Настройки сопоставляются по `check_id`:

- PASS всегда остаётся PASS.
- FAIL по умолчанию даёт BLOCK; `on_fail` может изменить реакцию на WARN.
- UNKNOWN по умолчанию даёт WARN; `on_unknown` может изменить реакцию на BLOCK.
- Итог определяется приоритетом BLOCK > WARN > PASS.
- Пустой набор даёт WARN с причиной `no_checks`.

Повторяющиеся check_id внутри результатов или настроек вызывают `ValueError`.
Неиспользованные настройки допустимы; для результата без настройки действуют
значения по умолчанию. Причины решения сохраняют порядок результатов,
ссылаются на check_id и объясняют применённую реакцию. Policy не пересчитывает
метрики и не исполняет решение.

Вызывающий компонент собирает полный набор результатов для одной области
оценки. Policy не сопоставляет окна, не проверяет полноту ожидаемого набора
и не ждёт запаздывающие результаты. Контекст передаётся проверками без изменений.

## Пример на готовых наблюдениях

```python
from datetime import datetime, timedelta, timezone

from contracts.quality import (
    FreshnessConstraint, FreshnessObservation,
    NullRateConstraint, NullRateObservation, PolicyRule, QualityContext,
    QualityStatus, RowCountConstraint, RowCountObservation,
    SchemaConstraint, SchemaField, SchemaObservation,
)
from data_quality.checks import (
    check_freshness, check_null_rate, check_row_count, check_schema,
)
from data_quality.policies import evaluate_quality

context = QualityContext(dataset_id="orders", scope_id="batch-42")
evaluated_at = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
fields = (
    SchemaField(name="id", data_type="integer"),
    SchemaField(name="comment", data_type="string"),
)

results = [
    check_freshness(
        FreshnessObservation(
            last_updated_at=evaluated_at - timedelta(minutes=5), evaluated_at=evaluated_at,
        ),
        FreshnessConstraint(max_age=timedelta(minutes=10)),
        check_id="orders.freshness", context=context,
    ),
    check_null_rate(
        NullRateObservation(null_rate=0.15), NullRateConstraint(max_null_rate=0.1),
        check_id="orders.comment.null_rate",
        context=QualityContext(dataset_id="orders", scope_id="batch-42", column="comment"),
    ),
    check_row_count(
        RowCountObservation(row_count=100), RowCountConstraint(min_count=1),
        check_id="orders.row_count", context=context,
    ),
    check_schema(
        SchemaObservation(fields=fields), SchemaConstraint(fields=fields),
        check_id="orders.schema", context=context,
    ),
]
decision = evaluate_quality(
    results,
    [PolicyRule(check_id="orders.comment.null_rate", on_fail=QualityStatus.WARN)],
)
assert decision.status is QualityStatus.WARN
```

Тот же вызов доступен любому компоненту, подготовившему эти наблюдения.
Проверки не определяют расписание, происхождение порога или дальнейшую
маршрутизацию данных.

## Тесты

Тесты повторяют структуру пакетов проекта: `tests/contracts/`,
`tests/data_quality/checks/` и `tests/data_quality/policies/`.
Общее поведение проверок покрывается в `checks/test_common.py`.

Из корня проекта: `python -m pytest -q`.
На Windows с Python Launcher: `py -3.11 -m pytest -q`.
