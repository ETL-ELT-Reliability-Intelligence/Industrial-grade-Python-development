/** Shared DOM builders and display formatting for stored quality evidence. */
export const dates = new Intl.DateTimeFormat("ru-RU", { dateStyle: "short", timeStyle: "short" });
export const numbers = new Intl.NumberFormat("ru-RU");

export function node(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  if (className) item.className = className;
  return item;
}

export function button(text, handler) {
  const item = node("button", text, "button");
  item.type = "button";
  item.addEventListener("click", handler);
  return item;
}

export function timestamp(value) {
  if (!value) return node("span", "Не указано", "muted");
  const item = node("time", dates.format(new Date(value)));
  item.dateTime = value;
  item.title = value;
  return item;
}


export function table(headers, rows) {
  const wrap = node("div", undefined, "table-wrap");
  wrap.tabIndex = 0;
  wrap.setAttribute("role", "region");
  wrap.setAttribute("aria-label", `Таблица: ${headers.join(", ")}`);
  const grid = node("table");
  const head = node("thead");
  const tr = node("tr");
  headers.forEach(label => { const th = node("th", label); th.scope = "col"; tr.append(th); });
  head.append(tr);
  const body = node("tbody");
  rows.forEach(cells => {
    const row = node("tr");
    cells.forEach(value => { const cell = node("td"); cell.append(value instanceof Node ? value : node("span", String(value))); row.append(cell); });
    body.append(row);
  });
  grid.append(head, body); wrap.append(grid);
  return wrap;
}


const labels = {
  row_count: "Число строк", null_rate: "Доля null", min_count: "Минимум строк", max_count: "Максимум строк",
  max_null_rate: "Максимальная доля null", fields: "Поля схемы", name: "Имя", data_type: "Тип данных", allow_extra_fields: "Дополнительные поля разрешены",
  value: "Значение", minimum: "Нижняя граница", maximum: "Верхняя граница", measurements: "Измерения",
  distinct_count: "Различных непустых значений", distinct_ratio: "Доля различных значений",
  sample_count: "Строк в выборке", non_null_count: "Непустых значений", null_count: "Пропусков",
  invalid_count: "Нарушений", invalid_rate: "Доля нарушений", evaluated_count: "Проверено значений",
  valid_count: "Распознано значений", parse_errors: "Ошибок преобразования", frequencies: "Частоты",
  dataset_id: "Dataset", version: "Версия", checks: "Проверки", key: "Ключ", kind: "Тип", column: "Колонка",
  required: "Обязательная", params: "Параметры", retry: "Повторы", max_attempts: "Максимум попыток",
  delays_seconds: "Задержки между попытками (сек)", on_fail: "Реакция на FAIL", on_unknown: "Реакция на UNKNOWN",
  on_insufficient_history: "Реакция при недостатке истории", on_missing_schema: "Реакция при отсутствии схемы",
  implementation_version: "Версия реализации", mode: "Режим", history_window: "Размер истории",
  min_history: "Минимум исторических наблюдений", batch_ids: "Эталонные загрузки", median: "Медиана",
};

export const checkNames = { row_count: "Аномальный объём", null_rate: "Пропуски", schema: "Схема",
  cardinality: "Изменение кардинальности", distribution: "Сдвиг распределения", value_rule: "Правило значений" };

export function evidence(value) {
  if (value === null || value === undefined) return node("span", "Нет значения", "muted");
  if (Array.isArray(value)) {
    if (!value.length) return node("span", "Пустой список", "muted");
    const list = node("ul");
    value.forEach(item => { const li = node("li"); li.append(evidence(item)); list.append(li); });
    return list;
  }
  if (typeof value === "object") {
    const dl = node("dl", undefined, "evidence");
    for (const [key, item] of Object.entries(value)) {
      const dd = node("dd");
      dd.append(evidence(item));
      dl.append(node("dt", labels[key] || key), dd);
    }
    return dl;
  }
  return node("span", typeof value === "number" ? numbers.format(value) : typeof value === "boolean" ? (value ? "Да" : "Нет") : String(value));
}

export function badge(status) { return node("span", status, `badge status-${status.toLowerCase()}`); }

