import { node, evidence, badge, checkNames } from "./ui.js";

const parameterNames = {
  mode: "Режим", history_window: "Размер истории", min_history: "Минимум исторических наблюдений",
  mad_multiplier: "Множитель MAD", relative_tolerance: "Относительный допуск",
  absolute_tolerance: "Абсолютный допуск", min_count: "Минимум строк", max_count: "Максимум строк",
  max_null_rate: "Максимальная доля пропусков", min_increase: "Минимальный рост доли пропусков",
  min_samples: "Минимум наблюдений", where_column: "Колонка условия", where_value: "Значение условия",
  volume_tolerance: "Допуск изменения объёма", count_tolerance: "Допуск числа различных значений",
  ratio_tolerance: "Допуск доли различных значений", data_type: "Тип данных",
  bins: "Границы интервалов", max_distance: "Максимальное расстояние",
  rule: "Правило", other_column: "Колонка сравнения", allow_null: "Пропуски разрешены",
  max_invalid_rate: "Максимальная доля нарушений", fields: "Поля схемы",
  allow_extra_fields: "Дополнительные поля разрешены",
};
const reactionNames = {
  on_fail: "При FAIL", on_unknown: "При UNKNOWN",
  on_insufficient_history: "При недостатке истории", on_missing_schema: "При отсутствии схемы",
};
const checkFields = new Set([
  "key", "kind", "column", "required", "implementation_version", "params", ...Object.keys(reactionNames),
]);
const policyStatuses = new Set(["PASS", "WARN", "BLOCK"]);
const owns = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const isObject = value => value !== null && typeof value === "object" && !Array.isArray(value);

function scalar(value) {
  if (value === null || value === undefined || typeof value === "boolean") return evidence(value);
  // Configuration numbers retain their saved precision instead of locale rounding.
  return node("span", value === "" ? "Пустая строка" : String(value), "configuration-value");
}

function missingValue() { return node("span", "Не сохранено", "configuration-empty"); }

function compactValues(values) {
  if (!values.length) return node("span", "Пустой список", "configuration-empty");
  const line = node("span", undefined, "configuration-values");
  values.forEach((value, index) => {
    const item = node("span", undefined, "configuration-value-item");
    item.append(scalar(value));
    if (index < values.length - 1) item.append(node("span", ", "));
    line.append(item);
  });
  return line;
}

function structuredValue(value) {
  const details = node("details", undefined, "configuration-extra configuration-structured");
  details.append(node("summary", "Составное значение"), node("pre", JSON.stringify(value, null, 2), "configuration-raw"));
  return details;
}

function fieldTable(fields) {
  const region = node("div", undefined, "configuration-fields");
  region.tabIndex = 0;
  region.setAttribute("role", "region");
  region.setAttribute("aria-label", "Поля сохранённой схемы");
  const table = node("table", undefined, "configuration-field-table");
  const head = node("thead");
  const header = node("tr");
  const extra = fields.some(field => Object.keys(field).some(key => key !== "name" && key !== "data_type"));
  for (const label of ["Поле", "Тип данных", ...(extra ? ["Другие свойства"] : [])]) {
    const cell = node("th", label);
    cell.scope = "col";
    header.append(cell);
  }
  head.append(header);
  const body = node("tbody");
  for (const field of fields) {
    const row = node("tr");
    for (const key of ["name", "data_type"]) {
      const cell = node("td");
      cell.append(owns(field, key) ? valueView(field[key]) : node("span", "Не сохранено", "configuration-empty"));
      row.append(cell);
    }
    if (extra) {
      const cell = node("td");
      const values = Object.entries(field).filter(([key]) => key !== "name" && key !== "data_type");
      cell.append(values.length ? properties(values) : node("span", "Нет", "configuration-empty"));
      row.append(cell);
    }
    body.append(row);
  }
  table.append(head, body);
  region.append(table);
  return region;
}

function valueView(value, key) {
  if (Array.isArray(value)) {
    if (key === "fields" && value.length && value.every(isObject)) {
      const details = node("details", undefined, "configuration-fields-disclosure");
      details.append(node("summary", `Поля схемы (${value.length})`), fieldTable(value));
      return details;
    }
    if (value.every(item => item === null || typeof item !== "object")) return compactValues(value);
    return structuredValue(value);
  }
  return isObject(value) ? structuredValue(value) : scalar(value);
}

function identifierView(value) {
  if (typeof value !== "string") return valueView(value);
  if (!value.length) return scalar(value);
  const identifier = node("span", undefined, "configuration-identifier");
  for (const part of value.match(/[^._]+[._]?|[._]/g) ?? []) {
    identifier.append(node("span", part, "configuration-identifier-part"), node("wbr"));
  }
  return identifier;
}

function properties(entries, names = {}, className = "", reactions = false) {
  const list = node("dl", undefined, `configuration-properties ${className}`.trim());
  for (const [key, value] of entries) {
    const label = node("dt", owns(names, key) ? names[key] : key);
    label.title = key;
    const definition = node("dd");
    definition.dataset.configKey = key;
    definition.append(value === undefined ? missingValue()
      : reactions && owns(reactionNames, key) && policyStatuses.has(value) ? badge(value) : valueView(value, key));
    list.append(label, definition);
  }
  return list;
}

function section(title, content, className = "") {
  const part = node("section", undefined, `configuration-section ${className}`.trim());
  part.append(node("h4", title, "configuration-section-heading"), content);
  return part;
}

function additionalFields(value, knownFields) {
  const entries = Object.entries(value).filter(([key]) => !knownFields.has(key));
  if (!entries.length) return null;
  const details = node("details", undefined, "configuration-extra");
  details.append(node("summary", "Дополнительные сохранённые поля"), properties(entries));
  return details;
}

function checkView(spec, index) {
  const row = node("tr", undefined, "configuration-check");
  const identity = node("td", undefined, "configuration-identity");
  if (!isObject(spec)) {
    identity.append(node("strong", `Проверка ${index + 1}`, "configuration-check-key"));
    const content = node("td");
    content.colSpan = 5;
    content.append(valueView(spec));
    row.append(identity, content);
    return row;
  }
  const key = node("strong", undefined, "configuration-check-key");
  key.append(owns(spec, "key") ? identifierView(spec.key) : missingValue());
  identity.append(key);
  if (typeof spec.key === "string") row.dataset.checkKey = spec.key;
  if (owns(spec, "kind")) {
    const kind = typeof spec.kind === "string" ? spec.kind : JSON.stringify(spec.kind);
    const label = node("p", owns(checkNames, kind) ? checkNames[kind] : kind, "configuration-check-kind");
    label.title = kind;
    identity.append(label);
  } else {
    identity.append(missingValue());
  }
  const column = node("td", undefined, "configuration-column");
  if (owns(spec, "column")) {
    column.append(spec.column === null ? node("span", "Весь dataset") : identifierView(spec.column));
  } else {
    column.append(missingValue());
  }
  const parameters = node("td", undefined, "configuration-parameters-cell");
  const params = isObject(spec.params)
    ? Object.entries(spec.params).length
      ? properties(Object.entries(spec.params), parameterNames, "configuration-parameters")
      : node("p", "Нет сохранённых параметров.", "configuration-empty")
    : owns(spec, "params") ? valueView(spec.params) : missingValue();
  parameters.append(params);
  row.append(identity, column, parameters);
  for (const [field, className] of [["on_fail", "configuration-fail"], ["on_unknown", "configuration-unknown"]]) {
    const cell = node("td", undefined, className);
    cell.append(!owns(spec, field) ? missingValue()
      : policyStatuses.has(spec[field]) ? badge(spec[field]) : valueView(spec[field]));
    row.append(cell);
  }
  const extraCell = node("td", undefined, "configuration-extra-cell");
  const details = node("details", undefined, "configuration-check-properties");
  details.append(node("summary", "Свойства"));
  const metadata = ["required", "implementation_version", "on_insufficient_history", "on_missing_schema"]
    .map(field => [field, owns(spec, field) ? spec[field] : undefined]);
  details.append(properties(metadata, {
    required: "Обязательная", implementation_version: "Версия реализации", ...reactionNames,
  }, "configuration-check-metadata", true));
  const extra = additionalFields(spec, checkFields);
  if (extra) details.append(extra);
  extraCell.append(details);
  row.append(extraCell);
  return row;
}

function checksTable(checks) {
  const region = node("div", undefined, "configuration-table-region");
  region.tabIndex = 0;
  region.setAttribute("role", "region");
  region.setAttribute("aria-label", "Проверки в конфигурации");
  const table = node("table", undefined, "configuration-check-table");
  const columns = node("colgroup");
  for (const name of ["identity", "column", "parameters", "fail", "unknown", "extra"]) {
    columns.append(node("col", undefined, `configuration-col-${name}`));
  }
  const head = node("thead");
  const header = node("tr");
  for (const label of ["Проверка", "Колонка", "Параметры", "При FAIL", "При UNKNOWN", "Дополнительно"]) {
    const cell = node("th", label);
    cell.scope = "col";
    header.append(cell);
  }
  head.append(header);
  const body = node("tbody");
  checks.forEach((spec, index) => body.append(checkView(spec, index)));
  table.append(columns, head, body);
  region.append(table);
  return region;
}

/** Render the saved suite only; execution defaults and policy stay upstream. */
export function configurationView(evaluation) {
  const root = node("section", undefined, "evaluation-configuration");
  root.setAttribute("aria-label", "Сохранённая конфигурация расчёта");
  const suite = evaluation?.suite;
  if (!isObject(suite)) {
    root.append(node("p", "Конфигурация не сохранена.", "configuration-empty"));
    if (suite !== undefined) root.append(valueView(suite));
    return root;
  }
  const header = node("div", undefined, "configuration-header");
  const overview = Object.entries(suite).filter(([key]) => key === "dataset_id" || key === "version");
  if (Array.isArray(suite.checks)) overview.push(["check_count", suite.checks.length]);
  header.append(properties(overview, {
    dataset_id: "Dataset", version: "Версия конфигурации", check_count: "Количество проверок",
  }, "configuration-overview"));
  if (owns(suite, "retry")) {
    const retry = isObject(suite.retry)
      ? properties(Object.entries(suite.retry), {
        max_attempts: "Максимум попыток", delays_seconds: "Задержки между попытками, сек",
      }, "configuration-retry")
      : valueView(suite.retry);
    header.append(section("Повторы", retry));
  }
  root.append(header);
  if (owns(evaluation, "config_hash")) {
    const fingerprint = node("p", undefined, "configuration-fingerprint");
    fingerprint.append(node("span", "Хеш конфигурации: "));
    fingerprint.append(typeof evaluation.config_hash === "string"
      ? node("code", evaluation.config_hash) : valueView(evaluation.config_hash));
    root.append(fingerprint);
  }
  const checks = node("div", undefined, "configuration-checks");
  if (Array.isArray(suite.checks)) {
    checks.append(checksTable(suite.checks));
    if (!suite.checks.length) checks.append(node("p", "Пустой список проверок.", "configuration-empty"));
  } else {
    checks.append(node("p", "Список проверок не сохранён.", "configuration-empty"));
    if (owns(suite, "checks")) checks.append(valueView(suite.checks));
  }
  root.append(checks);
  const extra = additionalFields(suite, new Set(["dataset_id", "version", "retry", "checks"]));
  if (extra) root.append(extra);
  return root;
}
