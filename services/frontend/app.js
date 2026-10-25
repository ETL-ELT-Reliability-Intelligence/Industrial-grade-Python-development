"use strict";

const PAGE_SIZE = 50;
const panels = Object.fromEntries(["catalog", "dataset", "batches", "scopes", "quality"].map(key => [key, document.getElementById(key)]));
const pending = new Map();
const offsets = { catalog: 0, batches: 0, scopes: 0 };
let selection = readSelection();
const dates = new Intl.DateTimeFormat("ru-RU", { dateStyle: "short", timeStyle: "short" });
const numbers = new Intl.NumberFormat("ru-RU");

function node(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  if (className) item.className = className;
  return item;
}

function button(text, handler) {
  const item = node("button", text, "button");
  item.type = "button";
  item.addEventListener("click", handler);
  return item;
}

function timestamp(value) {
  if (!value) return node("span", "Не указано", "muted");
  const item = node("time", dates.format(new Date(value)));
  item.dateTime = value;
  item.title = value;
  return item;
}

function readSelection() {
  const params = new URLSearchParams(location.search);
  return { dataset: params.get("dataset"), scope: params.get("scope") };
}

function select(dataset, scope = null) {
  const url = new URL(location.href);
  url.search = "";
  if (dataset) url.searchParams.set("dataset", dataset);
  if (dataset && scope) url.searchParams.set("scope", scope);
  history.pushState(null, "", url);
  applySelection();
}

function applySelection() {
  const previous = selection;
  selection = readSelection();
  if (selection.dataset !== previous.dataset) {
    offsets.batches = offsets.scopes = 0;
    loadDataset(); loadBatches(); loadScopes();
  }
  markSelection();
  loadQuality();
}

function markSelection() {
  document.querySelectorAll("[data-dataset], [data-scope]").forEach(item => {
    const selected = item.hasAttribute("data-dataset")
      ? item.dataset.dataset === selection.dataset : item.dataset.scope === selection.scope;
    item.classList.toggle("selected", selected);
    item.setAttribute("aria-pressed", String(selected));
  });
}

function cancel(key) {
  pending.get(key)?.abort();
  pending.delete(key);
}

function message(key, text) {
  cancel(key);
  panels[key].replaceChildren(node("p", text, "message"));
  panels[key].setAttribute("aria-busy", "false");
}

async function load(key, path, render, retry) {
  cancel(key);
  const controller = new AbortController();
  pending.set(key, controller);
  const root = panels[key];
  root.setAttribute("aria-busy", "true");
  root.replaceChildren(node("p", "Загрузка…", "message"));
  try {
    const response = await fetch(`/api/v1${path}`, { signal: controller.signal });
    if (!response.ok) {
      const descriptions = { 404: "Dataset не найден", 503: "Хранилище недоступно. Проверьте подключение backend к PostgreSQL" };
      throw new Error(descriptions[response.status] || `Ошибка API (${response.status})`);
    }
    const data = await response.json();
    if (pending.get(key) !== controller) return;
    root.replaceChildren();
    render(data, root);
    markSelection();
    document.getElementById("refreshed-at").textContent = `Последний успешный ответ API: ${dates.format(new Date())}. Это время загрузки экрана, а не свежесть исходных данных.`;
  } catch (error) {
    if (controller.signal.aborted || pending.get(key) !== controller) return;
    const box = node("div", undefined, "message error");
    box.setAttribute("role", "alert");
    box.append(node("p", error instanceof TypeError ? "Не удалось связаться с API." : error.message), button("Повторить", retry));
    root.replaceChildren(box);
  } finally {
    if (pending.get(key) === controller) {
      root.setAttribute("aria-busy", "false");
      pending.delete(key);
    }
  }
}

function pagination(root, data, key, reload) {
  const bar = node("nav", undefined, "pager");
  bar.setAttribute("aria-label", "Страницы списка");
  const previous = button("Назад", () => { offsets[key] = Math.max(0, data.offset - data.limit); reload(); });
  const next = button("Далее", () => { offsets[key] = data.offset + data.limit; reload(); });
  previous.disabled = data.offset === 0;
  next.disabled = !data.has_more;
  bar.append(previous, node("span", `Страница ${Math.floor(data.offset / data.limit) + 1}`), next);
  root.append(bar);
}

function listPath(path, key) { return `${path}?limit=${PAGE_SIZE}&offset=${offsets[key]}`; }
function datasetPath() { return `/datasets/${encodeURIComponent(selection.dataset)}`; }

function table(headers, rows) {
  const wrap = node("div", undefined, "table-wrap");
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

function loadCatalog() {
  return load("catalog", listPath("/datasets", "catalog"), (data, root) => {
    for (const item of data.items) {
      const entry = button(item.dataset_id, () => select(item.dataset_id));
      entry.className = "catalog-item incident-row";
      entry.setAttribute("aria-label", item.dataset_id);
      entry.dataset.dataset = item.dataset_id;
      const registered = node("span", "Зарегистрирован: ", "incident-time");
      registered.append(timestamp(item.created_at));
      entry.replaceChildren(node("strong", item.dataset_id, "incident-title"), registered);
      root.append(entry);
    }
    if (!data.items.length) root.append(node("p", "На этой странице нет зарегистрированных datasets.", "message"));
    pagination(root, data, "catalog", loadCatalog);
  }, loadCatalog);
}

function loadDataset() {
  if (!selection.dataset) return message("dataset", "Выберите dataset в каталоге.");
  return load("dataset", datasetPath(), (data, root) => {
    root.append(node("h2", data.dataset_id, "detail-heading"));
    const registered = node("p", "Зарегистрирован: ", "muted");
    registered.append(timestamp(data.created_at)); root.append(registered);
    if (data.dataset_id.startsWith("sample-app-")) {
      const banner = node("div", undefined, "demo-banner");
      banner.append(node("strong", "Тестовые данные"), node("span", "Добавлены вручную для просмотра приложения."));
      root.append(banner);
    }
  }, loadDataset);
}

function loadBatches() {
  if (!selection.dataset) return message("batches", "Сначала выберите dataset.");
  return load("batches", listPath(`${datasetPath()}/batches`, "batches"), (data, root) => {
    if (data.items.length) root.append(table(["Batch ID", "Строк", "Получен", "Версия схемы", "Источник"], data.items.map(item => [
      item.batch_id, numbers.format(item.records), timestamp(item.received_at), item.schema_version,
      item.object_key ?? "Не указан",
    ])));
    else root.append(node("p", "На этой странице нет сохранённых загрузок.", "message"));
    pagination(root, data, "batches", loadBatches);
  }, loadBatches);
}

function loadScopes() {
  if (!selection.dataset) return message("scopes", "Сначала выберите dataset.");
  return load("scopes", listPath(`${datasetPath()}/quality-scopes`, "scopes"), (data, root) => {
    if (data.items.length) root.append(table(["Scope ID", "Последняя сохранённая проверка / решение"], data.items.map(item => {
      const choice = button(item.scope_id, () => select(selection.dataset, item.scope_id));
      choice.dataset.scope = item.scope_id;
      return [choice, timestamp(item.last_saved_at)];
    })));
    else root.append(node("p", "На этой странице нет областей с сохранёнными результатами качества.", "message"));
    pagination(root, data, "scopes", loadScopes);
  }, loadScopes);
}

const labels = {
  row_count: "Число строк", null_rate: "Доля null", min_count: "Минимум строк", max_count: "Максимум строк",
  max_null_rate: "Максимальная доля null", last_updated_at: "Последнее обновление", evaluated_at: "Момент проверки",
  max_age: "Допустимый возраст (ISO 8601)", fields: "Поля схемы", name: "Имя", data_type: "Тип данных", allow_extra_fields: "Дополнительные поля разрешены",
};

function evidence(value) {
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
      dd.append(item && ["last_updated_at", "evaluated_at"].includes(key) ? timestamp(item) : evidence(item));
      dl.append(node("dt", labels[key] || key), dd);
    }
    return dl;
  }
  return node("span", typeof value === "number" ? numbers.format(value) : typeof value === "boolean" ? (value ? "Да" : "Нет") : String(value));
}

function badge(status) { return node("span", status, `badge status-${status.toLowerCase()}`); }

function renderQuality(data, root) {
  root.append(node("p", `Scope: ${data.scope_id}`, "scope-label"));
  const decision = node("div", undefined, "decision cause");
  decision.append(node("h3", "Сохранённое решение policy"));
  if (data.decision) {
    decision.append(badge(data.decision.status));
    const at = node("p", "Время решения: ", "muted"); at.append(timestamp(data.decided_at)); decision.append(at);
    const reasons = node("ul");
    data.decision.reasons.forEach(reason => reasons.append(node("li", `${reason.code}${reason.check_id ? ` · ${reason.check_id}` : ""}: ${reason.message}`)));
    decision.append(reasons);
  } else decision.append(node("p", "Решение ещё не сохранено. Это не PASS.", "message"));
  root.append(decision, node("h3", "Проверки"), node("p", "PASS — соответствует; FAIL — не соответствует; UNKNOWN — недостаточно наблюдений. Решения policy: PASS, WARN или BLOCK.", "muted"));
  if (!data.checks.length) { root.append(node("p", "Проверки для этой области не сохранены.", "message")); return; }
  const rows = data.checks.map(stored => {
    const result = stored.result;
    const identity = node("div");
    identity.append(node("strong", result.check_id), node("p", result.kind, "muted"), node("p", `Колонка: ${result.context?.column ?? "Весь dataset"}`, "muted"));
    const diagnostic = node("div");
    diagnostic.append(node("p", result.reason_code), node("p", result.message));
    if (result.details.length) diagnostic.append(evidence(result.details));
    return [identity, badge(result.status), evidence(result.observation), evidence(result.constraint), diagnostic, timestamp(stored.recorded_at)];
  });
  root.append(table(["Проверка", "Результат", "Наблюдение", "Ограничение", "Диагностика", "Сохранено"], rows));
}

function loadQuality() {
  if (!selection.dataset || !selection.scope) return message("quality", "Выберите область проверки. Она не выбирается автоматически по последней загрузке.");
  return load("quality", `${datasetPath()}/quality?${new URLSearchParams({ scope_id: selection.scope })}`, renderQuality, loadQuality);
}

function refresh() { return Promise.all([loadCatalog(), loadDataset(), loadBatches(), loadScopes(), loadQuality()]); }
document.getElementById("refresh").addEventListener("click", async event => {
  const control = event.currentTarget;
  control.disabled = true;
  try { await refresh(); } finally { control.disabled = false; }
});
window.addEventListener("popstate", applySelection);
refresh();
