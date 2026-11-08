import { node, button, timestamp, table, dates, numbers } from "./ui.js";
import { ApiClient } from "./api-client.js";
import { EvaluationController } from "./evaluations.js";
import { QualityView } from "./quality-view.js?v=configuration-table-2";

const PAGE_SIZE = 50;
const panels = Object.fromEntries(["catalog", "dataset", "batches", "scopes", "quality"].map(key => [key, document.getElementById(key)]));
const pending = new Map();
const offsets = { catalog: 0, batches: 0, scopes: 0 };
let selection = readSelection();
const api = new ApiClient(() => {
  document.getElementById("refreshed-at").textContent = `Последний успешный ответ API: ${dates.format(new Date())}. Это время загрузки экрана, а не свежесть исходных данных.`;
});
const evaluations = new EvaluationController(
  api,
  new QualityView(document.getElementById("evaluations"), panels.quality),
  rememberEvaluation,
);
function readSelection() {
  const params = new URLSearchParams(location.search);
  return { dataset: params.get("dataset"), scope: params.get("scope"), evaluation: params.get("evaluation") };
}

function select(dataset, scope = null) {
  const url = new URL(location.href);
  url.searchParams.delete("dataset");
  url.searchParams.delete("scope");
  url.searchParams.delete("evaluation");
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
    const data = await api.get(path, controller.signal);
    if (pending.get(key) !== controller) return;
    root.replaceChildren();
    render(data, root);
    markSelection();
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

function loadCatalog() {
  return load("catalog", listPath("/datasets", "catalog"), (data, root) => {
    for (const item of data.items) {
      const entry = button(item.dataset_id, () => select(item.dataset_id));
      entry.className = "catalog-item incident-row";
      entry.setAttribute("aria-label", item.dataset_id);
      entry.dataset.dataset = item.dataset_id;
      const registered = node("span", undefined, "incident-time");
      registered.append(node("span", "Зарегистрирован: ", "registered-label"));
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
    if (data.items.length) {
      const grid = table(["Batch ID", "Строк", "Получен", "Схема", "Источник"], data.items.map(item => [
        item.batch_id, numbers.format(item.records), batchTime(item.received_at), item.schema_version,
        batchSource(item.object_key),
      ]));
      grid.classList.add("batch-table-wrap");
      const schemaHeading = grid.querySelectorAll("th")[3];
      schemaHeading.title = "Версия схемы";
      schemaHeading.setAttribute("aria-label", "Версия схемы");
      root.append(grid);
    } else root.append(node("p", "На этой странице нет сохранённых загрузок.", "message"));
    pagination(root, data, "batches", loadBatches);
  }, loadBatches);
}

const batchDates = new Intl.DateTimeFormat("ru-RU", { dateStyle: "short" });
const batchTimes = new Intl.DateTimeFormat("ru-RU", { timeStyle: "short" });

function batchTime(value) {
  const time = node("time", undefined, "batch-time");
  const date = new Date(value);
  time.dateTime = value;
  time.title = value;
  time.append(node("span", batchDates.format(date)), node("span", batchTimes.format(date), "muted"));
  return time;
}

function batchSource(value) {
  if (value === null || value === undefined) return node("span", "Не указан", "muted");
  const source = node("span", undefined, "batch-source");
  // Keep ordinary path segments together; unusually long segments can still wrap.
  const prefix = value.match(/^[a-z][a-z\d+.-]*:\/\/[^/]*\/?/i)?.[0] ?? "";
  const segments = value.slice(prefix.length).match(/[^/\\]+[/\\]?|[/\\]/g) ?? [];
  for (const segment of [...(prefix ? [prefix] : []), ...segments]) {
    source.append(node("span", segment, "batch-source-segment"), node("wbr"));
  }
  return source;
}

function loadScopes() {
  if (!selection.dataset) return message("scopes", "Сначала выберите dataset.");
  return load("scopes", listPath(`${datasetPath()}/quality-scopes`, "scopes"), (data, root) => {
    if (data.items.length) root.append(table(["Scope ID", "Последнее сохранение качества / расчёта"], data.items.map(item => {
      const choice = button(item.scope_id, () => select(selection.dataset, item.scope_id));
      choice.dataset.scope = item.scope_id;
      return [choice, timestamp(item.last_saved_at)];
    })));
    else root.append(node("p", "На этой странице нет областей с сохранёнными результатами качества или расчётами.", "message"));
    pagination(root, data, "scopes", loadScopes);
  }, loadScopes);
}

function loadQuality() { return evaluations.show(selection); }

function refresh() { return Promise.all([loadCatalog(), loadDataset(), loadBatches(), loadScopes(), loadQuality()]); }
document.getElementById("refresh").addEventListener("click", async event => {
  const control = event.currentTarget;
  control.disabled = true;
  try { await refresh(); } finally { control.disabled = false; }
});
window.addEventListener("popstate", applySelection);
refresh();

function rememberEvaluation(evaluationId, replace) {
  const url = new URL(location.href);
  url.searchParams.set("evaluation", evaluationId);
  history[replace ? "replaceState" : "pushState"](null, "", url);
  selection = { ...selection, evaluation: evaluationId };
}
