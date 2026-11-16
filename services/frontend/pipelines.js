import { node, button, timestamp, table, dates } from "./ui.js";
import { ApiClient } from "./api-client.js";

const PAGE_SIZE = 50;
const panels = {
  catalog: document.getElementById("pipelines"),
  runs: document.getElementById("pipeline-runs"),
};
const offsets = { catalog: 0, runs: 0 };
const pending = new Map();
let pipeline = readSelection();
const api = new ApiClient(() => {
  document.getElementById("refreshed-at").textContent = `Последний успешный ответ API: ${dates.format(new Date())}. Это время загрузки экрана.`;
});

function readSelection() { return new URLSearchParams(location.search).get("pipeline"); }

function select(pipelineId) {
  const url = new URL(location.href);
  url.searchParams.set("pipeline", pipelineId);
  history.pushState(null, "", url);
  applySelection();
}

function applySelection() {
  const selected = readSelection();
  if (selected !== pipeline) offsets.runs = 0;
  pipeline = selected;
  markSelection();
  loadRuns();
}

function markSelection() {
  document.querySelectorAll("[data-pipeline]").forEach(item => {
    const selected = item.dataset.pipeline === pipeline;
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
  const request = new AbortController();
  pending.set(key, request);
  const root = panels[key];
  root.setAttribute("aria-busy", "true");
  root.replaceChildren(node("p", "Загрузка…", "message"));
  try {
    const data = await api.get(path, request.signal);
    if (pending.get(key) !== request) return;
    root.replaceChildren();
    render(data, root);
    markSelection();
  } catch (error) {
    if (request.signal.aborted || pending.get(key) !== request) return;
    const box = node("div", undefined, "message error");
    box.setAttribute("role", "alert");
    box.append(node("p", error instanceof TypeError ? "Не удалось связаться с API." : error.message), button("Повторить", retry));
    root.replaceChildren(box);
  } finally {
    if (pending.get(key) === request) {
      root.setAttribute("aria-busy", "false");
      pending.delete(key);
    }
  }
}

function pagination(root, data, key, reload) {
  const bar = node("nav", undefined, "pager");
  bar.setAttribute("aria-label", key === "catalog" ? "Страницы pipelines" : "Страницы запусков");
  const previous = button("Назад", () => { offsets[key] = Math.max(0, data.offset - data.limit); reload(); });
  const next = button("Далее", () => { offsets[key] = data.offset + data.limit; reload(); });
  previous.disabled = data.offset === 0;
  next.disabled = !data.has_more;
  bar.append(previous, node("span", `Страница ${Math.floor(data.offset / data.limit) + 1}`), next);
  root.append(bar);
}

function loadCatalog() {
  const query = new URLSearchParams({ limit: PAGE_SIZE, offset: offsets.catalog });
  return load("catalog", `/pipelines?${query}`, (data, root) => {
    for (const item of data.items) {
      const entry = button(item.pipeline_id, () => select(item.pipeline_id));
      entry.className = "catalog-item pipeline-item incident-row";
      entry.dataset.pipeline = item.pipeline_id;
      root.append(entry);
    }
    if (!data.items.length) root.append(node("p", "На этой странице нет pipelines с сохранёнными запусками.", "message"));
    pagination(root, data, "catalog", loadCatalog);
  }, loadCatalog);
}

function loadRuns() {
  const heading = document.getElementById("pipeline-heading");
  heading.textContent = pipeline === null ? "История запусков" : `Pipeline: ${pipeline}`;
  if (pipeline === null) return message("runs", "Выберите pipeline в каталоге.");
  if (!pipeline.trim()) return message("runs", "В ссылке указан пустой идентификатор pipeline.");
  const query = new URLSearchParams({ pipeline_id: pipeline, limit: PAGE_SIZE, offset: offsets.runs });
  return load("runs", `/pipeline-runs?${query}`, (data, root) => {
    if (data.items.length) root.append(table(["Run ID", "Статус", "Начало", "Завершение", "Ошибка"], data.items.map(run => [
      run.run_id,
      node("strong", run.status.toUpperCase(), `status-${{ running: "running", success: "completed", failed: "error" }[run.status]}`),
      timestamp(run.started_at),
      run.finished_at === null ? "Ещё выполняется" : timestamp(run.finished_at),
      node("span", run.error ?? "Не указана", "pipeline-error"),
    ])));
    else root.append(node("p", "На этой странице нет сохранённых запусков выбранного pipeline.", "message"));
    pagination(root, data, "runs", loadRuns);
  }, loadRuns);
}

async function refresh() { await Promise.all([loadCatalog(), loadRuns()]); }
document.getElementById("refresh").addEventListener("click", async event => {
  const control = event.currentTarget;
  control.disabled = true;
  try { await refresh(); } finally { control.disabled = false; }
});
window.addEventListener("popstate", applySelection);
refresh();
