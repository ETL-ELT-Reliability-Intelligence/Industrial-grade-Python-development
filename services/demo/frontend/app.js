"use strict";

const statusLabels = { open: "Открыт", investigating: "В расследовании", resolved: "Закрыт" };
const severityLabels = { critical: "Критический", warning: "Предупреждение", info: "Информация" };
const number = new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 2 });
const date = new Intl.DateTimeFormat("ru-RU", { dateStyle: "short", timeStyle: "short" });
const list = document.querySelector("#incident-list");
const detail = document.querySelector("#incident-detail");
const overview = document.querySelector("#overview");
const filter = document.querySelector("#status");
let selectedId = null;
let listController;
let detailController;
let overviewController;

// API text always enters the DOM as text, never as HTML.
function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

async function request(path, signal) {
  const response = await fetch(`/api/v1${path}`, { signal });
  if (!response.ok) throw new Error(response.status === 404 ? "Инцидент не найден." : `API вернул ошибку ${response.status}.`);
  return response.json();
}

function showError(container, error, retry) {
  const box = element("div", "error-state");
  box.setAttribute("role", "alert");
  box.append(element("p", "", error.message === "Failed to fetch" ? "Не удалось подключиться к API." : error.message));
  const button = element("button", "button", "Повторить");
  button.type = "button";
  button.addEventListener("click", retry);
  box.append(button);
  container.replaceChildren(box);
}

function badge(text, type) { return element("span", `badge ${type}`, text); }
function section(title) {
  const box = element("section", "detail-section");
  box.append(element("h3", "", title));
  return box;
}

async function loadOverview() {
  overviewController?.abort();
  const controller = overviewController = new AbortController();
  overview.replaceChildren(element("p", "state-message", "Загрузка обзора…"));
  document.querySelector("#snapshot").textContent = "Загрузка состояния…";
  try {
    const data = await request("/overview", controller.signal);
    const stats = [
      ["Активные инциденты", data.active_incidents, ""],
      ["Критические", data.critical_active_incidents, "critical"],
      ["Затронутые datasets", data.affected_datasets, ""],
      ["Закрытые инциденты", data.resolved_incidents, ""],
    ];
    overview.replaceChildren(...stats.map(([label, value, type]) => {
      const box = element("div", "stat");
      box.append(element("p", "stat-label", label), element("p", `stat-value ${type}`, number.format(value)));
      return box;
    }));
    document.querySelector("#snapshot").textContent = `Снимок данных: ${date.format(new Date(data.snapshot_at))} · время вашего браузера · ${data.data_mode}`;
  } catch (error) {
    if (error.name !== "AbortError") {
      document.querySelector("#snapshot").textContent = "Снимок данных недоступен.";
      showError(overview, error, loadOverview);
    }
  }
}

function markSelected() {
  list.querySelectorAll(".incident-row").forEach(row => {
    const selected = row.dataset.id === selectedId;
    row.classList.toggle("selected", selected);
    row.setAttribute("aria-pressed", String(selected));
  });
}

async function loadList() {
  listController?.abort();
  const controller = listController = new AbortController();
  list.setAttribute("aria-busy", "true");
  list.replaceChildren(element("p", "state-message", "Загрузка инцидентов…"));
  document.querySelector("#incident-count").textContent = "—";
  try {
    const query = filter.value ? `?status=${encodeURIComponent(filter.value)}` : "";
    const data = await request(`/incidents${query}`, controller.signal);
    document.querySelector("#incident-count").textContent = number.format(data.total);
    list.replaceChildren(...data.items.map(item => {
      const row = element("button", "incident-row");
      row.type = "button";
      row.dataset.id = item.id;
      const meta = element("div", "incident-meta");
      meta.append(element("span", "", item.id), badge(severityLabels[item.severity], item.severity), element("span", "", statusLabels[item.status]));
      row.append(meta, element("span", "incident-title", item.title), element("span", "incident-time", date.format(new Date(item.detected_at))));
      row.addEventListener("click", () => {
        if (location.hash === `#${encodeURIComponent(item.id)}`) loadDetail(item.id);
        else location.hash = encodeURIComponent(item.id);
      });
      return row;
    }));
    if (!data.items.length) list.append(element("p", "state-message", "Инцидентов с этим статусом нет."));
    markSelected();
  } catch (error) {
    if (error.name !== "AbortError") showError(list, error, loadList);
  } finally {
    if (listController === controller) list.setAttribute("aria-busy", "false");
  }
}

function renderIncident(item) {
  const meta = element("div", "incident-meta");
  meta.append(element("span", "", item.id), badge(severityLabels[item.severity], item.severity), element("span", "", statusLabels[item.status]));
  const heading = element("h2", "detail-heading", item.title);
  heading.tabIndex = -1;
  detail.replaceChildren(heading, meta, element("p", "detail-summary", item.summary));

  const causes = section("Возможная первопричина");
  for (const candidate of item.root_cause_candidates) {
    const box = element("div", "cause");
    box.append(element("strong", "", candidate.dataset_id), element("p", "", candidate.description), element("p", "hint", `Оценка ранжирования: ${number.format(candidate.score)} / 1`));
    causes.append(box);
  }
  if (!item.root_cause_candidates.length) causes.append(element("p", "hint", "Кандидаты причин пока не определены."));
  causes.append(element("p", "hint", "Оценка алгоритма не доказывает причинность и не является вероятностью причины."));
  detail.append(causes);

  const observations = section("Наблюдения");
  const wrap = element("div", "table-wrap");
  const table = element("table");
  const head = element("thead");
  const headerRow = element("tr");
  for (const label of ["Dataset / метрика", "Ожидалось", "Получено"]) headerRow.append(element("th", "", label));
  head.append(headerRow);
  const body = element("tbody");
  for (const observation of item.observations) {
    const row = element("tr");
    const name = element("td");
    name.append(element("strong", "", observation.dataset_id), element("div", "hint", `${observation.metric} · ${observation.unit}`));
    row.append(name, element("td", "", number.format(observation.expected)), element("td", "", number.format(observation.actual)));
    body.append(row);
  }
  table.append(head, body);
  wrap.append(table);
  observations.append(wrap);
  if (!item.observations.length) observations.append(element("p", "hint", "Наблюдения пока отсутствуют."));
  detail.append(observations);

  const impact = section("Потенциальная зона влияния");
  for (const [label, ids] of [["Datasets", item.impact.potential_dataset_ids], ["Pipelines", item.impact.potential_pipeline_ids], ["Dashboards", item.impact.potential_dashboard_ids]]) {
    impact.append(element("p", "hint", label));
    const chips = element("div", "chips");
    chips.append(...(ids.length ? ids.map(id => element("span", "chip", id)) : [element("span", "hint", "Не указаны")]));
    impact.append(chips);
  }
  detail.append(impact);

  const timeline = section("Хронология");
  const events = element("ol", "timeline");
  for (const event of item.timeline) {
    const row = element("li");
    const time = element("time", "", date.format(new Date(event.at)));
    time.dateTime = event.at;
    row.append(time, element("span", "", event.description));
    events.append(row);
  }
  timeline.append(events);
  detail.append(timeline);
  heading.focus({ preventScroll: true });
}

async function loadDetail(id) {
  detailController?.abort();
  const controller = detailController = new AbortController();
  selectedId = id;
  markSelected();
  detail.setAttribute("aria-busy", "true");
  if (!id) {
    detail.replaceChildren(element("p", "state-message", "Выберите инцидент в списке."));
    detail.setAttribute("aria-busy", "false");
    return;
  }
  detail.replaceChildren(element("p", "state-message", "Загрузка карточки…"));
  try {
    renderIncident(await request(`/incidents/${encodeURIComponent(id)}`, controller.signal));
  } catch (error) {
    if (error.name !== "AbortError") showError(detail, error, () => loadDetail(id));
  } finally {
    if (detailController === controller) detail.setAttribute("aria-busy", "false");
  }
}

function idFromHash() {
  try { return decodeURIComponent(location.hash.slice(1)) || null; }
  catch { return location.hash.slice(1); }
}

filter.addEventListener("change", loadList);
window.addEventListener("hashchange", () => loadDetail(idFromHash()));
document.querySelector("#refresh").addEventListener("click", async () => {
  const button = document.querySelector("#refresh");
  button.disabled = true;
  try { await Promise.all([loadOverview(), loadList(), loadDetail(selectedId)]); }
  finally { button.disabled = false; }
});
loadOverview();
loadList();
if (location.hash) loadDetail(idFromHash());
