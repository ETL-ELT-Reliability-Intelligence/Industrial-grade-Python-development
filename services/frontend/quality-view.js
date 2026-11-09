/** Render saved quality decisions, execution history and diagnostic evidence. */
import { node, button, timestamp, table, evidence, badge, checkNames } from "./ui.js";
import { configurationView } from "./configuration-view.js?v=table-2";

const executionNames = {
  PENDING: "Ожидание", RUNNING: "Выполняется", COMPLETED: "Завершено",
  SKIPPED: "Пропущено", ERROR: "Ошибка выполнения",
};

function executionBadge(state) {
  const result = badge(state);
  result.append(node("span", ` · ${executionNames[state] ?? state}`));
  return result;
}

function disclosure(title, content) {
  const details = node("details");
  details.append(node("summary", title), content);
  return details;
}

function attemptsTable(attempts) {
  if (!attempts.length) return node("p", "Попытки не сохранены.", "muted");
  return table(["Попытка", "Начало", "Завершение", "Код", "Сообщение"], attempts.map(attempt => [
    attempt.number, timestamp(attempt.started_at), timestamp(attempt.finished_at),
    attempt.code ?? "Не указан", attempt.message ?? "Не указано",
  ]));
}

function policyDecision(decision) {
  const box = node("div", undefined, "decision cause");
  box.append(node("h3", "Сохранённое решение policy"));
  if (!decision) {
    box.append(node("p", "Решение ещё не сохранено. Это не PASS.", "message"));
    return box;
  }
  box.append(badge(decision.status));
  const reasons = node("ul");
  decision.reasons.forEach(reason => reasons.append(node("li", `${reason.code}${reason.check_id ? ` · ${reason.check_id}` : ""}: ${reason.message}`)));
  box.append(reasons);
  return box;
}

/** DOM rendering only; no requests, URL changes or evaluation inference. */
export class QualityView {
  constructor(historyRoot, detailRoot) {
    this.historyRoot = historyRoot;
    this.detailRoot = detailRoot;
  }

  clear() {
    this.historyRoot.replaceChildren();
    this.detailRoot.replaceChildren(node("p", "Выберите область проверки. Она не выбирается автоматически по последней загрузке.", "message"));
    this.idle();
  }

  loading() {
    for (const root of [this.historyRoot, this.detailRoot]) {
      root.replaceChildren(node("p", "Загрузка…", "message"));
      root.setAttribute("aria-busy", "true");
    }
  }

  idle() {
    this.historyRoot.setAttribute("aria-busy", "false");
    this.detailRoot.setAttribute("aria-busy", "false");
  }

  history(page, selectedId, onSelect, onPage) {
    this.historyRoot.replaceChildren(node("h3", "История расчётов"));
    if (page.items.length) {
      const rows = page.items.map(item => {
        const choice = button(item.evaluation_id, () => onSelect(item.evaluation_id));
        choice.classList.toggle("selected", item.evaluation_id === selectedId);
        choice.setAttribute("aria-pressed", String(item.evaluation_id === selectedId));
        return [choice, executionBadge(item.state), item.decision_status ? badge(item.decision_status) : "Решения нет", timestamp(item.evaluated_at)];
      });
      this.historyRoot.append(table(["Расчёт", "Выполнение", "Policy", "Время оценки"], rows));
    } else {
      this.historyRoot.append(node("p", page.offset ? "На этой странице расчётов нет." : "История расчётов для этой области отсутствует.", "message"));
    }
    const pager = node("nav", undefined, "pager");
    pager.setAttribute("aria-label", "Страницы истории расчётов");
    const previous = button("Назад", () => onPage(Math.max(0, page.offset - page.limit)));
    const next = button("Далее", () => onPage(page.offset + page.limit));
    previous.disabled = page.offset === 0;
    next.disabled = !page.has_more;
    pager.append(previous, node("span", `Страница ${Math.floor(page.offset / page.limit) + 1}`), next);
    this.historyRoot.append(pager);
  }

  detail(record) {
    const root = this.detailRoot;
    const evaluation = record.evaluation;
    root.replaceChildren(node("h3", `Расчёт: ${evaluation.evaluation_id}`));
    root.append(node("p", `Scope: ${record.scope_id}`, "scope-label"));
    const state = node("p", "Выполнение расчёта: ");
    state.append(executionBadge(evaluation.state));
    root.append(state, node("p", `Подготовка наблюдений: ${evaluation.prepared ? "сохранена" : "ещё не завершена"}. Публикация: ${evaluation.published ? "выполнена" : "не подтверждена"}.`, "muted"));
    for (const [label, value] of [
      ["Время оценки (evaluated_at)", evaluation.evaluated_at],
      ["Создание записи расчёта", record.created_at], ["Обновление записи расчёта", record.updated_at],
    ]) {
      const line = node("p", `${label}: `, "muted");
      line.append(timestamp(value));
      root.append(line);
    }
    root.append(node("p", "Время оценки и обновление записи не являются временем завершения всех проверок.", "muted"));
    root.append(policyDecision(evaluation.decision));
    root.append(disclosure("Конфигурация расчёта", configurationView(evaluation)));
    root.append(disclosure(`Попытки подготовки (${evaluation.preparation_attempts.length})`, attemptsTable(evaluation.preparation_attempts)));
    root.append(disclosure(`Попытки доставки (${evaluation.delivery_attempts.length})`, attemptsTable(evaluation.delivery_attempts)));
    root.append(node("h3", "Проверки"));
    root.append(node("p", "Выполнение показывает ход проверки. PASS / FAIL / UNKNOWN — соответствие данных. PASS / WARN / BLOCK — решение policy.", "muted"));
    const rows = evaluation.checks.map(check => {
      const identity = node("div");
      identity.append(node("strong", check.check_id), node("p", checkNames[check.spec.kind] ?? check.spec.kind, "muted"), node("p", `Колонка: ${check.spec.column ?? "Весь dataset"}`, "muted"));
      const diagnostic = node("div");
      if (check.code) diagnostic.append(node("p", check.code));
      if (check.message) diagnostic.append(node("p", check.message));
      if (check.result) {
        diagnostic.append(node("p", check.result.reason_code), node("p", check.result.message));
        if (check.result.details.length) diagnostic.append(disclosure("Детали результата", evidence(check.result.details)));
      }
      const measurements = node("div");
      measurements.append(node("h4", "Наблюдение"), evidence(check.observation), node("h4", "Ограничение"), evidence(check.constraint));
      diagnostic.append(disclosure("Наблюдение и ограничение", measurements));
      diagnostic.append(disclosure("Эталон сравнения", evidence(check.baseline)));
      diagnostic.append(disclosure(`Попытки (${check.attempts.length})`, attemptsTable(check.attempts)));
      return [identity, executionBadge(check.state), check.result ? badge(check.result.status) : "Результата нет", diagnostic];
    });
    root.append(table(["Проверка", "Выполнение", "Результат", "Диагностика"], rows));
  }

  legacy(quality) {
    this.detailRoot.replaceChildren(node("p", "Истории расчётов нет. Ниже — сохранённые результаты без идентификатора расчёта.", "message"));
    renderQuality(quality, this.detailRoot);
  }

  error(text, retry, historyLoaded) {
    const box = node("div", undefined, "message error");
    box.setAttribute("role", "alert");
    box.append(node("p", text), button("Повторить", retry));
    if (historyLoaded) this.detailRoot.replaceChildren(box);
    else {
      this.historyRoot.replaceChildren(box);
      this.detailRoot.replaceChildren(node("p", "Результаты не загружены: история расчётов недоступна.", "message"));
    }
  }
}

function renderQuality(data, root) {
  root.append(node("p", `Scope: ${data.scope_id}`, "scope-label"));
  const decision = policyDecision(data.decision);
  if (data.decision) {
    const at = node("p", "Время решения: ", "muted");
    at.append(timestamp(data.decided_at));
    decision.append(at);
  }
  root.append(decision, node("h3", "Проверки"), node("p", "PASS — соответствует; FAIL — не соответствует; UNKNOWN — недостаточно наблюдений. Решения policy: PASS, WARN или BLOCK.", "muted"));
  if (!data.checks.length) { root.append(node("p", "Проверки для этой области не сохранены.", "message")); return; }
  const rows = data.checks.map(stored => {
    const result = stored.result;
    const identity = node("div");
    identity.append(node("strong", result.check_id), node("p", checkNames[result.kind] || result.kind, "muted"), node("p", `Колонка: ${result.context?.column ?? "Весь dataset"}`, "muted"));
    const diagnostic = node("div");
    diagnostic.append(node("p", result.reason_code), node("p", result.message));
    if (result.details.length) diagnostic.append(evidence(result.details));
    return [identity, badge(result.status), evidence(result.observation), evidence(result.constraint), diagnostic, timestamp(stored.recorded_at)];
  });
  root.append(table(["Проверка", "Результат", "Наблюдение", "Ограничение", "Диагностика", "Сохранено"], rows));
}

