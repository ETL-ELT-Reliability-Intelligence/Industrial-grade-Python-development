/** Show saved detector evidence without inferring normality from missing records. */
import { node, table, badge, timestamp } from "./ui.js";

const reasons = {
  not_recorded: "Результат детектора ещё не сохранён.",
  not_applicable: "ML-прогноз доступен для почасовых mock_orders из источника mock.",
  insufficient_training_history: "Нет модели с достаточной историей обучения.",
  insufficient_lag_history: "Не хватает предшествующих часов для прогноза.",
  insufficient_history: "Недостаточно сопоставимой истории.",
  incomplete_measurements: "Часть показателей не оценена: подробности в таблице.",
  missing_profile: "Профиль партии недоступен.",
};

export function detectionView(data) {
  const root = node("section", undefined, "detector-evidence");
  root.append(node("h3", "Аномалии и прогноз объёма"));
  for (const [title, result] of [["Статистический детектор", data.rule], ["ML-прогноз", data.ml]]) {
    const section = node("section");
    section.append(node("h4", title), badge(result.status));
    if (result.reason) section.append(node("p", reasons[result.reason] ?? result.reason));
    section.append(node("p", `Версия: ${result.model_version ?? "модель отсутствует"}`, "muted"));
    const checks = result.checks.length ? result.checks : result.actual != null || result.expected != null
      ? [{ metric: "Объём заказов", ...result, reason: null }] : [];
    if (checks.length) section.append(table(["Показатель", "Факт", "Ожидание", "Интервал", "Статус", "Причина"], checks.map(item => [
      item.metric, item.actual ?? "—", item.expected ?? "—",
      item.lower == null || item.upper == null ? "—" : `${item.lower.toFixed(2)} … ${item.upper.toFixed(2)}`,
      badge(item.status), reasons[item.reason] ?? item.reason ?? "—",
    ])));
    if (result.anomalies.length) section.append(table(["Аномалия", "Показатель", "Обнаружена"],
      result.anomalies.map(item => [item.anomaly_id, item.metric, timestamp(item.detected_at)])));
    else section.append(node("p", result.status === "PASS" ? "Отклонений не обнаружено." : "Связанные аномалии не сохранены."));
    root.append(section);
  }
  if (data.training) {
    const training = data.training;
    const diagnostics = training.diagnostics;
    root.append(node("h4", "Последняя попытка обучения"), badge(training.status));
    root.append(node("p", `Пригодных целевых часов: ${diagnostics.usable_targets} из ${diagnostics.required_targets}.`));
    const last = node("p", "Последнее успешное обучение: ");
    if (training.last_successful_training_at) last.append(timestamp(training.last_successful_training_at));
    else last.append(node("span", "не было"));
    root.append(last, node("p", `Граница данных модели: ${training.last_model_cutoff ?? "модели нет"}`));
    if (training.reason) root.append(node("p", reasons[training.reason] ?? training.reason));
    if (training.warnings?.includes("wide_prediction_interval")) root.append(node("p",
      "Калибровка сильно расширила интервал. Чувствительность к новым нарушениям может быть снижена.", "message error"));
    const details = node("details");
    details.append(node("summary", "Недостающие периоды и история лагов"),
      node("pre", JSON.stringify({ missing_targets: diagnostics.missing_targets, missing_lag_targets: diagnostics.missing_lag_targets,
        missing_history_periods: diagnostics.missing_history_periods }, null, 2)));
    root.append(details);
  }
  return root;
}
