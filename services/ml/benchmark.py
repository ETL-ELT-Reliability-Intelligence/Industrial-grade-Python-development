"""Seeded chronological benchmark of boosting, rolling median and weekly persistence."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import platform
import random
from statistics import mean

from contracts.events import DatasetIngestedEvent
from contracts.profiles import BatchProfile
from services.ml.volume import HOUR, OBSERVATION_VERSION, Predictor, lag_features, prepare_training, train

START = datetime(2026, 1, 5, tzinfo=timezone.utc)
CUTOFF = START + timedelta(days=28, hours=1)
TEST_START = START + timedelta(days=29)
TEST_HOURS = 14 * 24


def generated_history(seed, contaminated=False):
    """Keep receipt time after each closed hour; never impute missing hours."""
    rng = random.Random(seed)
    profiles, labels = [], {}
    for index in range(43 * 24):
        hour = START + index * HOUR
        seasonal = (170 + 85 * math.sin(2 * math.pi * (hour.hour - 7) / 24))
        seasonal *= 0.72 if hour.weekday() >= 5 else 1.0
        count = max(1, round(seasonal + rng.gauss(0, 8)))
        relative = int((hour - TEST_START) / HOUR)
        kind = "normal"
        if relative in {12, 180, 288}:
            count *= 3
            kind = f"spike-{relative}"
        elif 48 <= relative < 72:
            count = round(count * 0.25)
            kind = "sustained-drop"
        elif 120 <= relative < 168:
            count = round(count * (0.9 - 0.65 * (relative - 120) / 47))
            kind = "gradual-degradation"
        elif relative in {90, 91, 240}:
            kind = "missing"
        if contaminated and 21 * 24 + 20 <= index < 21 * 24 + 44:
            count = round(count * (0.15 if index % 2 else 3.0))
        if relative >= 0:
            labels[hour] = {"kind": kind, "actual": None if kind == "missing" else count}
        if kind == "missing":
            continue
        event = DatasetIngestedEvent(
            dataset_id="mock_orders", source_id="mock", batch_id=f"benchmark-{index}",
            received_at=hour + HOUR + timedelta(minutes=5), period_start=hour,
            period_end=hour + HOUR, granularity="hour", complete=True, records=count, schema_version=1,
        )
        profiles.append(BatchProfile(event=event, config_hash="benchmark-v1", row_count=count,
                                     order_volume={"version": OBSERVATION_VERSION, "count": count}))
    return profiles, labels


def baseline_expected(inputs, name):
    from services.ml.volume import FEATURES
    key = "rolling_168_median" if name == "median" else "lag_168"
    return inputs["values"][FEATURES.index(key)]


def summarize(records):
    """Compute alert metrics only for observed targets, tracking skipped coverage separately."""
    observed = [r for r in records if r["actual"] is not None]
    evaluated = [r for r in observed if r["status"] in {"PASS", "WARN"}]
    alerts = [r for r in evaluated if r["status"] == "WARN"]
    abnormal = [r for r in observed if r["kind"] != "normal"]
    normal = [r for r in evaluated if r["kind"] == "normal"]
    true = sum(r["kind"] != "normal" for r in alerts)
    false = len(alerts) - true
    delays = []
    episodes = sorted({r["kind"] for r in abnormal})
    for episode in episodes:
        members = [r for r in observed if r["kind"] == episode]
        detected = [r for r in members if r["status"] == "WARN"]
        delays.append({"episode": episode, "delay_hours": detected[0]["index"] - members[0]["index"] if detected else None})
    return {
        "precision": true / len(alerts) if alerts else None,
        "recall": true / len(abnormal) if abnormal else None,
        "false_alarms": false, "false_positive_rate": false / len(normal) if normal else None,
        "evaluation_coverage": len(evaluated) / len(records),
        "interval_coverage_normal": mean(r["lower"] <= r["actual"] <= r["upper"] for r in normal) if normal else None,
        "interval_coverage_all": mean(r["lower"] <= r["actual"] <= r["upper"] for r in evaluated) if evaluated else None,
        "mean_interval_width": mean(r["upper"] - r["lower"] for r in evaluated) if evaluated else None,
        "mean_relative_width": mean((r["upper"] - r["lower"]) / max(r["expected"], 1) for r in evaluated) if evaluated else None,
        "skipped": len(records) - len(evaluated), "detection_delays": delays,
        "detected_episodes": sum(r["delay_hours"] is not None for r in delays), "episodes": len(episodes),
    }


def experiment(seed):
    import sklearn
    from threadpoolctl import threadpool_limits

    output = {"seed": seed, "generator": "seasonal-noisy-orders-v1", "python": platform.python_version(),
              "sklearn": sklearn.__version__, "training_days": 14, "calibration_days": 7,
              "warmup_days": 7, "cutoff": CUTOFF.isoformat(), "test_start": TEST_START.isoformat(),
              "test_hours": TEST_HOURS, "scenarios": {}}
    with threadpool_limits(limits=2):
        for contaminated in (False, True):
            profiles, labels = generated_history(seed, contaminated)
            rows, diagnostics = prepare_training(profiles, CUTOFF)
            model = train(rows, CUTOFF)
            predictor = Predictor()
            pads = {}
            for name in ("median", "weekly"):
                errors = [abs(row[1] - baseline_expected(row[3], name)) / max(baseline_expected(row[3], name), 1)
                          for row in rows[336:]]
                pads[name] = max(0.2, sorted(errors)[math.ceil(0.99 * len(errors)) - 1])
            records = {name: [] for name in ("boosting", "median", "weekly")}
            for index in range(TEST_HOURS):
                hour = TEST_START + index * HOUR
                label = labels[hour]
                inputs = lag_features(hour, profiles)
                for name in records:
                    result = {"index": index, "hour": hour.isoformat(), **label}
                    if label["actual"] is None or inputs is None:
                        result.update(status="SKIPPED", reason="missing_target" if label["actual"] is None else "missing_lags")
                    elif name == "boosting":
                        predicted = predictor.predict(model, hour, label["actual"], inputs)
                        result.update({k: predicted[k] for k in ("status", "actual", "expected", "lower", "upper")})
                    else:
                        expected = baseline_expected(inputs, name)
                        lower, upper = max(0, expected * (1 - pads[name])), expected * (1 + pads[name])
                        result.update(expected=expected, lower=lower, upper=upper,
                                      status="WARN" if not lower <= label["actual"] <= upper else "PASS")
                    records[name].append(result)
            output["scenarios"]["contaminated_calibration" if contaminated else "clean"] = {
                "training_diagnostics": diagnostics, "model_version": model["model_version"],
                "interval_padding": model["metadata"]["interval_padding"],
                "baseline_padding": pads, "metrics": {name: summarize(values) for name, values in records.items()},
                "predictions": records,
            }
    return output


def report(document):
    lines = ["# Оценка обнаружения отклонений объёма", "",
             "Фиксированная синтетическая история: суточная и недельная сезонность, шум, всплески, длительное падение, постепенная деградация и пропуски.",
             "Разделение по времени: 7 дней лагов, 14 обучения, 7 калибровки; затем отдельные 14 дней теста. received_at = конец часа + 5 минут.",
             "", "| История | Метод | Precision | Recall | Ложные тревоги | Доля оценённых часов | Покрытие нормы | Средняя ширина |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for scenario, data in document["scenarios"].items():
        for name, metrics in data["metrics"].items():
            values = [metrics[k] for k in ("precision", "recall", "false_alarms", "evaluation_coverage", "interval_coverage_normal", "mean_interval_width")]
            lines.append(f"| {scenario} | {name} | " + " | ".join("—" if v is None else f"{v:.3f}" for v in values) + " |")
    clean = document["scenarios"]["clean"]["metrics"]
    dirty = document["scenarios"]["contaminated_calibration"]["metrics"]["boosting"]
    lines += ["", f"На чистой истории recall бустинга — {clean['boosting']['recall']:.1%}, "
              f"медианы — {clean['median']['recall']:.1%}, прошлой недели — {clean['weekly']['recall']:.1%}. "
              f"Ложных тревог соответственно {clean['boosting']['false_alarms']}, {clean['median']['false_alarms']} и {clean['weekly']['false_alarms']}.",
              f"После загрязнения калибровки средняя ширина интервала бустинга выросла "
              f"в {dirty['mean_interval_width'] / clean['boosting']['mean_interval_width']:.2f} раза, recall составил {dirty['recall']:.1%}."]
    lines += ["", "Полные прогнозы, задержки по каждому эпизоду, причины пропусков, версии и границы интервалов сохранены в JSON.",
              "", "Загрязнение внесено только в калибровочную неделю: 24 часа чередующихся провалов и всплесков. Это проверка чувствительности интервала; модель не считает PASS меткой нормы.",
              "Пропущенные часы не заменяются нулями. Precision без тревог не определена; необнаруженный эпизод имеет delay_hours=null. Recall учитывает пропущенные оценки наблюдавшихся нарушений как недетектированные.",
              "", "Результат относится к этому генератору. Он не доказывает превосходство на реальном Olist (месячные данные не подходят почасовой модели). Сравнивайте recall вместе с ложными тревогами, шириной и покрытием; загрязнённая калибровка может скрыть новые сбои.",
              "", "Повтор: `python -m services.ml.benchmark --seed 42 --output reports/ml`."]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("reports/ml"))
    args = parser.parse_args()
    document = experiment(args.seed)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "benchmark.json").write_text(json.dumps(document, indent=2), encoding="utf-8")
    (args.output / "README.md").write_text(report(document), encoding="utf-8")
    print(report(document))


if __name__ == "__main__":
    main()
