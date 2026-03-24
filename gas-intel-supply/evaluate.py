"""Fixed evaluation entrypoint for the Supply engine."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from data_pipeline import load_dataset
from model import train_and_predict


RESULTS_PATH = Path(__file__).resolve().parent / "results.tsv"


def _mae(predictions: list[dict[str, Any]]) -> float:
    if not predictions:
        raise RuntimeError("Cannot compute MAE because there are no predictions.")
    return sum(
        abs(float(row["actual_price"]) - float(row["predicted_price"])) for row in predictions
    ) / len(predictions)


def _read_last_metric() -> float | None:
    if not RESULTS_PATH.exists():
        return None
    lines = [line.strip() for line in RESULTS_PATH.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len(lines) <= 1:
        return None
    last_fields = lines[-1].split("\t")
    if len(last_fields) < 3:
        return None
    try:
        return float(last_fields[2])
    except ValueError:
        return None


def _append_results(hypothesis: str, metric_value: float) -> None:
    previous_metric = _read_last_metric()
    delta_vs_prev = None if previous_metric is None else metric_value - previous_metric
    kept = "kept" if previous_metric is None or metric_value <= previous_metric else "discarded"
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    line = "\t".join(
        [
            timestamp,
            hypothesis,
            f"{metric_value}",
            "" if delta_vs_prev is None else f"{delta_vs_prev}",
            kept,
        ]
    )
    with RESULTS_PATH.open("a", encoding="utf-8") as handle:
        if RESULTS_PATH.stat().st_size == 0:
            handle.write("timestamp\thypothesis\tmetric_value\tdelta_vs_prev\tkept\n")
        handle.write(f"{line}\n")


def score_run() -> dict[str, Any]:
    """Run the current model against the fixed dataset contract."""
    dataset = load_dataset()
    result = train_and_predict(dataset)
    predictions = result["predictions"]
    metric_value = _mae(predictions)
    _append_results(result["hypothesis"], metric_value)
    return {
        "engine": dataset["engine"],
        "hypothesis": result["hypothesis"],
        "metric_name": "mae_usd_mmbtu",
        "metric_value": metric_value,
        "train_rows": len(dataset["train_rows"]),
        "validation_rows": len(dataset["validation_rows"]),
        "predictions": predictions,
        "model_artifacts": result["model_artifacts"],
    }


if __name__ == "__main__":
    print(score_run())
