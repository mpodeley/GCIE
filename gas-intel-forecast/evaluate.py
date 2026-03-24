"""Fixed evaluation entrypoint for the Demand Forecast engine."""

from __future__ import annotations

from typing import Any

from data_pipeline import load_dataset
from model import train_and_predict


def _weighted_mape(predictions: list[dict[str, Any]]) -> float:
    weighted_error = 0.0
    total_weight = 0.0
    skipped_zero_actuals = 0

    for row in predictions:
        actual = float(row["actual_volume"])
        predicted = float(row["predicted_volume"])
        if actual <= 0:
            skipped_zero_actuals += 1
            continue
        weighted_error += abs(actual - predicted)
        total_weight += actual

    if total_weight == 0:
        raise RuntimeError("Cannot compute weighted MAPE because validation weights sum to zero.")
    if skipped_zero_actuals:
        # Keep the behavior explicit; zero actuals carry no weight in this metric.
        pass
    return weighted_error / total_weight


def score_run() -> dict[str, Any]:
    """Run the current model against the fixed dataset contract."""
    dataset = load_dataset()
    result = train_and_predict(dataset)
    predictions = result["predictions"]
    metric_value = _weighted_mape(predictions)
    return {
        "engine": dataset["engine"],
        "hypothesis": result["hypothesis"],
        "metric_name": "weighted_mape",
        "metric_value": metric_value,
        "train_rows": len(dataset["train_rows"]),
        "validation_rows": len(dataset["validation_rows"]),
        "predictions": predictions,
        "model_artifacts": result["model_artifacts"],
    }


if __name__ == "__main__":
    print(score_run())
