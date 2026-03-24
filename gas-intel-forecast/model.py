"""Editable model surface for the Demand Forecast engine."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def _safe_mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _compute_segment_profiles(train_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in train_rows:
        grouped_rows[row["segmento"]].append(row)

    profiles: dict[str, dict[str, Any]] = {}
    for segment, rows in grouped_rows.items():
        segment_mean = _safe_mean([row["actual_volume"] for row in rows])
        dow_buckets: dict[int, list[float]] = defaultdict(list)
        month_buckets: dict[int, list[float]] = defaultdict(list)
        for row in rows:
            dow_buckets[row["day_of_week"]].append(row["actual_volume"])
            month_buckets[row["month"]].append(row["actual_volume"])
        profiles[segment] = {
            "segment_mean": segment_mean,
            "dow_mean": {key: _safe_mean(values) for key, values in dow_buckets.items()},
            "month_mean": {key: _safe_mean(values) for key, values in month_buckets.items()},
        }
    return profiles


def _predict_row(row: dict[str, Any], profile: dict[str, Any]) -> float:
    lag_signal = (
        0.55 * float(row["lag_7"])
        + 0.30 * float(row["lag_14"])
        + 0.15 * float(row["lag_28"])
    )
    segment_mean = float(profile["segment_mean"])
    dow_mean = float(profile["dow_mean"].get(row["day_of_week"], segment_mean))
    month_mean = float(profile["month_mean"].get(row["month"], segment_mean))

    prediction = 0.70 * lag_signal + 0.20 * dow_mean + 0.10 * month_mean
    return max(prediction, 0.0)


def train_and_predict(dataset: dict[str, Any]) -> dict[str, Any]:
    """Train a candidate model and return predictions plus run metadata."""
    train_rows = dataset["train_rows"]
    validation_rows = dataset["validation_rows"]
    profiles = _compute_segment_profiles(train_rows)

    predictions: list[dict[str, Any]] = []
    for row in validation_rows:
        profile = profiles.get(row["segmento"])
        if profile is None:
            raise RuntimeError(f"Missing training profile for segment {row['segmento']!r}.")
        prediction = _predict_row(row, profile)
        predictions.append(
            {
                "fecha": row["fecha"],
                "segmento": row["segmento"],
                "actual_volume": float(row["actual_volume"]),
                "predicted_volume": prediction,
            }
        )

    return {
        "hypothesis": "Lag-weighted baseline with segment/day/month priors",
        "predictions": predictions,
        "model_artifacts": {
            "segments": sorted(profiles),
            "training_rows": len(train_rows),
            "validation_rows": len(validation_rows),
        },
    }
