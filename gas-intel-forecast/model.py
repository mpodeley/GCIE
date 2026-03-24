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
        month_hdd_buckets: dict[int, list[float]] = defaultdict(list)
        weather_pairs: list[tuple[float, float]] = []
        for row in rows:
            dow_buckets[row["day_of_week"]].append(row["actual_volume"])
            month_buckets[row["month"]].append(row["actual_volume"])
            if row["hdd"] is not None:
                month_hdd_buckets[row["month"]].append(float(row["hdd"]))
                weather_pairs.append((float(row["hdd"]), row["actual_volume"]))

        hdd_slope = 0.0
        if len(weather_pairs) >= 2:
            hdd_values = [pair[0] for pair in weather_pairs]
            volume_values = [pair[1] for pair in weather_pairs]
            hdd_mean = _safe_mean(hdd_values)
            volume_mean = _safe_mean(volume_values)
            numerator = sum(
                (hdd_value - hdd_mean) * (volume - volume_mean)
                for hdd_value, volume in weather_pairs
            )
            denominator = sum((hdd_value - hdd_mean) ** 2 for hdd_value in hdd_values)
            if denominator > 0:
                hdd_slope = numerator / denominator

        profiles[segment] = {
            "segment_mean": segment_mean,
            "dow_mean": {key: _safe_mean(values) for key, values in dow_buckets.items()},
            "month_mean": {key: _safe_mean(values) for key, values in month_buckets.items()},
            "month_hdd_mean": {key: _safe_mean(values) for key, values in month_hdd_buckets.items()},
            "hdd_slope": hdd_slope,
        }
    return profiles


def _lag_signal(row: dict[str, Any]) -> float:
    lag_keys = row["lag_keys"]
    if row["cadence"] == "monthly":
        lag_weights = [0.50, 0.20, 0.30][: len(lag_keys)]
    else:
        lag_weights = [0.55, 0.30, 0.15][: len(lag_keys)]
    return sum(weight * float(row[key]) for weight, key in zip(lag_weights, lag_keys))


def _predict_row(row: dict[str, Any], profile: dict[str, Any]) -> float:
    lag_signal = _lag_signal(row)
    segment_mean = float(profile["segment_mean"])
    dow_mean = float(profile["dow_mean"].get(row["day_of_week"], segment_mean))
    month_mean = float(profile["month_mean"].get(row["month"], segment_mean))
    if row["cadence"] == "monthly":
        prediction = 0.20 * lag_signal + 0.80 * month_mean
        if row["hdd"] is not None:
            month_hdd_mean = float(profile["month_hdd_mean"].get(row["month"], row["hdd"]))
            prediction += float(profile["hdd_slope"]) * (float(row["hdd"]) - month_hdd_mean)
    else:
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
        row_with_lags = dict(row)
        row_with_lags["lag_keys"] = dataset["lag_keys"]
        row_with_lags["cadence"] = dataset["cadence"]
        prediction = _predict_row(row_with_lags, profile)
        predictions.append(
            {
                "fecha": row["fecha"],
                "segmento": row["segmento"],
                "actual_volume": float(row["actual_volume"]),
                "predicted_volume": prediction,
            }
        )

    return {
        "hypothesis": "Seasonal monthly blend with HDD adjustment and lag fallback",
        "predictions": predictions,
        "model_artifacts": {
            "cadence": dataset["cadence"],
            "lag_keys": dataset["lag_keys"],
            "segments": sorted(profiles),
            "training_rows": len(train_rows),
            "validation_rows": len(validation_rows),
        },
    }
