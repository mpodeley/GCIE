"""Editable model surface for the Supply engine."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def _safe_mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _fit_slope(rows: list[dict[str, Any]], feature_name: str) -> float:
    pairs = [
        (float(row[feature_name]), float(row["actual_price"]))
        for row in rows
        if row.get(feature_name) is not None
    ]
    if len(pairs) < 2:
        return 0.0

    feature_values = [pair[0] for pair in pairs]
    target_values = [pair[1] for pair in pairs]
    feature_mean = _safe_mean(feature_values)
    target_mean = _safe_mean(target_values)
    numerator = sum(
        (feature_value - feature_mean) * (target_value - target_mean)
        for feature_value, target_value in pairs
    )
    denominator = sum((feature_value - feature_mean) ** 2 for feature_value in feature_values)
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _compute_profiles(train_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in train_rows:
        grouped_rows[row["cuenca"]].append(row)

    global_mean = _safe_mean([float(row["actual_price"]) for row in train_rows])
    profiles: dict[str, dict[str, Any]] = {}
    for cuenca, rows in grouped_rows.items():
        basin_group = rows[0]["basin_group"]
        price_mean = _safe_mean([float(row["actual_price"]) for row in rows])
        month_buckets: dict[int, list[float]] = defaultdict(list)
        for row in rows:
            month_buckets[row["month"]].append(float(row["actual_price"]))

        profiles[cuenca] = {
            "basin_group": basin_group,
            "price_mean": price_mean,
            "month_mean": {month: _safe_mean(values) for month, values in month_buckets.items()},
            "usd_mean": _safe_mean(
                [float(row["usd_ars"]) for row in rows if row.get("usd_ars") is not None]
            ),
            "gas_no_conv_mean": _safe_mean(
                [
                    float(row["gas_no_convencional_mm3"])
                    for row in rows
                    if row.get("gas_no_convencional_mm3") is not None
                ]
            ),
            "oil_no_conv_mean": _safe_mean(
                [
                    float(row["oil_no_convencional_m3"])
                    for row in rows
                    if row.get("oil_no_convencional_m3") is not None
                ]
            ),
            "gas_share_mean": _safe_mean(
                [float(row["gas_no_conv_share"]) for row in rows if row.get("gas_no_conv_share") is not None]
            ),
            "oil_share_mean": _safe_mean(
                [float(row["oil_no_conv_share"]) for row in rows if row.get("oil_no_conv_share") is not None]
            ),
            "usd_slope": _fit_slope(rows, "usd_ars"),
            "gas_no_conv_slope": _fit_slope(rows, "gas_no_convencional_mm3"),
            "oil_no_conv_slope": _fit_slope(rows, "oil_no_convencional_m3"),
            "gas_share_slope": _fit_slope(rows, "gas_no_conv_share"),
            "oil_share_slope": _fit_slope(rows, "oil_no_conv_share"),
            "global_mean": global_mean,
        }
    return profiles


def _lag_signal(row: dict[str, Any], lag_keys: list[str]) -> float:
    lag_weights = [0.60, 0.25, 0.15][: len(lag_keys)]
    return sum(weight * float(row[key]) for weight, key in zip(lag_weights, lag_keys))


def _predict_row(row: dict[str, Any], profile: dict[str, Any], lag_keys: list[str]) -> float:
    lag_signal = _lag_signal(row, lag_keys)
    month_mean = float(profile["month_mean"].get(row["month"], profile["price_mean"]))
    prediction = 0.55 * lag_signal + 0.45 * month_mean

    if row.get("usd_ars") is not None and profile["usd_mean"] > 0:
        prediction += 0.20 * float(profile["usd_slope"]) * (
            float(row["usd_ars"]) - float(profile["usd_mean"])
        )

    if row.get("gas_no_convencional_mm3") is not None and profile["gas_no_conv_mean"] > 0:
        prediction += 0.10 * float(profile["gas_no_conv_slope"]) * (
            float(row["gas_no_convencional_mm3"]) - float(profile["gas_no_conv_mean"])
        )

    if row.get("oil_no_convencional_m3") is not None and profile["oil_no_conv_mean"] > 0:
        prediction += 0.10 * float(profile["oil_no_conv_slope"]) * (
            float(row["oil_no_convencional_m3"]) - float(profile["oil_no_conv_mean"])
        )

    if row.get("gas_no_conv_share") is not None:
        prediction += 0.10 * float(profile["gas_share_slope"]) * (
            float(row["gas_no_conv_share"]) - float(profile["gas_share_mean"])
        )

    if row.get("oil_no_conv_share") is not None:
        prediction += 0.05 * float(profile["oil_share_slope"]) * (
            float(row["oil_no_conv_share"]) - float(profile["oil_share_mean"])
        )

    return max(prediction, 0.0)


def train_and_predict(dataset: dict[str, Any]) -> dict[str, Any]:
    """Train a candidate model and return predictions plus run metadata."""
    train_rows = dataset["train_rows"]
    validation_rows = dataset["validation_rows"]
    lag_keys = dataset["lag_keys"]
    profiles = _compute_profiles(train_rows)

    predictions: list[dict[str, Any]] = []
    for row in validation_rows:
        profile = profiles.get(row["cuenca"])
        if profile is None:
            raise RuntimeError(f"Missing training profile for cuenca {row['cuenca']!r}.")
        prediction = _predict_row(row, profile, lag_keys)
        predictions.append(
            {
                "fecha": row["fecha"],
                "cuenca": row["cuenca"],
                "basin_group": row["basin_group"],
                "actual_price": float(row["actual_price"]),
                "predicted_price": prediction,
            }
        )

    return {
        "hypothesis": "Seasonal supply price blend with FX and unconventional output adjustments",
        "predictions": predictions,
        "model_artifacts": {
            "lag_keys": lag_keys,
            "cuencas": sorted(profiles),
            "training_rows": len(train_rows),
            "validation_rows": len(validation_rows),
        },
    }
