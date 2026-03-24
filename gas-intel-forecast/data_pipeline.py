"""Fixed data loading contract for the Demand Forecast engine."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any


ENGINE_NAME = "gas-intel-forecast"
DUCKDB_PATH = Path(__file__).resolve().parents[1] / "gas-intel-datalake" / "duckdb" / "gas_intel.duckdb"
REQUIRED_TABLES = ("consumo_diario", "clima", "calendario")
DAILY_LAGS = (7, 14, 28)
MONTHLY_LAGS = (1, 2, 3)


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb Python package is required for SP1. Install it before running evaluate.py."
        ) from exc
    return duckdb


def _ensure_database_available() -> None:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"Expected DuckDB snapshot at {DUCKDB_PATH}, but it does not exist yet."
        )


def _ensure_tables_exist(conn: Any) -> None:
    existing_tables = {
        row[0]
        for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    }
    missing_tables = [table for table in REQUIRED_TABLES if table not in existing_tables]
    if missing_tables:
        raise RuntimeError(f"Missing required DuckDB tables for SP1: {', '.join(missing_tables)}")


def _fetch_demand_rows(conn: Any) -> list[dict[str, Any]]:
    query = """
        SELECT
            CAST(fecha AS DATE) AS fecha,
            segmento,
            SUM(volumen_m3) AS actual_volume
        FROM consumo_diario
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    rows = conn.execute(query).fetchall()
    if not rows:
        raise RuntimeError("SP1 query returned no rows from consumo_diario.")

    demand_rows = []
    for row in rows:
        fecha = row[0]
        if not isinstance(fecha, date):
            raise RuntimeError("SP1 expected DATE values from DuckDB.")
        demand_rows.append(
            {
                "fecha": fecha,
                "segmento": row[1],
                "actual_volume": float(row[2]),
            }
        )
    return demand_rows


def _fetch_weather_by_date(conn: Any) -> dict[date, dict[str, float | None]]:
    rows = conn.execute(
        """
        SELECT
            CAST(fecha AS DATE) AS fecha,
            AVG(temp_media) AS temp_media,
            AVG(hdd) AS hdd,
            AVG(cdd) AS cdd
        FROM clima
        GROUP BY 1
        """
    ).fetchall()
    weather = {}
    for row in rows:
        weather[row[0]] = {
            "temp_media": None if row[1] is None else float(row[1]),
            "hdd": None if row[2] is None else float(row[2]),
            "cdd": None if row[3] is None else float(row[3]),
        }
    return weather


def _fetch_calendar_by_date(conn: Any) -> dict[date, dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            CAST(fecha AS DATE) AS fecha,
            es_feriado,
            es_laborable,
            mes,
            trimestre,
            estacion
        FROM calendario
        """
    ).fetchall()
    calendar = {}
    for row in rows:
        calendar[row[0]] = {
            "es_feriado": bool(row[1]) if row[1] is not None else False,
            "es_laborable": bool(row[2]) if row[2] is not None else False,
            "month": int(row[3]) if row[3] is not None else row[0].month,
            "quarter": int(row[4]) if row[4] is not None else ((row[0].month - 1) // 3) + 1,
            "estacion": row[5] if row[5] is not None else "unknown",
        }
    return calendar


def _is_monthly_cadence(demand_rows: list[dict[str, Any]]) -> bool:
    unique_dates = sorted({row["fecha"] for row in demand_rows})
    if len(unique_dates) < 2:
        return False
    if any(d.day != 1 for d in unique_dates):
        return False
    return True


def _aggregate_weather_monthly(weather_by_date: dict[date, dict[str, float | None]]) -> dict[tuple[int, int], dict[str, float | None]]:
    buckets: dict[tuple[int, int], dict[str, list[float]]] = defaultdict(
        lambda: {"temp_media": [], "hdd": [], "cdd": []}
    )
    for weather_date, metrics in weather_by_date.items():
        key = (weather_date.year, weather_date.month)
        for field in ("temp_media", "hdd", "cdd"):
            value = metrics[field]
            if value is not None:
                buckets[key][field].append(value)

    result = {}
    for key, metrics in buckets.items():
        result[key] = {
            field: (sum(values) / len(values) if values else None)
            for field, values in metrics.items()
        }
    return result


def _fetch_base_rows(conn: Any) -> tuple[list[dict[str, Any]], str, tuple[int, ...]]:
    demand_rows = _fetch_demand_rows(conn)
    weather_by_date = _fetch_weather_by_date(conn)
    calendar_by_date = _fetch_calendar_by_date(conn)

    cadence = "monthly" if _is_monthly_cadence(demand_rows) else "daily"
    lag_periods = MONTHLY_LAGS if cadence == "monthly" else DAILY_LAGS
    weather_by_month = _aggregate_weather_monthly(weather_by_date) if cadence == "monthly" else {}

    base_rows: list[dict[str, Any]] = []
    for row in demand_rows:
        fecha = row["fecha"]
        if cadence == "monthly":
            weather = weather_by_month.get((fecha.year, fecha.month), {})
            calendar = {
                "es_feriado": False,
                "es_laborable": True,
                "month": fecha.month,
                "quarter": ((fecha.month - 1) // 3) + 1,
                "estacion": calendar_by_date.get(fecha, {}).get("estacion", "unknown"),
            }
        else:
            weather = weather_by_date.get(fecha, {})
            calendar = calendar_by_date.get(
                fecha,
                {
                    "es_feriado": False,
                    "es_laborable": False,
                    "month": fecha.month,
                    "quarter": ((fecha.month - 1) // 3) + 1,
                    "estacion": "unknown",
                },
            )

        base_rows.append(
            {
                "fecha": fecha,
                "segmento": row["segmento"],
                "actual_volume": row["actual_volume"],
                "temp_media": weather.get("temp_media"),
                "hdd": weather.get("hdd"),
                "cdd": weather.get("cdd"),
                "es_feriado": calendar["es_feriado"],
                "es_laborable": calendar["es_laborable"],
                "month": calendar["month"],
                "quarter": calendar["quarter"],
                "estacion": calendar["estacion"],
                "day_of_week": fecha.weekday(),
            }
        )
    return base_rows, cadence, lag_periods


def _add_lag_features(base_rows: list[dict[str, Any]], lag_periods: tuple[int, ...]) -> list[dict[str, Any]]:
    per_segment_history: dict[str, list[float]] = defaultdict(list)
    enriched_rows: list[dict[str, Any]] = []

    for row in base_rows:
        history = per_segment_history[row["segmento"]]
        enriched = dict(row)
        for lag in lag_periods:
            enriched[f"lag_{lag}"] = history[-lag] if len(history) >= lag else None
        history.append(row["actual_volume"])
        enriched_rows.append(enriched)

    filtered_rows = [
        row for row in enriched_rows if all(row[f"lag_{lag}"] is not None for lag in lag_periods)
    ]
    if not filtered_rows:
        raise RuntimeError("SP1 could not build lag features; not enough historical depth.")
    return filtered_rows


def _split_train_validation(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    months = sorted({(row["fecha"].year, row["fecha"].month) for row in rows})
    if len(months) < 4:
        raise RuntimeError("SP1 needs at least 4 distinct months to hold out the last 3 months.")

    validation_months = set(months[-3:])
    train_rows = [row for row in rows if (row["fecha"].year, row["fecha"].month) not in validation_months]
    validation_rows = [row for row in rows if (row["fecha"].year, row["fecha"].month) in validation_months]

    if not train_rows or not validation_rows:
        raise RuntimeError("SP1 train/validation split produced an empty partition.")
    return train_rows, validation_rows


def load_dataset() -> dict[str, Any]:
    """Return the dataset payload expected by model.py and evaluate.py."""
    _ensure_database_available()
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        _ensure_tables_exist(conn)
        base_rows, cadence, lag_periods = _fetch_base_rows(conn)
    finally:
        conn.close()

    feature_rows = _add_lag_features(base_rows, lag_periods)
    train_rows, validation_rows = _split_train_validation(feature_rows)
    return {
        "engine": ENGINE_NAME,
        "cadence": cadence,
        "duckdb_path": str(DUCKDB_PATH),
        "lag_keys": [f"lag_{lag}" for lag in lag_periods],
        "features": [
            *[f"lag_{lag}" for lag in lag_periods],
            "temp_media",
            "hdd",
            "cdd",
            "day_of_week",
            "month",
            "quarter",
            "es_feriado",
            "es_laborable",
            "estacion",
        ],
        "train_rows": train_rows,
        "validation_rows": validation_rows,
    }
