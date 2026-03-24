"""Fixed data loading contract for the Demand Forecast engine."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any


ENGINE_NAME = "gas-intel-forecast"
DUCKDB_PATH = Path(__file__).resolve().parents[1] / "gas-intel-datalake" / "duckdb" / "gas_intel.duckdb"
REQUIRED_TABLES = ("consumo_diario", "clima", "calendario")
LAG_DAYS = (7, 14, 28)


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


def _fetch_base_rows(conn: Any) -> list[dict[str, Any]]:
    query = """
        WITH demand AS (
            SELECT
                CAST(fecha AS DATE) AS fecha,
                segmento,
                SUM(volumen_m3) AS actual_volume
            FROM consumo_diario
            GROUP BY 1, 2
        ),
        weather AS (
            SELECT
                CAST(fecha AS DATE) AS fecha,
                AVG(temp_media) AS temp_media,
                AVG(hdd) AS hdd,
                AVG(cdd) AS cdd
            FROM clima
            GROUP BY 1
        )
        SELECT
            demand.fecha,
            demand.segmento,
            demand.actual_volume,
            weather.temp_media,
            weather.hdd,
            weather.cdd,
            calendario.es_feriado,
            calendario.es_laborable,
            calendario.mes,
            calendario.trimestre,
            calendario.estacion
        FROM demand
        LEFT JOIN weather ON weather.fecha = demand.fecha
        LEFT JOIN calendario ON calendario.fecha = demand.fecha
        ORDER BY demand.fecha, demand.segmento
    """
    rows = conn.execute(query).fetchall()
    if not rows:
        raise RuntimeError("SP1 query returned no rows from the data lake.")

    base_rows: list[dict[str, Any]] = []
    for row in rows:
        fecha = row[0]
        if not isinstance(fecha, date):
            raise RuntimeError("SP1 expected DATE values from DuckDB.")
        base_rows.append(
            {
                "fecha": fecha,
                "segmento": row[1],
                "actual_volume": float(row[2]),
                "temp_media": None if row[3] is None else float(row[3]),
                "hdd": None if row[4] is None else float(row[4]),
                "cdd": None if row[5] is None else float(row[5]),
                "es_feriado": bool(row[6]) if row[6] is not None else False,
                "es_laborable": bool(row[7]) if row[7] is not None else False,
                "month": int(row[8]) if row[8] is not None else fecha.month,
                "quarter": int(row[9]) if row[9] is not None else ((fecha.month - 1) // 3) + 1,
                "estacion": row[10] if row[10] is not None else "unknown",
                "day_of_week": fecha.weekday(),
            }
        )
    return base_rows


def _add_lag_features(base_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    per_segment_history: dict[str, list[float]] = defaultdict(list)
    enriched_rows: list[dict[str, Any]] = []

    for row in base_rows:
        history = per_segment_history[row["segmento"]]
        enriched = dict(row)
        for lag in LAG_DAYS:
            enriched[f"lag_{lag}"] = history[-lag] if len(history) >= lag else None
        history.append(row["actual_volume"])
        enriched_rows.append(enriched)

    filtered_rows = [
        row for row in enriched_rows if all(row[f"lag_{lag}"] is not None for lag in LAG_DAYS)
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
        base_rows = _fetch_base_rows(conn)
    finally:
        conn.close()

    feature_rows = _add_lag_features(base_rows)
    train_rows, validation_rows = _split_train_validation(feature_rows)
    return {
        "engine": ENGINE_NAME,
        "duckdb_path": str(DUCKDB_PATH),
        "features": [
            "lag_7",
            "lag_14",
            "lag_28",
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
