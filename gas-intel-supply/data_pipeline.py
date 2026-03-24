"""Fixed data loading contract for the Supply engine."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any


ENGINE_NAME = "gas-intel-supply"
DUCKDB_PATH = Path(__file__).resolve().parents[1] / "gas-intel-datalake" / "duckdb" / "gas_intel.duckdb"
REQUIRED_TABLES = (
    "precios_boca_pozo",
    "pozos_no_convencional",
    "tipo_cambio",
    "transporte_utilizacion_mensual",
)
TARGET_BASIN_MAP = {
    "Austral Santa Cruz": "AUSTRAL",
    "Austral Tierra del Fuego": "AUSTRAL",
    "Neuquina": "NEUQUINA",
    "Noroeste": "NOROESTE",
    "Golfo de San Jorge": "SAN_JORGE",
    "Total Cuenca": "TOTAL",
}
BASIN_TRANSPORT_MAP = {
    "NEUQUINA": {
        ("TGS", "Neuba I+II"),
        ("TGN", "Centro Oeste"),
        ("Distribuidoras", "Malargüe (1)"),
    },
    "AUSTRAL": {
        ("TGS", "Gral. San Martin"),
        ("TGS", "San Martín"),
        ("Distribuidoras", "Sur (2)"),
    },
    "NOROESTE": {
        ("TGN", "Norte"),
    },
    "SAN_JORGE": {
        ("TGS", "San Martín"),
        ("Distribuidoras", "Sur (2)"),
    },
}
LAG_PERIODS = (1, 3, 6)


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb Python package is required for SP2. Install it before running evaluate.py."
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
        raise RuntimeError(f"Missing required DuckDB tables for SP2: {', '.join(missing_tables)}")


def _fetch_target_rows(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            CAST(fecha AS DATE) AS fecha,
            cuenca,
            precio_referencia_mmbtu
        FROM precios_boca_pozo
        WHERE precio_referencia_mmbtu IS NOT NULL
        ORDER BY 1, 2
        """
    ).fetchall()
    if not rows:
        raise RuntimeError("SP2 query returned no rows from precios_boca_pozo.")

    targets: list[dict[str, Any]] = []
    for row in rows:
        fecha = row[0]
        cuenca = row[1]
        basin_group = TARGET_BASIN_MAP.get(cuenca, "OTHER")
        if not isinstance(fecha, date):
            raise RuntimeError("SP2 expected DATE values from DuckDB.")
        targets.append(
            {
                "fecha": fecha,
                "cuenca": cuenca,
                "basin_group": basin_group,
                "actual_price": float(row[2]),
            }
        )
    return targets


def _fetch_no_conv_features(conn: Any) -> dict[tuple[date, str], dict[str, float | None]]:
    rows = conn.execute(
        """
        SELECT
            CAST(date_trunc('month', fecha) AS DATE) AS fecha,
            cuenca,
            SUM(gas_no_convencional_mm3) AS gas_no_convencional_mm3,
            SUM(oil_no_convencional_m3) AS oil_no_convencional_m3,
            SUM(gas_convencional_mm3) AS gas_convencional_mm3,
            SUM(oil_convencional_m3) AS oil_convencional_m3
        FROM pozos_no_convencional
        GROUP BY 1, 2
        """
    ).fetchall()

    features: dict[tuple[date, str], dict[str, float | None]] = {}
    for row in rows:
        gas_no_conv = 0.0 if row[2] is None else float(row[2])
        oil_no_conv = 0.0 if row[3] is None else float(row[3])
        gas_conv = 0.0 if row[4] is None else float(row[4])
        oil_conv = 0.0 if row[5] is None else float(row[5])
        total_gas = gas_no_conv + gas_conv
        total_oil = oil_no_conv + oil_conv
        features[(row[0], row[1])] = {
            "gas_no_convencional_mm3": gas_no_conv,
            "oil_no_convencional_m3": oil_no_conv,
            "gas_convencional_mm3": gas_conv,
            "oil_convencional_m3": oil_conv,
            "gas_no_conv_share": gas_no_conv / total_gas if total_gas > 0 else None,
            "oil_no_conv_share": oil_no_conv / total_oil if total_oil > 0 else None,
        }
    return features


def _fetch_fx_by_month(conn: Any) -> dict[date, float]:
    rows = conn.execute(
        """
        SELECT
            CAST(date_trunc('month', fecha) AS DATE) AS fecha,
            AVG(usd_ars) AS usd_ars
        FROM tipo_cambio
        GROUP BY 1
        """
    ).fetchall()
    return {row[0]: float(row[1]) for row in rows if row[1] is not None}


def _fetch_transport_features(conn: Any) -> dict[tuple[date, str], dict[str, float | bool | None]]:
    rows = conn.execute(
        """
        SELECT
            CAST(fecha AS DATE) AS fecha,
            transportista,
            gasoducto,
            utilization_ratio_proxy,
            headroom_proxy_m3,
            congestion_flag_proxy
        FROM transporte_utilizacion_mensual
        """
    ).fetchall()

    by_month: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_month[row[0]].append(
            {
                "transportista": row[1],
                "gasoducto": row[2],
                "utilization_ratio_proxy": None if row[3] is None else float(row[3]),
                "headroom_proxy_m3": None if row[4] is None else float(row[4]),
                "congestion_flag_proxy": bool(row[5]) if row[5] is not None else False,
            }
        )

    result: dict[tuple[date, str], dict[str, float | bool | None]] = {}
    for month, month_rows in by_month.items():
        for basin_group in {"TOTAL", *BASIN_TRANSPORT_MAP}:
            if basin_group == "TOTAL":
                relevant_rows = month_rows
            else:
                corridors = BASIN_TRANSPORT_MAP[basin_group]
                relevant_rows = [
                    row
                    for row in month_rows
                    if (row["transportista"], row["gasoducto"]) in corridors
                ]

            utilization_values = [
                float(row["utilization_ratio_proxy"])
                for row in relevant_rows
                if row["utilization_ratio_proxy"] is not None
            ]
            headroom_values = [
                float(row["headroom_proxy_m3"])
                for row in relevant_rows
                if row["headroom_proxy_m3"] is not None
            ]
            result[(month, basin_group)] = {
                "transport_util_mean": (
                    sum(utilization_values) / len(utilization_values) if utilization_values else None
                ),
                "transport_util_max": max(utilization_values) if utilization_values else None,
                "transport_headroom_min_m3": min(headroom_values) if headroom_values else None,
                "winter_congestion_flag": any(
                    bool(row["congestion_flag_proxy"]) for row in relevant_rows
                ),
            }
    return result


def _build_base_rows(conn: Any) -> list[dict[str, Any]]:
    target_rows = _fetch_target_rows(conn)
    no_conv_features = _fetch_no_conv_features(conn)
    fx_by_month = _fetch_fx_by_month(conn)
    transport_features = _fetch_transport_features(conn)

    base_rows: list[dict[str, Any]] = []
    for row in target_rows:
        row_date = row["fecha"]
        basin_group = row["basin_group"]
        no_conv = no_conv_features.get((row_date, basin_group), {})
        transport = transport_features.get((row_date, basin_group), {})

        base_rows.append(
            {
                "fecha": row_date,
                "cuenca": row["cuenca"],
                "basin_group": basin_group,
                "actual_price": row["actual_price"],
                "usd_ars": fx_by_month.get(row_date),
                "gas_no_convencional_mm3": no_conv.get("gas_no_convencional_mm3"),
                "oil_no_convencional_m3": no_conv.get("oil_no_convencional_m3"),
                "gas_convencional_mm3": no_conv.get("gas_convencional_mm3"),
                "oil_convencional_m3": no_conv.get("oil_convencional_m3"),
                "gas_no_conv_share": no_conv.get("gas_no_conv_share"),
                "oil_no_conv_share": no_conv.get("oil_no_conv_share"),
                "transport_util_mean": transport.get("transport_util_mean"),
                "transport_util_max": transport.get("transport_util_max"),
                "transport_headroom_min_m3": transport.get("transport_headroom_min_m3"),
                "winter_congestion_flag": bool(transport.get("winter_congestion_flag", False)),
                "month": row_date.month,
                "quarter": ((row_date.month - 1) // 3) + 1,
            }
        )
    return base_rows


def _add_lag_features(base_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    per_cuenca_history: dict[str, list[float]] = defaultdict(list)
    feature_rows: list[dict[str, Any]] = []

    for row in sorted(base_rows, key=lambda item: (item["cuenca"], item["fecha"])):
        history = per_cuenca_history[row["cuenca"]]
        enriched = dict(row)
        for lag in LAG_PERIODS:
            enriched[f"lag_{lag}"] = history[-lag] if len(history) >= lag else None
        history.append(row["actual_price"])
        feature_rows.append(enriched)

    filtered_rows = [
        row for row in feature_rows if all(row[f"lag_{lag}"] is not None for lag in LAG_PERIODS)
    ]
    if not filtered_rows:
        raise RuntimeError("SP2 could not build lag features; not enough historical depth.")
    return filtered_rows


def _split_train_validation(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    months = sorted({row["fecha"] for row in rows})
    if len(months) < 18:
        raise RuntimeError("SP2 needs at least 18 monthly observations to hold out the last 12 months.")

    validation_months = set(months[-12:])
    train_rows = [row for row in rows if row["fecha"] not in validation_months]
    validation_rows = [row for row in rows if row["fecha"] in validation_months]

    if not train_rows or not validation_rows:
        raise RuntimeError("SP2 train/validation split produced an empty partition.")
    return train_rows, validation_rows


def load_dataset() -> dict[str, Any]:
    """Return the dataset payload expected by model.py and evaluate.py."""
    _ensure_database_available()
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        _ensure_tables_exist(conn)
        base_rows = _build_base_rows(conn)
    finally:
        conn.close()

    feature_rows = _add_lag_features(base_rows)
    train_rows, validation_rows = _split_train_validation(feature_rows)
    return {
        "engine": ENGINE_NAME,
        "duckdb_path": str(DUCKDB_PATH),
        "lag_keys": [f"lag_{lag}" for lag in LAG_PERIODS],
        "features": [
            *[f"lag_{lag}" for lag in LAG_PERIODS],
            "usd_ars",
            "gas_no_convencional_mm3",
            "oil_no_convencional_m3",
            "gas_no_conv_share",
            "oil_no_conv_share",
            "transport_util_mean",
            "transport_util_max",
            "transport_headroom_min_m3",
            "winter_congestion_flag",
            "month",
            "quarter",
        ],
        "train_rows": train_rows,
        "validation_rows": validation_rows,
    }
