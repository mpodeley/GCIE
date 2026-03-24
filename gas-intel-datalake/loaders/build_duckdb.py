"""Build the immutable DuckDB snapshot consumed by the GCIE engines."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
DUCKDB_DIR = ROOT_DIR / "duckdb"
DUCKDB_PATH = DUCKDB_DIR / "gas_intel.duckdb"

TABLE_SOURCES = {
    "produccion_diaria": "produccion_sesco_*.parquet",
    "gas_asociado_ratio": "gas_asociado_ratio_*.parquet",
    "consumo_diario": "consumo_diario*.parquet",
    "clima": "clima.parquet",
    "calendario": "calendario.parquet",
    "tipo_cambio": "tipo_cambio.parquet",
}
OPTIONAL_TABLE_SOURCES = {
    "pozos_no_convencional": "pozos_no_convencional.parquet",
    "precios_boca_pozo": "precios_boca_pozo.parquet",
    "transporte_flujo_mensual": "transporte_flujo_mensual.parquet",
    "transporte_capacidad_firme": "transporte_capacidad_firme.parquet",
    "transporte_utilizacion_mensual": "transporte_utilizacion_mensual.parquet",
    "red_nodos": "red_nodos.parquet",
    "red_tramos": "red_tramos.parquet",
    "red_tramo_alias": "red_tramo_alias.parquet",
    "red_tramo_metricas_mensuales": "red_tramo_metricas_mensuales.parquet",
    "red_nodos_canonica": "red_nodos_canonica.parquet",
    "red_tramos_canonica": "red_tramos_canonica.parquet",
    "red_tramo_alias_canonica": "red_tramo_alias_canonica.parquet",
    "red_topologia_diagnostico": "red_topologia_diagnostico.parquet",
    "red_nodo_metricas_mensuales": "red_nodo_metricas_mensuales.parquet",
    "red_nodo_roles_proxy": "red_nodo_roles_proxy.parquet",
    "red_nodo_exogenos_mensuales": "red_nodo_exogenos_mensuales.parquet",
    "red_balance_escenario_mensual": "red_balance_escenario_mensual.parquet",
    "red_solver_tramos_mensuales": "red_solver_tramos_mensuales.parquet",
    "red_solver_balance_nodal_mensual": "red_solver_balance_nodal_mensual.parquet",
    "red_solver_resumen_mensual": "red_solver_resumen_mensual.parquet",
    "red_pandapipes_junctions": "red_pandapipes_junctions.parquet",
    "red_pandapipes_pipes": "red_pandapipes_pipes.parquet",
}


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to build gas_intel.duckdb. Install the Python package first."
        ) from exc
    return duckdb


def _latest_match(pattern: str) -> Path | None:
    matches = sorted(PROCESSED_DIR.glob(pattern))
    return matches[-1] if matches else None


def _resolve_sources() -> tuple[dict[str, Path], list[str]]:
    resolved: dict[str, Path] = {}
    missing: list[str] = []
    for table_name, pattern in TABLE_SOURCES.items():
        path = _latest_match(pattern)
        if path is None:
            missing.append(f"{table_name} ({pattern})")
        else:
            resolved[table_name] = path
    for table_name, pattern in OPTIONAL_TABLE_SOURCES.items():
        path = _latest_match(pattern)
        if path is not None:
            resolved[table_name] = path
    return resolved, missing


def build_database() -> Path:
    duckdb = _import_duckdb()
    sources, missing = _resolve_sources()
    if missing:
        raise FileNotFoundError(
            "Cannot build gas_intel.duckdb. Missing processed parquet files: "
            + ", ".join(missing)
        )

    DUCKDB_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH))
    try:
        for table_name, parquet_path in sources.items():
            log.info("Materializing %s from %s", table_name, parquet_path)
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(
                f"""
                CREATE TABLE {table_name} AS
                SELECT *
                FROM read_parquet(?)
                """,
                [str(parquet_path)],
            )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    log.info("DuckDB snapshot ready at %s", DUCKDB_PATH)
    return DUCKDB_PATH


if __name__ == "__main__":
    build_database()
