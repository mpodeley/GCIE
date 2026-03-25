"""
F24 — Activos Fisicos de Refuerzo de la Red Canonica
Source: Manual canonical templates seeded from official project data and public operator disclosures
Tier 1 — Manual + derived
Tables: red_compresoras_canonica, red_loops_canonica, red_tramos_parametros_canonica
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SNAPSHOTS_DIR = ROOT_DIR / "data" / "snapshots"
TEMPLATES_DIR = ROOT_DIR / "templates"

NODES_PATH = PROCESSED_DIR / "red_nodos_canonica.parquet"
EDGES_PATH = PROCESSED_DIR / "red_tramos_canonica.parquet"
ENARGAS_CROSSWALK_PATH = PROCESSED_DIR / "red_gasoductos_enargas_vs_modelada.parquet"
COMPRESSORS_PATH = TEMPLATES_DIR / "red_compresoras_override.csv"
LOOPS_PATH = TEMPLATES_DIR / "red_loops_override.csv"


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def _load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns].fillna(pd.NA)


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [path.name for path in (NODES_PATH, EDGES_PATH) if not path.exists()]
    if missing:
        raise FileNotFoundError("F24 requires F20b outputs first. Missing: " + ", ".join(missing))
    crosswalk = pd.DataFrame(
        columns=[
            "edge_id",
            "official_object_ids",
            "official_component_count",
            "official_tramos",
            "official_gasoductos",
            "official_tipos",
            "official_representative_object_id",
            "official_representative_tipo",
            "official_representative_gasoducto",
            "official_corridor_length_km",
            "official_total_component_length_km",
            "official_length_km",
            "official_component_summary",
            "official_components_json",
            "official_physical_pipe_count_assumed",
            "official_troncal_component_count",
            "official_paralelo_component_count",
            "official_loop_component_count",
            "match_strategy",
            "match_status",
        ]
    )
    if ENARGAS_CROSSWALK_PATH.exists():
        crosswalk = pd.read_parquet(ENARGAS_CROSSWALK_PATH)
    compressors = _load_csv(
        COMPRESSORS_PATH,
        [
            "asset_id",
            "nombre",
            "node_id",
            "gasoducto",
            "potencia_hp",
            "estado",
            "source_confidence",
            "topology_status",
            "notes",
        ],
    )
    loops = _load_csv(
        LOOPS_PATH,
        [
            "asset_id",
            "nombre",
            "edge_id",
            "gasoducto",
            "length_km",
            "diameter_m",
            "capacity_mm3_dia_incremental",
            "estado",
            "source_confidence",
            "topology_status",
            "notes",
        ],
    )
    return (
        pd.read_parquet(NODES_PATH),
        pd.read_parquet(EDGES_PATH),
        crosswalk,
        compressors,
        loops,
    )


def _build_compressors(nodes_df: pd.DataFrame, compressors_df: pd.DataFrame) -> pd.DataFrame:
    result = compressors_df.merge(
        nodes_df[["node_id", "nombre", "latitud", "longitud", "x_mercator", "y_mercator"]],
        on="node_id",
        how="left",
        suffixes=("", "_node"),
    )
    result["potencia_hp"] = pd.to_numeric(result["potencia_hp"], errors="coerce")
    result["source"] = "manual_canonical_assets"
    return result.sort_values(["gasoducto", "nombre"]).reset_index(drop=True)


def _build_loops(edges_df: pd.DataFrame, loops_df: pd.DataFrame) -> pd.DataFrame:
    result = loops_df.merge(
        edges_df[
            [
                "edge_id",
                "ruta",
                "origen",
                "destino",
                "source_node_id",
                "target_node_id",
                "capacidad_mm3_dia_override",
                "diameter_m_override",
                "length_km_override",
            ]
        ],
        on="edge_id",
        how="left",
    )
    for column in ("length_km", "diameter_m", "capacity_mm3_dia_incremental"):
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result["source"] = "manual_canonical_assets"
    return result.sort_values(["gasoducto", "nombre"]).reset_index(drop=True)


def _build_edge_parameters(edges_df: pd.DataFrame, crosswalk_df: pd.DataFrame, loops_df: pd.DataFrame) -> pd.DataFrame:
    active_loops = loops_df[loops_df["estado"].astype(str).str.lower() == "active"].copy()
    loop_summary = (
        active_loops.groupby("edge_id", dropna=False)
        .agg(
            active_loop_count=("asset_id", "count"),
            loop_length_km_total=("length_km", "sum"),
            loop_capacity_increment_mm3_dia=("capacity_mm3_dia_incremental", "sum"),
            loop_max_diameter_m=("diameter_m", "max"),
        )
        .reset_index()
    )

    crosswalk_columns = [
        "edge_id",
        "official_object_ids",
        "official_component_count",
        "official_tramos",
        "official_gasoductos",
        "official_tipos",
        "official_representative_object_id",
        "official_representative_tipo",
        "official_representative_gasoducto",
        "official_corridor_length_km",
        "official_total_component_length_km",
        "official_length_km",
        "official_component_summary",
        "official_components_json",
        "official_physical_pipe_count_assumed",
        "official_troncal_component_count",
        "official_paralelo_component_count",
        "official_loop_component_count",
        "match_strategy",
        "match_status",
    ]
    available_crosswalk_columns = [
        column for column in crosswalk_columns if column in crosswalk_df.columns
    ]
    result = edges_df.merge(
        crosswalk_df[available_crosswalk_columns],
        on="edge_id",
        how="left",
    )
    result = result.merge(loop_summary, on="edge_id", how="left").fillna(
        {
            "active_loop_count": 0,
            "loop_length_km_total": 0.0,
            "loop_capacity_increment_mm3_dia": 0.0,
        }
    )
    result["base_capacity_mm3_dia"] = pd.to_numeric(
        result["capacidad_mm3_dia_override"], errors="coerce"
    )
    result["effective_capacity_mm3_dia"] = (
        result["base_capacity_mm3_dia"].fillna(0.0) + result["loop_capacity_increment_mm3_dia"]
    )
    result["effective_diameter_m"] = pd.to_numeric(result["diameter_m_override"], errors="coerce").fillna(
        pd.to_numeric(result["loop_max_diameter_m"], errors="coerce")
    )
    result["official_component_count"] = pd.to_numeric(result["official_component_count"], errors="coerce")
    result["official_physical_pipe_count_assumed"] = pd.to_numeric(
        result["official_physical_pipe_count_assumed"], errors="coerce"
    )
    result["official_troncal_component_count"] = pd.to_numeric(
        result["official_troncal_component_count"], errors="coerce"
    )
    result["official_paralelo_component_count"] = pd.to_numeric(
        result["official_paralelo_component_count"], errors="coerce"
    )
    result["official_loop_component_count"] = pd.to_numeric(
        result["official_loop_component_count"], errors="coerce"
    )
    result["official_corridor_length_km"] = pd.to_numeric(
        result["official_corridor_length_km"], errors="coerce"
    )
    result["official_total_component_length_km"] = pd.to_numeric(
        result["official_total_component_length_km"], errors="coerce"
    )
    result["official_length_km"] = pd.to_numeric(result["official_length_km"], errors="coerce")
    result["effective_length_km"] = pd.to_numeric(result["length_km_override"], errors="coerce").fillna(
        result["official_corridor_length_km"].fillna(result["official_length_km"])
    )
    result["source"] = "derived_from_red_tramos_canonica_and_f24_assets"
    return result.sort_values(["gasoducto", "ruta"]).reset_index(drop=True)


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    nodes_df, edges_df, crosswalk_df, compressors_df, loops_df = _load_inputs()
    compressors_out = _build_compressors(nodes_df, compressors_df)
    loops_out = _build_loops(edges_df, loops_df)
    edge_params_out = _build_edge_parameters(edges_df, crosswalk_df, loops_out)

    outputs = {
        "red_compresoras_canonica.parquet": compressors_out,
        "red_loops_canonica.parquet": loops_out,
        "red_tramos_parametros_canonica.parquet": edge_params_out,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(compressors_out, "red_compresoras_canonica")
    _save_snapshot(loops_out, "red_loops_canonica")
    _save_snapshot(edge_params_out, "red_tramos_parametros_canonica")
    return compressors_out, loops_out, edge_params_out


if __name__ == "__main__":
    run()
