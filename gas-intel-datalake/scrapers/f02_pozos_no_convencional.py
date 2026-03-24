"""
F02 — Produccion SESCO + Tight y Shale Capitulo IV
Source: Secretaria de Energia / datos.energia.gob.ar
Tier 1 — Automated
Table: pozos_no_convencional
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OIL_URL = (
    "http://datos.energia.gob.ar/dataset/590d1284-fd6d-4686-afd8-b3da5d90a6e9/"
    "resource/83a2b597-b087-4815-b17d-cd70990d6a79/download/"
    "produccin-petrleo-sesco-tight-y-shale-captulo-iv-por-yacimiento.csv"
)
GAS_URL = (
    "http://datos.energia.gob.ar/dataset/590d1284-fd6d-4686-afd8-b3da5d90a6e9/"
    "resource/931cfb07-37b7-414a-ae8b-528dff6f9f14/download/"
    "produccin-gas-sesco-tight-y-shale-captulo-iv-por-yacimiento.csv"
)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "capitulo_iv"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"
RELEVANT_BASINS = {"NEUQUINA", "AUSTRAL", "NOROESTE"}

OIL_CONCEPT_MAP = {
    "produccion convencional": "oil_convencional_m3",
    "shale oil": "oil_shale_m3",
    "tight_oil": "oil_tight_m3",
}
GAS_CONCEPT_MAP = {
    "produccion convencional": "gas_convencional_mm3",
    "shale gas": "gas_shale_mm3",
    "tight gas": "gas_tight_mm3",
}


def _download(url: str, slug: str) -> Path:
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(resp.content).hexdigest()[:8]
    path = RAW_DIR / f"{slug}_{datetime.now().strftime('%Y%m%d')}_{digest}.csv"
    path.write_bytes(resp.content)
    log.info("Saved %s", path)
    return path


def _load_and_normalize(path: Path, value_column: str, concept_map: dict[str, str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [column.strip().lower() for column in df.columns]
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["cuenca"] = df["cuenca"].astype(str).str.upper().str.strip()
    df = df[df["fecha"].notna()]
    df = df[df["cuenca"].isin(RELEVANT_BASINS)]
    df["concepto"] = df["concepto"].astype(str).str.lower().str.strip()
    df = df[df["concepto"].isin(concept_map)]
    df[value_column] = pd.to_numeric(df[value_column], errors="coerce").fillna(0.0)
    df["metric"] = df["concepto"].map(concept_map)

    grouped = (
        df.groupby(
            [
                "fecha",
                "empresa",
                "areapermisoconcesion",
                "idareapermisoconcesion",
                "areayacimiento",
                "idareayacimiento",
                "cuenca",
                "provincia",
                "metric",
            ],
            dropna=False,
        )[value_column]
        .sum()
        .reset_index()
    )
    pivot = grouped.pivot_table(
        index=[
            "fecha",
            "empresa",
            "areapermisoconcesion",
            "idareapermisoconcesion",
            "areayacimiento",
            "idareayacimiento",
            "cuenca",
            "provincia",
        ],
        columns="metric",
        values=value_column,
        fill_value=0.0,
        aggfunc="sum",
    ).reset_index()
    pivot.columns.name = None
    return pivot


def _merge_frames(oil_df: pd.DataFrame, gas_df: pd.DataFrame) -> pd.DataFrame:
    df = oil_df.merge(
        gas_df,
        on=[
            "fecha",
            "empresa",
            "areapermisoconcesion",
            "idareapermisoconcesion",
            "areayacimiento",
            "idareayacimiento",
            "cuenca",
            "provincia",
        ],
        how="outer",
    ).fillna(0.0)

    for column in [
        "oil_convencional_m3",
        "oil_shale_m3",
        "oil_tight_m3",
        "gas_convencional_mm3",
        "gas_shale_mm3",
        "gas_tight_mm3",
    ]:
        if column not in df.columns:
            df[column] = 0.0

    df["oil_no_convencional_m3"] = df["oil_shale_m3"] + df["oil_tight_m3"]
    df["gas_no_convencional_mm3"] = df["gas_shale_mm3"] + df["gas_tight_mm3"]

    df = df.rename(
        columns={
            "empresa": "operador",
            "areapermisoconcesion": "area_permiso_concesion",
            "idareapermisoconcesion": "area_permiso_concesion_id",
            "areayacimiento": "area_yacimiento",
            "idareayacimiento": "yacimiento_id",
        }
    )
    return df.sort_values(["fecha", "cuenca", "operador", "area_yacimiento"]).reset_index(drop=True)


def save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"pozos_no_convencional_{ts}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> pd.DataFrame:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    oil_path = _download(OIL_URL, "capitulo_iv_oil")
    gas_path = _download(GAS_URL, "capitulo_iv_gas")

    oil_df = _load_and_normalize(oil_path, "cantidad_m3", OIL_CONCEPT_MAP)
    gas_df = _load_and_normalize(gas_path, "cantidad_mm3", GAS_CONCEPT_MAP)
    merged = _merge_frames(oil_df, gas_df)

    processed_path = PROCESSED_DIR / "pozos_no_convencional.parquet"
    merged.to_parquet(processed_path, index=False)
    log.info(
        "Saved processed: %s (%s rows, %s -> %s)",
        processed_path,
        len(merged),
        merged["fecha"].min(),
        merged["fecha"].max(),
    )
    save_snapshot(merged)
    return merged


if __name__ == "__main__":
    run()
