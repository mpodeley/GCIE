"""
F04 — Precios de Gas Natural
Source: Secretaria de Energia / datos.energia.gob.ar
Tier 1 — Automated
Table: precios_boca_pozo

The source provides two CSVs:
- a basin-level file from 2019 onward
- a total-country aggregate file for 2018-12 / 2019-01 transition
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

CSV_BASIN_URL = (
    "http://datos.energia.gob.ar/dataset/5ddbdfbb-b6f9-4bc1-9e71-0055e86cf552/"
    "resource/d87ca6ab-2979-474b-994a-e4ba259bb217/download/precios-de-gas-natural-.csv"
)
CSV_TOTAL_URL = (
    "http://datos.energia.gob.ar/dataset/5ddbdfbb-b6f9-4bc1-9e71-0055e86cf552/"
    "resource/50f8a2e1-96a1-4407-b036-718e73c0921d/download/precios-de-gas-natural-.csv"
)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "precios_gas"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"


def _download_csv(url: str, slug: str) -> Path:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(resp.content).hexdigest()[:8]
    path = RAW_DIR / f"{slug}_{datetime.now().strftime('%Y%m%d')}_{digest}.csv"
    path.write_bytes(resp.content)
    log.info("Saved %s", path)
    return path


def _safe_mean(row: pd.Series, columns: list[str]) -> float | None:
    values = [pd.to_numeric(row[column], errors="coerce") for column in columns if column in row.index]
    values = [float(value) for value in values if pd.notna(value) and float(value) > 0]
    if not values:
        return None
    return sum(values) / len(values)


def _normalize_basin_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df[df["fecha"].notna()]
    df["contrato"] = df["contrato"].astype(str).str.upper().str.strip()
    df = df[df["contrato"] == "TOTAL"]
    price_columns = [
        "precio_distribuidora",
        "precio_gnc",
        "precio_usina",
        "precio_industria",
        "precio_otros",
        "precio_ppp",
        "precio_expo",
    ]
    df["precio_referencia_mmbtu"] = df.apply(lambda row: _safe_mean(row, price_columns), axis=1)
    df = df[df["precio_referencia_mmbtu"].notna()]
    df["cuenca"] = df["cuenca"].astype(str).str.strip()
    df["resolucion_se"] = "Res 1/2018"
    return df[["fecha", "cuenca", "precio_referencia_mmbtu", "resolucion_se"]]


def _normalize_total_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fecha"] = pd.to_datetime(
        {
            "year": pd.to_numeric(df["anio"], errors="coerce"),
            "month": pd.to_numeric(df["mes"], errors="coerce"),
            "day": 1,
        },
        errors="coerce",
    )
    df = df[df["fecha"].notna()]
    price_columns = ["distribuidoras", "gnc", "usina", "industria", "otros", "ppp"]
    df["precio_referencia_mmbtu"] = df.apply(lambda row: _safe_mean(row, price_columns), axis=1)
    df = df[df["precio_referencia_mmbtu"].notna()]
    df["cuenca"] = "Total Cuenca"
    df["resolucion_se"] = "Res 1/2018"
    return df[["fecha", "cuenca", "precio_referencia_mmbtu", "resolucion_se"]]


def save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"precios_boca_pozo_{ts}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> pd.DataFrame:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    basin_path = _download_csv(CSV_BASIN_URL, "precios_gas_basin")
    total_path = _download_csv(CSV_TOTAL_URL, "precios_gas_total")

    basin_df = pd.read_csv(basin_path)
    total_df = pd.read_csv(total_path)

    normalized = pd.concat(
        [
            _normalize_total_prices(total_df),
            _normalize_basin_prices(basin_df),
        ],
        ignore_index=True,
    )
    normalized = (
        normalized.sort_values(["fecha", "cuenca"])
        .drop_duplicates(subset=["fecha", "cuenca"], keep="last")
        .reset_index(drop=True)
    )

    processed_path = PROCESSED_DIR / "precios_boca_pozo.parquet"
    normalized.to_parquet(processed_path, index=False)
    log.info(
        "Saved processed: %s (%s rows, %s -> %s)",
        processed_path,
        len(normalized),
        normalized["fecha"].min(),
        normalized["fecha"].max(),
    )
    save_snapshot(normalized)
    return normalized


if __name__ == "__main__":
    run()
