"""
F01 — Producción de Petróleo y Gas (SESCO)
Source: Secretaría de Energía / datos.gob.ar
Tier 1 — Automated
Tables: produccion_diaria, gas_asociado_ratio
"""

from __future__ import annotations

import hashlib
import io
import logging
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Resource page and direct ZIP payload currently served by datos.gob.ar / energia.gob.ar.
RESOURCE_PAGE_DESDE_2019 = "https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_3752bb79-7229-4a3b-8f61-c617bfb17677"
ZIP_URL_DESDE_2019 = "http://www.energia.gob.ar/contenidos/archivos/Reorganizacion/informacion_del_mercado/mercado_hidrocarburos/tablas_dinamicas/upstream/sescoweb_produccion.zip"

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "sesco"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"

MONTH_MAP = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def download_zip(url: str, dest_path: Path) -> Path:
    """Download the current SESCO ZIP workbook. Returns path to saved file."""
    log.info(f"Downloading {url}")
    resp = requests.get(url, timeout=120, allow_redirects=True)
    resp.raise_for_status()

    content = resp.content
    sha = hashlib.sha256(content).hexdigest()[:8]
    fname = dest_path / f"sesco_{datetime.now().strftime('%Y%m%d')}_{sha}.zip"
    fname.write_bytes(content)
    log.info(f"Saved {len(content):,} bytes → {fname}")
    return fname


def _read_workbook(zip_path: Path) -> dict[str, pd.DataFrame]:
    with zipfile.ZipFile(zip_path) as zf:
        xlsx_members = [name for name in zf.namelist() if name.lower().endswith(".xlsx")]
        if not xlsx_members:
            raise ValueError(f"No .xlsx workbook found inside {zip_path}")
        workbook_bytes = io.BytesIO(zf.read(xlsx_members[0]))
    return {
        "oil": pd.read_excel(workbook_bytes, sheet_name="Producción Oil m3", header=None),
        "gas": pd.read_excel(io.BytesIO(workbook_bytes.getvalue()), sheet_name="Producción Gas miles m3", header=None),
    }


def _extract_year(raw_sheet: pd.DataFrame) -> int:
    year_rows = raw_sheet[raw_sheet[0].astype(str).str.strip().str.lower() == "año"]
    if year_rows.empty:
        raise ValueError("Could not locate 'Año' filter row in SESCO workbook.")
    return int(year_rows.iloc[0, 1])


def _extract_monthly_table(raw_sheet: pd.DataFrame, value_column_name: str) -> pd.DataFrame:
    header_index = raw_sheet.index[
        raw_sheet[0].astype(str).str.strip().str.lower() == "empresa"
    ]
    if len(header_index) == 0:
        raise ValueError("Could not locate the 'empresa' header row in SESCO workbook.")

    start = int(header_index[0])
    year = _extract_year(raw_sheet)
    header_row = raw_sheet.iloc[start].tolist()
    data = raw_sheet.iloc[start + 1 :].copy()
    data.columns = header_row
    data = data.rename(columns={"empresa": "operador"})
    data = data.dropna(subset=["operador"])
    data["operador"] = data["operador"].astype(str).str.strip()
    data = data[data["operador"] != ""]
    data = data[~data["operador"].str.lower().str.startswith("total")]

    month_columns = [
        column
        for column in data.columns
        if isinstance(column, str) and column.strip().lower() in MONTH_MAP
    ]
    if not month_columns:
        raise ValueError("Could not locate monthly value columns in SESCO workbook.")

    long_df = data.melt(
        id_vars=["operador"],
        value_vars=month_columns,
        var_name="mes_nombre",
        value_name=value_column_name,
    )
    long_df[value_column_name] = pd.to_numeric(long_df[value_column_name], errors="coerce")
    long_df = long_df.dropna(subset=[value_column_name])
    long_df["month"] = long_df["mes_nombre"].astype(str).str.strip().str.lower().map(MONTH_MAP)
    long_df = long_df.dropna(subset=["month"])
    long_df["fecha"] = pd.to_datetime(
        {
            "year": year,
            "month": long_df["month"].astype(int),
            "day": 1,
        }
    )
    return long_df[["fecha", "operador", value_column_name]].reset_index(drop=True)


def normalize_produccion(zip_path: Path) -> pd.DataFrame:
    """Normalize the current SESCO workbook into a monthly production table."""
    sheets = _read_workbook(zip_path)
    oil_df = _extract_monthly_table(sheets["oil"], "petroleo_m3")
    gas_df = _extract_monthly_table(sheets["gas"], "gas_miles_m3")

    result = oil_df.merge(gas_df, on=["fecha", "operador"], how="outer")
    result["cuenca"] = None
    result["yacimiento_id"] = None
    result["formacion"] = None
    result["tipo_gas"] = "unknown"
    result = result[
        [
            "fecha",
            "cuenca",
            "operador",
            "yacimiento_id",
            "formacion",
            "tipo_gas",
            "gas_miles_m3",
            "petroleo_m3",
        ]
    ]
    return result.sort_values(["fecha", "operador"]).reset_index(drop=True)


def calculate_gor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Gas-Oil Ratio (GOR) by basin/month.
    GOR = gas_total / petroleo_total per basin per month.
    High GOR = gas asociado is significant fraction of production.
    """
    if "gas_miles_m3" not in df.columns or "petroleo_m3" not in df.columns:
        log.warning("Missing columns for GOR calculation")
        return pd.DataFrame()

    df["year_month"] = df["fecha"].dt.to_period("M")
    if "cuenca" not in df.columns or df["cuenca"].isna().all():
        df["cuenca"] = "ALL"

    gor_df = (
        df.groupby(["year_month", "cuenca"])
        .agg(
            prod_gas_total=("gas_miles_m3", "sum"),
            prod_petroleo_total=("petroleo_m3", "sum"),
        )
        .reset_index()
    )

    # GOR in m3 gas / m3 oil (raw units before normalization)
    gor_df["ratio_gor"] = gor_df["prod_gas_total"] / gor_df["prod_petroleo_total"].replace(0, float("nan"))

    # Classify gas type: "asociado" if significant oil production coexists
    # Heuristic: if petroleo > 0 and GOR < threshold → likely asociado
    GOR_LIBRE_THRESHOLD = 5000  # m3 gas per m3 oil — above this likely free gas field
    gor_df["tipo_gas_dominante"] = gor_df["ratio_gor"].apply(
        lambda x: "libre" if x > GOR_LIBRE_THRESHOLD else "asociado" if pd.notna(x) else "libre"
    )

    gor_df["fecha"] = gor_df["year_month"].dt.to_timestamp()
    return gor_df.drop(columns=["year_month"])


def save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    """Save DataFrame as immutable Parquet snapshot with hash."""
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Calculate hash of content
    content_hash = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{ts}_{content_hash}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Snapshot saved: {path} ({len(df):,} rows)")
    return path


def run():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== F01 SESCO Production Scraper ===")

    log.info(f"Source page: {RESOURCE_PAGE_DESDE_2019}")

    # Download current workbook payload (from 2019 resource page)
    try:
        zip_path = download_zip(ZIP_URL_DESDE_2019, RAW_DIR)
    except Exception as e:
        log.error(f"Download failed: {e}")
        raise

    # Parse and normalize
    prod_df = normalize_produccion(zip_path)

    log.info(f"Normalized produccion: {len(prod_df):,} rows, date range: {prod_df['fecha'].min()} → {prod_df['fecha'].max()}")

    # Save processed
    out_path = PROCESSED_DIR / f"produccion_sesco_{datetime.now().strftime('%Y%m%d')}.parquet"
    prod_df.to_parquet(out_path, index=False)
    log.info(f"Processed saved: {out_path}")

    # Calculate GOR
    gor_df = calculate_gor(prod_df)
    if len(gor_df) > 0:
        gor_path = PROCESSED_DIR / f"gas_asociado_ratio_{datetime.now().strftime('%Y%m%d')}.parquet"
        gor_df.to_parquet(gor_path, index=False)
        log.info(f"GOR table saved: {gor_path} ({len(gor_df):,} rows)")

    # Save snapshots
    save_snapshot(prod_df, "produccion_diaria")
    if len(gor_df) > 0:
        save_snapshot(gor_df, "gas_asociado_ratio")

    log.info("=== F01 complete ===")
    return prod_df, gor_df


if __name__ == "__main__":
    run()
