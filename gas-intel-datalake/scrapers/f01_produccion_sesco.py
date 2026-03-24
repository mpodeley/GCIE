"""
F01 — Producción de Petróleo y Gas (SESCO)
Source: Secretaría de Energía / datos.gob.ar
Tier 1 — Automated
Tables: produccion_diaria, gas_asociado_ratio
"""
import hashlib
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# URLs estables del recurso CSV en datos.gob.ar
URL_DESDE_2019 = "https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_3752bb79-7229-4a3b-8f61-c617bfb17677"
URL_HASTA_2008 = "https://datos.gob.ar/dataset/energia-produccion-petroleo-gas-sesco/archivo/energia_f4cf0c95-68c7-476e-b279-89e0d43b1b71"

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "sesco"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"

CUENCAS_RELEVANTES = ["NEUQUINA", "AUSTRAL", "NOROESTE"]


def download_csv(url: str, dest_path: Path) -> Path:
    """Download CSV from datos.gob.ar. Returns path to saved file."""
    log.info(f"Downloading {url}")
    # datos.gob.ar sometimes redirects to actual CSV; follow redirects
    resp = requests.get(url, timeout=120, allow_redirects=True)
    resp.raise_for_status()

    # Detect actual CSV URL from redirect or content
    content = resp.content
    sha = hashlib.sha256(content).hexdigest()[:8]
    fname = dest_path / f"sesco_{datetime.now().strftime('%Y%m%d')}_{sha}.csv"
    fname.write_bytes(content)
    log.info(f"Saved {len(content):,} bytes → {fname}")
    return fname


def parse_sesco(csv_path: Path) -> pd.DataFrame:
    """Parse SESCO CSV into normalized DataFrame."""
    # SESCO CSVs typically use ; separator and Latin-1 encoding
    df = None
    for sep in [";", ","]:
        for enc in ["latin-1", "utf-8", "utf-8-sig"]:
            try:
                df = pd.read_csv(csv_path, sep=sep, encoding=enc, low_memory=False)
                if len(df.columns) > 3:
                    log.info(f"Parsed with sep={sep!r} enc={enc}: {len(df):,} rows, {len(df.columns)} cols")
                    break
            except Exception:
                continue
        if df is not None and len(df.columns) > 3:
            break

    if df is None or len(df.columns) <= 3:
        raise ValueError(f"Could not parse {csv_path}")

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def normalize_produccion(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw SESCO DataFrame to produccion_diaria schema."""
    # Detect date column (varies across SESCO versions)
    date_cols = [c for c in df.columns if "fecha" in c or "periodo" in c or "anio" in c or "año" in c]
    if not date_cols:
        raise ValueError(f"No date column found. Columns: {list(df.columns)}")

    date_col = date_cols[0]
    log.info(f"Date column: {date_col}")

    # Detect production columns
    gas_cols = [c for c in df.columns if "gas" in c and ("produc" in c or "mm3" in c or "miles" in c or "volumen" in c)]
    oil_cols = [c for c in df.columns if ("petroleo" in c or "petróleo" in c or "crude" in c) and ("produc" in c or "volumen" in c)]

    log.info(f"Gas cols: {gas_cols}")
    log.info(f"Oil cols: {oil_cols}")

    # Build normalized output
    result = pd.DataFrame()
    result["fecha"] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)

    # Cuenca
    cuenca_col = next((c for c in df.columns if "cuenca" in c), None)
    if cuenca_col:
        result["cuenca"] = df[cuenca_col].str.upper().str.strip()

    # Empresa/operador
    emp_col = next((c for c in df.columns if "empresa" in c or "operador" in c), None)
    if emp_col:
        result["operador"] = df[emp_col].str.strip()

    # Yacimiento
    yac_col = next((c for c in df.columns if "yacimiento" in c), None)
    if yac_col:
        result["yacimiento_id"] = df[yac_col].str.strip()

    # Formación
    form_col = next((c for c in df.columns if "formac" in c), None)
    if form_col:
        result["formacion"] = df[form_col].str.strip()

    # Producción gas (en miles m3 → convertir a mm3/d aproximado)
    if gas_cols:
        result["gas_miles_m3"] = pd.to_numeric(df[gas_cols[0]], errors="coerce")

    # Producción petróleo (en m3)
    if oil_cols:
        result["petroleo_m3"] = pd.to_numeric(df[oil_cols[0]], errors="coerce")

    # Drop rows without fecha
    result = result.dropna(subset=["fecha"])

    # Filter relevant basins if cuenca column exists
    if "cuenca" in result.columns and len(result) > 0:
        mask = result["cuenca"].isin(CUENCAS_RELEVANTES)
        n_before = len(result)
        result = result[mask]
        log.info(f"Filtered to relevant basins: {n_before} → {len(result)} rows")

    return result.reset_index(drop=True)


def calculate_gor(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Gas-Oil Ratio (GOR) by basin/month.
    GOR = gas_total / petroleo_total per basin per month.
    High GOR = gas asociado is significant fraction of production.
    """
    if "cuenca" not in df.columns or "gas_miles_m3" not in df.columns or "petroleo_m3" not in df.columns:
        log.warning("Missing columns for GOR calculation")
        return pd.DataFrame()

    df["year_month"] = df["fecha"].dt.to_period("M")

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
    content_hash = hashlib.sha256(df.to_json().encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{ts}_{content_hash}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Snapshot saved: {path} ({len(df):,} rows)")
    return path


def run():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== F01 SESCO Production Scraper ===")

    # Download main dataset (from 2019)
    try:
        csv_path = download_csv(URL_DESDE_2019, RAW_DIR)
    except Exception as e:
        log.error(f"Download failed: {e}")
        raise

    # Parse and normalize
    raw_df = parse_sesco(csv_path)
    prod_df = normalize_produccion(raw_df)

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
