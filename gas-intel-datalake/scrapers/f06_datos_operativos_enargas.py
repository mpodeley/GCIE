"""
F06 — Datos Operativos de Gas Natural (ENARGAS)
Source: ENARGAS
Tier 1 — Automated
Table: consumo_diario

This scraper uses the official GETD workbook ("Gas Entregado Total Sistema")
and maps the historical monthly totals by user type into the consumo_diario
contract expected by downstream engines.
"""

from __future__ import annotations

import hashlib
import io
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GETD_URL = "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-estadisticos/GETD/GETD.xlsx"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "enargas"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"

SEGMENT_MAP = {
    ("Dis", "Residencial"): "residential",
    ("Dis", "Comercial"): "commercial",
    ("Dis", "Entes Oficiales"): "public_sector",
    ("Dis", "Industria"): "industrial",
    ("Dis", "Centrales eléctricas"): "power_generation",
    ("Dis", "Subdistribuidor"): "subdistributor",
    ("Dis", "GNC"): "gnc",
    ("Tra", "Industria"): "industrial_bypass",
    ("Tra", "RTP"): "rtp_cerri",
    ("Tra", "Centrales eléctricas"): "power_generation_bypass",
    ("Tra", "Subdistribuidor"): "subdistributor_bypass",
    ("Tra", "Exportaciones"): "exports",
    ("Off", "Centrales eléctricas"): "power_generation_off_system",
}


def download_workbook() -> bytes:
    log.info("Downloading %s", GETD_URL)
    resp = requests.get(GETD_URL, timeout=120)
    resp.raise_for_status()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(resp.content).hexdigest()[:8]
    raw_path = RAW_DIR / f"getd_{datetime.now().strftime('%Y%m%d')}_{digest}.xlsx"
    raw_path.write_bytes(resp.content)
    log.info("Saved raw workbook: %s", raw_path)
    return resp.content


def parse_consumption(workbook_bytes: bytes) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(workbook_bytes), sheet_name="TipoUsuario", header=None)
    top_headers = pd.Series(raw.iloc[12]).ffill().tolist()
    sub_headers = raw.iloc[13].tolist()

    rows = []
    for col_idx in range(1, len(top_headers)):
        top = top_headers[col_idx]
        sub = sub_headers[col_idx]
        if pd.isna(top) or pd.isna(sub):
            continue
        segment = SEGMENT_MAP.get((str(top).strip(), str(sub).strip()))
        if segment is None:
            continue
        rows.append((col_idx, segment))

    if not rows:
        raise RuntimeError("Could not map ENARGAS GETD TipoUsuario columns to GCIE segments.")

    records = []
    data_rows = raw.iloc[14:].copy()
    for _, row in data_rows.iterrows():
        fecha = pd.to_datetime(row.iloc[0], errors="coerce")
        if pd.isna(fecha):
            continue
        for col_idx, segment in rows:
            value = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(value):
                continue
            records.append(
                {
                    "fecha": fecha,
                    "punto_entrega_id": "total_sistema",
                    "segmento": segment,
                    "volumen_m3": float(value) * 1000.0,
                    "source": "enargas_getd_tipo_usuario",
                }
            )

    if not records:
        raise RuntimeError("ENARGAS GETD workbook produced no consumption records.")

    df = pd.DataFrame(records).sort_values(["fecha", "segmento"]).reset_index(drop=True)
    return df


def save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"consumo_diario_{ts}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> pd.DataFrame:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    workbook_bytes = download_workbook()
    df = parse_consumption(workbook_bytes)

    processed_path = PROCESSED_DIR / "consumo_diario_enargas.parquet"
    df.to_parquet(processed_path, index=False)
    log.info(
        "Saved processed: %s (%s rows, %s -> %s)",
        processed_path,
        len(df),
        df["fecha"].min(),
        df["fecha"].max(),
    )
    save_snapshot(df)
    return df


if __name__ == "__main__":
    run()
