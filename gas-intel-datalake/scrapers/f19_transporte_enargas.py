"""
F19 — Transporte y Congestion de Gas Natural (ENARGAS)
Source: ENARGAS
Tier 1 — Automated
Tables: transporte_flujo_mensual, transporte_capacidad_firme, transporte_utilizacion_mensual

The official ENARGAS workbooks currently expose:
- monthly physical flow by gasoducto (`Gas Recibido`, `Gas Cargado`)
- monthly firm transport capacity contracted (`Contratos de Transporte Firme`)

The flow tables are gasoducto-level. The capacity workbook is transporter-level,
not tramo-level, so this scraper materializes both raw layers plus a proxy
utilization table that normalizes each gasoducto against its own observed peak.
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

FLOW_URLS = {
    "gas_recibido": "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-estadisticos/GRT/GRT.xlsx",
    "gas_cargado": "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-estadisticos/GCD/GCD.xlsx",
}
CONTRACTS_URL = (
    "https://www.enargas.gob.ar/secciones/transporte-y-distribucion/datos-estadisticos/"
    "Contratos/Contratos.xlsx"
)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "enargas"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"


def _download_file(url: str, slug: str) -> bytes:
    log.info("Downloading %s", url)
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(response.content).hexdigest()[:8]
    raw_path = RAW_DIR / f"{slug}_{datetime.now().strftime('%Y%m%d')}_{digest}.xlsx"
    raw_path.write_bytes(response.content)
    log.info("Saved raw workbook: %s", raw_path)
    return response.content


def _clean_string(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).replace("\n", " ").strip()
    return cleaned or None


def _parse_flow_sheet(workbook_bytes: bytes, metric_name: str) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(workbook_bytes), sheet_name="Gasoducto", header=None)

    transporter_row = raw.iloc[:, 1:].iloc[10 if metric_name == "gas_cargado" else 15]
    gasoducto_row = raw.iloc[:, 1:].iloc[11 if metric_name == "gas_cargado" else 16]
    data = raw.iloc[12 if metric_name == "gas_cargado" else 17 :, :].copy()

    column_meta: list[tuple[int, str, str]] = []
    current_transporter: str | None = None
    for offset, gasoducto in enumerate(gasoducto_row, start=1):
        transporter = transporter_row.iloc[offset - 1]
        transporter_clean = _clean_string(transporter)
        gasoducto_clean = _clean_string(gasoducto)
        if transporter_clean is not None:
            current_transporter = transporter_clean
        if gasoducto_clean is None or gasoducto_clean.lower().startswith("total"):
            continue
        column_meta.append((offset, current_transporter or "unknown", gasoducto_clean))

    records: list[dict[str, object]] = []
    for _, row in data.iterrows():
        fecha = pd.to_datetime(row.iloc[0], errors="coerce")
        if pd.isna(fecha):
            continue
        for col_idx, transportista, gasoducto in column_meta:
            value = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(value):
                continue
            records.append(
                {
                    "fecha": fecha,
                    "transportista": transportista,
                    "gasoducto": gasoducto,
                    "metric": metric_name,
                    "volumen_miles_m3": float(value),
                    "volumen_m3": float(value) * 1000.0,
                    "source": "enargas_datos_operativos_gasoducto",
                }
            )

    if not records:
        raise RuntimeError(f"ENARGAS {metric_name} workbook produced no gasoducto flow rows.")
    return pd.DataFrame(records).sort_values(["fecha", "metric", "transportista", "gasoducto"]).reset_index(drop=True)


def _parse_contracts_sheet(workbook_bytes: bytes) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(workbook_bytes), sheet_name="Contratos", header=None)
    group_row = raw.iloc[5]
    category_row = raw.iloc[6]
    shipper_row = raw.iloc[7]
    data = raw.iloc[8:].copy()

    columns: list[tuple[int, str, str, str]] = []
    current_transportista: str | None = None
    current_category: str | None = None
    for col_idx in range(1, raw.shape[1]):
        group_value = _clean_string(group_row.iloc[col_idx])
        category_value = _clean_string(category_row.iloc[col_idx])
        shipper_value = _clean_string(shipper_row.iloc[col_idx])
        if group_value is not None:
            current_transportista = group_value
        if category_value is not None:
            current_category = category_value
        if shipper_value is None:
            continue
        columns.append(
            (
                col_idx,
                current_transportista or "unknown",
                current_category or "unknown",
                shipper_value,
            )
        )

    records: list[dict[str, object]] = []
    for _, row in data.iterrows():
        fecha = pd.to_datetime(row.iloc[0], errors="coerce")
        if pd.isna(fecha):
            continue
        for col_idx, transportista, categoria, cargador in columns:
            value = pd.to_numeric(row.iloc[col_idx], errors="coerce")
            if pd.isna(value):
                continue
            records.append(
                {
                    "fecha": fecha,
                    "transportista": transportista,
                    "categoria_cargador": categoria,
                    "cargador": cargador,
                    "capacidad_firme_miles_m3_dia": float(value),
                    "capacidad_firme_m3_dia": float(value) * 1000.0,
                    "source": "enargas_contratos_transporte_firme",
                }
            )

    if not records:
        raise RuntimeError("ENARGAS contracts workbook produced no firm capacity rows.")
    return pd.DataFrame(records).sort_values(["fecha", "transportista", "categoria_cargador", "cargador"]).reset_index(drop=True)


def _build_utilization_proxy(flow_df: pd.DataFrame, capacity_df: pd.DataFrame) -> pd.DataFrame:
    flow_pivot = (
        flow_df.pivot_table(
            index=["fecha", "transportista", "gasoducto"],
            columns="metric",
            values="volumen_m3",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )
    flow_pivot.columns.name = None
    if "gas_recibido" not in flow_pivot.columns:
        flow_pivot["gas_recibido"] = 0.0
    if "gas_cargado" not in flow_pivot.columns:
        flow_pivot["gas_cargado"] = 0.0

    flow_pivot["flujo_principal_m3"] = flow_pivot[["gas_recibido", "gas_cargado"]].max(axis=1)

    peak_by_gasoducto = (
        flow_pivot.groupby(["transportista", "gasoducto"])["flujo_principal_m3"].max().rename("capacidad_proxy_m3")
    )
    flow_pivot = flow_pivot.merge(
        peak_by_gasoducto.reset_index(),
        on=["transportista", "gasoducto"],
        how="left",
    )
    flow_pivot["utilization_ratio_proxy"] = flow_pivot["flujo_principal_m3"] / flow_pivot["capacidad_proxy_m3"].replace(0, pd.NA)
    flow_pivot["headroom_proxy_m3"] = flow_pivot["capacidad_proxy_m3"] - flow_pivot["flujo_principal_m3"]

    firm_capacity = (
        capacity_df.groupby(["fecha", "transportista"], dropna=False)["capacidad_firme_m3_dia"]
        .sum()
        .reset_index()
        .rename(columns={"capacidad_firme_m3_dia": "capacidad_firme_total_m3_dia"})
    )
    flow_pivot = flow_pivot.merge(firm_capacity, on=["fecha", "transportista"], how="left")
    flow_pivot["observed_daily_flow_m3_dia"] = flow_pivot["flujo_principal_m3"] / flow_pivot["fecha"].dt.days_in_month
    flow_pivot["utilization_vs_firm_contract_ratio"] = (
        flow_pivot["observed_daily_flow_m3_dia"] / flow_pivot["capacidad_firme_total_m3_dia"].replace(0, pd.NA)
    )
    flow_pivot["congestion_flag_proxy"] = flow_pivot["utilization_ratio_proxy"].fillna(0.0) >= 0.9
    flow_pivot["source"] = "enargas_flow_plus_proxy_capacity"

    return flow_pivot.sort_values(["fecha", "transportista", "gasoducto"]).reset_index(drop=True)


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    recibidos_bytes = _download_file(FLOW_URLS["gas_recibido"], "enargas_grt")
    cargados_bytes = _download_file(FLOW_URLS["gas_cargado"], "enargas_gcd")
    contracts_bytes = _download_file(CONTRACTS_URL, "enargas_contratos")

    flow_df = pd.concat(
        [
            _parse_flow_sheet(recibidos_bytes, "gas_recibido"),
            _parse_flow_sheet(cargados_bytes, "gas_cargado"),
        ],
        ignore_index=True,
    )
    capacity_df = _parse_contracts_sheet(contracts_bytes)
    utilization_df = _build_utilization_proxy(flow_df, capacity_df)

    outputs = {
        "transporte_flujo_mensual.parquet": flow_df,
        "transporte_capacidad_firme.parquet": capacity_df,
        "transporte_utilizacion_mensual.parquet": utilization_df,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(flow_df, "transporte_flujo_mensual")
    _save_snapshot(capacity_df, "transporte_capacidad_firme")
    _save_snapshot(utilization_df, "transporte_utilizacion_mensual")
    return flow_df, capacity_df, utilization_df


if __name__ == "__main__":
    run()
