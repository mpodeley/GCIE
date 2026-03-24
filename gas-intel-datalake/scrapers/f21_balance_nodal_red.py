"""
F21 — Balance Nodal de la Red de Transporte
Source: Derived from F20 modeled transport graph
Tier 1 — Automated
Tables: red_nodo_metricas_mensuales, red_nodo_roles_proxy

This script derives node-level inflow, outflow, net flow and a coarse operational
role from the modeled transport graph built in F20.
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

TRAMOS_PATH = PROCESSED_DIR / "red_tramos.parquet"
METRICAS_PATH = PROCESSED_DIR / "red_tramo_metricas_mensuales.parquet"
NODOS_PATH = PROCESSED_DIR / "red_nodos.parquet"


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [path.name for path in (TRAMOS_PATH, METRICAS_PATH, NODOS_PATH) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "F21 requires F20 outputs first. Missing: " + ", ".join(missing)
        )
    return (
        pd.read_parquet(TRAMOS_PATH),
        pd.read_parquet(METRICAS_PATH),
        pd.read_parquet(NODOS_PATH),
    )


def _build_node_metrics(
    tramos_df: pd.DataFrame,
    metricas_df: pd.DataFrame,
    nodos_df: pd.DataFrame,
) -> pd.DataFrame:
    edge_metrics = metricas_df.merge(
        tramos_df[["edge_id", "source_node_id", "target_node_id", "gasoducto"]],
        on="edge_id",
        how="inner",
    )

    outbound = (
        edge_metrics.groupby(["fecha", "source_node_id", "gasoducto"], dropna=False)
        .agg(outflow_mm3_dia=("caudal_mm3_dia", "sum"))
        .reset_index()
        .rename(columns={"source_node_id": "node_id"})
    )
    inbound = (
        edge_metrics.groupby(["fecha", "target_node_id", "gasoducto"], dropna=False)
        .agg(inflow_mm3_dia=("caudal_mm3_dia", "sum"))
        .reset_index()
        .rename(columns={"target_node_id": "node_id"})
    )

    node_metrics = outbound.merge(
        inbound,
        on=["fecha", "node_id", "gasoducto"],
        how="outer",
    ).fillna({"outflow_mm3_dia": 0.0, "inflow_mm3_dia": 0.0})

    capacity_source = (
        edge_metrics.groupby(["fecha", "source_node_id", "gasoducto"], dropna=False)
        .agg(outbound_capacity_mm3_dia=("capacidad_mm3_dia", "sum"))
        .reset_index()
        .rename(columns={"source_node_id": "node_id"})
    )
    capacity_target = (
        edge_metrics.groupby(["fecha", "target_node_id", "gasoducto"], dropna=False)
        .agg(inbound_capacity_mm3_dia=("capacidad_mm3_dia", "sum"))
        .reset_index()
        .rename(columns={"target_node_id": "node_id"})
    )
    node_metrics = node_metrics.merge(
        capacity_source,
        on=["fecha", "node_id", "gasoducto"],
        how="left",
    ).merge(
        capacity_target,
        on=["fecha", "node_id", "gasoducto"],
        how="left",
    )

    node_metrics["outbound_capacity_mm3_dia"] = node_metrics["outbound_capacity_mm3_dia"].fillna(0.0)
    node_metrics["inbound_capacity_mm3_dia"] = node_metrics["inbound_capacity_mm3_dia"].fillna(0.0)
    node_metrics["net_flow_mm3_dia"] = node_metrics["outflow_mm3_dia"] - node_metrics["inflow_mm3_dia"]
    node_metrics["throughput_mm3_dia"] = node_metrics["outflow_mm3_dia"] + node_metrics["inflow_mm3_dia"]
    node_metrics["imbalance_abs_mm3_dia"] = node_metrics["net_flow_mm3_dia"].abs()
    node_metrics["node_utilization_ratio"] = node_metrics["throughput_mm3_dia"] / (
        node_metrics["outbound_capacity_mm3_dia"] + node_metrics["inbound_capacity_mm3_dia"]
    ).replace(0.0, pd.NA)

    node_metrics = node_metrics.merge(
        nodos_df[["node_id", "nombre", "latitud", "longitud"]],
        on="node_id",
        how="left",
    )
    node_metrics["source"] = "derived_from_red_tramo_metricas_mensuales"
    return node_metrics.sort_values(["fecha", "node_id", "gasoducto"]).reset_index(drop=True)


def _build_node_roles_proxy(node_metrics_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        node_metrics_df.groupby(["node_id", "nombre"], dropna=False)
        .agg(
            avg_inflow_mm3_dia=("inflow_mm3_dia", "mean"),
            avg_outflow_mm3_dia=("outflow_mm3_dia", "mean"),
            avg_net_flow_mm3_dia=("net_flow_mm3_dia", "mean"),
            avg_throughput_mm3_dia=("throughput_mm3_dia", "mean"),
            max_node_utilization_ratio=("node_utilization_ratio", "max"),
            months_observed=("fecha", "nunique"),
        )
        .reset_index()
    )

    def classify(row: pd.Series) -> str:
        throughput = float(row["avg_throughput_mm3_dia"] or 0.0)
        net_flow = float(row["avg_net_flow_mm3_dia"] or 0.0)
        if throughput <= 0.1:
            return "inactive"
        if abs(net_flow) <= max(0.5, throughput * 0.1):
            return "transit"
        if net_flow > 0:
            return "source_proxy"
        return "sink_proxy"

    summary["role_proxy"] = summary.apply(classify, axis=1)
    summary["source"] = "derived_from_red_nodo_metricas_mensuales"
    return summary.sort_values(["role_proxy", "avg_throughput_mm3_dia"], ascending=[True, False]).reset_index(drop=True)


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    tramos_df, metricas_df, nodos_df = _load_inputs()
    node_metrics_df = _build_node_metrics(tramos_df, metricas_df, nodos_df)
    node_roles_df = _build_node_roles_proxy(node_metrics_df)

    outputs = {
        "red_nodo_metricas_mensuales.parquet": node_metrics_df,
        "red_nodo_roles_proxy.parquet": node_roles_df,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(node_metrics_df, "red_nodo_metricas_mensuales")
    _save_snapshot(node_roles_df, "red_nodo_roles_proxy")
    return node_metrics_df, node_roles_df


if __name__ == "__main__":
    run()
