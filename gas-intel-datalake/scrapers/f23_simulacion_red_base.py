"""
F23 — Simulacion Base de Red Canonica
Source: Derived from F20b canonical network, F20 metrics and F22 exogenous balance
Tier 1 — Automated + heuristic dispatch
Tables:
  - red_solver_tramos_mensuales
  - red_solver_balance_nodal_mensual
  - red_solver_resumen_mensual
  - red_pandapipes_junctions
  - red_pandapipes_pipes

This script does two things:
1. Builds a pandapipes-ready export layer from the canonical GCIE gas network.
2. Runs a simple auditable monthly dispatch heuristic on the directed graph to
   quantify congestion, unmet demand and curtailed supply without requiring a
   full hydraulic solver.
"""

from __future__ import annotations

import hashlib
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SNAPSHOTS_DIR = ROOT_DIR / "data" / "snapshots"

NODES_PATH = PROCESSED_DIR / "red_nodos_canonica.parquet"
EDGES_PATH = PROCESSED_DIR / "red_tramos_canonica.parquet"
METRICS_PATH = PROCESSED_DIR / "red_tramo_metricas_mensuales.parquet"
EXOGENOUS_PATH = PROCESSED_DIR / "red_nodo_exogenos_mensuales.parquet"


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [path.name for path in (NODES_PATH, EDGES_PATH, METRICS_PATH, EXOGENOUS_PATH) if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "F23 requires F20b, F20 and F22 outputs first. Missing: " + ", ".join(missing)
        )
    return (
        pd.read_parquet(NODES_PATH),
        pd.read_parquet(EDGES_PATH),
        pd.read_parquet(METRICS_PATH),
        pd.read_parquet(EXOGENOUS_PATH),
    )


def _build_pandapipes_exports(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    active_nodes = nodes_df[nodes_df["is_active"]].copy()
    active_edges = edges_df[edges_df["is_active"]].copy()

    junctions = active_nodes[
        [
            "node_id",
            "nombre",
            "canonical_name",
            "latitud",
            "longitud",
            "tipo_nodo",
            "source_confidence",
            "topology_status",
            "source",
        ]
    ].rename(
        columns={
            "node_id": "junction_id",
            "nombre": "junction_name",
        }
    )
    junctions["pn_bar_assumed"] = 50.0
    junctions["tfluid_k_assumed"] = 288.15
    junctions["export_target"] = "pandapipes_junction"

    pipes = active_edges[
        [
            "edge_id",
            "ruta",
            "canonical_name",
            "gasoducto",
            "source_node_id",
            "target_node_id",
            "latitud_origen",
            "longitud_origen",
            "latitud_destino",
            "longitud_destino",
            "capacidad_mm3_dia_override",
            "source_confidence",
            "topology_status",
            "source",
        ]
    ].rename(
        columns={
            "edge_id": "pipe_id",
            "ruta": "pipe_name",
            "source_node_id": "from_junction_id",
            "target_node_id": "to_junction_id",
        }
    )
    pipes["length_km_proxy"] = (
        ((pipes["latitud_origen"] - pipes["latitud_destino"]) ** 2 + (pipes["longitud_origen"] - pipes["longitud_destino"]) ** 2)
        ** 0.5
        * 111.0
    )
    pipes["diameter_m_assumed"] = pd.NA
    pipes["roughness_mm_assumed"] = pd.NA
    pipes["export_target"] = "pandapipes_pipe"
    return junctions.reset_index(drop=True), pipes.reset_index(drop=True)


def _resolve_edge_capacities(edges_df: pd.DataFrame, metrics_df: pd.DataFrame, months: pd.Series) -> pd.DataFrame:
    active_edges = edges_df[edges_df["is_active"]].copy()
    active_edges["capacidad_mm3_dia_override"] = pd.to_numeric(
        active_edges.get("capacidad_mm3_dia_override"), errors="coerce"
    )

    metrics = metrics_df.copy()
    metrics["fecha"] = pd.to_datetime(metrics["fecha"]).dt.to_period("M").dt.to_timestamp()

    monthly_metrics = metrics.groupby(["fecha", "edge_id"], dropna=False).agg(
        observed_capacity_mm3_dia=("capacidad_mm3_dia", "max"),
        observed_flow_mm3_dia=("caudal_mm3_dia", "sum"),
        observed_utilization_ratio=("utilization_ratio", "max"),
    ).reset_index()

    gasoducto_monthly = metrics.merge(
        active_edges[["edge_id", "gasoducto"]],
        on="edge_id",
        how="inner",
    ).groupby(["fecha", "gasoducto"], dropna=False).agg(
        gasoducto_capacity_mm3_dia=("capacidad_mm3_dia", "median")
    ).reset_index()

    gasoducto_global = metrics.merge(
        active_edges[["edge_id", "gasoducto"]],
        on="edge_id",
        how="inner",
    ).groupby("gasoducto", dropna=False).agg(
        gasoducto_capacity_global_mm3_dia=("capacidad_mm3_dia", "median")
    ).reset_index()

    month_frame = pd.DataFrame({"fecha": pd.to_datetime(pd.Series(months).drop_duplicates()).sort_values()})
    edge_months = month_frame.assign(_k=1).merge(
        active_edges.assign(_k=1),
        on="_k",
        how="inner",
    ).drop(columns="_k")

    resolved = edge_months.merge(
        monthly_metrics,
        on=["fecha", "edge_id"],
        how="left",
    ).merge(
        gasoducto_monthly,
        on=["fecha", "gasoducto"],
        how="left",
    ).merge(
        gasoducto_global,
        on="gasoducto",
        how="left",
    )

    global_capacity = float(metrics["capacidad_mm3_dia"].median())
    resolved["resolved_capacity_mm3_dia"] = (
        resolved["observed_capacity_mm3_dia"]
        .fillna(resolved["capacidad_mm3_dia_override"])
        .fillna(resolved["gasoducto_capacity_mm3_dia"])
        .fillna(resolved["gasoducto_capacity_global_mm3_dia"])
        .fillna(global_capacity)
    )

    capacity_source = pd.Series("global_capacity_median_fallback", index=resolved.index)
    capacity_source.loc[resolved["gasoducto_capacity_global_mm3_dia"].notna()] = "gasoducto_capacity_global_median"
    capacity_source.loc[resolved["gasoducto_capacity_mm3_dia"].notna()] = "gasoducto_capacity_monthly_median"
    capacity_source.loc[resolved["capacidad_mm3_dia_override"].notna()] = "manual_override"
    capacity_source.loc[resolved["observed_capacity_mm3_dia"].notna()] = "observed_edge_capacity"
    resolved["capacity_source"] = capacity_source
    return resolved.reset_index(drop=True)


def _find_path(
    source_node: str,
    sinks_remaining: dict[str, float],
    adjacency: dict[str, list[tuple[str, str]]],
    residual_capacity: dict[str, float],
) -> tuple[list[str], str] | None:
    candidate_sinks = {node_id for node_id, demand in sinks_remaining.items() if demand > 1e-9}
    if not candidate_sinks:
        return None

    queue = deque([(source_node, [])])
    visited = {source_node}
    while queue:
        current_node, path_edges = queue.popleft()
        if current_node in candidate_sinks and path_edges:
            return path_edges, current_node
        for edge_id, next_node in adjacency.get(current_node, []):
            if next_node in visited or residual_capacity.get(edge_id, 0.0) <= 1e-9:
                continue
            visited.add(next_node)
            queue.append((next_node, path_edges + [edge_id]))
    return None


def _simulate_month(
    month_edges: pd.DataFrame,
    month_exogenous: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    adjacency: dict[str, list[tuple[str, str]]] = {}
    residual_capacity: dict[str, float] = {}
    edge_lookup = month_edges.set_index("edge_id").to_dict("index")
    for _, edge in month_edges.iterrows():
        adjacency.setdefault(str(edge["source_node_id"]), []).append(
            (str(edge["edge_id"]), str(edge["target_node_id"]))
        )
        residual_capacity[str(edge["edge_id"])] = float(edge["resolved_capacity_mm3_dia"])

    source_remaining = {
        str(row["node_id"]): float(row["supply_mm3_dia_proxy"])
        for _, row in month_exogenous.iterrows()
        if float(row["supply_mm3_dia_proxy"]) > 1e-9
    }
    sink_remaining = {
        str(row["node_id"]): float(row["withdrawal_mm3_dia_proxy"])
        for _, row in month_exogenous.iterrows()
        if float(row["withdrawal_mm3_dia_proxy"]) > 1e-9
    }

    edge_flow: dict[str, float] = {edge_id: 0.0 for edge_id in edge_lookup}
    sink_served: dict[str, float] = {node_id: 0.0 for node_id in sink_remaining}
    source_dispatched: dict[str, float] = {node_id: 0.0 for node_id in source_remaining}

    for source_node, initial_supply in sorted(source_remaining.items(), key=lambda item: item[1], reverse=True):
        while source_remaining[source_node] > 1e-9:
            path_result = _find_path(source_node, sink_remaining, adjacency, residual_capacity)
            if path_result is None:
                break
            path_edges, sink_node = path_result
            path_bottleneck = min(residual_capacity[edge_id] for edge_id in path_edges)
            dispatch = min(source_remaining[source_node], sink_remaining[sink_node], path_bottleneck)
            if dispatch <= 1e-9:
                break
            for edge_id in path_edges:
                edge_flow[edge_id] += dispatch
                residual_capacity[edge_id] -= dispatch
            source_remaining[source_node] -= dispatch
            sink_remaining[sink_node] -= dispatch
            source_dispatched[source_node] += dispatch
            sink_served[sink_node] += dispatch

    edge_records: list[dict[str, Any]] = []
    for edge_id, edge in edge_lookup.items():
        simulated_flow = edge_flow[edge_id]
        resolved_capacity = float(edge["resolved_capacity_mm3_dia"])
        edge_records.append(
            {
                "fecha": edge["fecha"],
                "edge_id": edge_id,
                "ruta": edge["ruta"],
                "canonical_name": edge["canonical_name"],
                "gasoducto": edge["gasoducto"],
                "source_node_id": edge["source_node_id"],
                "target_node_id": edge["target_node_id"],
                "resolved_capacity_mm3_dia": resolved_capacity,
                "capacity_source": edge["capacity_source"],
                "observed_flow_mm3_dia": float(edge.get("observed_flow_mm3_dia") or 0.0),
                "observed_utilization_ratio": edge.get("observed_utilization_ratio"),
                "simulated_flow_mm3_dia": simulated_flow,
                "simulated_utilization_ratio": simulated_flow / resolved_capacity if resolved_capacity > 0 else pd.NA,
                "residual_capacity_mm3_dia": residual_capacity[edge_id],
                "is_saturated": residual_capacity[edge_id] <= 1e-9,
                "source_confidence": edge["source_confidence"],
                "topology_status": edge["topology_status"],
                "source": "f23_heuristic_dispatch",
            }
        )

    month_exogenous = month_exogenous.copy()
    inflow_by_node: dict[str, float] = {}
    outflow_by_node: dict[str, float] = {}
    for edge_id, flow in edge_flow.items():
        edge = edge_lookup[edge_id]
        outflow_by_node[edge["source_node_id"]] = outflow_by_node.get(edge["source_node_id"], 0.0) + flow
        inflow_by_node[edge["target_node_id"]] = inflow_by_node.get(edge["target_node_id"], 0.0) + flow

    node_records: list[dict[str, Any]] = []
    for _, row in month_exogenous.iterrows():
        node_id = str(row["node_id"])
        supply = float(row["supply_mm3_dia_proxy"])
        demand = float(row["withdrawal_mm3_dia_proxy"])
        dispatched = source_dispatched.get(node_id, 0.0)
        served = sink_served.get(node_id, 0.0)
        node_records.append(
            {
                "fecha": row["fecha"],
                "node_id": node_id,
                "nombre": row["nombre"],
                "role_proxy": row.get("role_proxy"),
                "supply_mm3_dia_proxy": supply,
                "withdrawal_mm3_dia_proxy": demand,
                "served_withdrawal_mm3_dia": served,
                "unmet_withdrawal_mm3_dia": max(demand - served, 0.0),
                "dispatched_supply_mm3_dia": dispatched,
                "curtailed_supply_mm3_dia": max(supply - dispatched, 0.0),
                "simulated_inflow_mm3_dia": inflow_by_node.get(node_id, 0.0),
                "simulated_outflow_mm3_dia": outflow_by_node.get(node_id, 0.0),
                "simulated_net_flow_mm3_dia": outflow_by_node.get(node_id, 0.0) - inflow_by_node.get(node_id, 0.0),
                "latitud": row["latitud"],
                "longitud": row["longitud"],
                "source": "f23_heuristic_dispatch",
            }
        )

    summary = {
        "fecha": month_exogenous["fecha"].iloc[0],
        "total_supply_mm3_dia_proxy": sum(float(v) for v in source_dispatched.values()) + sum(float(v) for v in source_remaining.values()),
        "total_withdrawal_mm3_dia_proxy": sum(float(v) for v in sink_served.values()) + sum(float(v) for v in sink_remaining.values()),
        "served_withdrawal_mm3_dia": sum(float(v) for v in sink_served.values()),
        "unmet_withdrawal_mm3_dia": sum(float(v) for v in sink_remaining.values()),
        "dispatched_supply_mm3_dia": sum(float(v) for v in source_dispatched.values()),
        "curtailed_supply_mm3_dia": sum(float(v) for v in source_remaining.values()),
        "simulated_transport_mm3_dia": sum(float(v) for v in edge_flow.values()),
        "saturated_edge_count": sum(1 for edge_id in edge_flow if residual_capacity[edge_id] <= 1e-9),
        "total_edge_count": len(edge_flow),
        "source": "f23_heuristic_dispatch",
    }
    return edge_records, node_records, summary


def _run_simulation(
    edge_capacities_df: pd.DataFrame,
    exogenous_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    edge_records: list[dict[str, Any]] = []
    node_records: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    active_months = sorted(
        set(pd.to_datetime(edge_capacities_df["fecha"]).dt.to_period("M").dt.to_timestamp())
        & set(pd.to_datetime(exogenous_df["fecha"]).dt.to_period("M").dt.to_timestamp())
    )

    for month in active_months:
        month_edges = edge_capacities_df[edge_capacities_df["fecha"] == month].copy()
        month_exogenous = exogenous_df[
            pd.to_datetime(exogenous_df["fecha"]).dt.to_period("M").dt.to_timestamp() == month
        ].copy()
        month_exogenous = month_exogenous[
            (month_exogenous["supply_mm3_dia_proxy"] > 1e-9)
            | (month_exogenous["withdrawal_mm3_dia_proxy"] > 1e-9)
        ].copy()
        if month_edges.empty or month_exogenous.empty:
            continue
        month_edge_records, month_node_records, month_summary = _simulate_month(month_edges, month_exogenous)
        edge_records.extend(month_edge_records)
        node_records.extend(month_node_records)
        summaries.append(month_summary)

    return (
        pd.DataFrame(edge_records).sort_values(["fecha", "gasoducto", "ruta"]).reset_index(drop=True),
        pd.DataFrame(node_records).sort_values(["fecha", "node_id"]).reset_index(drop=True),
        pd.DataFrame(summaries).sort_values("fecha").reset_index(drop=True),
    )


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    nodes_df, edges_df, metrics_df, exogenous_df = _load_inputs()
    junctions_df, pipes_df = _build_pandapipes_exports(nodes_df, edges_df)
    edge_capacities_df = _resolve_edge_capacities(edges_df, metrics_df, exogenous_df["fecha"])
    edge_sim_df, node_sim_df, summary_df = _run_simulation(edge_capacities_df, exogenous_df)

    outputs = {
        "red_pandapipes_junctions.parquet": junctions_df,
        "red_pandapipes_pipes.parquet": pipes_df,
        "red_solver_tramos_mensuales.parquet": edge_sim_df,
        "red_solver_balance_nodal_mensual.parquet": node_sim_df,
        "red_solver_resumen_mensual.parquet": summary_df,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(junctions_df, "red_pandapipes_junctions")
    _save_snapshot(pipes_df, "red_pandapipes_pipes")
    _save_snapshot(edge_sim_df, "red_solver_tramos_mensuales")
    _save_snapshot(node_sim_df, "red_solver_balance_nodal_mensual")
    _save_snapshot(summary_df, "red_solver_resumen_mensual")
    return junctions_df, pipes_df, edge_sim_df, node_sim_df, summary_df


if __name__ == "__main__":
    run()
