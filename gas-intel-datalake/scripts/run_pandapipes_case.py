"""Build and optionally solve a monthly pandapipes case from the GCIE canonical gas network."""

from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict, deque
from pathlib import Path

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

JUNCTIONS_PATH = PROCESSED_DIR / "red_pandapipes_junctions.parquet"
PIPES_PATH = PROCESSED_DIR / "red_pandapipes_pipes.parquet"
NODE_BALANCE_PATH = PROCESSED_DIR / "red_solver_balance_nodal_mensual.parquet"
EDGE_BALANCE_PATH = PROCESSED_DIR / "red_solver_tramos_mensuales.parquet"
SUMMARY_PATH = PROCESSED_DIR / "red_solver_resumen_mensual.parquet"

ASSUMED_GAS_DENSITY_KG_M3 = 0.8
DEFAULT_PRESSURE_BAR = 55.0
DEFAULT_TFLUID_K = 288.15


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="Case month in YYYY-MM-DD format. Defaults to latest month with demand.")
    parser.add_argument(
        "--fluid",
        default="lgas",
        help="pandapipes fluid library name. Defaults to lgas.",
    )
    parser.add_argument(
        "--demand-mode",
        choices=["total", "served"],
        default="total",
        help="Use total withdrawal or the heuristic served withdrawal from F23 as sink demand.",
    )
    parser.add_argument(
        "--supply-mode",
        choices=["dispatched", "total"],
        default="dispatched",
        help="Use dispatched supply from F23 or the raw exogenous supply proxy as source injection.",
    )
    parser.add_argument(
        "--skip-pipeflow",
        action="store_true",
        help="Only build and export the pandapipes net without solving it.",
    )
    parser.add_argument(
        "--output-json",
        help="Optional output path for the pandapipes JSON export. Defaults under data/processed.",
    )
    return parser.parse_args()


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [
        path.name
        for path in (JUNCTIONS_PATH, PIPES_PATH, NODE_BALANCE_PATH, EDGE_BALANCE_PATH, SUMMARY_PATH)
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("Missing F23 outputs: " + ", ".join(missing))
    return (
        pd.read_parquet(JUNCTIONS_PATH),
        pd.read_parquet(PIPES_PATH),
        pd.read_parquet(NODE_BALANCE_PATH),
        pd.read_parquet(EDGE_BALANCE_PATH),
        pd.read_parquet(SUMMARY_PATH),
    )


def _select_month(summary_df: pd.DataFrame, explicit_month: str | None) -> pd.Timestamp:
    summary = summary_df.copy()
    summary["fecha"] = pd.to_datetime(summary["fecha"]).dt.to_period("M").dt.to_timestamp()
    if explicit_month:
        return pd.Timestamp(explicit_month).to_period("M").to_timestamp()
    candidates = summary[summary["total_withdrawal_mm3_dia_proxy"] > 0].sort_values("fecha")
    if candidates.empty:
        raise RuntimeError("No months with withdrawal demand were found in red_solver_resumen_mensual.")
    return pd.Timestamp(candidates["fecha"].iloc[-1])


def _compute_components(pipes_df: pd.DataFrame) -> dict[str, int]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    nodes: set[str] = set()
    for _, row in pipes_df.iterrows():
        src = str(row["from_junction_id"])
        dst = str(row["to_junction_id"])
        nodes.add(src)
        nodes.add(dst)
        adjacency[src].add(dst)
        adjacency[dst].add(src)

    component_by_node: dict[str, int] = {}
    component_id = 0
    for node_id in sorted(nodes):
        if node_id in component_by_node:
            continue
        queue = deque([node_id])
        component_by_node[node_id] = component_id
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, set()):
                if neighbor not in component_by_node:
                    component_by_node[neighbor] = component_id
                    queue.append(neighbor)
        component_id += 1
    return component_by_node


def _mm3_dia_to_kg_s(volume_mm3_dia: float) -> float:
    return float(volume_mm3_dia) * 1_000_000.0 * ASSUMED_GAS_DENSITY_KG_M3 / 86400.0


def _diameter_from_capacity(capacity_mm3_dia: float) -> float:
    capacity = max(float(capacity_mm3_dia), 0.1)
    diameter = 0.18 + 0.11 * (capacity ** 0.5)
    return max(0.25, min(diameter, 1.10))


def _build_case_frames(
    junctions_df: pd.DataFrame,
    pipes_df: pd.DataFrame,
    node_balance_df: pd.DataFrame,
    edge_balance_df: pd.DataFrame,
    month: pd.Timestamp,
    demand_mode: str,
    supply_mode: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    month_nodes = node_balance_df[
        pd.to_datetime(node_balance_df["fecha"]).dt.to_period("M").dt.to_timestamp() == month
    ].copy()
    month_edges = edge_balance_df[
        pd.to_datetime(edge_balance_df["fecha"]).dt.to_period("M").dt.to_timestamp() == month
    ].copy()
    if month_nodes.empty or month_edges.empty:
        raise RuntimeError(f"No F23 data found for month {month.date()}.")

    case_junctions = junctions_df.merge(
        month_nodes[
            [
                "node_id",
                "nombre",
                "role_proxy",
                "supply_mm3_dia_proxy",
                "withdrawal_mm3_dia_proxy",
                "served_withdrawal_mm3_dia",
                "unmet_withdrawal_mm3_dia",
                "dispatched_supply_mm3_dia",
                "curtailed_supply_mm3_dia",
            ]
        ],
        left_on="junction_id",
        right_on="node_id",
        how="left",
    )
    case_junctions["sink_demand_mm3_dia"] = case_junctions[
        "served_withdrawal_mm3_dia" if demand_mode == "served" else "withdrawal_mm3_dia_proxy"
    ].fillna(0.0)
    case_junctions["source_supply_mm3_dia"] = case_junctions[
        "dispatched_supply_mm3_dia" if supply_mode == "dispatched" else "supply_mm3_dia_proxy"
    ].fillna(0.0)

    case_pipes = pipes_df.merge(
        month_edges[
            [
                "edge_id",
                "resolved_capacity_mm3_dia",
                "capacity_source",
                "simulated_flow_mm3_dia",
                "simulated_utilization_ratio",
                "is_saturated",
            ]
        ],
        left_on="pipe_id",
        right_on="edge_id",
        how="left",
    )
    case_pipes["resolved_capacity_mm3_dia"] = case_pipes["resolved_capacity_mm3_dia"].fillna(
        pd.to_numeric(case_pipes.get("capacidad_mm3_dia_override"), errors="coerce")
    )
    case_pipes["diameter_m_proxy"] = pd.to_numeric(case_pipes.get("diameter_m_override"), errors="coerce").fillna(
        case_pipes["resolved_capacity_mm3_dia"].fillna(5.0).apply(_diameter_from_capacity)
    )
    case_pipes["length_km_proxy"] = case_pipes["length_km_proxy"].fillna(1.0).clip(lower=1.0)
    return case_junctions.reset_index(drop=True), case_pipes.reset_index(drop=True)


def _build_net(case_junctions: pd.DataFrame, case_pipes: pd.DataFrame, fluid: str):
    import pandapipes as pp

    net = pp.create_empty_network(name="GCIE canonical gas network", fluid=fluid)
    component_by_node = _compute_components(case_pipes)

    junction_idx_by_id: dict[str, int] = {}
    for _, row in case_junctions.iterrows():
        node_id = str(row["junction_id"])
        junction_idx_by_id[node_id] = pp.create_junction(
            net,
            pn_bar=float(row.get("pn_bar_assumed", DEFAULT_PRESSURE_BAR) or DEFAULT_PRESSURE_BAR),
            tfluid_k=float(row.get("tfluid_k_assumed", DEFAULT_TFLUID_K) or DEFAULT_TFLUID_K),
            name=str(row["junction_name"]),
            type=str(row.get("tipo_nodo") or "junction"),
            geodata=(float(row["longitud"]), float(row["latitud"])),
            gc_ie_node_id=node_id,
            source_confidence=row.get("source_confidence"),
            topology_status=row.get("topology_status"),
        )

    for _, row in case_pipes.iterrows():
        pp.create_pipe_from_parameters(
            net,
            from_junction=junction_idx_by_id[str(row["from_junction_id"])],
            to_junction=junction_idx_by_id[str(row["to_junction_id"])],
            length_km=float(row["length_km_proxy"]),
            diameter_m=float(row["diameter_m_proxy"]),
            k_mm=0.05,
            name=str(row["pipe_name"]),
            geodata=[
                (float(row["longitud_origen"]), float(row["latitud_origen"])),
                (float(row["longitud_destino"]), float(row["latitud_destino"])),
            ],
            gc_ie_pipe_id=str(row["pipe_id"]),
            gasoducto=row.get("gasoducto"),
            capacity_mm3_dia=row.get("resolved_capacity_mm3_dia"),
            capacity_source=row.get("capacity_source"),
        )

    case_junctions = case_junctions.copy()
    case_junctions["component_id"] = case_junctions["junction_id"].map(component_by_node).fillna(-1).astype(int)

    for component_id, component_nodes in case_junctions.groupby("component_id", dropna=False):
        if component_id < 0 or component_nodes.empty:
            continue
        slack_row = component_nodes.sort_values(
            ["source_supply_mm3_dia", "sink_demand_mm3_dia"], ascending=[False, False]
        ).iloc[0]
        slack_node_id = str(slack_row["junction_id"])
        pp.create_ext_grid(
            net,
            junction=junction_idx_by_id[slack_node_id],
            p_bar=DEFAULT_PRESSURE_BAR,
            t_k=DEFAULT_TFLUID_K,
            type="pt",
            name=f"slack_component_{component_id}_{slack_row['junction_name']}",
        )

        for _, row in component_nodes.iterrows():
            node_id = str(row["junction_id"])
            demand = float(row["sink_demand_mm3_dia"] or 0.0)
            supply = float(row["source_supply_mm3_dia"] or 0.0)
            if node_id != slack_node_id and supply > 1e-9:
                pp.create_source(
                    net,
                    junction=junction_idx_by_id[node_id],
                    mdot_kg_per_s=_mm3_dia_to_kg_s(supply),
                    name=f"source_{row['junction_name']}",
                )
            if demand > 1e-9:
                pp.create_sink(
                    net,
                    junction=junction_idx_by_id[node_id],
                    mdot_kg_per_s=_mm3_dia_to_kg_s(demand),
                    name=f"sink_{row['junction_name']}",
                )

    return net, case_junctions


def _summarize_case(
    case_junctions: pd.DataFrame,
    case_pipes: pd.DataFrame,
    month: pd.Timestamp,
    demand_mode: str,
    supply_mode: str,
    converged: bool,
    run_error: str | None,
) -> dict[str, object]:
    summary = {
        "month": month.strftime("%Y-%m-%d"),
        "demand_mode": demand_mode,
        "supply_mode": supply_mode,
        "junction_count": int(len(case_junctions)),
        "pipe_count": int(len(case_pipes)),
        "component_count": int(case_junctions["component_id"].nunique()),
        "total_supply_mm3_dia": float(case_junctions["source_supply_mm3_dia"].sum()),
        "total_sink_demand_mm3_dia": float(case_junctions["sink_demand_mm3_dia"].sum()),
        "total_unmet_proxy_mm3_dia": float(case_junctions["unmet_withdrawal_mm3_dia"].fillna(0.0).sum()),
        "converged": bool(converged),
        "run_error": run_error,
    }
    return summary


def main() -> None:
    args = _parse_args()
    junctions_df, pipes_df, node_balance_df, edge_balance_df, summary_df = _load_inputs()
    month = _select_month(summary_df, args.month)
    case_junctions, case_pipes = _build_case_frames(
        junctions_df,
        pipes_df,
        node_balance_df,
        edge_balance_df,
        month,
        args.demand_mode,
        args.supply_mode,
    )

    net, case_junctions = _build_net(case_junctions, case_pipes, args.fluid)
    output_json = Path(args.output_json) if args.output_json else (
        PROCESSED_DIR / f"pandapipes_case_{month.strftime('%Y%m01')}.json"
    )

    import pandapipes as pp

    converged = False
    run_error: str | None = None
    if not args.skip_pipeflow:
        try:
            pp.pipeflow(net, mode="hydraulics")
            converged = bool(getattr(net, "converged", False))
        except Exception as exc:  # pragma: no cover - exploratory runtime path
            run_error = str(exc)
            log.warning("pandapipes pipeflow failed: %s", exc)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    pp.to_json(net, str(output_json))

    case_summary = _summarize_case(
        case_junctions,
        case_pipes,
        month,
        args.demand_mode,
        args.supply_mode,
        converged,
        run_error,
    )
    summary_path = output_json.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(case_summary, indent=2), encoding="utf-8")

    print(json.dumps(case_summary, indent=2))
    print(f"pandapipes_json={output_json}")
    print(f"summary_json={summary_path}")


if __name__ == "__main__":
    main()
