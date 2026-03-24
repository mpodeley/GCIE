"""
F22 — Escenario Base de Oferta y Demanda sobre la Red
Source: Derived from F20/F21 plus SP0 production and consumption tables
Tier 1 — Automated
Tables: red_nodo_exogenos_mensuales, red_balance_escenario_mensual

This script maps upstream supply and downstream demand proxies onto the modeled
transport network nodes to create a first auditable baseline scenario for
network balance simulations.
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

ROLES_PATH = PROCESSED_DIR / "red_nodo_roles_proxy.parquet"
NODE_METRICS_PATH = PROCESSED_DIR / "red_nodo_metricas_mensuales.parquet"
NODES_PATH = PROCESSED_DIR / "red_nodos.parquet"
CONSUMPTION_PATH = PROCESSED_DIR / "consumo_diario_enargas.parquet"
PRODUCTION_PATH = PROCESSED_DIR / "pozos_no_convencional.parquet"

BASIN_SOURCE_WEIGHTS = {
    "NEUQUINA": {
        "Loma La Lata": 0.60,
        "Sierra Barrosa": 0.25,
        "Beazley": 0.15,
    },
    "AUSTRAL": {
        "San Sebastián": 0.45,
        "Magallanes": 0.45,
        "Beazley": 0.10,
    },
    "NOROESTE": {
        "Campo Durán": 0.75,
        "Bolivia": 0.25,
    },
}


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def _load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [
        path.name
        for path in (
            ROLES_PATH,
            NODE_METRICS_PATH,
            NODES_PATH,
            CONSUMPTION_PATH,
            PRODUCTION_PATH,
        )
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(
            "F22 requires prior SP0/F20/F21 outputs first. Missing: " + ", ".join(missing)
        )
    return (
        pd.read_parquet(ROLES_PATH),
        pd.read_parquet(NODE_METRICS_PATH),
        pd.read_parquet(NODES_PATH),
        pd.read_parquet(CONSUMPTION_PATH),
        pd.read_parquet(PRODUCTION_PATH),
    )


def _monthly_consumption(consumption_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        consumption_df.assign(
            fecha=lambda df: pd.to_datetime(df["fecha"]).dt.to_period("M").dt.to_timestamp()
        )
        .groupby("fecha", dropna=False)
        .agg(total_withdrawal_m3=("volumen_m3", "sum"))
        .reset_index()
    )
    monthly["withdrawal_mm3_dia_proxy"] = (
        monthly["total_withdrawal_m3"] / monthly["fecha"].dt.days_in_month / 1_000_000.0
    )
    return monthly[["fecha", "withdrawal_mm3_dia_proxy"]]


def _monthly_supply(production_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        production_df.assign(
            fecha=lambda df: pd.to_datetime(df["fecha"]).dt.to_period("M").dt.to_timestamp()
        )
        .groupby(["fecha", "cuenca"], dropna=False)
        .agg(supply_mm3_mes_proxy=("gas_no_convencional_mm3", "sum"))
        .reset_index()
    )
    # Capitulo IV gas volumes are expressed in thousands of cubic meters by month.
    # Convert to MMm3/d for consistency with the network views.
    monthly["supply_mm3_dia_proxy"] = (
        (monthly["supply_mm3_mes_proxy"] * 1000.0) / monthly["fecha"].dt.days_in_month / 1_000_000.0
    )
    return monthly


def _sink_weights(roles_df: pd.DataFrame) -> dict[str, float]:
    sinks = roles_df[roles_df["role_proxy"] == "sink_proxy"].copy()
    sinks["weight_base"] = sinks["avg_inflow_mm3_dia"].fillna(0.0).clip(lower=0.0)
    total = float(sinks["weight_base"].sum())
    if total <= 0:
        raise RuntimeError("F22 could not compute sink weights from red_nodo_roles_proxy.")
    return {
        str(row["node_id"]): float(row["weight_base"]) / total
        for _, row in sinks.iterrows()
    }


def _source_fallback_weights(roles_df: pd.DataFrame) -> dict[str, float]:
    sources = roles_df[roles_df["role_proxy"] == "source_proxy"].copy()
    sources["weight_base"] = (
        sources["avg_outflow_mm3_dia"]
        .fillna(0.0)
        .clip(lower=0.0)
    )
    total = float(sources["weight_base"].sum())
    if total <= 0:
        raise RuntimeError("F22 could not compute source weights from red_nodo_roles_proxy.")
    return {
        str(row["node_id"]): float(row["weight_base"]) / total
        for _, row in sources.iterrows()
    }


def _source_weights_by_basin(roles_df: pd.DataFrame) -> dict[tuple[str, str], float]:
    source_names = set(roles_df[roles_df["role_proxy"] == "source_proxy"]["nombre"].astype(str))
    weights: dict[tuple[str, str], float] = {}
    for basin, basin_weights in BASIN_SOURCE_WEIGHTS.items():
        filtered = {
            node_name: weight
            for node_name, weight in basin_weights.items()
            if node_name in source_names
        }
        total = sum(filtered.values())
        if total <= 0:
            continue
        for node_name, weight in filtered.items():
            weights[(basin, node_name)] = weight / total
    return weights


def _monthly_observed_source_weights(
    node_metrics_df: pd.DataFrame,
    roles_df: pd.DataFrame,
) -> pd.DataFrame:
    source_node_ids = set(
        roles_df.loc[roles_df["role_proxy"] == "source_proxy", "node_id"].astype(str)
    )
    observed = node_metrics_df.copy()
    observed["fecha"] = pd.to_datetime(observed["fecha"]).dt.to_period("M").dt.to_timestamp()
    observed["node_id"] = observed["node_id"].astype(str)
    observed = observed[observed["node_id"].isin(source_node_ids)]
    observed["weight_base"] = pd.to_numeric(observed["outflow_mm3_dia"], errors="coerce").fillna(0.0).clip(lower=0.0)
    observed = (
        observed.groupby(["fecha", "node_id"], dropna=False)
        .agg(weight_base=("weight_base", "sum"))
        .reset_index()
    )
    observed["month_total"] = observed.groupby("fecha")["weight_base"].transform("sum")
    observed = observed[observed["month_total"] > 0].copy()
    observed["observed_source_share"] = observed["weight_base"] / observed["month_total"]
    return observed[["fecha", "node_id", "observed_source_share"]]


def _build_node_exogenous(
    roles_df: pd.DataFrame,
    node_metrics_df: pd.DataFrame,
    nodes_df: pd.DataFrame,
    consumption_df: pd.DataFrame,
    production_df: pd.DataFrame,
) -> pd.DataFrame:
    sink_weight_by_node = _sink_weights(roles_df)
    source_fallback_weight_by_node = _source_fallback_weights(roles_df)
    source_weight_by_basin_node = _source_weights_by_basin(roles_df)
    monthly_source_weights = _monthly_observed_source_weights(node_metrics_df, roles_df)

    node_lookup = nodes_df.set_index("node_id")["nombre"].astype(str).to_dict()
    node_id_by_name = {str(name): node_id for node_id, name in node_lookup.items()}
    source_name_by_node_id = {node_id: name for node_id, name in node_lookup.items() if node_id in source_fallback_weight_by_node}

    monthly_consumption = _monthly_consumption(consumption_df)
    monthly_nc_supply = _monthly_supply(production_df)

    nc_supply_records: list[dict[str, object]] = []
    for _, row in monthly_nc_supply.iterrows():
        basin = str(row["cuenca"])
        for (weight_basin, node_name), weight in source_weight_by_basin_node.items():
            if weight_basin != basin:
                continue
            node_id = node_id_by_name.get(node_name)
            if node_id is None:
                continue
            nc_supply_records.append(
                {
                    "fecha": row["fecha"],
                    "node_id": node_id,
                    "supply_non_conventional_mm3_dia_proxy": float(row["supply_mm3_dia_proxy"]) * float(weight),
                }
            )

    total_supply_records: list[dict[str, object]] = []
    source_weight_lookup = {
        (pd.Timestamp(row["fecha"]), str(row["node_id"])): float(row["observed_source_share"])
        for _, row in monthly_source_weights.iterrows()
    }
    source_node_ids = sorted(source_fallback_weight_by_node)
    for _, row in monthly_consumption.iterrows():
        fecha = pd.Timestamp(row["fecha"])
        month_weights = {
            node_id: source_weight_lookup.get((fecha, node_id))
            for node_id in source_node_ids
        }
        if all(value is None for value in month_weights.values()):
            normalized_weights = source_fallback_weight_by_node
            source_method = "fallback_avg_source_outflow_weights"
        else:
            normalized_weights = {
                node_id: value
                for node_id, value in month_weights.items()
                if value is not None and value > 0
            }
            total = sum(normalized_weights.values())
            normalized_weights = {
                node_id: value / total
                for node_id, value in normalized_weights.items()
            }
            source_method = "observed_monthly_source_outflow_weights"
        for node_id, weight in normalized_weights.items():
            total_supply_records.append(
                {
                    "fecha": fecha,
                    "node_id": node_id,
                    "supply_mm3_dia_proxy": float(row["withdrawal_mm3_dia_proxy"]) * float(weight),
                    "withdrawal_mm3_dia_proxy": 0.0,
                    "supply_method": source_method,
                }
            )

    demand_records: list[dict[str, object]] = []
    for _, row in monthly_consumption.iterrows():
        for node_id, weight in sink_weight_by_node.items():
            demand_records.append(
                {
                    "fecha": row["fecha"],
                    "node_id": node_id,
                    "supply_mm3_dia_proxy": 0.0,
                    "withdrawal_mm3_dia_proxy": float(row["withdrawal_mm3_dia_proxy"]) * float(weight),
                    "supply_method": None,
                }
            )

    exogenous = pd.DataFrame(total_supply_records + demand_records)
    exogenous = (
        exogenous.groupby(["fecha", "node_id"], dropna=False)[
            ["supply_mm3_dia_proxy", "withdrawal_mm3_dia_proxy"]
        ]
        .sum()
        .reset_index()
    )
    nc_supply = pd.DataFrame(nc_supply_records)
    if nc_supply.empty:
        nc_supply = pd.DataFrame(columns=["fecha", "node_id", "supply_non_conventional_mm3_dia_proxy"])
    else:
        nc_supply = (
            nc_supply.groupby(["fecha", "node_id"], dropna=False)["supply_non_conventional_mm3_dia_proxy"]
            .sum()
            .reset_index()
        )

    supply_method_df = pd.DataFrame(total_supply_records)
    if supply_method_df.empty:
        supply_method_df = pd.DataFrame(columns=["fecha", "node_id", "supply_method"])
    else:
        supply_method_df = (
            supply_method_df[supply_method_df["supply_mm3_dia_proxy"] > 0][["fecha", "node_id", "supply_method"]]
            .drop_duplicates(subset=["fecha", "node_id"])
        )

    observed = (
        node_metrics_df.assign(
            fecha=lambda df: pd.to_datetime(df["fecha"]).dt.to_period("M").dt.to_timestamp()
        )
        .groupby(["fecha", "node_id"], dropna=False)
        .agg(
            observed_inflow_mm3_dia=("inflow_mm3_dia", "sum"),
            observed_outflow_mm3_dia=("outflow_mm3_dia", "sum"),
            observed_net_flow_mm3_dia=("net_flow_mm3_dia", "sum"),
            observed_throughput_mm3_dia=("throughput_mm3_dia", "sum"),
        )
        .reset_index()
    )

    result = observed.merge(exogenous, on=["fecha", "node_id"], how="outer")
    result = result.merge(nc_supply, on=["fecha", "node_id"], how="left")
    result = result.merge(supply_method_df, on=["fecha", "node_id"], how="left")
    result = result.fillna(
        {
            "observed_inflow_mm3_dia": 0.0,
            "observed_outflow_mm3_dia": 0.0,
            "observed_net_flow_mm3_dia": 0.0,
            "observed_throughput_mm3_dia": 0.0,
            "supply_mm3_dia_proxy": 0.0,
            "supply_non_conventional_mm3_dia_proxy": 0.0,
            "withdrawal_mm3_dia_proxy": 0.0,
        }
    )
    result["exogenous_net_mm3_dia_proxy"] = (
        result["supply_mm3_dia_proxy"] - result["withdrawal_mm3_dia_proxy"]
    )
    result["balancing_gap_mm3_dia_proxy"] = (
        result["exogenous_net_mm3_dia_proxy"] - result["observed_net_flow_mm3_dia"]
    )
    result = result.merge(
        nodes_df[["node_id", "nombre", "latitud", "longitud"]],
        on="node_id",
        how="left",
    ).merge(
        roles_df[["node_id", "role_proxy"]],
        on="node_id",
        how="left",
    )
    result["source"] = "balanced_total_system_consumption_allocated_by_observed_source_shares"
    return result.sort_values(["fecha", "node_id"]).reset_index(drop=True)


def _build_scenario_balance(node_exogenous_df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        node_exogenous_df.groupby("fecha", dropna=False)
        .agg(
            total_supply_mm3_dia_proxy=("supply_mm3_dia_proxy", "sum"),
            total_withdrawal_mm3_dia_proxy=("withdrawal_mm3_dia_proxy", "sum"),
            total_observed_net_flow_mm3_dia=("observed_net_flow_mm3_dia", "sum"),
            total_observed_throughput_mm3_dia=("observed_throughput_mm3_dia", "sum"),
            total_balancing_gap_mm3_dia_proxy=("balancing_gap_mm3_dia_proxy", "sum"),
        )
        .reset_index()
    )
    monthly["scenario_net_mm3_dia_proxy"] = (
        monthly["total_supply_mm3_dia_proxy"] - monthly["total_withdrawal_mm3_dia_proxy"]
    )
    monthly["source"] = "derived_from_red_nodo_exogenos_mensuales"
    return monthly.sort_values("fecha").reset_index(drop=True)


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    roles_df, node_metrics_df, nodes_df, consumption_df, production_df = _load_inputs()
    node_exogenous_df = _build_node_exogenous(
        roles_df,
        node_metrics_df,
        nodes_df,
        consumption_df,
        production_df,
    )
    scenario_balance_df = _build_scenario_balance(node_exogenous_df)

    outputs = {
        "red_nodo_exogenos_mensuales.parquet": node_exogenous_df,
        "red_balance_escenario_mensual.parquet": scenario_balance_df,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(node_exogenous_df, "red_nodo_exogenos_mensuales")
    _save_snapshot(scenario_balance_df, "red_balance_escenario_mensual")
    return node_exogenous_df, scenario_balance_df


if __name__ == "__main__":
    run()
