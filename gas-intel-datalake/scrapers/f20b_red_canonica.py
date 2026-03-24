"""
F20b — Red Canonica con Overrides Manuales
Source: Derived from F20 modeled network plus manual templates
Tier 1 — Automated + manual overrides
Tables: red_nodos_canonica, red_tramos_canonica, red_tramo_alias_canonica, red_topologia_diagnostico
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SNAPSHOTS_DIR = ROOT_DIR / "data" / "snapshots"
TEMPLATES_DIR = ROOT_DIR / "templates"

NODES_PATH = PROCESSED_DIR / "red_nodos.parquet"
EDGES_PATH = PROCESSED_DIR / "red_tramos.parquet"
ALIASES_PATH = PROCESSED_DIR / "red_tramo_alias.parquet"
NODE_OVERRIDE_PATH = TEMPLATES_DIR / "red_nodos_override.csv"
EDGE_OVERRIDE_PATH = TEMPLATES_DIR / "red_tramos_override.csv"


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def _load_base_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [path.name for path in (NODES_PATH, EDGES_PATH, ALIASES_PATH) if not path.exists()]
    if missing:
        raise FileNotFoundError("F20b requires F20 outputs first. Missing: " + ", ".join(missing))
    return (
        pd.read_parquet(NODES_PATH),
        pd.read_parquet(EDGES_PATH),
        pd.read_parquet(ALIASES_PATH),
    )


def _load_overrides(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns].fillna(pd.NA)


def _normalize_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _apply_node_overrides(nodes_df: pd.DataFrame, overrides_df: pd.DataFrame) -> pd.DataFrame:
    canonical = nodes_df.copy()
    canonical["canonical_name"] = canonical["nombre"]
    canonical["notes"] = pd.NA
    canonical["source_confidence"] = "powerbi_route"
    canonical["topology_status"] = "observed"
    canonical["is_active"] = True

    for _, row in overrides_df.iterrows():
        action = _normalize_text(row["action"])
        if action is None:
            continue
        node_id = _normalize_text(row["node_id"])
        if action == "update":
            if node_id is None or node_id not in set(canonical["node_id"]):
                raise RuntimeError(f"Node override update requires an existing node_id. Got {node_id!r}.")
            mask = canonical["node_id"] == node_id
        elif action == "add":
            node_id = node_id or f"manual_node_{hashlib.sha1(str(row.to_dict()).encode()).hexdigest()[:10]}"
            if node_id in set(canonical["node_id"]):
                raise RuntimeError(f"Node override add conflicts with existing node_id {node_id!r}.")
            canonical = pd.concat(
                [
                    canonical,
                    pd.DataFrame(
                        [
                            {
                                "node_id": node_id,
                                "nombre": _normalize_text(row["nombre"]),
                                "latitud": float(row["latitud"]),
                                "longitud": float(row["longitud"]),
                                "x_mercator": pd.NA,
                                "y_mercator": pd.NA,
                                "tipo_nodo": _normalize_text(row["tipo_nodo"]) or "junction",
                                "source": "manual_override",
                                "canonical_name": _normalize_text(row["canonical_name"]) or _normalize_text(row["nombre"]),
                                "notes": _normalize_text(row["notes"]),
                                "source_confidence": _normalize_text(row["source_confidence"]) or "manual_override",
                                "topology_status": _normalize_text(row["topology_status"]) or "manual_added",
                                "is_active": True,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            continue
        elif action == "deactivate":
            if node_id is None or node_id not in set(canonical["node_id"]):
                raise RuntimeError(f"Node override deactivate requires an existing node_id. Got {node_id!r}.")
            canonical.loc[canonical["node_id"] == node_id, "is_active"] = False
            canonical.loc[canonical["node_id"] == node_id, "topology_status"] = "manual_deactivated"
            continue
        else:
            raise RuntimeError(f"Unsupported node override action {action!r}.")

        for column, target in (
            ("nombre", "nombre"),
            ("latitud", "latitud"),
            ("longitud", "longitud"),
            ("tipo_nodo", "tipo_nodo"),
            ("canonical_name", "canonical_name"),
            ("notes", "notes"),
            ("source_confidence", "source_confidence"),
            ("topology_status", "topology_status"),
        ):
            value = row[column]
            if pd.notna(value):
                canonical.loc[mask, target] = value
        canonical.loc[mask, "source"] = "manual_override"
    return canonical


def _apply_edge_overrides(edges_df: pd.DataFrame, overrides_df: pd.DataFrame, nodes_df: pd.DataFrame) -> pd.DataFrame:
    canonical = edges_df.copy()
    canonical["canonical_name"] = canonical["ruta"]
    canonical["capacidad_mm3_dia_override"] = pd.NA
    canonical["diameter_m_override"] = pd.NA
    canonical["length_km_override"] = pd.NA
    canonical["notes"] = pd.NA
    canonical["source_confidence"] = "powerbi_route"
    canonical["topology_status"] = "observed"
    canonical["is_active"] = True

    existing_nodes = set(nodes_df["node_id"])
    for _, row in overrides_df.iterrows():
        action = _normalize_text(row["action"])
        if action is None:
            continue
        edge_id = _normalize_text(row["edge_id"])
        if action == "update":
            if edge_id is None or edge_id not in set(canonical["edge_id"]):
                raise RuntimeError(f"Edge override update requires an existing edge_id. Got {edge_id!r}.")
            mask = canonical["edge_id"] == edge_id
        elif action == "add":
            source_node_id = _normalize_text(row["source_node_id"])
            target_node_id = _normalize_text(row["target_node_id"])
            if source_node_id not in existing_nodes or target_node_id not in existing_nodes:
                raise RuntimeError("Manual edge add requires valid source_node_id and target_node_id.")
            edge_id = edge_id or f"manual_edge_{hashlib.sha1(str(row.to_dict()).encode()).hexdigest()[:10]}"
            canonical = pd.concat(
                [
                    canonical,
                    pd.DataFrame(
                        [
                            {
                                "edge_id": edge_id,
                                "ruta": _normalize_text(row["ruta"]),
                                "gasoducto": _normalize_text(row["gasoducto"]),
                                "origen": _normalize_text(row["origen"]),
                                "destino": _normalize_text(row["destino"]),
                                "source_node_id": source_node_id,
                                "target_node_id": target_node_id,
                                "latitud_origen": row["latitud_origen"],
                                "longitud_origen": row["longitud_origen"],
                                "latitud_destino": row["latitud_destino"],
                                "longitud_destino": row["longitud_destino"],
                                "capacidad_mm3_dia_override": row["capacidad_mm3_dia_override"],
                                "diameter_m_override": row["diameter_m_override"],
                                "length_km_override": row["length_km_override"],
                                "source": "manual_override",
                                "canonical_name": _normalize_text(row["canonical_name"]) or _normalize_text(row["ruta"]),
                                "notes": _normalize_text(row["notes"]),
                                "source_confidence": _normalize_text(row["source_confidence"]) or "manual_override",
                                "topology_status": _normalize_text(row["topology_status"]) or "manual_added",
                                "is_active": True,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )
            continue
        elif action == "deactivate":
            if edge_id is None or edge_id not in set(canonical["edge_id"]):
                raise RuntimeError(f"Edge override deactivate requires an existing edge_id. Got {edge_id!r}.")
            canonical.loc[canonical["edge_id"] == edge_id, "is_active"] = False
            canonical.loc[canonical["edge_id"] == edge_id, "topology_status"] = "manual_deactivated"
            continue
        else:
            raise RuntimeError(f"Unsupported edge override action {action!r}.")

        for column in (
            "ruta",
            "gasoducto",
            "origen",
            "destino",
            "source_node_id",
            "target_node_id",
            "latitud_origen",
            "longitud_origen",
            "latitud_destino",
            "longitud_destino",
            "capacidad_mm3_dia_override",
            "diameter_m_override",
            "length_km_override",
            "canonical_name",
            "notes",
            "source_confidence",
            "topology_status",
        ):
            value = row[column]
            if pd.notna(value):
                canonical.loc[mask, column] = value
        canonical.loc[mask, "source"] = "manual_override"
    return canonical


def _build_aliases(edges_df: pd.DataFrame, base_aliases_df: pd.DataFrame) -> pd.DataFrame:
    aliases = base_aliases_df.copy()
    aliases = aliases[aliases["edge_id"].isin(set(edges_df.loc[edges_df["is_active"], "edge_id"]))]
    manual_edges = edges_df[edges_df["source"] == "manual_override"]
    manual_records: list[dict[str, Any]] = []
    for _, row in manual_edges.iterrows():
        if not bool(row["is_active"]):
            continue
        for alias_type, alias_value in (
            ("ruta", row["ruta"]),
            ("gasoducto", row["gasoducto"]),
            ("origen", row["origen"]),
            ("destino", row["destino"]),
            ("corridor", f"{row['origen']}->{row['destino']}"),
        ):
            if _normalize_text(alias_value) is None:
                continue
            manual_records.append(
                {
                    "edge_id": row["edge_id"],
                    "alias_type": alias_type,
                    "alias_value": alias_value,
                    "source": "manual_override",
                }
            )
    if manual_records:
        aliases = pd.concat([aliases, pd.DataFrame(manual_records)], ignore_index=True)
    aliases = aliases.drop_duplicates(subset=["edge_id", "alias_type", "alias_value"]).reset_index(drop=True)
    return aliases


def _compute_connected_components(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> dict[str, int]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    active_nodes = set(nodes_df.loc[nodes_df["is_active"], "node_id"])
    for _, row in edges_df.loc[edges_df["is_active"]].iterrows():
        src = row["source_node_id"]
        dst = row["target_node_id"]
        adjacency[src].add(dst)
        adjacency[dst].add(src)

    component_by_node: dict[str, int] = {}
    component_id = 0
    for node_id in active_nodes:
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


def _build_diagnostics(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> pd.DataFrame:
    active_nodes = nodes_df[nodes_df["is_active"]].copy()
    active_edges = edges_df[edges_df["is_active"]].copy()
    component_by_node = _compute_connected_components(active_nodes, active_edges)

    degree_count: dict[str, int] = defaultdict(int)
    for _, row in active_edges.iterrows():
        degree_count[str(row["source_node_id"])] += 1
        degree_count[str(row["target_node_id"])] += 1

    records: list[dict[str, Any]] = []
    duplicate_counts = (
        active_edges.groupby(["source_node_id", "target_node_id", "gasoducto"], dropna=False)["edge_id"]
        .count()
        .reset_index(name="n")
    )
    for _, row in duplicate_counts[duplicate_counts["n"] > 1].iterrows():
        records.append(
            {
                "entity_type": "edge_group",
                "entity_id": f"{row['source_node_id']}->{row['target_node_id']}:{row['gasoducto']}",
                "severity": "warning",
                "issue_type": "duplicate_edge",
                "issue_detail": f"{int(row['n'])} active edges share the same endpoints and gasoducto.",
                "source": "derived_from_red_tramos_canonica",
            }
        )

    component_sizes = pd.Series(component_by_node).value_counts().to_dict()
    largest_component = max(component_sizes, key=component_sizes.get) if component_sizes else None
    for _, row in active_nodes.iterrows():
        node_id = str(row["node_id"])
        degree = degree_count.get(node_id, 0)
        component = component_by_node.get(node_id)
        if degree == 0:
            records.append(
                {
                    "entity_type": "node",
                    "entity_id": node_id,
                    "severity": "error",
                    "issue_type": "isolated_node",
                    "issue_detail": f"Node {row['nombre']} has no active incident edges.",
                    "source": "derived_from_red_nodos_canonica",
                }
            )
        elif degree == 1:
            records.append(
                {
                    "entity_type": "node",
                    "entity_id": node_id,
                    "severity": "warning",
                    "issue_type": "dangling_node",
                    "issue_detail": f"Node {row['nombre']} has degree 1 and may indicate a missing connection or boundary.",
                    "source": "derived_from_red_nodos_canonica",
                }
            )
        if largest_component is not None and component != largest_component:
            records.append(
                {
                    "entity_type": "node",
                    "entity_id": node_id,
                    "severity": "warning",
                    "issue_type": "disconnected_component",
                    "issue_detail": f"Node {row['nombre']} is outside the largest connected component.",
                    "source": "derived_from_red_nodos_canonica",
                }
            )

    for _, row in active_edges.iterrows():
        if row["source_node_id"] == row["target_node_id"]:
            records.append(
                {
                    "entity_type": "edge",
                    "entity_id": row["edge_id"],
                    "severity": "error",
                    "issue_type": "self_loop",
                    "issue_detail": f"Edge {row['ruta']} connects the same node to itself.",
                    "source": "derived_from_red_tramos_canonica",
                }
            )

    if not records:
        records.append(
            {
                "entity_type": "graph",
                "entity_id": "red_canonica",
                "severity": "info",
                "issue_type": "no_issues_detected",
                "issue_detail": "No topology diagnostics were triggered with the current canonical graph.",
                "source": "derived_from_red_canonica",
            }
        )

    return pd.DataFrame(records).drop_duplicates().reset_index(drop=True)


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

    nodes_df, edges_df, aliases_df = _load_base_tables()
    node_overrides = _load_overrides(
        NODE_OVERRIDE_PATH,
        [
            "action",
            "node_id",
            "nombre",
            "latitud",
            "longitud",
            "tipo_nodo",
            "canonical_name",
            "notes",
            "source_confidence",
            "topology_status",
        ],
    )
    edge_overrides = _load_overrides(
        EDGE_OVERRIDE_PATH,
        [
            "action",
            "edge_id",
            "ruta",
            "gasoducto",
            "origen",
            "destino",
            "source_node_id",
            "target_node_id",
            "latitud_origen",
            "longitud_origen",
            "latitud_destino",
            "longitud_destino",
            "capacidad_mm3_dia_override",
            "diameter_m_override",
            "length_km_override",
            "canonical_name",
            "notes",
            "source_confidence",
            "topology_status",
        ],
    )

    canonical_nodes = _apply_node_overrides(nodes_df, node_overrides)
    canonical_edges = _apply_edge_overrides(edges_df, edge_overrides, canonical_nodes)
    canonical_aliases = _build_aliases(canonical_edges, aliases_df)
    diagnostics = _build_diagnostics(canonical_nodes, canonical_edges)

    outputs = {
        "red_nodos_canonica.parquet": canonical_nodes,
        "red_tramos_canonica.parquet": canonical_edges,
        "red_tramo_alias_canonica.parquet": canonical_aliases,
        "red_topologia_diagnostico.parquet": diagnostics,
    }
    for filename, df in outputs.items():
        path = PROCESSED_DIR / filename
        df.to_parquet(path, index=False)
        log.info("Saved processed: %s (%s rows)", path, len(df))

    _save_snapshot(canonical_nodes, "red_nodos_canonica")
    _save_snapshot(canonical_edges, "red_tramos_canonica")
    _save_snapshot(canonical_aliases, "red_tramo_alias_canonica")
    _save_snapshot(diagnostics, "red_topologia_diagnostico")
    return canonical_nodes, canonical_edges, canonical_aliases, diagnostics


if __name__ == "__main__":
    run()
