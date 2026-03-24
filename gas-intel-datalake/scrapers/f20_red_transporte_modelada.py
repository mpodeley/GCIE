"""
F20 — Red de Transporte Modelada (ENARGAS Power BI)
Source: ENARGAS public Power BI report
Tier 1 — Automated
Tables: red_nodos, red_tramos, red_tramo_alias, red_tramo_metricas_mensuales

This scraper reconstructs an operational transport graph from the public ENARGAS
Power BI report that exposes route-level flow/capacity data with endpoint
coordinates. The result is a graph-ready monthly network layer for downstream
optimization and simulation work.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
import requests


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RESOURCE_KEY = "948aa3aa-1c05-4cc3-b68c-6dd65a53c694"
MODEL_ID = 11730893
API_ROOT = "https://wabi-south-central-us-api.analysis.windows.net/public/reports"
MERCATOR_RADIUS = 6378137.0

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "enargas"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"


def _make_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "ActivityId": str(uuid4()),
        "RequestId": str(uuid4()),
        "Content-Type": "application/json",
        "X-PowerBI-ResourceKey": RESOURCE_KEY,
    }


def _fetch_json(url: str, method: str = "GET", body: dict[str, Any] | None = None) -> Any:
    response = requests.request(
        method=method,
        url=url,
        headers=_make_headers(),
        json=body,
        timeout=180,
    )
    response.raise_for_status()
    return response.json()


def _build_column(source: str, property_name: str, name: str) -> dict[str, Any]:
    return {
        "Column": {
            "Expression": {
                "SourceRef": {"Source": source},
            },
            "Property": property_name,
        },
        "Name": name,
    }


def _build_date_condition(source: str, property_name: str, iso_date: str) -> dict[str, Any]:
    return {
        "Condition": {
            "Comparison": {
                "ComparisonKind": 0,
                "Left": {
                    "Column": {
                        "Expression": {"SourceRef": {"Source": source}},
                        "Property": property_name,
                    }
                },
                "Right": {
                    "Literal": {"Value": f"datetime'{iso_date}T00:00:00'"}
                },
            }
        }
    }


def _query_data(
    *,
    from_entities: list[dict[str, Any]],
    select: list[dict[str, Any]],
    projections: list[int],
    where: list[dict[str, Any]] | None = None,
    top: int = 500,
) -> Any:
    body = {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {
                    "Commands": [
                        {
                            "SemanticQueryDataShapeCommand": {
                                "Query": {
                                    "Version": 2,
                                    "From": from_entities,
                                    "Select": select,
                                    **({"Where": where} if where else {}),
                                },
                                "Binding": {
                                    "Primary": {
                                        "Groupings": [{"Projections": projections}]
                                    },
                                    "DataReduction": {
                                        "DataVolume": 3,
                                        "Primary": {
                                            "Top": {"Count": top}
                                        },
                                    },
                                    "Version": 1,
                                },
                                "ExecutionMetricsKind": 1,
                            }
                        }
                    ]
                }
            }
        ],
        "modelId": MODEL_ID,
    }
    return _fetch_json(
        f"{API_ROOT}/querydata?synchronous=true",
        method="POST",
        body=body,
    )


def _fix_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if "Ã" not in value and "Â" not in value:
        return value
    try:
        return value.encode("latin1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


def _decode_value(raw: Any, schema_entry: dict[str, Any], value_dicts: dict[str, Any]) -> Any:
    if raw is None:
        return None
    if schema_entry.get("DN"):
        dictionary = value_dicts.get(schema_entry["DN"], {})
        if isinstance(dictionary, list):
            try:
                return dictionary[int(raw)]
            except (ValueError, TypeError, IndexError):
                return raw
        if isinstance(dictionary, dict):
            return dictionary.get(str(raw), dictionary.get(raw, raw))
        return raw
    if schema_entry.get("T") == 7:
        return pd.to_datetime(raw, unit="ms", utc=True).date().isoformat()
    if schema_entry.get("T") in {3, 4}:
        return float(raw)
    return _fix_text(raw)


def _parse_dsr_rows(query_result: dict[str, Any]) -> list[list[Any]]:
    dataset = query_result["results"][0]["result"]["data"]["dsr"]["DS"][0]
    row_set = dataset["PH"][0]["DM0"]
    if not row_set:
        return []

    schema = row_set[0]["S"]
    value_dicts = dataset.get("ValueDicts", {})
    rows: list[list[Any]] = []
    previous = [None] * len(schema)

    for entry in row_set:
        current: list[Any] = []
        if "C" not in entry:
            for index, schema_entry in enumerate(schema):
                current.append(_decode_value(entry.get(schema_entry["N"]), schema_entry, value_dicts))
        else:
            compressed = entry.get("C", [])
            repeat_mask = entry.get("R", 0)
            cursor = 0
            for index, schema_entry in enumerate(schema):
                should_repeat = (repeat_mask & (1 << index)) != 0
                if should_repeat:
                    value = previous[index]
                else:
                    value = compressed[cursor] if cursor < len(compressed) else None
                    cursor += 1
                current.append(_decode_value(value, schema_entry, value_dicts))
        rows.append(current)
        previous = current
    return rows


def _rows_to_objects(rows: list[list[Any]], keys: list[str]) -> list[dict[str, Any]]:
    return [
        {key: row[index] if index < len(row) else None for index, key in enumerate(keys)}
        for row in rows
    ]


def _project_mercator(lon: float, lat: float) -> tuple[float, float]:
    clamped_lat = max(-85.05112878, min(85.05112878, lat))
    lambda_value = math.radians(lon)
    phi_value = math.radians(clamped_lat)
    x = MERCATOR_RADIUS * lambda_value
    y = MERCATOR_RADIUS * math.log(math.tan(math.pi / 4 + phi_value / 2))
    return x, y


def _get_available_dates() -> list[str]:
    payload = _query_data(
        from_entities=[{"Name": "h", "Entity": "Fechas", "Type": 0}],
        select=[_build_column("h", "Fecha", "Fechas.Fecha")],
        projections=[0],
        top=2000,
    )
    dates = [_row[0] for _row in _parse_dsr_rows(payload) if _row and _row[0]]
    return sorted(set(dates))


def _select_monthly_dates(dates: list[str], keep_last: int = 48) -> list[str]:
    by_month: dict[str, str] = {}
    for date in dates:
        by_month[date[:7]] = date
    return sorted(by_month.values())[-keep_last:]


def _get_routes() -> list[dict[str, Any]]:
    payload = _query_data(
        from_entities=[{"Name": "b", "Entity": "BaseRutas", "Type": 0}],
        select=[
            _build_column("b", "Ruta", "BaseRutas.Ruta"),
            _build_column("b", "Origen", "BaseRutas.Origen"),
            _build_column("b", "Destino", "BaseRutas.Destino"),
            _build_column("b", "Gasoducto", "BaseRutas.Gasoducto"),
            _build_column("b", "Latitud Origen", "BaseRutas.Latitud Origen"),
            _build_column("b", "Longitud Origen", "BaseRutas.Longitud Origen"),
            _build_column("b", "Latitud Destino", "BaseRutas.Latitud Destino"),
            _build_column("b", "Longitud Destino", "BaseRutas.Longitud Destino"),
        ],
        projections=[0, 1, 2, 3, 4, 5, 6, 7],
        top=200,
    )
    routes = _rows_to_objects(
        _parse_dsr_rows(payload),
        [
            "ruta",
            "origen",
            "destino",
            "gasoducto",
            "latitud_origen",
            "longitud_origen",
            "latitud_destino",
            "longitud_destino",
        ],
    )
    return routes


def _get_flows_for_date(date: str) -> list[dict[str, Any]]:
    payload = _query_data(
        from_entities=[{"Name": "f", "Entity": "Flujos", "Type": 0}],
        select=[
            _build_column("f", "Fecha", "Flujos.Fecha"),
            _build_column("f", "Ruta", "Flujos.Ruta"),
            _build_column("f", "Caudal", "Flujos.Caudal"),
            _build_column("f", "F-CF", "Flujos.F-CF"),
            _build_column("f", "Flujo-ContrFlujo", "Flujos.Flujo-ContrFlujo"),
        ],
        where=[_build_date_condition("f", "Fecha", date)],
        projections=[0, 1, 2, 3, 4],
        top=800,
    )
    return _rows_to_objects(
        _parse_dsr_rows(payload),
        ["fecha", "ruta", "caudal", "fcf", "sentido"],
    )


def _get_capacity_for_date(date: str) -> list[dict[str, Any]]:
    payload = _query_data(
        from_entities=[{"Name": "c", "Entity": "Capacidad", "Type": 0}],
        select=[
            _build_column("c", "Fecha", "Capacidad.Fecha"),
            _build_column("c", "Ruta", "Capacidad.Ruta"),
            _build_column("c", "Capacidad", "Capacidad.Capacidad"),
        ],
        where=[_build_date_condition("c", "Fecha", date)],
        projections=[0, 1, 2],
        top=800,
    )
    return _rows_to_objects(
        _parse_dsr_rows(payload),
        ["fecha", "ruta", "capacidad"],
    )


def _fetch_operational_history(monthly_dates: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    flow_rows: list[dict[str, Any]] = []
    capacity_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        flow_futures = {
            executor.submit(_get_flows_for_date, date): date for date in monthly_dates
        }
        capacity_futures = {
            executor.submit(_get_capacity_for_date, date): date for date in monthly_dates
        }
        for future in as_completed(flow_futures):
            date = flow_futures[future]
            log.info("Fetched route flows for %s", date)
            flow_rows.extend(future.result())
        for future in as_completed(capacity_futures):
            date = capacity_futures[future]
            log.info("Fetched route capacity for %s", date)
            capacity_rows.extend(future.result())
    return flow_rows, capacity_rows


def _build_nodes(routes: list[dict[str, Any]]) -> pd.DataFrame:
    buckets: dict[tuple[str, float, float], dict[str, Any]] = {}

    for route in routes:
        for role in ("origen", "destino"):
            label = _fix_text(route[role])
            lat = float(route[f"latitud_{role}"])
            lon = float(route[f"longitud_{role}"])
            key = (str(label).strip(), round(lat, 4), round(lon, 4))
            bucket = buckets.setdefault(
                key,
                {
                    "label": label,
                    "latitudes": [],
                    "longitudes": [],
                    "roles": set(),
                },
            )
            bucket["latitudes"].append(lat)
            bucket["longitudes"].append(lon)
            bucket["roles"].add(role)

    records = []
    for index, bucket in enumerate(sorted(buckets.values(), key=lambda item: str(item["label"]))):
        lat = sum(bucket["latitudes"]) / len(bucket["latitudes"])
        lon = sum(bucket["longitudes"]) / len(bucket["longitudes"])
        x_mercator, y_mercator = _project_mercator(lon, lat)
        node_id = f"node_{index:04d}"
        records.append(
            {
                "node_id": node_id,
                "nombre": bucket["label"],
                "latitud": lat,
                "longitud": lon,
                "x_mercator": x_mercator,
                "y_mercator": y_mercator,
                "tipo_nodo": "junction",
                "source": "enargas_powerbi_routes",
            }
        )
    return pd.DataFrame(records)


def _build_edges(routes: list[dict[str, Any]], nodes_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    node_lookup = {
        (row["nombre"], round(float(row["latitud"]), 4), round(float(row["longitud"]), 4)): row["node_id"]
        for _, row in nodes_df.iterrows()
    }

    edge_records: list[dict[str, Any]] = []
    alias_records: list[dict[str, Any]] = []
    for route in routes:
        ruta = str(_fix_text(route["ruta"])).strip()
        origen = str(_fix_text(route["origen"])).strip()
        destino = str(_fix_text(route["destino"])).strip()
        gasoducto = str(_fix_text(route["gasoducto"])).strip()
        source_node_id = node_lookup[(origen, round(float(route["latitud_origen"]), 4), round(float(route["longitud_origen"]), 4))]
        target_node_id = node_lookup[(destino, round(float(route["latitud_destino"]), 4), round(float(route["longitud_destino"]), 4))]
        edge_id = hashlib.sha1(f"{ruta}|{origen}|{destino}|{gasoducto}".encode("utf-8")).hexdigest()[:16]

        edge_records.append(
            {
                "edge_id": edge_id,
                "ruta": ruta,
                "gasoducto": gasoducto,
                "origen": origen,
                "destino": destino,
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "latitud_origen": float(route["latitud_origen"]),
                "longitud_origen": float(route["longitud_origen"]),
                "latitud_destino": float(route["latitud_destino"]),
                "longitud_destino": float(route["longitud_destino"]),
                "source": "enargas_powerbi_routes",
            }
        )

        for alias_type, alias_value in (
            ("ruta", ruta),
            ("gasoducto", gasoducto),
            ("origen", origen),
            ("destino", destino),
            ("corridor", f"{origen}->{destino}"),
        ):
            alias_records.append(
                {
                    "edge_id": edge_id,
                    "alias_type": alias_type,
                    "alias_value": alias_value,
                    "source": "enargas_powerbi_routes",
                }
            )

    return pd.DataFrame(edge_records), pd.DataFrame(alias_records)


def _build_edge_metrics(
    edges_df: pd.DataFrame,
    flow_rows: list[dict[str, Any]],
    capacity_rows: list[dict[str, Any]],
) -> pd.DataFrame:
    edge_lookup = {
        row["ruta"]: row["edge_id"]
        for _, row in edges_df.iterrows()
    }
    capacity_lookup = {
        (str(_fix_text(row["fecha"])), str(_fix_text(row["ruta"]))): row.get("capacidad")
        for row in capacity_rows
    }

    records: list[dict[str, Any]] = []
    for row in flow_rows:
        fecha = str(_fix_text(row["fecha"]))
        ruta = str(_fix_text(row["ruta"]))
        edge_id = edge_lookup.get(ruta)
        if edge_id is None:
            continue
        caudal = None if row.get("caudal") is None else float(row["caudal"])
        capacidad = capacity_lookup.get((fecha, ruta))
        capacidad_value = None if capacidad is None else float(capacidad)
        utilization = (
            caudal / capacidad_value
            if caudal is not None and capacidad_value not in (None, 0.0)
            else None
        )
        records.append(
            {
                "fecha": pd.to_datetime(fecha),
                "edge_id": edge_id,
                "caudal_mm3_dia": caudal,
                "capacidad_mm3_dia": capacidad_value,
                "utilization_ratio": utilization,
                "fcf": _fix_text(row.get("fcf")),
                "sentido": _fix_text(row.get("sentido")),
                "source": "enargas_powerbi_routes",
            }
        )

    return pd.DataFrame(records).sort_values(["fecha", "edge_id"]).reset_index(drop=True)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Fetching ENARGAS Power BI metadata")
    models_and_exploration = _fetch_json(
        f"{API_ROOT}/{RESOURCE_KEY}/modelsAndExploration?preferReadOnlySession=true"
    )
    conceptual_schema = _fetch_json(f"{API_ROOT}/{RESOURCE_KEY}/conceptualschema")
    available_dates = _get_available_dates()
    monthly_dates = _select_monthly_dates(available_dates)
    routes = _get_routes()
    flow_rows, capacity_rows = _fetch_operational_history(monthly_dates)

    metadata = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": "ENARGAS Power BI public report",
        "resource_key": RESOURCE_KEY,
        "model_id": MODEL_ID,
        "latest_date": monthly_dates[-1] if monthly_dates else None,
        "available_dates": len(available_dates),
        "selected_monthly_dates": monthly_dates,
        "route_count": len(routes),
        "entities": [
            {
                "name": entity["Name"],
                "columns": [prop["Name"] for prop in entity["Properties"]],
            }
            for entity in conceptual_schema["schemas"][0]["schema"]["Entities"]
        ],
        "report_name": models_and_exploration["models"][0]["displayName"],
        "last_refresh_time": models_and_exploration["models"][0].get("LastRefreshTime"),
    }
    _write_json(RAW_DIR / "enargas_powerbi_transport_metadata.json", metadata)
    _write_json(RAW_DIR / "enargas_powerbi_routes.json", routes)

    nodes_df = _build_nodes(routes)
    edges_df, aliases_df = _build_edges(routes, nodes_df)
    metrics_df = _build_edge_metrics(edges_df, flow_rows, capacity_rows)

    outputs = {
        "red_nodos.parquet": nodes_df,
        "red_tramos.parquet": edges_df,
        "red_tramo_alias.parquet": aliases_df,
        "red_tramo_metricas_mensuales.parquet": metrics_df,
    }
    for filename, df in outputs.items():
        output_path = PROCESSED_DIR / filename
        df.to_parquet(output_path, index=False)
        log.info("Saved processed: %s (%s rows)", output_path, len(df))

    _save_snapshot(nodes_df, "red_nodos")
    _save_snapshot(edges_df, "red_tramos")
    _save_snapshot(aliases_df, "red_tramo_alias")
    _save_snapshot(metrics_df, "red_tramo_metricas_mensuales")
    return nodes_df, edges_df, aliases_df, metrics_df


if __name__ == "__main__":
    run()
