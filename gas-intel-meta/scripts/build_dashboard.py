"""Build a static dashboard for the current GCIE workspace state."""

from __future__ import annotations

import csv
import json
import sys
from collections import deque
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
FORECAST_DIR = ROOT / "gas-intel-forecast"
DATALAKE_DIR = ROOT / "gas-intel-datalake"
PROCESSED_DIR = DATALAKE_DIR / "data" / "processed"
DUCKDB_PATH = DATALAKE_DIR / "duckdb" / "gas_intel.duckdb"
OUTPUT_PATH = ROOT / "gas-intel-meta" / "dashboard" / "index.html"
ASSETS_DIR = ROOT / "gas-intel-meta" / "assets"
OUTLINE_PATH = ASSETS_DIR / "argentina-outline-3857.json"
CLAUDE_DOCS = [
    ("gas-intel-datalake", ROOT / "gas-intel-datalake" / "CLAUDE.md"),
    ("gas-intel-forecast", ROOT / "gas-intel-forecast" / "CLAUDE.md"),
    ("gas-intel-supply", ROOT / "gas-intel-supply" / "CLAUDE.md"),
    ("gas-intel-pricing", ROOT / "gas-intel-pricing" / "CLAUDE.md"),
    ("gas-intel-risk", ROOT / "gas-intel-risk" / "CLAUDE.md"),
    ("gas-intel-scoring", ROOT / "gas-intel-scoring" / "CLAUDE.md"),
    ("gas-intel-portfolio", ROOT / "gas-intel-portfolio" / "CLAUDE.md"),
    ("gas-intel-meta", ROOT / "gas-intel-meta" / "CLAUDE.md"),
]


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to build the dashboard. Run this script with the GCIE virtualenv."
        ) from exc
    return duckdb


def _extract_markdown_section(text: str, heading: str) -> str | None:
    marker = f"## {heading}"
    if marker not in text:
        return None
    section = text.split(marker, maxsplit=1)[1]
    next_heading = section.find("\n## ")
    if next_heading != -1:
        section = section[:next_heading]
    section = section.strip()
    return section or None


def _first_meaningful_line(text: str | None) -> str | None:
    if not text:
        return None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("- "):
            return line[2:].strip()
        if line.startswith("|"):
            continue
        return line
    return None


def _load_project_status() -> dict[str, Any]:
    root_text = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    summary = _first_meaningful_line(_extract_markdown_section(root_text, "Project overview")) or ""
    roadmap = _first_meaningful_line(_extract_markdown_section(root_text, "Near-term roadmap")) or ""

    cards = []
    for repo, path in CLAUDE_DOCS:
        text = path.read_text(encoding="utf-8")
        title = text.splitlines()[0].lstrip("# ").strip()
        role = _first_meaningful_line(_extract_markdown_section(text, "Role")) or ""
        current = (
            _first_meaningful_line(_extract_markdown_section(text, "Current implementation"))
            or _first_meaningful_line(_extract_markdown_section(text, "Current state"))
            or _first_meaningful_line(_extract_markdown_section(text, "What exists today"))
            or role
        )
        score = _first_meaningful_line(_extract_markdown_section(text, "Current score"))
        next_step = (
            _first_meaningful_line(_extract_markdown_section(text, "Near-term research direction"))
            or _first_meaningful_line(_extract_markdown_section(text, "Near-term direction"))
            or _first_meaningful_line(_extract_markdown_section(text, "Current gap"))
            or _first_meaningful_line(_extract_markdown_section(text, "Implementation rule"))
            or ""
        )
        cards.append(
            {
                "repo": repo,
                "title": title,
                "role": role,
                "current": current,
                "score": score,
                "next_step": next_step,
            }
        )

    return {
        "summary": summary,
        "roadmap": roadmap,
        "cards": cards,
    }


def _load_argentina_outline() -> dict[str, Any]:
    if not OUTLINE_PATH.exists():
        return {"polygons": []}
    raw = json.loads(OUTLINE_PATH.read_text(encoding="utf-8"))
    polygons = []
    for polygon in raw.get("polygons", []):
        polygons.append([{"x": point["x"], "y": point["y"]} for point in polygon])
    return {"polygons": polygons}


def _load_forecast_summary() -> dict[str, Any]:
    sys.path.insert(0, str(FORECAST_DIR))
    from data_pipeline import load_dataset  # type: ignore
    from evaluate import score_run  # type: ignore

    dataset = load_dataset()
    result = score_run()
    predictions = result["predictions"]
    validation_lookup = {
        (str(row["fecha"]), row["segmento"]): row for row in dataset["validation_rows"]
    }
    prediction_lookup = {
        (str(row["fecha"]), row["segmento"]): row for row in predictions
    }
    segment_errors: dict[str, list[float]] = {}
    segment_series: dict[str, list[dict[str, Any]]] = {}
    history_series: dict[str, list[dict[str, Any]]] = {}
    driver_series: dict[str, list[dict[str, Any]]] = {}

    all_rows = dataset["train_rows"] + dataset["validation_rows"]
    for row in all_rows:
        segment = row["segmento"]
        key = (str(row["fecha"]), segment)
        prediction = prediction_lookup.get(key)
        history_series.setdefault(segment, []).append(
            {
                "fecha": str(row["fecha"]),
                "actual_volume": int(row["actual_volume"]),
                "predicted_volume": None if prediction is None else int(prediction["predicted_volume"]),
                "is_validation": key in validation_lookup,
            }
        )
        if row["hdd"] is not None:
            driver_series.setdefault(segment, []).append(
                {
                    "fecha": str(row["fecha"]),
                    "hdd": round(float(row["hdd"]), 2),
                    "actual_volume": int(row["actual_volume"]),
                    "is_validation": key in validation_lookup,
                }
            )

    for row in predictions:
        actual = float(row["actual_volume"])
        predicted = float(row["predicted_volume"])
        if actual <= 0:
            continue
        ape = abs(actual - predicted) / actual
        segment_errors.setdefault(row["segmento"], []).append(ape)
        feature_row = validation_lookup[(str(row["fecha"]), row["segmento"])]
        segment_series.setdefault(row["segmento"], []).append(
            {
                "fecha": str(row["fecha"]),
                "segmento": row["segmento"],
                "actual_volume": int(actual),
                "predicted_volume": int(predicted),
                "ape": ape,
                "hdd": None if feature_row["hdd"] is None else round(float(feature_row["hdd"]), 2),
                "cdd": None if feature_row["cdd"] is None else round(float(feature_row["cdd"]), 2),
                "temp_media": None
                if feature_row["temp_media"] is None
                else round(float(feature_row["temp_media"]), 2),
                "lag_values": {
                    lag_key: int(feature_row[lag_key])
                    for lag_key in result["model_artifacts"]["lag_keys"]
                },
            }
        )

    by_segment = [
        {
            "segmento": segment,
            "mean_ape": sum(errors) / len(errors),
            "n": len(errors),
        }
        for segment, errors in sorted(segment_errors.items())
    ]

    recent_predictions = [
        {
            "fecha": str(row["fecha"]),
            "segmento": row["segmento"],
            "actual_volume": int(row["actual_volume"]),
            "predicted_volume": int(row["predicted_volume"]),
        }
        for row in predictions
    ]

    chart_segments = [
        {
            "segmento": segment,
            "points": sorted(points, key=lambda item: item["fecha"]),
        }
        for segment, points in sorted(segment_series.items())
    ]
    history_segments = [
        {
            "segmento": segment,
            "points": sorted(points, key=lambda item: item["fecha"])[-24:],
        }
        for segment, points in sorted(history_series.items())
    ]
    driver_segments = [
        {
            "segmento": segment,
            "points": sorted(points, key=lambda item: item["fecha"])[-36:],
        }
        for segment, points in sorted(driver_series.items())
    ]

    return {
        "hypothesis": result["hypothesis"],
        "metric_name": result["metric_name"],
        "metric_value": result["metric_value"],
        "train_rows": result["train_rows"],
        "validation_rows": result["validation_rows"],
        "model_artifacts": result["model_artifacts"],
        "by_segment": by_segment,
        "recent_predictions": recent_predictions,
        "chart_segments": chart_segments,
        "history_segments": history_segments,
        "driver_segments": driver_segments,
    }


def _load_results_table(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _load_datalake_summary() -> dict[str, Any]:
    duckdb = _import_duckdb()
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(f"DuckDB snapshot not found at {DUCKDB_PATH}")

    table_specs = [
        ("produccion_diaria", True),
        ("gas_asociado_ratio", True),
        ("consumo_diario", True),
        ("clima", True),
        ("calendario", True),
        ("precios_boca_pozo", True),
        ("tipo_cambio", True),
        ("transporte_flujo_mensual", True),
        ("transporte_capacidad_firme", True),
        ("transporte_utilizacion_mensual", True),
        ("red_nodos_canonica", False),
        ("red_tramos_canonica", False),
        ("red_compresoras_canonica", False),
        ("red_loops_canonica", False),
        ("red_solver_resumen_mensual", True),
    ]

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        table_summary: dict[str, Any] = {}
        for table, has_fecha in table_specs:
            if has_fecha:
                count, min_fecha, max_fecha = conn.execute(
                    f"SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM {table}"
                ).fetchone()
            else:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                min_fecha = None
                max_fecha = None
            table_summary[table] = {
                "rows": int(count),
                "min_fecha": None if min_fecha is None else str(min_fecha),
                "max_fecha": None if max_fecha is None else str(max_fecha),
            }
    finally:
        conn.close()
    return table_summary


def _compute_component_count(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> int:
    adjacency: dict[str, set[str]] = {str(node["node_id"]): set() for node in nodes}
    for edge in edges:
        source = str(edge["source_node_id"])
        target = str(edge["target_node_id"])
        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set()).add(source)

    visited: set[str] = set()
    components = 0
    for node_id in adjacency:
        if node_id in visited:
            continue
        components += 1
        queue: deque[str] = deque([node_id])
        visited.add(node_id)
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
    return components


def _load_latest_pandapipes_summary() -> dict[str, Any] | None:
    matches = sorted(PROCESSED_DIR.glob("pandapipes_case_*.summary.json"))
    if not matches:
        return None
    return json.loads(matches[-1].read_text(encoding="utf-8"))


def _load_network_summary() -> dict[str, Any]:
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        latest_observed_month = conn.execute(
            """
            SELECT MAX(fecha)
            FROM red_tramo_metricas_mensuales
            WHERE COALESCE(caudal_mm3_dia, 0) > 0
            """
        ).fetchone()[0]
        latest_solver_month = conn.execute(
            """
            SELECT MAX(fecha)
            FROM red_solver_resumen_mensual
            WHERE COALESCE(total_withdrawal_mm3_dia_proxy, 0) > 0
            """
        ).fetchone()[0]

        nodes_df = conn.execute(
            """
            SELECT
              node_id,
              nombre,
              latitud,
              longitud,
              x_mercator,
              y_mercator,
              source_confidence,
              topology_status
            FROM red_nodos_canonica
            WHERE is_active
            ORDER BY nombre
            """
        ).fetchdf()
        compressors_df = conn.execute(
            """
            SELECT
              asset_id,
              nombre,
              node_id,
              gasoducto,
              potencia_hp,
              estado,
              source_confidence,
              topology_status
            FROM red_compresoras_canonica
            ORDER BY gasoducto, nombre
            """
        ).fetchdf()
        loops_df = conn.execute(
            """
            SELECT
              asset_id,
              nombre,
              edge_id,
              gasoducto,
              length_km,
              diameter_m,
              capacity_mm3_dia_incremental,
              estado,
              source_confidence,
              topology_status
            FROM red_loops_canonica
            ORDER BY gasoducto, nombre
            """
        ).fetchdf()
        edges_df = conn.execute(
            """
            SELECT
              e.edge_id,
              e.ruta,
              e.gasoducto,
              e.origen,
              e.destino,
              e.source_node_id,
              e.target_node_id,
              e.source_confidence,
              e.topology_status,
              p.effective_capacity_mm3_dia,
              p.effective_diameter_m,
              p.effective_length_km,
              COALESCE(p.active_loop_count, 0) AS active_loop_count,
              COALESCE(p.loop_capacity_increment_mm3_dia, 0) AS loop_capacity_increment_mm3_dia
            FROM red_tramos_canonica e
            LEFT JOIN red_tramos_parametros_canonica p USING(edge_id)
            WHERE e.is_active
            ORDER BY e.gasoducto, e.ruta
            """
        ).fetchdf()
        edge_snapshots_df = conn.execute(
            """
            SELECT
              m.fecha,
              e.edge_id,
              e.ruta,
              e.gasoducto,
              e.origen,
              e.destino,
              COALESCE(m.caudal_mm3_dia, 0) AS observed_flow_mm3_dia,
              m.capacidad_mm3_dia AS observed_capacity_mm3_dia,
              m.utilization_ratio AS observed_utilization_ratio
            FROM red_tramo_metricas_mensuales m
            INNER JOIN red_tramos_canonica e USING(edge_id)
            WHERE e.is_active
            ORDER BY m.fecha, e.gasoducto, e.ruta
            """
        ).fetchdf()
        node_exogenous_df = conn.execute(
            """
            SELECT
              fecha,
              node_id,
              nombre,
              role_proxy,
              observed_inflow_mm3_dia,
              observed_outflow_mm3_dia,
              observed_throughput_mm3_dia,
              supply_mm3_dia_proxy,
              withdrawal_mm3_dia_proxy,
              exogenous_net_mm3_dia_proxy,
              source
            FROM red_nodo_exogenos_mensuales
            ORDER BY fecha, nombre
            """
        ).fetchdf()
        diagnostics_df = conn.execute(
            """
            SELECT entity_type, entity_id, severity, issue_type, issue_detail
            FROM red_topologia_diagnostico
            ORDER BY
              CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
              issue_type,
              entity_id
            """
        ).fetchdf()
        solver_row = conn.execute(
            """
            SELECT
              fecha,
              total_supply_mm3_dia_proxy,
              total_withdrawal_mm3_dia_proxy,
              served_withdrawal_mm3_dia,
              unmet_withdrawal_mm3_dia,
              dispatched_supply_mm3_dia,
              curtailed_supply_mm3_dia,
              saturated_edge_count,
              total_edge_count
            FROM red_solver_resumen_mensual
            WHERE fecha = ?
            """,
            [latest_solver_month],
        ).fetchone()
    finally:
        conn.close()

    compressor_power_by_node: dict[str, float] = {}
    compressor_count_by_node: dict[str, int] = {}
    for row in compressors_df.to_dict("records"):
        node_id = str(row["node_id"])
        compressor_power_by_node[node_id] = compressor_power_by_node.get(node_id, 0.0) + float(
            row["potencia_hp"] or 0.0
        )
        compressor_count_by_node[node_id] = compressor_count_by_node.get(node_id, 0) + 1

    nodes = []
    for row in nodes_df.to_dict("records"):
        node_id = str(row["node_id"])
        nodes.append(
            {
                **row,
                "has_compressor": compressor_count_by_node.get(node_id, 0) > 0,
                "compressor_count": compressor_count_by_node.get(node_id, 0),
                "compressor_power_hp": compressor_power_by_node.get(node_id, 0.0),
            }
        )

    edges = edges_df.to_dict("records")
    edge_snapshots = [
        {
            **row,
            "fecha": str(row["fecha"]),
        }
        for row in edge_snapshots_df.to_dict("records")
    ]
    node_exogenous = [
        {
            **row,
            "fecha": str(row["fecha"]),
        }
        for row in node_exogenous_df.to_dict("records")
    ]
    diagnostics = diagnostics_df.to_dict("records")
    gasoductos = sorted({str(edge["gasoducto"]) for edge in edges})
    available_months = sorted({str(row["fecha"]) for row in edge_snapshots})
    components = _compute_component_count(nodes, edges)
    active_loop_count = sum(1 for loop in loops_df.to_dict("records") if str(loop["estado"]).lower() == "active")
    error_count = sum(1 for row in diagnostics if row["severity"] == "error")
    warning_count = sum(1 for row in diagnostics if row["severity"] == "warning")

    latest_case_summary = _load_latest_pandapipes_summary()
    solver_summary = None
    if solver_row is not None:
        solver_summary = {
            "fecha": str(solver_row[0]),
            "total_supply_mm3_dia_proxy": float(solver_row[1]),
            "total_withdrawal_mm3_dia_proxy": float(solver_row[2]),
            "served_withdrawal_mm3_dia": float(solver_row[3]),
            "unmet_withdrawal_mm3_dia": float(solver_row[4]),
            "dispatched_supply_mm3_dia": float(solver_row[5]),
            "curtailed_supply_mm3_dia": float(solver_row[6]),
            "saturated_edge_count": int(solver_row[7]),
            "total_edge_count": int(solver_row[8]),
        }

    overview = {
        "active_nodes": len(nodes),
        "active_edges": len(edges),
        "active_components": components,
        "compressors": int(len(compressors_df)),
        "active_loops": active_loop_count,
        "diagnostic_errors": error_count,
        "diagnostic_warnings": warning_count,
        "latest_observed_month": None if latest_observed_month is None else str(latest_observed_month),
        "latest_solver_month": None if latest_solver_month is None else str(latest_solver_month),
        "max_supply_proxy": max((float(row["supply_mm3_dia_proxy"] or 0.0) for row in node_exogenous), default=0.0),
        "max_withdrawal_proxy": max((float(row["withdrawal_mm3_dia_proxy"] or 0.0) for row in node_exogenous), default=0.0),
        "max_observed_throughput": max((float(row["observed_throughput_mm3_dia"] or 0.0) for row in node_exogenous_df.to_dict("records")), default=0.0),
    }

    return {
        "overview": overview,
        "nodes": nodes,
        "edges": edges,
        "compressors": compressors_df.to_dict("records"),
        "loops": loops_df.to_dict("records"),
        "available_months": available_months,
        "edge_snapshots": edge_snapshots,
        "node_exogenous": node_exogenous,
        "diagnostics": diagnostics[:12],
        "gasoductos": gasoductos,
        "solver_summary": solver_summary,
        "latest_case_summary": latest_case_summary,
    }


def _render_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GCIE Dashboard</title>
  <style>
    :root {{
      --bg: #f4f1ea;
      --panel: #fffaf1;
      --ink: #1e2430;
      --muted: #6a7280;
      --line: #d8cfbf;
      --accent: #1f6f5f;
      --accent-2: #b85c38;
      --stress-1: #53e0a1;
      --stress-2: #ffe06d;
      --stress-3: #ff9d4d;
      --stress-4: #ff5f87;
      --deep: #243041;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(31,111,95,0.14), transparent 28%),
        radial-gradient(circle at top right, rgba(184,92,56,0.12), transparent 24%),
        var(--bg);
    }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1, h2 {{ margin: 0 0 12px; font-family: "IBM Plex Serif", Georgia, serif; }}
    p {{ margin: 0; color: var(--muted); }}
    .hero {{
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
      margin-bottom: 24px;
    }}
    .panel {{
      background: color-mix(in srgb, var(--panel) 92%, white 8%);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 8px 24px rgba(25, 35, 45, 0.05);
    }}
    .grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      margin-bottom: 24px;
    }}
    .value {{ font-size: 2rem; font-weight: 700; color: var(--accent); }}
    .small {{ font-size: 0.92rem; color: var(--muted); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.94rem; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .two {{
      display: grid;
      gap: 16px;
      grid-template-columns: 1.1fr 0.9fr;
      margin-bottom: 24px;
    }}
    .stack {{
      display: grid;
      gap: 16px;
      margin-bottom: 24px;
    }}
    .chart-shell {{
      display: grid;
      gap: 14px;
      grid-template-columns: 1.5fr 0.8fr;
      align-items: start;
    }}
    .three {{
      display: grid;
      gap: 16px;
      grid-template-columns: 1fr 1fr;
      margin-bottom: 24px;
    }}
    .toolbar {{
      display: flex;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }}
    select, input[type="checkbox"] {{
      accent-color: var(--accent);
    }}
    select {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      background: white;
      color: var(--ink);
      font: inherit;
    }}
    button {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 12px;
      background: white;
      color: var(--ink);
      font: inherit;
      cursor: pointer;
    }}
    .check {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .viz {{
      width: 100%;
      min-height: 320px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background:
        linear-gradient(180deg, rgba(31,111,95,0.04), transparent 42%),
        white;
      padding: 12px;
    }}
    .viz-tall {{
      min-height: 720px;
      background:
        radial-gradient(circle at 12% 12%, rgba(31,111,95,0.08), transparent 24%),
        linear-gradient(180deg, rgba(36,48,65,0.02), transparent 30%),
        white;
    }}
    .legend {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 999px;
      margin-right: 6px;
    }}
    .legend .actual::before {{ background: var(--accent); }}
    .legend .pred::before {{ background: var(--accent-2); }}
    .legend .u0::before {{ background: var(--stress-1); }}
    .legend .u1::before {{ background: var(--stress-2); }}
    .legend .u2::before {{ background: var(--stress-3); }}
    .legend .u3::before {{ background: var(--stress-4); }}
    .legend .comp::before {{ background: var(--deep); border-radius: 0; transform: rotate(45deg); }}
    .legend .loop::before {{ background: transparent; border: 2px dashed var(--accent-2); border-radius: 0; width: 12px; height: 8px; }}
    .explain-grid {{
      display: grid;
      gap: 10px;
      grid-template-columns: 1fr 1fr;
      margin-top: 12px;
    }}
    .explain-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: rgba(255,255,255,0.85);
    }}
    .mono {{ font-family: "IBM Plex Mono", "SFMono-Regular", monospace; }}
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(31,111,95,0.1);
      color: var(--accent);
      font-size: 0.84rem;
      font-weight: 700;
    }}
    .network-shell {{
      display: grid;
      gap: 16px;
      grid-template-columns: 1.55fr 0.85fr;
      align-items: start;
    }}
    .route-list {{
      display: grid;
      gap: 8px;
    }}
    .status-grid {{
      grid-template-columns: repeat(4, minmax(0, 1fr));
      margin-bottom: 0;
    }}
    .status-card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: rgba(255,255,255,0.78);
      display: grid;
      gap: 10px;
      min-height: 220px;
    }}
    .status-kicker {{
      letter-spacing: 0.04em;
      text-transform: uppercase;
      font-size: 0.75rem;
      color: var(--accent);
      font-weight: 700;
    }}
    .country-fill {{
      fill: rgba(36,48,65,0.05);
    }}
    .country-outline {{
      fill: none;
      stroke: rgba(36,48,65,0.28);
      stroke-width: 1.2;
      stroke-linejoin: round;
    }}
    .map-ocean {{
      fill: rgba(31,111,95,0.04);
    }}
    .map-frame {{
      fill: none;
      stroke: rgba(36,48,65,0.08);
      stroke-width: 1.4;
    }}
    .route-item {{
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(255,255,255,0.82);
      cursor: pointer;
    }}
    .route-item.is-active {{
      border-color: var(--accent);
      box-shadow: inset 0 0 0 1px rgba(31,111,95,0.2);
      background: rgba(31,111,95,0.06);
    }}
    .route-title {{
      font-weight: 700;
      color: var(--ink);
      margin-bottom: 4px;
    }}
    .route-meta {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.86rem;
    }}
    .diag-list {{
      display: grid;
      gap: 8px;
    }}
    .diag-item {{
      border-left: 3px solid var(--line);
      padding-left: 10px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    .diag-item.warning {{ border-left-color: var(--stress-3); }}
    .diag-item.error {{ border-left-color: var(--stress-4); }}
    @media (max-width: 980px) {{
      .hero, .grid, .two, .three, .chart-shell, .explain-grid, .network-shell {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <section class="panel">
        <span class="badge">GCIE Live Snapshot</span>
        <h1>Data, Model, Network, and Current State</h1>
        <p>Single-page view of the current datalake snapshot, the active SP1 baseline, the canonical transport network, and the latest solver status.</p>
      </section>
      <section class="panel">
        <h2>SP1 Metric</h2>
        <div class="value" id="metric-value"></div>
        <p class="small" id="metric-label"></p>
      </section>
    </div>

    <section class="grid" id="top-cards"></section>

    <section class="panel stack">
      <div class="toolbar">
        <div>
          <h2>Project State</h2>
          <p id="project-summary"></p>
        </div>
      </div>
      <section class="grid status-grid" id="project-status-grid"></section>
    </section>

    <section class="panel stack">
      <div class="toolbar">
        <div style="flex:1 1 320px">
          <h2>Canonical Network</h2>
          <p>Observed ENARGAS flows plus canonical overrides, compresoras, loops, operational stress, and optional source/sink proxy bubbles by month.</p>
        </div>
        <span class="badge" id="network-month-badge"></span>
        <button type="button" id="network-play">Play</button>
        <select id="network-month-select"></select>
        <select id="network-gasoducto"></select>
        <label class="check"><input type="checkbox" id="network-critical"> Stress only</label>
        <label class="check"><input type="checkbox" id="network-show-sources" checked> Source Proxy</label>
        <label class="check"><input type="checkbox" id="network-show-sinks" checked> Sink Proxy</label>
        <label class="check"><input type="checkbox" id="network-show-observed" checked> Observed Activity</label>
      </div>
      <div class="network-shell">
        <div>
          <svg class="viz viz-tall" id="network-map" viewBox="0 0 920 760" preserveAspectRatio="xMidYMid meet"></svg>
          <div class="legend">
            <span class="u0">Util < 50%</span>
            <span class="u1">50-80%</span>
            <span class="u2">80-100%</span>
            <span class="u3">>= 100%</span>
            <span class="comp">Compresora</span>
            <span class="loop">Loop activo</span>
            <span class="actual">Source Proxy</span>
            <span class="pred">Sink Proxy</span>
            <span class="u0">Observed Activity</span>
          </div>
        </div>
        <div class="stack" style="margin-bottom:0">
          <div class="explain-card">
            <div class="small">Selection Detail</div>
            <div class="route-title" id="network-route-title"></div>
            <div class="route-meta" id="network-route-meta"></div>
            <div class="explain-grid">
              <div class="explain-card"><div class="small" id="network-metric-1-label">Observed Util</div><div class="value" id="network-util"></div></div>
              <div class="explain-card"><div class="small" id="network-metric-2-label">Observed Flow</div><div class="value" id="network-flow"></div></div>
              <div class="explain-card"><div class="small">Eff Capacity</div><div class="value" id="network-capacity"></div></div>
              <div class="explain-card"><div class="small" id="network-metric-4-label">Loops / Assets</div><div class="value" id="network-loops"></div></div>
            </div>
            <div class="explain-card" style="margin-top:10px">
              <div class="small">Topology / Provenance</div>
              <div class="mono" id="network-status" style="margin-top:8px"></div>
            </div>
          </div>
          <div class="explain-card">
            <div class="small">Most Stressed Routes</div>
            <div class="route-list" id="network-route-list" style="margin-top:10px"></div>
          </div>
          <div class="explain-card">
            <div class="small">Route Timeline</div>
            <p class="small">Flow and utilization by month for the selected route.</p>
            <svg class="viz" id="network-history-chart" viewBox="0 0 520 220" preserveAspectRatio="none"></svg>
          </div>
          <div class="explain-card">
            <div class="small">Solver Snapshot</div>
            <div class="route-meta" id="solver-summary" style="margin-top:10px"></div>
          </div>
          <div class="explain-card">
            <div class="small">Open Diagnostics</div>
            <div class="diag-list" id="network-diagnostics" style="margin-top:10px"></div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel stack">
      <div class="toolbar">
        <div>
          <h2>SP1 Visual Drilldown</h2>
          <p>Real vs model by segment, with the variables the baseline is leaning on.</p>
        </div>
        <select id="segment-select"></select>
      </div>
      <div class="chart-shell">
        <div>
          <svg class="viz" id="forecast-chart" viewBox="0 0 720 320" preserveAspectRatio="none"></svg>
          <div class="legend">
            <span class="actual">Actual</span>
            <span class="pred">Predicted</span>
          </div>
        </div>
        <div class="panel" style="padding:14px">
          <h2 id="explain-title">Prediction Story</h2>
          <p class="small" id="explain-subtitle"></p>
          <div class="explain-grid">
            <div class="explain-card"><div class="small">Actual</div><div class="value" id="story-actual"></div></div>
            <div class="explain-card"><div class="small">Predicted</div><div class="value" id="story-pred"></div></div>
            <div class="explain-card"><div class="small">HDD</div><div class="value" id="story-hdd"></div></div>
            <div class="explain-card"><div class="small">Temp Media</div><div class="value" id="story-temp"></div></div>
          </div>
          <div class="explain-card" style="margin-top:10px">
            <div class="small">Lag Inputs</div>
            <div class="mono" id="story-lags" style="margin-top:8px"></div>
          </div>
        </div>
      </div>
    </section>

    <section class="three">
      <div class="panel">
        <h2>Recent History</h2>
        <p class="small">Last 24 observations for the selected segment. Validation months include the model overlay.</p>
        <svg class="viz" id="history-chart" viewBox="0 0 720 320" preserveAspectRatio="none"></svg>
      </div>
      <div class="panel">
        <h2>Driver View</h2>
        <p class="small">Observed volume vs HDD for the selected segment. Validation points are highlighted.</p>
        <svg class="viz" id="driver-chart" viewBox="0 0 720 320" preserveAspectRatio="none"></svg>
      </div>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Datalake Tables</h2>
        <table>
          <thead><tr><th>Table</th><th>Rows</th><th>Range</th></tr></thead>
          <tbody id="datalake-table"></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>SP1 By Segment</h2>
        <table>
          <thead><tr><th>Segment</th><th>Mean APE</th><th>N</th></tr></thead>
          <tbody id="segment-table"></tbody>
        </table>
      </div>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Latest Predictions</h2>
        <table>
          <thead><tr><th>Date</th><th>Segment</th><th>Actual</th><th>Predicted</th></tr></thead>
          <tbody id="prediction-table"></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Experiment Log</h2>
        <table>
          <thead><tr><th>Timestamp</th><th>Hypothesis</th><th>Metric</th><th>Delta</th><th>Kept</th></tr></thead>
          <tbody id="results-table"></tbody>
        </table>
      </div>
    </section>
  </div>
  <script>
    const data = {payload_json};

    const fmtInt = value => new Intl.NumberFormat('en-US').format(value);
    const fmtPct = value => (value * 100).toFixed(2) + '%';
    const fmtShort = value => new Intl.NumberFormat('en-US', {{ notation: 'compact', maximumFractionDigits: 1 }}).format(value);
    const fmtNum = value => value == null || Number.isNaN(value) ? '-' : new Intl.NumberFormat('en-US', {{ maximumFractionDigits: 2 }}).format(value);
    const fmtMm = value => value == null || Number.isNaN(value) ? '-' : fmtNum(value) + ' MMm3/d';

    document.getElementById('metric-value').textContent = fmtPct(data.forecast.metric_value);
    document.getElementById('metric-label').textContent =
      `${{data.forecast.metric_name}} | cadence: ${{data.forecast.model_artifacts.cadence}} | hypothesis: ${{data.forecast.hypothesis}}`;

    const cards = [
      ['Train Rows', data.forecast.train_rows],
      ['Validation Rows', data.forecast.validation_rows],
      ['Canonical Nodes', data.network.overview.active_nodes],
      ['Canonical Edges', data.network.overview.active_edges],
      ['Active Compressors', data.network.overview.compressors],
      ['Open Diagnostics', data.network.overview.diagnostic_errors + data.network.overview.diagnostic_warnings],
    ];
    const cardsNode = document.getElementById('top-cards');
    cards.forEach(([label, value]) => {{
      const el = document.createElement('section');
      el.className = 'panel';
      el.innerHTML = `<div class="small">${{label}}</div><div class="value">${{fmtInt(value)}}</div>`;
      cardsNode.appendChild(el);
    }});

    const projectSummaryNode = document.getElementById('project-summary');
    projectSummaryNode.textContent = `${{data.project.summary}} ${{data.project.roadmap ? 'Next: ' + data.project.roadmap : ''}}`;
    const projectGridNode = document.getElementById('project-status-grid');
    data.project.cards.forEach(item => {{
      const el = document.createElement('article');
      el.className = 'status-card';
      el.innerHTML = `
        <div class="status-kicker">${{item.repo}}</div>
        <div>
          <div class="route-title">${{item.title}}</div>
          <p class="small">${{item.role}}</p>
        </div>
        <div>
          <div class="small">Current</div>
          <div>${{item.current || '-'}}</div>
        </div>
        <div>
          <div class="small">Signal</div>
          <div>${{item.score || item.next_step || '-'}}</div>
        </div>
      `;
      projectGridNode.appendChild(el);
    }});

    const datalakeNode = document.getElementById('datalake-table');
    Object.entries(data.datalake).forEach(([table, summary]) => {{
      const row = document.createElement('tr');
      row.innerHTML = `<td>${{table}}</td><td>${{fmtInt(summary.rows)}}</td><td>${{summary.min_fecha || '-'}} -> ${{summary.max_fecha || '-'}}`;
      datalakeNode.appendChild(row);
    }});

    const segmentNode = document.getElementById('segment-table');
    data.forecast.by_segment.forEach(item => {{
      const row = document.createElement('tr');
      row.innerHTML = `<td>${{item.segmento}}</td><td>${{fmtPct(item.mean_ape)}}</td><td>${{item.n}}</td>`;
      segmentNode.appendChild(row);
    }});

    const predictionNode = document.getElementById('prediction-table');
    data.forecast.recent_predictions.forEach(item => {{
      const row = document.createElement('tr');
      row.innerHTML = `<td>${{item.fecha}}</td><td>${{item.segmento}}</td><td>${{fmtInt(item.actual_volume)}}</td><td>${{fmtInt(item.predicted_volume)}}</td>`;
      predictionNode.appendChild(row);
    }});

    const resultsNode = document.getElementById('results-table');
    if (data.results.length === 0) {{
      const row = document.createElement('tr');
      row.innerHTML = '<td colspan="5">No experiments logged yet.</td>';
      resultsNode.appendChild(row);
    }} else {{
      data.results.slice().reverse().forEach(item => {{
        const row = document.createElement('tr');
        row.innerHTML = `<td>${{item.timestamp}}</td><td>${{item.hypothesis}}</td><td>${{item.metric_value}}</td><td>${{item.delta_vs_prev}}</td><td>${{item.kept}}</td>`;
        resultsNode.appendChild(row);
      }});
    }}

    const segmentSelect = document.getElementById('segment-select');
    const chart = document.getElementById('forecast-chart');
    const historyChart = document.getElementById('history-chart');
    const driverChart = document.getElementById('driver-chart');

    data.forecast.chart_segments.forEach((item, idx) => {{
      const option = document.createElement('option');
      option.value = item.segmento;
      option.textContent = item.segmento;
      if (idx === 0) option.selected = true;
      segmentSelect.appendChild(option);
    }});

    const linePath = (points, xMap, yMap) => points.map((p, idx) =>
      `${{idx === 0 ? 'M' : 'L'}} ${{xMap(idx)}} ${{yMap(p)}}`
    ).join(' ');

    function renderStory(point) {{
      document.getElementById('explain-title').textContent = `${{point.segmento}} | ${{point.fecha}}`;
      document.getElementById('explain-subtitle').textContent = `APE: ${{fmtPct(point.ape)}}`;
      document.getElementById('story-actual').textContent = fmtShort(point.actual_volume);
      document.getElementById('story-pred').textContent = fmtShort(point.predicted_volume);
      document.getElementById('story-hdd').textContent = point.hdd == null ? '-' : point.hdd;
      document.getElementById('story-temp').textContent = point.temp_media == null ? '-' : point.temp_media + ' C';
      document.getElementById('story-lags').textContent = Object.entries(point.lag_values)
        .map(([key, value]) => `${{key}}: ${{fmtShort(value)}}`)
        .join(' | ');
    }}

    function renderHistory(segment) {{
      const segmentData = data.forecast.history_segments.find(item => item.segmento === segment);
      if (!segmentData) return;
      const points = segmentData.points;
      const maxY = Math.max(...points.map(p => p.actual_volume), ...points.map(p => p.predicted_volume || 0)) * 1.1;
      const left = 52, top = 20, width = 630, height = 240, bottom = top + height;
      const xMap = idx => left + (points.length === 1 ? width / 2 : (idx * width) / (points.length - 1));
      const yMap = value => bottom - (value / maxY) * height;
      const grid = [0, 0.25, 0.5, 0.75, 1].map(tick => {{
        const y = top + height * tick;
        const value = maxY * (1 - tick);
        return `
          <line x1="${{left}}" y1="${{y}}" x2="${{left + width}}" y2="${{y}}" stroke="rgba(106,114,128,0.16)" />
          <text x="8" y="${{y + 4}}" fill="#6a7280" font-size="11">${{fmtShort(value)}}</text>
        `;
      }}).join('');
      const actualPath = linePath(points.map(p => p.actual_volume), xMap, yMap);
      const predPoints = points.filter(p => p.predicted_volume != null);
      const predPath = predPoints.length
        ? linePath(predPoints.map(p => p.predicted_volume), idx => xMap(points.findIndex(src => src.fecha === predPoints[idx].fecha)), yMap)
        : '';
      const xLabels = points.filter((_, idx) => idx % Math.ceil(points.length / 6) === 0 || idx === points.length - 1)
        .map(point => {{
          const realIdx = points.findIndex(src => src.fecha === point.fecha);
          return `<text x="${{xMap(realIdx)}}" y="${{bottom + 24}}" text-anchor="middle" fill="#6a7280" font-size="11">${{point.fecha.slice(0, 7)}}</text>`;
        }}).join('');
      const dots = points.map((point, idx) => {{
        const validationColor = point.is_validation ? "var(--accent-2)" : "var(--accent)";
        const predDot = point.predicted_volume == null ? '' : `<circle cx="${{xMap(idx)}}" cy="${{yMap(point.predicted_volume)}}" r="4" fill="var(--accent-2)" />`;
        return `<circle cx="${{xMap(idx)}}" cy="${{yMap(point.actual_volume)}}" r="4" fill="${{validationColor}}" />${{predDot}}`;
      }}).join('');
      historyChart.innerHTML = `
        ${{grid}}
        <path d="${{actualPath}}" fill="none" stroke="var(--accent)" stroke-width="3" stroke-linecap="round" />
        ${{predPath ? `<path d="${{predPath}}" fill="none" stroke="var(--accent-2)" stroke-width="3" stroke-dasharray="6 6" stroke-linecap="round" />` : ''}}
        ${{dots}}
        ${{xLabels}}
      `;
    }}

    function renderDrivers(segment) {{
      const segmentData = data.forecast.driver_segments.find(item => item.segmento === segment);
      if (!segmentData) return;
      const points = segmentData.points;
      const maxX = Math.max(...points.map(p => p.hdd), 1);
      const maxY = Math.max(...points.map(p => p.actual_volume)) * 1.1;
      const left = 52, top = 20, width = 630, height = 240, bottom = top + height;
      const xMap = value => left + (value / maxX) * width;
      const yMap = value => bottom - (value / maxY) * height;
      const grid = [0, 0.25, 0.5, 0.75, 1].map(tick => {{
        const y = top + height * tick;
        const value = maxY * (1 - tick);
        return `
          <line x1="${{left}}" y1="${{y}}" x2="${{left + width}}" y2="${{y}}" stroke="rgba(106,114,128,0.16)" />
          <text x="8" y="${{y + 4}}" fill="#6a7280" font-size="11">${{fmtShort(value)}}</text>
        `;
      }}).join('');
      const xTicks = [0, 0.25, 0.5, 0.75, 1].map(tick => {{
        const value = maxX * tick;
        const x = left + width * tick;
        return `<text x="${{x}}" y="${{bottom + 24}}" text-anchor="middle" fill="#6a7280" font-size="11">${{value.toFixed(1)}}</text>`;
      }}).join('');
      const dots = points.map(point => `
        <circle cx="${{xMap(point.hdd)}}" cy="${{yMap(point.actual_volume)}}" r="5" fill="${{point.is_validation ? 'var(--accent-2)' : 'var(--accent)'}}" fill-opacity="0.82" />
      `).join('');
      driverChart.innerHTML = `
        ${{grid}}
        <text x="8" y="16" fill="#6a7280" font-size="11">Volume</text>
        <text x="${{left + width - 20}}" y="${{bottom + 24}}" fill="#6a7280" font-size="11">HDD</text>
        ${{dots}}
        ${{xTicks}}
      `;
    }}

    function renderChart(segment) {{
      const segmentData = data.forecast.chart_segments.find(item => item.segmento === segment);
      if (!segmentData) return;
      const points = segmentData.points;
      const maxY = Math.max(...points.flatMap(p => [p.actual_volume, p.predicted_volume])) * 1.1;
      const left = 52, top = 20, width = 630, height = 240, bottom = top + height;
      const xMap = idx => left + (points.length === 1 ? width / 2 : (idx * width) / (points.length - 1));
      const yMap = value => bottom - (value / maxY) * height;

      const grid = [0, 0.25, 0.5, 0.75, 1].map(tick => {{
        const y = top + height * tick;
        const value = maxY * (1 - tick);
        return `
          <line x1="${{left}}" y1="${{y}}" x2="${{left + width}}" y2="${{y}}" stroke="rgba(106,114,128,0.16)" />
          <text x="8" y="${{y + 4}}" fill="#6a7280" font-size="11">${{fmtShort(value)}}</text>
        `;
      }}).join('');

      const xLabels = points.map((point, idx) => `
        <text x="${{xMap(idx)}}" y="${{bottom + 24}}" text-anchor="middle" fill="#6a7280" font-size="11">${{point.fecha.slice(0, 7)}}</text>
      `).join('');

      const actualPath = linePath(points.map(p => p.actual_volume), xMap, yMap);
      const predPath = linePath(points.map(p => p.predicted_volume), xMap, yMap);

      const dots = points.map((point, idx) => `
        <circle cx="${{xMap(idx)}}" cy="${{yMap(point.actual_volume)}}" r="5" fill="var(--accent)" />
        <circle cx="${{xMap(idx)}}" cy="${{yMap(point.predicted_volume)}}" r="5" fill="var(--accent-2)" />
      `).join('');

      chart.innerHTML = `
        <rect x="0" y="0" width="720" height="320" rx="12" fill="transparent" />
        ${{grid}}
        <path d="${{actualPath}}" fill="none" stroke="var(--accent)" stroke-width="3" stroke-linecap="round" />
        <path d="${{predPath}}" fill="none" stroke="var(--accent-2)" stroke-width="3" stroke-linecap="round" />
        ${{dots}}
        ${{xLabels}}
      `;

      renderStory(points[points.length - 1]);
      renderHistory(segment);
      renderDrivers(segment);
    }}

    const networkMap = document.getElementById('network-map');
    const networkGasoducto = document.getElementById('network-gasoducto');
    const networkCritical = document.getElementById('network-critical');
    const networkShowSources = document.getElementById('network-show-sources');
    const networkShowSinks = document.getElementById('network-show-sinks');
    const networkShowObserved = document.getElementById('network-show-observed');
    const networkMonthSelect = document.getElementById('network-month-select');
    const networkPlayButton = document.getElementById('network-play');
    const networkMonthBadge = document.getElementById('network-month-badge');
    const networkRouteList = document.getElementById('network-route-list');
    const networkHistoryChart = document.getElementById('network-history-chart');
    let selectedEdgeId = null;
    let selectedNodeId = null;
    let isPlayingNetwork = false;
    let networkTimerId = null;

    const edgeSnapshotMap = new Map();
    data.network.edge_snapshots.forEach(item => {{
      const key = `${{item.fecha}}|${{item.edge_id}}`;
      edgeSnapshotMap.set(key, item);
    }});
    const routeHistoryMap = new Map();
    data.network.edge_snapshots.forEach(item => {{
      const history = routeHistoryMap.get(item.edge_id) || [];
      history.push(item);
      routeHistoryMap.set(item.edge_id, history);
    }});
    const nodeExogenousMap = new Map();
    data.network.node_exogenous.forEach(item => {{
      const bucket = nodeExogenousMap.get(item.fecha) || [];
      bucket.push(item);
      nodeExogenousMap.set(item.fecha, bucket);
    }});
    function nodeExogenousLookup(monthKey) {{
      return new Map((nodeExogenousMap.get(monthKey) || []).map(item => [item.node_id, item]));
    }}

    function utilizationColor(utilization) {{
      if (utilization == null) return '#4b648b';
      if (utilization >= 1) return 'var(--stress-4)';
      if (utilization >= 0.8) return 'var(--stress-3)';
      if (utilization >= 0.5) return 'var(--stress-2)';
      return 'var(--stress-1)';
    }}

    function monthStartKey(value) {{
      if (!value) return '';
      return value.slice(0, 7) + '-01 00:00:00';
    }}

    function projectNetwork(nodes, edges, outlinePolygons) {{
      const nodePoints = nodes.map(node => [node.x_mercator, node.y_mercator]).filter(point => point[0] != null && point[1] != null);
      const outlinePoints = outlinePolygons.flatMap(polygon => polygon.map(point => [point.x, point.y]));
      const allPoints = [...nodePoints, ...outlinePoints];
      const xs = allPoints.map(point => point[0]);
      const ys = allPoints.map(point => point[1]);
      const bounds = {{
        minX: Math.min(...xs),
        maxX: Math.max(...xs),
        minY: Math.min(...ys),
        maxY: Math.max(...ys),
      }};
      const pad = 64;
      const width = 920;
      const height = 760;
      const scaleX = (width - pad * 2) / ((bounds.maxX - bounds.minX) || 1);
      const scaleY = (height - pad * 2) / ((bounds.maxY - bounds.minY) || 1);
      const scale = Math.min(scaleX, scaleY);
      const usedWidth = (bounds.maxX - bounds.minX) * scale;
      const usedHeight = (bounds.maxY - bounds.minY) * scale;
      const offsetX = (width - usedWidth) / 2;
      const offsetY = (height - usedHeight) / 2;
      const project = (x, y) => ({{
        x: offsetX + (x - bounds.minX) * scale,
        y: height - (offsetY + (y - bounds.minY) * scale),
      }});
      const polygonPath = polygon => polygon.map((point, idx) => {{
        const projected = project(point.x, point.y);
        return `${{idx === 0 ? 'M' : 'L'}} ${{projected.x}} ${{projected.y}}`;
      }}).join(' ') + ' Z';
      const nodeMap = new Map(nodes.map(node => [node.node_id, {{ ...node, ...project(node.x_mercator, node.y_mercator) }}]));
      const projectedEdges = edges.map(edge => ({{
        ...edge,
        start: nodeMap.get(edge.source_node_id),
        end: nodeMap.get(edge.target_node_id),
      }})).filter(edge => edge.start && edge.end);
      const countryPath = outlinePolygons.map(polygonPath).join(' ');
      return {{ nodes: Array.from(nodeMap.values()), edges: projectedEdges, countryPath }};
    }}

    const projectedNetwork = projectNetwork(data.network.nodes, data.network.edges, data.outline.polygons);
    data.network.available_months.forEach((month, idx) => {{
      const option = document.createElement('option');
      option.value = month;
      option.textContent = month.slice(0, 7);
      if (month === data.network.overview.latest_observed_month) option.selected = true;
      networkMonthSelect.appendChild(option);
    }});
    data.network.gasoductos.forEach((gasoducto, idx) => {{
      const option = document.createElement('option');
      option.value = gasoducto;
      option.textContent = gasoducto;
      if (idx === 0) option.selected = gasoducto === 'Centro Oeste';
      networkGasoducto.appendChild(option);
    }});
    if (!data.network.gasoductos.includes('Centro Oeste')) {{
      networkGasoducto.insertAdjacentHTML('afterbegin', '<option value="Todos" selected>Todos</option>');
    }} else {{
      networkGasoducto.insertAdjacentHTML('afterbegin', '<option value="Todos">Todos</option>');
    }}
    networkMonthBadge.textContent = `Observed month: ${{(networkMonthSelect.value || '-').slice(0, 7)}}`;

    function renderDiagnostics() {{
      const diagnosticsNode = document.getElementById('network-diagnostics');
      diagnosticsNode.innerHTML = '';
      data.network.diagnostics.forEach(item => {{
        const row = document.createElement('div');
        row.className = `diag-item ${{item.severity}}`;
        row.innerHTML = `<div><strong>${{item.issue_type}}</strong> | ${{item.entity_id}}</div><div>${{item.issue_detail}}</div>`;
        diagnosticsNode.appendChild(row);
      }});
    }}

    function renderSolverSummary() {{
      const node = document.getElementById('solver-summary');
      const parts = [];
      if (data.network.solver_summary) {{
        parts.push(`F23 month: ${{data.network.solver_summary.fecha.slice(0, 7)}}`);
        parts.push(`unmet: ${{fmtMm(data.network.solver_summary.unmet_withdrawal_mm3_dia)}}`);
        parts.push(`saturated: ${{data.network.solver_summary.saturated_edge_count}}/${{data.network.solver_summary.total_edge_count}}`);
      }}
      if (data.network.latest_case_summary) {{
        parts.push(`pandapipes: ${{data.network.latest_case_summary.converged ? 'converged' : 'not converged'}}`);
        parts.push(`case month: ${{data.network.latest_case_summary.month}}`);
      }}
      node.innerHTML = parts.map(item => `<span>${{item}}</span>`).join('');
    }}

    function renderNetworkHistory(edgeId) {{
      const history = (routeHistoryMap.get(edgeId) || []).slice().sort((a, b) => a.fecha.localeCompare(b.fecha));
      if (!history.length) {{
        networkHistoryChart.innerHTML = '';
        return;
      }}
      const points = history.slice(-36);
      const left = 42, top = 16, width = 448, height = 150, bottom = top + height;
      const maxFlow = Math.max(...points.map(p => p.observed_flow_mm3_dia || 0), 1);
      const xMap = idx => left + (points.length === 1 ? width / 2 : (idx * width) / (points.length - 1));
      const yMapFlow = value => bottom - ((value || 0) / maxFlow) * height;
      const yMapUtil = value => bottom - Math.min(Math.max(value || 0, 0), 1.2) / 1.2 * height;
      const flowPath = linePath(points.map(p => p.observed_flow_mm3_dia || 0), xMap, yMapFlow);
      const utilPath = linePath(points.map(p => (p.observed_utilization_ratio || 0) * maxFlow / 1.2), xMap, yMapFlow);
      const xLabels = points.filter((_, idx) => idx % Math.ceil(points.length / 5) === 0 || idx === points.length - 1)
        .map(point => {{
          const realIdx = points.findIndex(src => src.fecha === point.fecha);
          return `<text x="${{xMap(realIdx)}}" y="${{bottom + 22}}" text-anchor="middle" fill="#6a7280" font-size="10">${{point.fecha.slice(0, 7)}}</text>`;
        }}).join('');
      const dots = points.map((point, idx) => {{
        const isCurrent = point.fecha === networkMonthSelect.value;
        return `
          <circle cx="${{xMap(idx)}}" cy="${{yMapFlow(point.observed_flow_mm3_dia)}}" r="${{isCurrent ? 4.8 : 3.2}}" fill="var(--accent)" />
          <circle cx="${{xMap(idx)}}" cy="${{yMapUtil(point.observed_utilization_ratio)}}" r="${{isCurrent ? 4.2 : 2.8}}" fill="var(--accent-2)" />
        `;
      }}).join('');
      networkHistoryChart.innerHTML = `
        <line x1="${{left}}" y1="${{bottom}}" x2="${{left + width}}" y2="${{bottom}}" stroke="rgba(106,114,128,0.18)" />
        <line x1="${{left}}" y1="${{top}}" x2="${{left}}" y2="${{bottom}}" stroke="rgba(106,114,128,0.18)" />
        <path d="${{flowPath}}" fill="none" stroke="var(--accent)" stroke-width="2.6" stroke-linecap="round" />
        <path d="${{utilPath}}" fill="none" stroke="var(--accent-2)" stroke-width="2.2" stroke-dasharray="6 5" stroke-linecap="round" />
        ${{dots}}
        ${{xLabels}}
        <text x="44" y="14" fill="#6a7280" font-size="10">Flow</text>
        <text x="92" y="14" fill="#6a7280" font-size="10">Utilization</text>
      `;
    }}

    function resetSelectionPanel() {{
      document.getElementById('network-route-title').textContent = 'No visible selection';
      document.getElementById('network-route-meta').textContent = '';
      document.getElementById('network-metric-1-label').textContent = 'Observed Util';
      document.getElementById('network-metric-2-label').textContent = 'Observed Flow';
      document.getElementById('network-metric-4-label').textContent = 'Loops / Assets';
      document.getElementById('network-util').textContent = '-';
      document.getElementById('network-flow').textContent = '-';
      document.getElementById('network-capacity').textContent = '-';
      document.getElementById('network-loops').textContent = '-';
      document.getElementById('network-status').textContent = '-';
      networkHistoryChart.innerHTML = '';
    }}

    function selectNode(nodeId, visibleNodes, exogenousLookup, monthKey) {{
      selectedNodeId = nodeId;
      selectedEdgeId = null;
      const node = visibleNodes.find(item => item.node_id === nodeId) || null;
      const exogenous = node ? exogenousLookup.get(node.node_id) : null;
      if (!node) {{
        resetSelectionPanel();
        return;
      }}
      document.getElementById('network-route-title').textContent = node.nombre;
      document.getElementById('network-route-meta').innerHTML = `
        <span>Node</span>
        <span>month: ${{monthKey.slice(0, 7)}}</span>
      `;
      document.getElementById('network-metric-1-label').textContent = 'Source Proxy';
      document.getElementById('network-metric-2-label').textContent = 'Sink Proxy';
      document.getElementById('network-metric-4-label').textContent = 'Net Proxy';
      document.getElementById('network-util').textContent = fmtMm(exogenous?.supply_mm3_dia_proxy);
      document.getElementById('network-flow').textContent = fmtMm(exogenous?.withdrawal_mm3_dia_proxy);
      document.getElementById('network-capacity').textContent =
        exogenous?.observed_throughput_mm3_dia != null && exogenous?.observed_throughput_mm3_dia > 0
          ? 'throughput ' + fmtMm(exogenous?.observed_throughput_mm3_dia)
          : (exogenous?.role_proxy || '-');
      document.getElementById('network-loops').textContent = fmtMm(exogenous?.exogenous_net_mm3_dia_proxy);
      document.getElementById('network-status').textContent =
        `source_confidence=${{node.source_confidence}} | topology_status=${{node.topology_status}} | source=${{exogenous?.source || 'n/a'}}`;
      networkHistoryChart.innerHTML = '';
      [...document.querySelectorAll('.route-item')].forEach(item => item.classList.remove('is-active'));
    }}

    function selectEdge(edgeId, visibleEdges) {{
      selectedEdgeId = edgeId;
      selectedNodeId = null;
      const edge = visibleEdges.find(item => item.edge_id === edgeId) || visibleEdges[0] || null;
      if (!edge) {{
        resetSelectionPanel();
        return;
      }}
      document.getElementById('network-metric-1-label').textContent = 'Observed Util';
      document.getElementById('network-metric-2-label').textContent = 'Observed Flow';
      document.getElementById('network-metric-4-label').textContent = 'Loops / Assets';
      document.getElementById('network-route-title').textContent = edge.ruta;
      document.getElementById('network-route-meta').innerHTML = `
        <span>${{edge.gasoducto}}</span>
        <span>${{edge.origen}} -> ${{edge.destino}}</span>
      `;
      document.getElementById('network-util').textContent = edge.observed_utilization_ratio == null ? '-' : fmtPct(edge.observed_utilization_ratio);
      document.getElementById('network-flow').textContent = fmtMm(edge.observed_flow_mm3_dia);
      document.getElementById('network-capacity').textContent = fmtMm(edge.effective_capacity_mm3_dia);
      document.getElementById('network-loops').textContent = `${{edge.active_loop_count || 0}} active loops`;
      document.getElementById('network-status').textContent =
        `source_confidence=${{edge.source_confidence}} | topology_status=${{edge.topology_status}}`;
      renderNetworkHistory(edge.edge_id);
      [...document.querySelectorAll('.route-item')].forEach(item => {{
        item.classList.toggle('is-active', item.dataset.edgeId === edge.edge_id);
      }});
    }}

    function renderNetwork() {{
      const selectedMonth = networkMonthSelect.value || data.network.overview.latest_observed_month;
      const selectedMonthStart = monthStartKey(selectedMonth);
      const gasoducto = networkGasoducto.value;
      const criticalOnly = networkCritical.checked;
      networkMonthBadge.textContent = `Observed month: ${{(selectedMonth || '-').slice(0, 7)}}`;
      const visibleEdges = projectedNetwork.edges.map(edge => {{
        const snapshot = edgeSnapshotMap.get(`${{selectedMonth}}|${{edge.edge_id}}`) || {{}};
        return {{
          ...edge,
          observed_flow_mm3_dia: snapshot.observed_flow_mm3_dia ?? 0,
          observed_capacity_mm3_dia: snapshot.observed_capacity_mm3_dia ?? null,
          observed_utilization_ratio: snapshot.observed_utilization_ratio ?? null,
        }};
      }}).filter(edge => {{
        const gasoductoMatch = gasoducto === 'Todos' || edge.gasoducto === gasoducto;
        const criticalMatch = !criticalOnly || (edge.observed_utilization_ratio != null && edge.observed_utilization_ratio >= 0.8) || edge.topology_status !== 'observed';
        return gasoductoMatch && criticalMatch;
      }});
      const nodeIds = new Set();
      visibleEdges.forEach(edge => {{
        nodeIds.add(edge.source_node_id);
        nodeIds.add(edge.target_node_id);
      }});
      const visibleNodes = projectedNetwork.nodes.filter(node => nodeIds.has(node.node_id));
      const maxFlow = Math.max(...visibleEdges.map(edge => edge.observed_flow_mm3_dia || 0), 1);
      const exogenousPoints = (nodeExogenousMap.get(selectedMonthStart) || []).filter(item => nodeIds.has(item.node_id));
      const exogenousLookup = nodeExogenousLookup(selectedMonthStart);
      const maxSource = Math.max(data.network.overview.max_supply_proxy || 0, 1);
      const maxSink = Math.max(data.network.overview.max_withdrawal_proxy || 0, 1);
      const maxObservedThroughput = Math.max(data.network.overview.max_observed_throughput || 0, 1);

      const grid = Array.from({{ length: 8 }}, (_, idx) => {{
        const x = 70 + idx * 110;
        const y = 70 + idx * 80;
        return `
          <line x1="${{x}}" y1="40" x2="${{x}}" y2="720" stroke="rgba(36,48,65,0.05)" />
          <line x1="40" y1="${{y}}" x2="880" y2="${{y}}" stroke="rgba(36,48,65,0.05)" />
        `;
      }}).join('');

      const edgesMarkup = visibleEdges.map(edge => {{
        const width = 1.8 + ((edge.observed_flow_mm3_dia || 0) / maxFlow) * 9;
        const color = utilizationColor(edge.observed_utilization_ratio);
        const dash = edge.active_loop_count > 0 ? '10 6' : '';
        const opacity = edge.source_confidence === 'powerbi_route' ? 0.92 : 0.78;
        const selected = selectedEdgeId === edge.edge_id;
        const accent = selected ? `
          <line x1="${{edge.start.x}}" y1="${{edge.start.y}}" x2="${{edge.end.x}}" y2="${{edge.end.y}}"
                stroke="rgba(31,111,95,0.22)" stroke-width="${{width + 10}}" stroke-linecap="round" />
        ` : '';
        return `
          ${{accent}}
          <line class="network-edge" data-edge-id="${{edge.edge_id}}"
                x1="${{edge.start.x}}" y1="${{edge.start.y}}" x2="${{edge.end.x}}" y2="${{edge.end.y}}"
                stroke="${{color}}" stroke-width="${{width.toFixed(2)}}"
                stroke-linecap="round" stroke-opacity="${{opacity}}" stroke-dasharray="${{dash}}" />
        `;
      }}).join('');

      const nodesMarkup = visibleNodes.map(node => {{
        const inferred = node.topology_status !== 'observed';
        const stroke = inferred ? 'var(--accent-2)' : 'rgba(36,48,65,0.28)';
        const fill = node.has_compressor ? 'var(--deep)' : '#ffffff';
        const radius = node.has_compressor ? 5.5 : 4.2;
        const selected = selectedNodeId === node.node_id;
        const label = node.has_compressor || inferred
          ? `<text x="${{node.x + 7}}" y="${{node.y - 7}}" font-size="10.5" fill="#5c6370">${{node.nombre}}</text>`
          : '';
        const square = node.has_compressor
          ? `<rect class="network-node" data-node-id="${{node.node_id}}" x="${{node.x - 5}}" y="${{node.y - 5}}" width="10" height="10" fill="${{fill}}" stroke="${{selected ? 'var(--accent)' : stroke}}" stroke-width="${{selected ? 2.4 : 1.5}}" transform="rotate(45 ${{node.x}} ${{node.y}})" />`
          : `<circle class="network-node" data-node-id="${{node.node_id}}" cx="${{node.x}}" cy="${{node.y}}" r="${{radius}}" fill="${{fill}}" stroke="${{selected ? 'var(--accent)' : stroke}}" stroke-width="${{selected ? 2.4 : 1.4}}" />`;
        return `${{square}}${{label}}`;
      }}).join('');
      const exogenousMarkup = exogenousPoints.map(item => {{
        const node = projectedNetwork.nodes.find(candidate => candidate.node_id === item.node_id);
        if (!node) return '';
        const parts = [];
        if (networkShowObserved.checked && (item.observed_throughput_mm3_dia || 0) > 0) {{
          const radius = 3 + Math.sqrt((item.observed_throughput_mm3_dia || 0) / maxObservedThroughput) * 18;
          parts.push(`
            <circle class="network-node-bubble" data-node-id="${{node.node_id}}" cx="${{node.x}}" cy="${{node.y}}" r="${{radius.toFixed(2)}}" fill="none" stroke="rgba(36,48,65,0.45)" stroke-width="1.5" />
          `);
        }}
        if (networkShowSources.checked && (item.supply_mm3_dia_proxy || 0) > 0) {{
          const radius = 4 + Math.sqrt((item.supply_mm3_dia_proxy || 0) / maxSource) * 24;
          parts.push(`
            <circle class="network-node-bubble" data-node-id="${{node.node_id}}" cx="${{node.x}}" cy="${{node.y}}" r="${{radius.toFixed(2)}}" fill="rgba(31,111,95,0.18)" stroke="var(--accent)" stroke-width="1.6" />
          `);
        }}
        if (networkShowSinks.checked && (item.withdrawal_mm3_dia_proxy || 0) > 0) {{
          const radius = 4 + Math.sqrt((item.withdrawal_mm3_dia_proxy || 0) / maxSink) * 24;
          parts.push(`
            <circle class="network-node-bubble" data-node-id="${{node.node_id}}" cx="${{node.x}}" cy="${{node.y}}" r="${{radius.toFixed(2)}}" fill="rgba(184,92,56,0.16)" stroke="var(--accent-2)" stroke-width="1.6" stroke-dasharray="5 4" />
          `);
        }}
        return parts.join('');
      }}).join('');

      networkMap.innerHTML = `
        <rect x="0" y="0" width="920" height="760" rx="18" class="map-ocean" />
        ${{grid}}
        <path d="${{projectedNetwork.countryPath}}" class="country-fill" />
        <rect x="32" y="32" width="856" height="696" rx="26" class="map-frame" />
        <path d="${{projectedNetwork.countryPath}}" class="country-outline" />
        ${{exogenousMarkup}}
        ${{edgesMarkup}}
        ${{nodesMarkup}}
      `;

      networkRouteList.innerHTML = '';
      const ranked = [...visibleEdges]
        .sort((a, b) => (b.observed_utilization_ratio || 0) - (a.observed_utilization_ratio || 0))
        .slice(0, 6);
      ranked.forEach(edge => {{
        const button = document.createElement('div');
        button.className = 'route-item';
        button.dataset.edgeId = edge.edge_id;
        button.innerHTML = `
          <div class="route-title">${{edge.ruta}}</div>
          <div class="route-meta">
            <span>${{edge.gasoducto}}</span>
            <span>util: ${{edge.observed_utilization_ratio == null ? '-' : fmtPct(edge.observed_utilization_ratio)}}</span>
            <span>cap: ${{fmtMm(edge.effective_capacity_mm3_dia)}}</span>
          </div>
        `;
        button.addEventListener('click', () => {{
          selectedEdgeId = edge.edge_id;
          renderNetwork();
        }});
        networkRouteList.appendChild(button);
      }});

      if (selectedNodeId && visibleNodes.find(node => node.node_id === selectedNodeId)) {{
        selectNode(selectedNodeId, visibleNodes, exogenousLookup, selectedMonthStart);
      }} else {{
        if (!visibleEdges.find(edge => edge.edge_id === selectedEdgeId)) {{
          selectedEdgeId = ranked[0]?.edge_id || visibleEdges[0]?.edge_id || null;
        }}
        selectEdge(selectedEdgeId, visibleEdges);
      }}
      [...document.querySelectorAll('.network-edge')].forEach(item => {{
        item.style.cursor = 'pointer';
        item.addEventListener('click', () => {{
          selectedEdgeId = item.dataset.edgeId;
          renderNetwork();
        }});
      }});
      [...document.querySelectorAll('.network-node')].forEach(item => {{
        item.style.cursor = 'pointer';
        item.addEventListener('click', () => {{
          selectedNodeId = item.dataset.nodeId;
          renderNetwork();
        }});
      }});
      [...document.querySelectorAll('.network-node-bubble')].forEach(item => {{
        item.style.cursor = 'pointer';
        item.addEventListener('click', () => {{
          selectedNodeId = item.dataset.nodeId;
          renderNetwork();
        }});
      }});
    }}

    function toggleNetworkPlayback() {{
      if (isPlayingNetwork) {{
        window.clearInterval(networkTimerId);
        isPlayingNetwork = false;
        networkPlayButton.textContent = 'Play';
        return;
      }}
      isPlayingNetwork = true;
      networkPlayButton.textContent = 'Pause';
      networkTimerId = window.setInterval(() => {{
        const currentIndex = data.network.available_months.findIndex(month => month === networkMonthSelect.value);
        const nextIndex = currentIndex + 1 >= data.network.available_months.length ? 0 : currentIndex + 1;
        networkMonthSelect.value = data.network.available_months[nextIndex];
        renderNetwork();
      }}, 1100);
    }}

    segmentSelect.addEventListener('change', event => renderChart(event.target.value));
    networkGasoducto.addEventListener('change', renderNetwork);
    networkCritical.addEventListener('change', renderNetwork);
    networkShowSources.addEventListener('change', renderNetwork);
    networkShowSinks.addEventListener('change', renderNetwork);
    networkShowObserved.addEventListener('change', renderNetwork);
    networkMonthSelect.addEventListener('change', renderNetwork);
    networkPlayButton.addEventListener('click', toggleNetworkPlayback);
    renderDiagnostics();
    renderSolverSummary();
    renderNetwork();
    if (data.forecast.chart_segments.length > 0) {{
      renderChart(data.forecast.chart_segments[0].segmento);
    }}
  </script>
</body>
</html>
"""


def main() -> None:
    payload = {
        "forecast": _load_forecast_summary(),
        "datalake": _load_datalake_summary(),
        "network": _load_network_summary(),
        "project": _load_project_status(),
        "outline": _load_argentina_outline(),
        "results": _load_results_table(FORECAST_DIR / "results.tsv"),
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(_render_html(payload), encoding="utf-8")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
