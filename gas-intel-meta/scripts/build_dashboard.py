"""Build a static dashboard for the current GCIE workspace state."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
FORECAST_DIR = ROOT / "gas-intel-forecast"
DATALAKE_DIR = ROOT / "gas-intel-datalake"
DUCKDB_PATH = DATALAKE_DIR / "duckdb" / "gas_intel.duckdb"
OUTPUT_PATH = ROOT / "gas-intel-meta" / "dashboard" / "index.html"


def _import_duckdb() -> Any:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required to build the dashboard. Run this script with the GCIE virtualenv."
        ) from exc
    return duckdb


def _load_forecast_summary() -> dict[str, Any]:
    sys.path.insert(0, str(FORECAST_DIR))
    from evaluate import score_run  # type: ignore

    result = score_run()
    predictions = result["predictions"]
    segment_errors: dict[str, list[float]] = {}
    for row in predictions:
        actual = float(row["actual_volume"])
        predicted = float(row["predicted_volume"])
        if actual <= 0:
            continue
        segment_errors.setdefault(row["segmento"], []).append(abs(actual - predicted) / actual)

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

    return {
        "hypothesis": result["hypothesis"],
        "metric_name": result["metric_name"],
        "metric_value": result["metric_value"],
        "train_rows": result["train_rows"],
        "validation_rows": result["validation_rows"],
        "model_artifacts": result["model_artifacts"],
        "by_segment": by_segment,
        "recent_predictions": recent_predictions,
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

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        table_summary = {}
        for table in [
            "produccion_diaria",
            "gas_asociado_ratio",
            "consumo_diario",
            "clima",
            "calendario",
            "tipo_cambio",
        ]:
            count, min_fecha, max_fecha = conn.execute(
                f"SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM {table}"
            ).fetchone()
            table_summary[table] = {
                "rows": int(count),
                "min_fecha": None if min_fecha is None else str(min_fecha),
                "max_fecha": None if max_fecha is None else str(max_fecha),
            }
    finally:
        conn.close()
    return table_summary


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
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 32px 20px 48px; }}
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
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .two {{
      display: grid;
      gap: 16px;
      grid-template-columns: 1.1fr 0.9fr;
      margin-bottom: 24px;
    }}
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(31,111,95,0.1);
      color: var(--accent);
      font-size: 0.84rem;
      font-weight: 700;
    }}
    @media (max-width: 900px) {{
      .hero, .grid, .two {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <section class="panel">
        <span class="badge">GCIE Live Snapshot</span>
        <h1>Data, Model, and Predictions</h1>
        <p>Single-page view of the current datalake snapshot, the active SP1 baseline, and the latest forecast outputs.</p>
      </section>
      <section class="panel">
        <h2>SP1 Metric</h2>
        <div class="value" id="metric-value"></div>
        <p class="small" id="metric-label"></p>
      </section>
    </div>

    <section class="grid" id="top-cards"></section>

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

    document.getElementById('metric-value').textContent = fmtPct(data.forecast.metric_value);
    document.getElementById('metric-label').textContent =
      `${{data.forecast.metric_name}} | cadence: ${{data.forecast.model_artifacts.cadence}} | hypothesis: ${{data.forecast.hypothesis}}`;

    const cards = [
      ['Train Rows', data.forecast.train_rows],
      ['Validation Rows', data.forecast.validation_rows],
      ['Tracked Segments', data.forecast.model_artifacts.segments.length],
    ];
    const cardsNode = document.getElementById('top-cards');
    cards.forEach(([label, value]) => {{
      const el = document.createElement('section');
      el.className = 'panel';
      el.innerHTML = `<div class="small">${{label}}</div><div class="value">${{fmtInt(value)}}</div>`;
      cardsNode.appendChild(el);
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
  </script>
</body>
</html>
"""


def main() -> None:
    payload = {
        "forecast": _load_forecast_summary(),
        "datalake": _load_datalake_summary(),
        "results": _load_results_table(FORECAST_DIR / "results.tsv"),
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(_render_html(payload), encoding="utf-8")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
