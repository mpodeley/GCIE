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
    select {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      background: white;
      color: var(--ink);
      font: inherit;
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
    @media (max-width: 900px) {{
      .hero, .grid, .two, .three, .chart-shell, .explain-grid {{ grid-template-columns: 1fr; }}
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
        .map((point, idx) => {{
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

    segmentSelect.addEventListener('change', event => renderChart(event.target.value));
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
        "results": _load_results_table(FORECAST_DIR / "results.tsv"),
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(_render_html(payload), encoding="utf-8")
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
