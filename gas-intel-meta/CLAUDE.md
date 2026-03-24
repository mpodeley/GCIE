# gas-intel-meta — Orchestration & Dashboard

## Role
Project telemetry and executive visibility layer for GCIE.
This repo now matters operationally because it is the main place to inspect data freshness, model outputs and the transport network state.

## What exists today
- Static dashboard snapshot at `dashboard/index.html`
- Generator script at `scripts/build_dashboard.py`
- Views for:
  - datalake inventory and table ranges
  - SP1 metrics and predictions
  - experiment logs
  - canonical gas network
  - monthly network timeline with playback
  - top stressed routes, route detail and recent solver snapshots

## Current contract
- The dashboard should explain project state, not just decorate it.
- Prefer pulling from DuckDB and generated summaries, not hardcoded examples.
- Network views should stay aligned with the canonical network tables in SP0.

## Useful assets
- `dashboard/index.html`
- `scripts/build_dashboard.py`
- generated `pandapipes_case_*.summary.json` consumed as supporting telemetry when available

## Near-term direction
- Add richer month-over-month network comparisons
- Expose more solver health and network bottleneck signals
- Keep documentation and dashboard synchronized when the project scope changes
