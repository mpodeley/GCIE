# SP3 — Pricing & Spread Engine

## Role
AutoResearch engine. Integrates Supply (cost side) and Demand Forecast (volume side) to optimize commercializer spread.

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY.
- model.py — THE ONLY FILE YOU CAN MODIFY.
- evaluate.py — FIXED. DO NOT MODIFY.
- program.md — Written by human only.
- results.tsv — Append-only.

## Metric
Average simulated net spread ($/MMBtu) on 12-month backtest. Higher is better.
Spread = Price_sale - Cost_acquisition - Transport - Tolls
Constraint: sale price cannot exceed MEGSA price + 5% (competitiveness constraint).

## Baseline model
Deterministic parametric: Price_sale = Cost_acquisition + Transport + Tolls + Margin_target.
Initial parameters manually calibrated.

## Research directions
- Seasonal margin adjustments
- Differentiated pricing by segment
- Volume discounts
- MEGSA-indexed formulas
- Partial pass-through of acquisition cost reductions (capture some gas asociado savings as extra margin)
- Strategy A: pass 100% to client (more competitive, more volume)
- Strategy B: partial pass-through (capture temporary margin)
- Strategy C: maintain price, increase pure margin

## Budget
2 minutes per experiment. ~30 experiments/hour.

## Dependencies (UPSTREAM — must be run first)
- Supply Engine outputs: acquisition cost + purchase mix
- Demand Forecast outputs: expected demand by client/segment
- Data Lake: transport tables (capacidad_transporte)
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb
