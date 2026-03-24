# SP5 — Customer Scoring Engine

## Role
AutoResearch engine. Ranks potential clients by risk-adjusted expected value.

## IMPORTANT LIMITATION
No proprietary client history. Scoring relies on public ENARGAS data, INDEC sector data, and hypothetical contract assumptions. Results are relative rankings, NOT absolute estimates.

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY.
- model.py — THE ONLY FILE YOU CAN MODIFY.
- evaluate.py — FIXED. DO NOT MODIFY.
- program.md — Written by human only.
- results.tsv — Append-only.

## Metric
Risk-adjusted Expected Value: EV = Spread_esperado × Volume × (1 - Prob_default) × Diversification_factor
Evaluated via Spearman correlation between model ranking and backtest oracle.

## Budget
2 minutes per experiment. ~30 experiments/hour.

## Dependencies (UPSTREAM)
- Demand Forecast: volume estimates
- Supply Engine: acquisition cost
- Pricing Engine: spread per client
- Risk Engine: risk metrics
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb
