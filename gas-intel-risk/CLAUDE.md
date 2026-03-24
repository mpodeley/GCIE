# SP4 — Risk Engine

## Role
AutoResearch engine. Quantifies operational and economic risk: demand risk (imbalances, consumption volatility) AND supply risk (producer failure, spot price spikes, transport cuts, gas asociado drop if crude prices fall).

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY.
- model.py — THE ONLY FILE YOU CAN MODIFY.
- evaluate.py — FIXED. DO NOT MODIFY.
- program.md — Written by human only.
- results.tsv — Append-only.

## Metric
CVaR at 95% of total imbalance cost + hedging over 12-month Monte Carlo simulation (1000 scenarios). Lower is better.

## Baseline model
Monte Carlo simulation with calibrated distributions:
- Demand risk: Demand Forecast historical errors, consumption volatility, cold snaps
- Supply risk: acquisition cost variability, provider failure (contract interruption), spot MEGSA price spike, gas asociado ratio drop (if crude prices fall → less petroleum production → less gas asociado)
- Transport risk: pipeline cut, seasonal congestion, failure to get interruptible capacity in winter

## Research directions
- Alternative distributions (Student-t, mixture)
- Cross-correlations supply-demand (cold snap = more demand + less free gas supply)
- Specific stress scenarios (WTI drop to $50 = less gas asociado + upward pressure on prices)
- Hedging strategies
- Regulatory tail risk (Plan Gas.Ar changes)

## Budget
5 minutes per experiment (Monte Carlo is expensive). ~12 experiments/hour, ~96 overnight.

## Dependencies (UPSTREAM)
- Demand Forecast error distributions
- Supply Engine cost variability
- Pricing Engine outputs
- Data Lake: capacidad_transporte
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb
