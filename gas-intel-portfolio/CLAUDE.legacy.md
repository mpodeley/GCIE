# SP6 — Portfolio Optimizer

## Role
AutoResearch engine. Selects optimal combination of clients + sourcing strategy maximizing total spread subject to transport capacity, risk diversification, seasonal balance, and supply availability constraints.

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY.
- model.py — THE ONLY FILE YOU CAN MODIFY.
- evaluate.py — FIXED. DO NOT MODIFY.
- program.md — Written by human only.
- results.tsv — Append-only.

## Metric
Sharpe-like ratio: Spread_total / Portfolio_volatility on 12-month backtest. Higher is better.
Optimizes BOTH client portfolio AND sourcing mix (how much firm vs. spot, which basin/producer).

## Baseline model
Convex optimization (scipy.optimize or cvxpy) with linear capacity constraints.

## Research directions
- Alternative objective functions
- Provider concentration penalties
- Greedy heuristics
- Robust optimization (worst-case scenarios)

## Budget
5 minutes per experiment. ~12 experiments/hour.

## Dependencies (ALL UPSTREAM engines)
All SP1-SP5 outputs required.
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb
