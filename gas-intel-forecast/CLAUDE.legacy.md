# SP1 — Demand Forecast Engine

## Role
AutoResearch engine. Predicts gas consumption by segment, zone, and time horizon.

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY. Loads snapshot from Data Lake, generates train/val/test splits, base feature engineering.
- model.py — THE ONLY FILE YOU CAN MODIFY. Features, algorithm, hyperparameters, postprocessing.
- evaluate.py — FIXED. DO NOT MODIFY. Runs model.py, calculates metric on validation set, returns score.
- program.md — Written by human only. Read it before every session.
- results.tsv — Append-only. Format: timestamp\thypothesis\tmetric_value\tdelta\tkept_discarded

## Metric
MAPE weighted by volume on validation set (last 3 months). Lower is better.
Formula: sum(|actual - predicted| / actual * volume) / sum(volume)

## Baseline model
LightGBM with: temperature, day_of_week, month, HDD/CDD, consumption lags (7/14/28 days), segment.

## Research directions (see program.md for current strategy)
- Temperature×segment interaction features
- Per-segment models vs. unified model
- Longer lag windows (60, 90 days)
- XGBoost, CatBoost alternatives
- Cyclic encoding for temporal features
- Trend decomposition features
- NEVER: data leakage, look-ahead bias

## Budget
3 minutes per experiment. ~20 experiments/hour, ~160 overnight.

## Data Lake dependency
Reads from: consumo_diario, clima, calendario, clientes_proxy
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb

## Branch strategy
- main: best validated model
- research/*: each overnight session gets its own branch
- Human reviews results.tsv + git log each morning and decides what to merge
