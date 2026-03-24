# SP2 — Supply & Sourcing Engine

## Role
AutoResearch engine. Models gas supply available for the commercializer: volume by basin/producer, expected acquisition price, libre/asociado mix evolution, spot market dynamics.

## KEY BUSINESS INSIGHT — Gas Asociado Dynamic
Vaca Muerta shale oil production is scaling fast (Bajada del Palo Oeste, La Amarga Chica, Rincón de Aranda). Each barrel of oil brings associated gas whose production decision is driven by oil economics, NOT gas economics. This gas enters the system at near-zero marginal cost (~0.3-0.8 USD/MMBtu, just processing + compression vs ~2-3 USD/MMBtu for free gas). This creates structural downward price pressure. The commercializer that models this dynamic first has a real sourcing advantage.

## AutoResearch pattern — FILE RULES
- data_pipeline.py — FIXED. DO NOT MODIFY.
- model.py — THE ONLY FILE YOU CAN MODIFY.
- evaluate.py — FIXED. DO NOT MODIFY.
- program.md — Written by human only. Read it before every session.
- results.tsv — Append-only.

## Metric
MAE of weighted average acquisition price ($/MMBtu) on 12-month backtest. Lower is better.

## Baseline model
LightGBM predicting monthly average acquisition price based on:
- Total gas production by basin
- Gas asociado / gas libre ratio (KEY FEATURE)
- SE reference price
- Seasonality
- VM petroleum production (proxy for future gas asociado)
- Injection vs. transport capacity (congestion)
- Historical MEGSA price

## Research directions
- Decompose acquisition price by contract type (firm vs. spot), optimize mix
- Model aggregate supply curve by basin (free gas at price X, asociado at ~0 + processing)
- Incorporate petroleum production forecasts as leading indicator of future asociado supply
- Detect structural breaks (entry of new shale oil blocks)
- Model supply seasonality (non-conventional gas winter decline vs. stable crude-asociado)
- Transport congestion features affecting basin prices

## Sub-models
1. Aggregate supply model: supply curve by basin separating libre vs. asociado
2. Optimal purchase mix: given supply forecast, minimize acquisition cost subject to volume + reliability constraints
3. Opportunity window detector: identifies periods of oversupply → spot prices below firm contract cost

## Budget
3 minutes per experiment. ~20 experiments/hour, ~160 overnight.

## Data Lake dependency
Reads from: produccion_diaria, gas_asociado_ratio, precios_megsa, precios_boca_pozo, plan_gas, inyeccion_sistema, contratos_compra
DuckDB path: ../gas-intel-datalake/duckdb/gas_intel.duckdb
