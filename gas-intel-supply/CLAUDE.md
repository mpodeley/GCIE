# SP2 — Supply & Sourcing Engine

## Role
Operational monthly acquisition-price baseline for the current GCIE snapshot.
This engine is already live and should be treated as an iterative production baseline, not a blank research stub.

## Current implementation
- `data_pipeline.py` builds a monthly target from `precios_boca_pozo`
- `model.py` contains the active supply-price baseline
- `evaluate.py` computes validation MAE and appends experiments to `results.tsv`

## Current score
Best known active baseline: `mae_usd_mmbtu = 0.29947537459945683`

## Data contract
Current baseline uses:
- `precios_boca_pozo`
- `tipo_cambio`
- `pozos_no_convencional`

Related context now available in SP0:
- `gas_asociado_ratio`
- transport utilization tables
- canonical network, overrides and solver summaries

DuckDB path: `../gas-intel-datalake/duckdb/gas_intel.duckdb`

## What we learned already
- A naive congestion adjustment did not improve the baseline and was intentionally not kept as active model.
- The transport layer still matters, but likely as regime, basis or deliverability context rather than as a simple linear price feature.
- Network realism is now a first-order dependency for the next SP2 upgrade.

## File rules
- `program.md` is human strategy and should stay aligned with the actual project state.
- `results.tsv` is append-only and should capture failed but informative transport experiments too.

## Near-term research direction
- improve the historical `gas_asociado_ratio` signal,
- connect sourcing to canonical corridor stress,
- move from plain acquisition price toward deliverable acquisition cost.
