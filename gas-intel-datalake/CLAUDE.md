# SP0 — Gas Intel Data Lake

## Role
Infrastructure repo for the live GCIE stack.
This is no longer just a passive prepare-step: it is the operational backbone for SP1, SP2, the dashboard, and the transport network modeling work.

## What exists today
- A buildable DuckDB at `duckdb/gas_intel.duckdb`
- Automated ingestion for production, non-conventional wells, ENARGAS demand, climate, exchange rate, calendar and gas prices
- Transport flow and capacity layers from ENARGAS
- A modeled network stack:
  - `F20` modeled transport network
  - `F20b` canonical overrides
  - `F21` nodal balances
  - `F22` exogenous supply-demand scenario
  - `F23` heuristic network solver
  - `F24` compressors, loops and physical parameters
  - `pandapipes` case export

## Main directories
- `scrapers/` data ingestion and derived network builders
- `loaders/` DuckDB materialization
- `templates/` manual overrides for canonical network assets
- `data/processed/` normalized parquet outputs
- `data/snapshots/` immutable snapshots
- `duckdb/` local analytical database

## Key tables
- `consumo_diario`
- `clima`
- `tipo_cambio`
- `precios_boca_pozo`
- `produccion_diaria`
- `gas_asociado_ratio`
- `pozos_no_convencional`
- `transporte_flujo_mensual`
- `transporte_capacidad_firme`
- `transporte_utilizacion_mensual`
- `red_nodos_canonica`
- `red_tramos_canonica`
- `red_tramo_metricas_mensuales`
- `red_solver_resumen_mensual`

## Operational rule
SP0 is controlled infrastructure. Changes are allowed, but they should improve one of these:
- source ingestion quality,
- schema clarity,
- canonical network quality,
- solver exportability,
- database reproducibility.

## Current gap
The main open issue is not data volume but physical realism:
- the canonical network still needs better official parameters,
- some strategic assets are still represented with manual overrides,
- `pandapipes` export exists but full-network convergence is not yet stable.

## Build contract
- Canonical rebuild entrypoint: `loaders/build_duckdb.py`
- Dashboard and engines should always consume the rebuilt DuckDB, not ad hoc CSVs.
- Keep provenance explicit when a table or asset is official, inferred or manually overridden.
