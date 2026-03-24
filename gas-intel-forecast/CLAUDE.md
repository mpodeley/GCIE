# SP1 — Demand Forecast Engine

## Role
Operational monthly demand forecast baseline for the current GCIE snapshot.
The focus today is not generic AutoResearch exploration but improving a working baseline that already runs end-to-end.

## Current implementation
- `data_pipeline.py` builds monthly demand features from `consumo_diario`, `clima` and `calendario`
- `model.py` contains the active seasonal baseline
- `evaluate.py` computes `weighted_mape` on the latest validation window
- `results.tsv` records experiments and active scores

## Current score
Best known active baseline: `weighted_mape = 0.15378441199363313`

## Data contract
Reads from:
- `consumo_diario`
- `clima`
- `calendario`

DuckDB path: `../gas-intel-datalake/duckdb/gas_intel.duckdb`

## What matters now
- Cadence is effectively monthly in the current historical demand source.
- Residential behavior is strongly seasonal and HDD-driven.
- Transport congestion is not yet part of the active SP1 baseline, but it is conceptually relevant for deliverability and winter stress.

## File rules
- `program.md` is human strategy, not agent scratch space.
- `results.tsv` is append-only.
- Prefer improving the existing baseline before swapping the whole modeling contract.

## Near-term research direction
- Better regime handling for winter months
- Segment-specific structure once the baseline contract stabilizes
- Later, add transport stress as a deliverability feature rather than as a naive linear control
