# GCIE — Gas Commercializer Intelligence Engine

## Project overview
Intelligence system for a gas commercializer in Argentina.
The current project has moved beyond a pure AutoResearch scaffold: SP0, SP1, SP2 and the dashboard are already running over a live DuckDB snapshot, while SP3-SP6 remain strategy-first engines waiting for their first real baselines.

**Platform:** GMKtec EVO-X2 / AMD Ryzen AI Max+ 395 / 96 GB
**Primary local environment:** Fedora Silverblue + Python venv at `/var/home/matias/.venvs/gcie`
**Main database:** `gas-intel-datalake/duckdb/gas_intel.duckdb`

## Current status

| Repo | SP | Status | Description |
|------|----|--------|-------------|
| gas-intel-datalake | SP0 | Live | Ingestion, DuckDB build, transport network, canonical overrides, solver exports |
| gas-intel-forecast | SP1 | Live baseline | Monthly demand forecast baseline running end-to-end |
| gas-intel-supply | SP2 | Live baseline | Monthly acquisition price baseline running end-to-end |
| gas-intel-pricing | SP3 | Spec only | Pricing engine strategy defined, implementation pending |
| gas-intel-risk | SP4 | Spec only | Risk engine strategy defined, implementation pending |
| gas-intel-scoring | SP5 | Spec only | Customer scoring strategy defined, implementation pending |
| gas-intel-portfolio | SP6 | Spec only | Portfolio optimizer strategy defined, implementation pending |
| gas-intel-meta | meta | Live | Dashboard, project telemetry and executive views |

## Dependency graph
```
SP0 (Data Lake + Canonical Network)
├── SP1 (Demand Forecast)  ──────────────┐
└── SP2 (Supply Engine)    ──────────────┤
                                          ▼
                               SP3 (Pricing Engine)
                                          │
                               SP4 (Risk Engine)
                                          │
                               SP5 (Customer Scoring)
                                          │
                               SP6 (Portfolio Optimizer)
```

## Working model
- `program.md` is the active human strategy document per engine.
- `CLAUDE.md` is the compact operational brief for agents.
- `*.legacy.md` preserves the older formulation when the active document was refreshed.
- `results.tsv` is the append-only experiment log for engines that already run real evaluations.

## What exists today
- SP0 ingests production, non-conventional wells, ENARGAS operational demand, climate, exchange rate, gas prices, transport flow/capacity, and a modeled transport network.
- SP0 also includes `F20` to `F24`: canonical network layer, nodal balances, base scenario, overrides, compressors, loops and `pandapipes` export support.
- SP1 has a working monthly baseline with real evaluation on the current DuckDB snapshot.
- SP2 has a working monthly supply-price baseline and a first unsuccessful transport-congestion experiment already logged.
- `gas-intel-meta/dashboard/index.html` exposes datalake status, SP1 metrics, experiment logs and the canonical network with monthly timeline playback.

## Core business view
The commercial edge is not just forecasting molecule availability at the PIST. It is forecasting deliverable gas:
- associated gas creates structural downward price pressure,
- transport saturation redistributes that pressure by corridor and season,
- winter bottlenecks can dominate acquisition cost and service quality even when aggregate supply looks comfortable.

## Execution rules
- Treat `gas-intel-datalake` as a controlled infrastructure repo. Changes are allowed when they materially improve ingestion, network modeling or database integrity.
- For SP1-SP6, `program.md` is human-owned strategy and should not be edited unless the human explicitly authorizes it.
- Prefer changes that keep DuckDB, dashboard and experiment logs aligned with each other.

## Near-term roadmap
- Stabilize the transport network model with better canonical parameters and physical assets.
- Use network state and deliverability to inform SP2 and later SP3.
- Stand up the first real SP3 baseline once SP1 and SP2 contracts are stable enough.
