# GCIE — Gas Commercializer Intelligence Engine

## Project overview
Intelligence system for a gas commercializer in Argentina.
Implements the AutoResearch pattern (Karpathy) adapted for gas market optimization on local hardware.

**Platform:** GMKtec EVO-X2 / AMD Ryzen AI Max+ 395 / 96 GB (Strix Halo, Fedora Silverblue)
**Agent stack:** Qwen 3.5 35B-A3B (port 8080) + Qwen 3.5 122B-A10B (port 8081) via llama.cpp + Vulkan

## Sub-projects

| Repo | SP | Status | Description |
|------|----|--------|-------------|
| gas-intel-datalake | SP0 | Active | Data Lake — FIXED, agents cannot modify |
| gas-intel-forecast | SP1 | Pending | Demand Forecast Engine |
| gas-intel-supply | SP2 | Pending | Supply & Sourcing Engine |
| gas-intel-pricing | SP3 | Pending | Pricing & Spread Engine |
| gas-intel-risk | SP4 | Pending | Risk Engine |
| gas-intel-scoring | SP5 | Pending | Customer Scoring |
| gas-intel-portfolio | SP6 | Pending | Portfolio Optimizer |
| gas-intel-meta | meta | Pending | Orchestration & Dashboard |

## Dependency graph
```
SP0 (Data Lake)
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

## AutoResearch pattern — CRITICAL
Each engine (SP1-SP6) has exactly these files:
- data_pipeline.py — FIXED. DO NOT MODIFY. Load + feature engineering.
- model.py — ONLY file agents modify.
- evaluate.py — FIXED. DO NOT MODIFY. Metric calculation.
- program.md — Human strategy document. Agents read it.
- results.tsv — Append-only experiment log.

## Key business insight
Associated gas (gas asociado) from Vaca Muerta petroleum production enters the system at near-zero marginal cost, creating structural downward price pressure. The commercializer's edge is modeling this dynamic ahead of the market.

## Roadmap
- Phase 0 (weeks 1-5): SP0 Data Lake complete
- Phase 1 (weeks 6-10): SP1 + SP2 overnight AutoResearch sessions
- Phase 2 (weeks 11-15): SP3 + SP4
- Phase 3 (weeks 16-20): SP5 + SP6 + dashboard
