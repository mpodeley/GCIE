# Network Sanitation Sources

## Current state
- `F25` is now implemented in `scrapers/f25_gasoductos_enargas.py`.
- It materializes the official ENARGAS GIS layer into `red_gasoductos_enargas_oficial.parquet`.
- It also emits `red_gasoductos_enargas_vs_modelada.parquet` as a first crosswalk / diagnostic against `red_tramos_canonica` or `red_tramos`.
- It now emits `red_tramos_enargas_componentes.parquet` so each proxy tramo can retain the list of official physical components behind the corridor.
- Manual resolution now lives in `templates/red_tramos_enargas_crosswalk.csv`.
- Manual component enrichment now has a dedicated placeholder in `templates/red_tramos_enargas_componentes_specs.csv`.
- `F24` already consumes that crosswalk so official ENARGAS corridor lengths and component metadata can flow into `red_tramos_parametros_canonica` when a tramo is resolved.

## Backlog candidates

| ID | Source | Tier | Likely contribution |
|----|--------|------|---------------------|
| F26 | Global Energy Monitor - Global Gas Infrastructure Tracker | Tier 1 | Capacity checks, route GIS cross-check, project status |
| F27 | EIA Argentina country brief | Tier 3 | Cross-reference for capacities and ownership |
| F28 | ENARGAS own transparency portal | Tier 2 | Line pack, losses, unaccounted-for gas |
| F29 | Energia Argentina / Perito Moreno construction portal | Tier 2 | Perito Moreno expansion and Mercedes-Cardales status |
| F30 | Gasoducto del Pacifico + Gas Andes / NorAndino operator sources | Tier 3 | Binational export flow and corridor validation |

## Notes from the first F25 pass
- The official GIS has many more tramos than the current modeled network, so direct name matches should be treated as a coverage floor, not as a complete reconciliation.
- ENARGAS GIS does not publish pipe diameter or contracted capacity per shapefile feature. It is good enough for corridor geometry, official names, tramo type and component counting, but not for a fully parameterized hydraulic model.
- Unmatched modeled routes currently cluster around:
  - Centro Oeste naming differences
  - export / binational corridors
  - manual connectors such as Mercedes-Cardales
  - synthetic or aggregate assets like Methanex and TF connectors
- The next practical step is not a fuzzy merge by default. It is a controlled crosswalk table that maps official `Tramo` and `Gasoducto` values onto canonical GCIE edge ids, and then a manual spec layer for diameters / capacities where one proxy corridor represents multiple physical pipes.
