# SP0 — Gas Intel Data Lake

## Role
Infrastructure sub-project. This is the equivalent of prepare.py in the AutoResearch pattern: FIXED, IMMUTABLE. Agents from other sub-projects consume this data but NEVER modify files in this repo.

## What this does
- Ingests data from Argentine gas market sources (ENARGAS, Secretaría de Energía, NOAA, MEGSA, BCRA, INDEC)
- Stores clean data in DuckDB (gas_intel.duckdb) backed by Parquet snapshots
- Provides versioned, immutable snapshots referenced by engine sub-projects

## Stack
- Python 3.11+
- DuckDB (analytics engine, in-process)
- Parquet (immutable snapshots, partitioned by year/month)
- pandas, requests, schedule

## Directory structure
- scrapers/ — One script per data source (f01_*.py, f02_*.py, etc.)
- loaders/ — Normalization + DuckDB ingestion scripts
- schemas/ — JSON schema per table for validation
- templates/ — Excel templates for manual Tier 3 ingestion
- data/raw/ — Downloaded files, never modified
- data/processed/ — Normalized CSVs
- data/snapshots/ — Immutable Parquet files with SHA256 hash
- duckdb/ — gas_intel.duckdb

## Data sources by tier
### Tier 1 — Automated (scrapers + cron)
- F01: Producción SESCO (datos.gob.ar CSV) → produccion_diaria, gas_asociado_ratio
- F02: Pozos no-convencional (SESCO Cap IV) → pozos_no_convencional
- F03: Regalías y precios boca de pozo (SE) → precios_boca_pozo
- F04: Precios Gas Natural (SE) → precios_boca_pozo
- F05: Balances de Gas (SESCO) → inyeccion_sistema, consumo_diario
- F06: Datos Operativos ENARGAS → consumo_diario
- F07: Clima NOAA CDO API → clima (token required: register at ncdc.noaa.gov/cdo-web/token)
- F17: Tipo de cambio BCRA API → tipo_cambio
- F18: Calendario Argentina → calendario (generated script, no external dependency)

### Tier 2 — Semi-automated (manual download + normalization script)
- F08: MEGSA PPP reports → precios_megsa (requires Pluspetrol agent license)
- F09: ENARGAS despacho web → consumo_diario, capacidad_transporte
- F10: Partes diarios ENARGAS (verify freshness first) → consumo_diario
- F11: Despacho diario ENARGAS → consumo_diario, inyeccion_sistema
- F12: Capacidad transporte firme ENARGAS → capacidad_transporte

### Tier 3 — Manual (Excel templates)
- F13: TGS/TGN capacity reports → capacidad_transporte, concursos_abiertos
- F14: Plan Gas.Ar → plan_gas
- F15: INDEC EMAE → clientes_proxy
- F16: Hypothetical contracts → contratos_compra, contratos_venta

## DuckDB schema — key tables
- produccion_diaria: fecha, yacimiento_id, cuenca, operador, gas_mm3d, petroleo_m3d, tipo_gas, formacion
- gas_asociado_ratio: fecha, cuenca, prod_petroleo_total, prod_gas_asociado, ratio_gor, costo_marginal_estimado
- consumo_diario: fecha, punto_entrega_id, volumen_m3, segmento
- clima: fecha, estacion_id, temp_min, temp_max, temp_media, hdd, cdd
- calendario: fecha, es_feriado, es_laborable, semana_gas, mes, trimestre, estacion
- precios_boca_pozo: fecha, cuenca, precio_referencia_mmbtu, resolucion_se
- precios_megsa: fecha, cuenca, precio_mmbtu, volumen_transado, tipo_operacion
- tipo_cambio: fecha, usd_ars

## Priority for implementation
P0 (Week 1): F01 (SESCO prod), F07 (NOAA clima), F17 (BCRA), F18 (Calendario)
P1 (Week 2): F03 (Regalías), F05 (Balances), F02 (Pozos noconv)
P2 (Week 3): F06 (ENARGAS ops), F11 (Despacho), F12 (Capacidad)
P3 (Week 4): F08 (MEGSA), F09 (ENARGAS web), F15 (INDEC)
P4 (Week 5): F13 (TGS/TGN), F14 (Plan Gas), F16 (Contratos hipotéticos)

## IMPORTANT: Snapshot versioning
Every ingestion creates an immutable Parquet snapshot:
- Filename: {table}_{YYYYMMDD_HHMMSS}_{sha256[:8]}.parquet
- Engines reference snapshots by hash, never live data
- Never delete snapshots; archive to snapshots/archive/ if disk is tight
