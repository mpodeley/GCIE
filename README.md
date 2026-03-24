# GCIE

GCIE is a local-first gas intelligence workspace for Argentina.
Today the project has four parts already usable on a real snapshot:
- `gas-intel-datalake` with DuckDB and network layers
- `gas-intel-forecast` with a live SP1 baseline
- `gas-intel-supply` with a live SP2 baseline
- `gas-intel-meta` with the dashboard

## Quick Start

### Windows PowerShell, no admin
Run from the repo root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_env.ps1
```

Optional network extras for `pandapipes`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap_env.ps1 -WithNetwork
```

### Linux / macOS
Run from the repo root:

```bash
bash ./scripts/bootstrap_env.sh
```

Optional network extras:

```bash
bash ./scripts/bootstrap_env.sh --with-network
```

## What Bootstrap Does
- creates a local `.venv`
- upgrades `pip`
- installs `requirements.txt`
- optionally installs `requirements-network.txt`

## Portable Runtime Snapshot

The repository does not track the local DuckDB or `data/processed` artifacts.
To move the current working state to another machine without rebuilding everything:

1. On the source machine, create a portable snapshot:

```bash
python ./scripts/package_runtime_snapshot.py
```

2. Copy the generated zip from `dist/` to the target machine.
3. Unzip it at the repository root on the target machine.

The package includes:
- `gas-intel-datalake/duckdb/gas_intel.duckdb`
- `gas-intel-datalake/data/processed/*`
- `gas-intel-meta/dashboard/index.html`

## First Useful Commands

Build DuckDB from local processed parquet:

```bash
python ./gas-intel-datalake/loaders/build_duckdb.py
```

Run SP1 evaluation:

```bash
cd ./gas-intel-forecast
python ./evaluate.py
```

Run SP2 evaluation:

```bash
cd ./gas-intel-supply
python ./evaluate.py
```

Regenerate dashboard:

```bash
python ./gas-intel-meta/scripts/build_dashboard.py
```

## Environment Notes
- `NOAA_CDO_TOKEN` is optional. If missing, the climate scraper falls back to Open-Meteo.
- `pandapipes` is optional and only needed for the network runner.
- The project is currently easiest to use on another machine by either:
  - cloning the repo and copying a packaged runtime snapshot, or
  - connecting remotely to the original machine via SSH / VS Code Remote SSH.
