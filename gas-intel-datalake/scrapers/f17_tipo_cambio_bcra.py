"""
F17 — Tipo de Cambio USD/ARS (BCRA API)
Source: Banco Central de la República Argentina
Tier 1 — Automated
Table: tipo_cambio
API: https://api.bcra.gob.ar/estadisticas/v4.0/monetarias/{idVariable}?desde=YYYY-MM-DD&hasta=YYYY-MM-DD
Variable 4 = TC minorista (vendedor BNA)
No authentication required.
"""
import hashlib
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BCRA_API_BASE = "https://api.bcra.gob.ar/estadisticas/v4.0/monetarias"
VARIABLE_TC_MINORISTA = 4  # TC vendedor BNA

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "bcra"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"


def fetch_tc(desde: date, hasta: date) -> list[dict]:
    """Fetch exchange rate data from BCRA API for a date range."""
    url = f"{BCRA_API_BASE}/{VARIABLE_TC_MINORISTA}"
    params = {"desde": desde.isoformat(), "hasta": hasta.isoformat()}
    log.info(f"GET {url} params={params}")

    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code == 404:
        log.warning(f"No data for range {desde} - {hasta}")
        return []

    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    if not results:
        return []

    detail_rows = results[0].get("detalle", [])
    if not isinstance(detail_rows, list):
        raise RuntimeError("Unexpected BCRA v4 response shape: missing results[0].detalle list.")

    log.info(f"Got {len(detail_rows)} records")
    return detail_rows


def fetch_historical(start_year: int = 2010) -> pd.DataFrame:
    """Fetch full historical TC data from start_year to today."""
    all_records = []

    current = date(start_year, 1, 1)
    today = date.today()

    while current <= today:
        # Fetch in 1-year chunks (API limit)
        chunk_end = min(date(current.year, 12, 31), today)

        try:
            records = fetch_tc(current, chunk_end)
            all_records.extend(records)
        except Exception as e:
            log.error(f"Error fetching {current} - {chunk_end}: {e}")

        current = date(current.year + 1, 1, 1)

    if not all_records:
        return pd.DataFrame(columns=["fecha", "usd_ars"])

    df = pd.DataFrame(all_records)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.rename(columns={"valor": "usd_ars"})
    df = df.sort_values("fecha").reset_index(drop=True)

    return df


def fetch_incremental(last_date: date) -> pd.DataFrame:
    """Fetch TC data from last_date to today (for daily cron)."""
    since = last_date + timedelta(days=1)
    today = date.today() - timedelta(days=1)  # Yesterday (today not yet available)

    if since > today:
        log.info("No new data to fetch")
        return pd.DataFrame(columns=["fecha", "usd_ars"])

    records = fetch_tc(since, today)
    if not records:
        return pd.DataFrame(columns=["fecha", "usd_ars"])

    df = pd.DataFrame(records)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.rename(columns={"valor": "usd_ars"})
    return df.sort_values("fecha").reset_index(drop=True)


def save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    content_hash = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"tipo_cambio_{ts}_{content_hash}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Snapshot: {path} ({len(df):,} rows)")
    return path


def run(historical: bool = False, start_year: int = 2010):
    """
    Run TC scraper.
    historical=True: fetch all data from start_year (first run / backfill)
    historical=False: incremental update (daily cron)
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=== F17 BCRA Tipo de Cambio ===")

    processed_path = PROCESSED_DIR / "tipo_cambio.parquet"

    if historical or not processed_path.exists():
        log.info(f"Historical fetch from {start_year}")
        df = fetch_historical(start_year)
    else:
        # Load existing and find last date
        existing = pd.read_parquet(processed_path)
        last_date = existing["fecha"].max().date()
        log.info(f"Incremental fetch from {last_date}")
        new_data = fetch_incremental(last_date)

        if len(new_data) > 0:
            df = pd.concat([existing, new_data], ignore_index=True)
            df = df.drop_duplicates(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)
        else:
            df = existing

    if df.empty:
        raise RuntimeError(
            "BCRA fetch produced no rows. Refusing to overwrite processed data with an empty snapshot."
        )

    log.info(f"Total records: {len(df):,}, range: {df['fecha'].min()} → {df['fecha'].max()}")

    # Save processed
    df.to_parquet(processed_path, index=False)
    log.info(f"Saved processed: {processed_path}")

    # Snapshot
    save_snapshot(df)

    log.info("=== F17 complete ===")
    return df


if __name__ == "__main__":
    import sys
    historical = "--historical" in sys.argv
    run(historical=historical)
