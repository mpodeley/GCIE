"""
F07 — Clima / Temperatura (NOAA CDO API)
Source: NOAA National Centers for Environmental Information (NCEI)
Tier 1 — Automated
Table: clima (with HDD/CDD calculated)
API docs: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
Token: register at https://www.ncdc.noaa.gov/cdo-web/token

CRITICAL for Demand Forecast: temperature is the #1 predictor of residential gas demand.
HDD (Heating Degree Days) = max(18 - T_avg, 0) — most important demand feature.
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

NOAA_API_BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2"
OPEN_METEO_API_BASE = "https://archive-api.open-meteo.com/v1/archive"
# Token from env var or config file
NOAA_TOKEN = os.environ.get("NOAA_CDO_TOKEN", "")

# Argentine weather stations (GHCND station IDs)
# Key cities for gas demand correlation
STATIONS = {
    "buenos_aires_ezeiza": {"noaa_id": "GHCND:AR000087582", "latitude": -34.82, "longitude": -58.53},
    "buenos_aires_aeroparque": {"noaa_id": "GHCND:AR000087585", "latitude": -34.56, "longitude": -58.42},
    "rosario": {"noaa_id": "GHCND:AR000087388", "latitude": -32.95, "longitude": -60.67},
    "cordoba": {"noaa_id": "GHCND:AR000087344", "latitude": -31.42, "longitude": -64.19},
    "mendoza": {"noaa_id": "GHCND:AR000087418", "latitude": -32.89, "longitude": -68.84},
    "neuquen": {"noaa_id": "GHCND:AR000087534", "latitude": -38.95, "longitude": -68.06},
    "bahia_blanca": {"noaa_id": "GHCND:AR000087750", "latitude": -38.72, "longitude": -62.27},
    "mar_del_plata": {"noaa_id": "GHCND:AR000087860", "latitude": -38.00, "longitude": -57.56},
    "tucuman": {"noaa_id": "GHCND:AR000087121", "latitude": -26.82, "longitude": -65.22},
    "comodoro_rivadavia": {"noaa_id": "GHCND:AR000087860", "latitude": -45.86, "longitude": -67.48},
}

RAW_DIR = Path(__file__).parent.parent / "data" / "raw" / "noaa"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"

HDD_BASE_TEMP = 18.0  # degrees Celsius (Argentine standard for gas demand)
CDD_BASE_TEMP = 24.0  # degrees Celsius


def check_token():
    if not NOAA_TOKEN:
        raise ValueError(
            "NOAA CDO token not set. Register at: https://www.ncdc.noaa.gov/cdo-web/token\n"
            "Then set: export NOAA_CDO_TOKEN=your_token"
        )


def fetch_station_data(
    station_id: str,
    start_date: date,
    end_date: date,
    datatypes: list[str] = None,
) -> list[dict]:
    """
    Fetch daily data for a station from NOAA CDO API.
    Rate limits: 5 req/sec, 10K req/day.
    Max 1 year per query.
    """
    if datatypes is None:
        datatypes = ["TMIN", "TMAX"]

    check_token()

    params = {
        "datasetid": "GHCND",
        "stationid": station_id,
        "startdate": start_date.isoformat(),
        "enddate": end_date.isoformat(),
        "datatypeid": ",".join(datatypes),
        "units": "metric",
        "limit": 1000,
        "offset": 1,
    }
    headers = {"token": NOAA_TOKEN}

    all_results = []
    while True:
        resp = requests.get(f"{NOAA_API_BASE}/data", params=params, headers=headers, timeout=30)

        if resp.status_code == 429:
            log.warning("Rate limited, sleeping 2s")
            time.sleep(2)
            continue

        if resp.status_code == 404:
            log.warning(f"No data for {station_id} {start_date} - {end_date}")
            return []

        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        # Check pagination
        metadata = data.get("metadata", {}).get("resultset", {})
        total = metadata.get("count", 0)
        offset = metadata.get("offset", 1)
        limit = metadata.get("limit", 1000)

        if offset + limit > total:
            break

        params["offset"] = offset + limit
        time.sleep(0.2)  # Respect rate limit

    return all_results


def fetch_station_year(station_name: str, station_id: str, year: int) -> pd.DataFrame:
    """Fetch one year of data for a station."""
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), date.today() - timedelta(days=1))

    log.info(f"Fetching {station_name} ({station_id}) {year}")
    records = fetch_station_data(station_id, start, end)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["estacion_id"] = station_name
    df["fecha"] = pd.to_datetime(df["date"])

    # Pivot TMIN/TMAX into columns
    pivot = df.pivot_table(index=["fecha", "estacion_id"], columns="datatype", values="value", aggfunc="first").reset_index()
    pivot.columns.name = None

    return pivot


def calculate_hdd_cdd(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate HDD and CDD from TMIN/TMAX."""
    if "TMIN" not in df.columns or "TMAX" not in df.columns:
        log.warning("Missing TMIN or TMAX columns")
        return df

    df["temp_min"] = df["TMIN"] / 10.0  # NOAA reports in tenths of degree
    df["temp_max"] = df["TMAX"] / 10.0
    df["temp_media"] = (df["temp_min"] + df["temp_max"]) / 2

    df["hdd"] = (HDD_BASE_TEMP - df["temp_media"]).clip(lower=0)
    df["cdd"] = (df["temp_media"] - CDD_BASE_TEMP).clip(lower=0)

    return df.drop(columns=["TMIN", "TMAX"], errors="ignore")


def fetch_open_meteo_station_year(station_name: str, latitude: float, longitude: float, year: int) -> pd.DataFrame:
    start = date(year, 1, 1)
    end = min(date(year, 12, 31), date.today() - timedelta(days=1))
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "America/Argentina/Buenos_Aires",
    }
    log.info("Open-Meteo %s %s", station_name, year)
    resp = requests.get(OPEN_METEO_API_BASE, params=params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    max_temps = daily.get("temperature_2m_max", [])
    min_temps = daily.get("temperature_2m_min", [])
    if not dates:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "fecha": pd.to_datetime(dates),
            "estacion_id": station_name,
            "temp_max": pd.to_numeric(max_temps, errors="coerce"),
            "temp_min": pd.to_numeric(min_temps, errors="coerce"),
        }
    )
    df["temp_media"] = (df["temp_min"] + df["temp_max"]) / 2
    df["hdd"] = (HDD_BASE_TEMP - df["temp_media"]).clip(lower=0)
    df["cdd"] = (df["temp_media"] - CDD_BASE_TEMP).clip(lower=0)
    return df


def run_historical_open_meteo(start_year: int = 2010) -> pd.DataFrame:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    log.info("=== F07 Open-Meteo Historical backfill from %s ===", start_year)

    all_dfs = []
    for station_name, station in STATIONS.items():
        for year in range(start_year, date.today().year + 1):
            try:
                df = fetch_open_meteo_station_year(
                    station_name,
                    station["latitude"],
                    station["longitude"],
                    year,
                )
                if len(df) > 0:
                    all_dfs.append(df)
                time.sleep(0.1)
            except Exception as e:
                log.error("Error %s %s: %s", station_name, year, e)

    if not all_dfs:
        raise RuntimeError("Open-Meteo fallback returned no climate records.")

    combined = pd.concat(all_dfs, ignore_index=True)
    out_path = PROCESSED_DIR / "clima.parquet"
    combined.to_parquet(out_path, index=False)
    log.info("Historical clima saved: %s (%s rows)", out_path, len(combined))
    _save_snapshot(combined)
    return combined


def run_historical(start_year: int = 2010):
    """Backfill all stations from start_year."""
    if not NOAA_TOKEN:
        log.warning("NOAA token missing; falling back to Open-Meteo archive API.")
        return run_historical_open_meteo(start_year=start_year)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info(f"=== F07 NOAA Historical backfill from {start_year} ===")
    all_dfs = []

    for station_name, station in STATIONS.items():
        for year in range(start_year, date.today().year + 1):
            try:
                df = fetch_station_year(station_name, station["noaa_id"], year)
                if len(df) > 0:
                    all_dfs.append(df)
                time.sleep(0.2)  # Rate limit
            except Exception as e:
                log.error(f"Error {station_name} {year}: {e}")

    if not all_dfs:
        log.warning("No data fetched")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    combined = calculate_hdd_cdd(combined)

    out_path = PROCESSED_DIR / "clima.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"Historical clima saved: {out_path} ({len(combined):,} rows)")

    # Snapshot
    _save_snapshot(combined)
    return combined


def run_daily():
    """Incremental daily update: fetch yesterday's data for all stations."""
    if not NOAA_TOKEN:
        log.warning("NOAA token missing; using Open-Meteo fallback with a full refresh.")
        return run_historical_open_meteo(start_year=2010)

    yesterday = date.today() - timedelta(days=1)
    log.info(f"=== F07 NOAA Daily update for {yesterday} ===")

    all_dfs = []
    for station_name, station in STATIONS.items():
        try:
            records = fetch_station_data(station["noaa_id"], yesterday, yesterday)
            if records:
                df = pd.DataFrame(records)
                df["estacion_id"] = station_name
                df["fecha"] = pd.to_datetime(df["date"])
                pivot = df.pivot_table(index=["fecha", "estacion_id"], columns="datatype", values="value", aggfunc="first").reset_index()
                pivot.columns.name = None
                all_dfs.append(pivot)
            time.sleep(0.2)
        except Exception as e:
            log.error(f"Error {station_name}: {e}")

    if not all_dfs:
        log.info("No new data")
        return

    new_data = pd.concat(all_dfs, ignore_index=True)
    new_data = calculate_hdd_cdd(new_data)

    # Merge with existing
    processed_path = PROCESSED_DIR / "clima.parquet"
    if processed_path.exists():
        existing = pd.read_parquet(processed_path)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["fecha", "estacion_id"]).sort_values(["estacion_id", "fecha"]).reset_index(drop=True)
    else:
        combined = new_data

    combined.to_parquet(processed_path, index=False)
    log.info(f"Updated clima: {len(combined):,} rows")
    _save_snapshot(new_data)


def _save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    content_hash = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"clima_{ts}_{content_hash}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"Snapshot: {path}")
    return path


if __name__ == "__main__":
    import sys
    if "--historical" in sys.argv:
        run_historical()
    else:
        run_daily()
