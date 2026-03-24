"""
F18 — Calendario Argentina
Source: generated locally
Tier 1 — Automated
Table: calendario

This generator creates a deterministic business calendar used by downstream
engines even before external holiday sources are integrated.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

try:
    import pandas as pd
except ModuleNotFoundError as exc:
    raise RuntimeError(
        "pandas is required to run F18 calendario generation."
    ) from exc


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
SNAPSHOTS_DIR = Path(__file__).parent.parent / "data" / "snapshots"

SEASONS = {
    12: "verano",
    1: "verano",
    2: "verano",
    3: "otono",
    4: "otono",
    5: "otono",
    6: "invierno",
    7: "invierno",
    8: "invierno",
    9: "primavera",
    10: "primavera",
    11: "primavera",
}


def _daterange(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _gas_week(day: date) -> int:
    """Return 1-based week number within the month."""
    return ((day.day - 1) // 7) + 1


def generate_calendar(start_date: date, end_date: date) -> pd.DataFrame:
    rows = []
    for day in _daterange(start_date, end_date):
        is_weekend = day.weekday() >= 5
        rows.append(
            {
                "fecha": day,
                "es_feriado": is_weekend,
                "es_laborable": not is_weekend,
                "semana_gas": _gas_week(day),
                "mes": day.month,
                "trimestre": ((day.month - 1) // 3) + 1,
                "estacion": SEASONS[day.month],
            }
        )
    return pd.DataFrame(rows)


def save_snapshot(df: pd.DataFrame) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    content_hash = hashlib.sha256(df.to_json(date_format="iso").encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"calendario_{ts}_{content_hash}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run(start_year: int = 2010, end_year: int | None = None) -> pd.DataFrame:
    if end_year is None:
        end_year = date.today().year + 1

    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    log.info("=== F18 calendario generation %s -> %s ===", start_date, end_date)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_calendar(start_date, end_date)
    processed_path = PROCESSED_DIR / "calendario.parquet"
    df.to_parquet(processed_path, index=False)
    log.info("Saved processed: %s (%s rows)", processed_path, len(df))
    save_snapshot(df)
    log.info("=== F18 complete ===")
    return df


if __name__ == "__main__":
    run()
