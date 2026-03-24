"""Fixed data loading contract for the Pricing engine."""

from __future__ import annotations

from pathlib import Path
from typing import Any


ENGINE_NAME = "gas-intel-pricing"
DUCKDB_PATH = Path(__file__).resolve().parents[1] / "gas-intel-datalake" / "duckdb" / "gas_intel.duckdb"


def load_dataset() -> dict[str, Any]:
    """Return the dataset payload expected by model.py and evaluate.py."""
    raise NotImplementedError(
        f"{ENGINE_NAME}: implement the fixed data loader against {DUCKDB_PATH}."
    )
