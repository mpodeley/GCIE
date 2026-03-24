"""Fixed evaluation entrypoint for the Risk engine."""

from __future__ import annotations

from typing import Any

from data_pipeline import load_dataset
from model import train_and_predict


def score_run() -> dict[str, Any]:
    """Run the current model against the fixed dataset contract."""
    dataset = load_dataset()
    return train_and_predict(dataset)


if __name__ == "__main__":
    print(score_run())
