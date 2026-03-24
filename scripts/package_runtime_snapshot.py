from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist"
PACKAGE_PATH = DIST_DIR / "gcie_runtime_snapshot.zip"

INCLUDE_PATHS = [
    ROOT / "gas-intel-datalake" / "duckdb" / "gas_intel.duckdb",
    ROOT / "gas-intel-datalake" / "data" / "processed",
    ROOT / "gas-intel-meta" / "dashboard" / "index.html",
]


def iter_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(item for item in path.rglob("*") if item.is_file())
    return []


def main() -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    with ZipFile(PACKAGE_PATH, mode="w", compression=ZIP_DEFLATED) as archive:
        for include_path in INCLUDE_PATHS:
            for file_path in iter_files(include_path):
                archive.write(file_path, file_path.relative_to(ROOT))
    print(PACKAGE_PATH)


if __name__ == "__main__":
    main()
