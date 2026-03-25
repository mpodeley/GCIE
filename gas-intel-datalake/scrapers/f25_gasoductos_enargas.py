"""
F25 - Gasoductos ENARGAS (GIS oficial)
Source: ENARGAS transparencia / datos.gob.ar mirror
Tier 1 - Automated
Tables:
  - red_gasoductos_enargas_oficial
  - red_gasoductos_enargas_vs_modelada

This scraper materializes the official ENARGAS transport pipeline GIS into a
queryable parquet layer. When the modeled network already exists, it also emits
an auditable string-match diagnostic to help sanitize route names and identify
gaps between the official GIS and the F20/F20b network.
"""

from __future__ import annotations

import hashlib
import logging
import math
import shutil
import subprocess
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import shapefile


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATASET_PAGE_URL = "https://transparencia.enargas.gob.ar/dataset/informacion-geografica-del-enargas"
RAR_URL = (
    "https://transparencia.enargas.gov.ar/dataset/08f48897-7b07-4572-8739-a707c55f14cd/"
    "resource/e3e3c428-3317-4bac-aee6-c041fc0883fd/download/gasoductos.rar"
)
CSV_URL = (
    "https://transparencia.enargas.gov.ar/dataset/08f48897-7b07-4572-8739-a707c55f14cd/"
    "resource/f4fe993a-5e76-428a-8e95-a480253c203c/download/gasoductos.csv"
)

ROOT_DIR = Path(__file__).parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw" / "enargas"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SNAPSHOTS_DIR = ROOT_DIR / "data" / "snapshots"
TEMPLATES_DIR = ROOT_DIR / "templates"
EXTRACT_DIR = RAW_DIR / "gasoductos_enargas"
SHP_PATH = EXTRACT_DIR / "Gasoductos" / "Gasoductos_del_Sistema_de_Transporte.shp"
CSV_PATH = RAW_DIR / "gasoductos_enargas.csv"
RAR_PATH = RAW_DIR / "gasoductos_enargas.rar"
OUTPUT_PATH = PROCESSED_DIR / "red_gasoductos_enargas_oficial.parquet"
DIAGNOSTIC_PATH = PROCESSED_DIR / "red_gasoductos_enargas_vs_modelada.parquet"
CANONICAL_MODELED_PATH = PROCESSED_DIR / "red_tramos_canonica.parquet"
BASE_MODELED_PATH = PROCESSED_DIR / "red_tramos.parquet"
MANUAL_CROSSWALK_PATH = TEMPLATES_DIR / "red_tramos_enargas_crosswalk.csv"
EARTH_RADIUS_KM = 6371.0088


def _download_file(url: str, destination: Path) -> Path:
    log.info("Downloading %s", url)
    response = requests.get(url, timeout=180)
    response.raise_for_status()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    log.info("Saved raw file: %s (%s bytes)", destination, len(response.content))
    return destination


def _extract_archive() -> Path:
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    extractors: list[tuple[str, list[str]]] = [
        ("tar", ["-xf", str(RAR_PATH), "-C", str(EXTRACT_DIR)]),
        ("bsdtar", ["-xf", str(RAR_PATH), "-C", str(EXTRACT_DIR)]),
        ("7z", ["x", str(RAR_PATH), f"-o{EXTRACT_DIR}", "-y"]),
        ("7za", ["x", str(RAR_PATH), f"-o{EXTRACT_DIR}", "-y"]),
        ("unar", ["-output-directory", str(EXTRACT_DIR), str(RAR_PATH)]),
    ]

    for executable, args in extractors:
        if shutil.which(executable) is None:
            continue
        try:
            subprocess.run([executable, *args], check=True, capture_output=True, text=True)
            if SHP_PATH.exists():
                log.info("Extracted GIS archive with %s", executable)
                return SHP_PATH
        except subprocess.CalledProcessError as exc:
            detail = exc.stderr.strip() or exc.stdout.strip()
            log.warning("Extractor %s failed: %s", executable, detail)

    raise RuntimeError(
        "Could not extract gasoductos.rar. Install bsdtar, 7z, or unar, or extract the shapefile into "
        f"{EXTRACT_DIR} manually."
    )


def _normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    normalized = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in normalized)
    return " ".join(cleaned.split())


def _reverse_tramo_key(value: str) -> str:
    if " - " in value:
        parts = [part.strip() for part in value.split(" - ") if part.strip()]
    elif "-" in value:
        parts = [part.strip() for part in value.split("-") if part.strip()]
    else:
        parts = [part.strip() for part in value.split() if part.strip()]
    if len(parts) == 2:
        return _normalize_text(f"{parts[1]} - {parts[0]}")
    return _normalize_text(value)


def _haversine_km(lon_a: float, lat_a: float, lon_b: float, lat_b: float) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, [lon_a, lat_a, lon_b, lat_b])
    delta_lon = lon2 - lon1
    delta_lat = lat2 - lat1
    step = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(step))


def _polyline_length_km(points: list[tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    return sum(
        _haversine_km(points[index][0], points[index][1], points[index + 1][0], points[index + 1][1])
        for index in range(len(points) - 1)
    )


def _linestring_wkt(points: list[tuple[float, float]]) -> str:
    serialized = ", ".join(f"{lon:.6f} {lat:.6f}" for lon, lat in points)
    return f"LINESTRING ({serialized})"


def _read_official_gis() -> pd.DataFrame:
    reader = shapefile.Reader(str(SHP_PATH), encoding="utf-8")
    rows: list[dict[str, Any]] = []

    for record_index, shape_record in enumerate(reader.iterShapeRecords()):
        attributes = shape_record.record.as_dict()
        shape = shape_record.shape
        parts = list(shape.parts) + [len(shape.points)]
        for part_index in range(len(parts) - 1):
            start = parts[part_index]
            end = parts[part_index + 1]
            points = [(float(lon), float(lat)) for lon, lat in shape.points[start:end]]
            if len(points) < 2:
                continue
            bbox_min_lon, bbox_min_lat, bbox_max_lon, bbox_max_lat = shape.bbox
            tramo = str(attributes.get("Tramo") or "").strip()
            gasoducto = str(attributes.get("Gasoducto") or "").strip()
            rows.append(
                {
                    "object_id": int(attributes["OBJECTID"]),
                    "record_index": record_index,
                    "part_index": part_index,
                    "part_count": len(parts) - 1,
                    "gasoducto": gasoducto,
                    "tramo": tramo,
                    "tipo_tramo": str(attributes.get("Tipo") or "").strip(),
                    "empresa": str(attributes.get("Empresa") or "").strip(),
                    "tramo_key": _normalize_text(tramo),
                    "tramo_key_reverse": _reverse_tramo_key(tramo),
                    "gasoducto_key": _normalize_text(gasoducto),
                    "point_count": len(points),
                    "start_lon": points[0][0],
                    "start_lat": points[0][1],
                    "end_lon": points[-1][0],
                    "end_lat": points[-1][1],
                    "bbox_min_lon": float(bbox_min_lon),
                    "bbox_min_lat": float(bbox_min_lat),
                    "bbox_max_lon": float(bbox_max_lon),
                    "bbox_max_lat": float(bbox_max_lat),
                    "length_km_geodesic": _polyline_length_km(points),
                    "geometry_wkt": _linestring_wkt(points),
                    "dataset_page": DATASET_PAGE_URL,
                    "source": "enargas_gis_gasoductos",
                }
            )

    if not rows:
        raise RuntimeError("Official ENARGAS gasoductos GIS produced no records.")

    return pd.DataFrame(rows).sort_values(["gasoducto", "tramo", "part_index"]).reset_index(drop=True)


def _build_official_segments(official_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        official_df.groupby(
            ["object_id", "gasoducto", "tramo", "tipo_tramo", "empresa", "tramo_key", "tramo_key_reverse", "gasoducto_key"],
            dropna=False,
        )
        .agg(
            official_part_count=("part_index", "count"),
            point_count=("point_count", "sum"),
            length_km_geodesic=("length_km_geodesic", "sum"),
        )
        .reset_index()
    )
    return grouped.sort_values(["gasoducto", "tramo", "object_id"]).reset_index(drop=True)


def _load_modeled_network() -> tuple[str, pd.DataFrame] | None:
    if CANONICAL_MODELED_PATH.exists():
        return "red_tramos_canonica", pd.read_parquet(CANONICAL_MODELED_PATH)
    if BASE_MODELED_PATH.exists():
        return "red_tramos", pd.read_parquet(BASE_MODELED_PATH)
    return None


def _build_match_lookup(official_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in official_df.to_dict(orient="records"):
        for key in (row["tramo_key"], row["tramo_key_reverse"]):
            if not key:
                continue
            lookup.setdefault(key, []).append(row)
    return lookup


def _load_manual_crosswalk() -> pd.DataFrame:
    columns = ["edge_id", "official_object_ids", "notes"]
    if not MANUAL_CROSSWALK_PATH.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(MANUAL_CROSSWALK_PATH)
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns].fillna(pd.NA)


def _parse_object_ids(value: Any) -> list[int]:
    if value is None or pd.isna(value):
        return []
    parsed: list[int] = []
    for chunk in str(value).replace(",", "|").split("|"):
        cleaned = chunk.strip()
        if not cleaned:
            continue
        parsed.append(int(cleaned))
    return parsed


def _build_diagnostic(official_df: pd.DataFrame) -> pd.DataFrame:
    modeled_payload = _load_modeled_network()
    if modeled_payload is None:
        return pd.DataFrame()

    modeled_source_name, modeled_df = modeled_payload
    lookup = _build_match_lookup(official_df)
    official_by_id = {int(row["object_id"]): row for row in official_df.to_dict(orient="records")}
    manual_crosswalk = _load_manual_crosswalk()
    manual_lookup = {
        str(row["edge_id"]): _parse_object_ids(row["official_object_ids"])
        for _, row in manual_crosswalk.iterrows()
        if str(row["edge_id"]).strip()
    }
    manual_notes = {
        str(row["edge_id"]): None if pd.isna(row["notes"]) else str(row["notes"]).strip()
        for _, row in manual_crosswalk.iterrows()
        if str(row["edge_id"]).strip()
    }
    diagnostic_rows: list[dict[str, Any]] = []

    for row in modeled_df.to_dict(orient="records"):
        route_key = _normalize_text(row.get("ruta"))
        auto_matches = lookup.get(route_key, [])
        auto_object_ids = sorted({int(match["object_id"]) for match in auto_matches})
        manual_object_ids = [
            object_id for object_id in manual_lookup.get(str(row.get("edge_id")), []) if object_id in official_by_id
        ]

        if manual_object_ids:
            resolved_object_ids = manual_object_ids
            match_strategy = "manual"
            match_status = "resolved_manual"
        elif len(auto_object_ids) == 1:
            resolved_object_ids = auto_object_ids
            match_strategy = "auto_exact_unique"
            match_status = "resolved_auto"
        elif len(auto_object_ids) > 1:
            resolved_object_ids = []
            match_strategy = "auto_exact_ambiguous"
            match_status = "needs_manual_resolution"
        else:
            resolved_object_ids = []
            match_strategy = "none"
            match_status = "unmatched"

        resolved_segments = [official_by_id[object_id] for object_id in resolved_object_ids]
        diagnostic_rows.append(
            {
                "modeled_source_table": modeled_source_name,
                "edge_id": row.get("edge_id"),
                "ruta": row.get("ruta"),
                "gasoducto": row.get("gasoducto"),
                "origen": row.get("origen"),
                "destino": row.get("destino"),
                "route_key": route_key,
                "auto_match_object_count": len(auto_object_ids),
                "auto_match_object_ids": "|".join(str(object_id) for object_id in auto_object_ids),
                "resolved_object_count": len(resolved_object_ids),
                "official_object_ids": "|".join(str(object_id) for object_id in resolved_object_ids),
                "official_tramos": "|".join(sorted({segment["tramo"] for segment in resolved_segments})),
                "official_gasoductos": "|".join(sorted({segment["gasoducto"] for segment in resolved_segments})),
                "official_tipos": "|".join(sorted({segment["tipo_tramo"] for segment in resolved_segments})),
                "official_length_km": (
                    sum(float(segment["length_km_geodesic"]) for segment in resolved_segments)
                    if resolved_segments
                    else None
                ),
                "match_strategy": match_strategy,
                "match_status": match_status,
                "notes": manual_notes.get(str(row.get("edge_id"))),
                "source": "crosswalk_red_tramos_vs_enargas_gis",
            }
        )

    return pd.DataFrame(diagnostic_rows).sort_values(["match_status", "gasoducto", "ruta"]).reset_index(drop=True)


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    _download_file(RAR_URL, RAR_PATH)
    _download_file(CSV_URL, CSV_PATH)
    _extract_archive()

    official_df = _read_official_gis()
    official_segments_df = _build_official_segments(official_df)
    official_df.to_parquet(OUTPUT_PATH, index=False)
    log.info(
        "Saved processed: %s (%s rows, %s unique tramos)",
        OUTPUT_PATH,
        len(official_df),
        official_segments_df["tramo_key"].nunique(),
    )
    _save_snapshot(official_df, "red_gasoductos_enargas_oficial")

    diagnostic_df = _build_diagnostic(official_segments_df)
    if not diagnostic_df.empty:
        diagnostic_df.to_parquet(DIAGNOSTIC_PATH, index=False)
        matched_count = int(diagnostic_df["match_status"].isin(["resolved_auto", "resolved_manual"]).sum())
        log.info(
            "Saved processed: %s (%s rows, %s resolved, %s unresolved)",
            DIAGNOSTIC_PATH,
            len(diagnostic_df),
            matched_count,
            len(diagnostic_df) - matched_count,
        )
        _save_snapshot(diagnostic_df, "red_gasoductos_enargas_vs_modelada")
    else:
        log.info("Modeled network not found yet. Skipping match diagnostic.")

    return official_df, diagnostic_df


if __name__ == "__main__":
    run()
