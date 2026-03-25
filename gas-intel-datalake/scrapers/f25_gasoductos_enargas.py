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
import json
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
COMPONENTS_PATH = PROCESSED_DIR / "red_tramos_enargas_componentes.parquet"
CANONICAL_MODELED_PATH = PROCESSED_DIR / "red_tramos_canonica.parquet"
BASE_MODELED_PATH = PROCESSED_DIR / "red_tramos.parquet"
MANUAL_CROSSWALK_PATH = TEMPLATES_DIR / "red_tramos_enargas_crosswalk.csv"
MANUAL_COMPONENT_SPECS_PATH = TEMPLATES_DIR / "red_tramos_enargas_componentes_specs.csv"
EARTH_RADIUS_KM = 6371.0088
COMPONENT_TYPE_PRIORITY = {
    "troncal": 0,
    "paralelo": 1,
    "loop": 2,
}


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
    columns = ["edge_id", "official_object_ids", "corridor_length_km_override", "notes"]
    if not MANUAL_CROSSWALK_PATH.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(MANUAL_CROSSWALK_PATH)
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df[columns].fillna(pd.NA)


def _load_manual_component_specs() -> pd.DataFrame:
    columns = [
        "edge_id",
        "official_object_id",
        "component_name",
        "pipe_count_override",
        "diameter_in_override",
        "diameter_m_override",
        "capacity_mm3_dia_override",
        "notes",
    ]
    if not MANUAL_COMPONENT_SPECS_PATH.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(MANUAL_COMPONENT_SPECS_PATH)
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


def _parse_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _component_sort_key(segment: dict[str, Any]) -> tuple[int, float, int]:
    tipo = _normalize_text(segment.get("tipo_tramo"))
    priority = COMPONENT_TYPE_PRIORITY.get(tipo, 99)
    length = float(segment.get("length_km_geodesic") or 0.0)
    object_id = int(segment.get("object_id") or 0)
    return priority, -length, object_id


def _choose_representative_component(segments: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not segments:
        return None
    return sorted(segments, key=_component_sort_key)[0]


def _format_component_summary(segments: list[dict[str, Any]]) -> str:
    if not segments:
        return ""
    grouped: dict[str, list[float]] = {}
    for segment in segments:
        label = str(segment.get("tipo_tramo") or "SinTipo").strip() or "SinTipo"
        grouped.setdefault(label, []).append(float(segment.get("length_km_geodesic") or 0.0))

    ordered_labels = sorted(
        grouped,
        key=lambda label: (COMPONENT_TYPE_PRIORITY.get(_normalize_text(label), 99), label),
    )
    chunks: list[str] = []
    for label in ordered_labels:
        lengths = sorted(grouped[label], reverse=True)
        serialized_lengths = ", ".join(f"{length:.1f}" for length in lengths)
        chunks.append(f"{len(lengths)}x {label} ({serialized_lengths} km)")
    return " + ".join(chunks)


def _serialize_components(segments: list[dict[str, Any]]) -> str:
    payload = [
        {
            "official_object_id": int(segment["object_id"]),
            "official_gasoducto": str(segment.get("gasoducto") or ""),
            "official_tramo": str(segment.get("tramo") or ""),
            "official_tipo": str(segment.get("tipo_tramo") or ""),
            "official_empresa": str(segment.get("empresa") or ""),
            "official_length_km": round(float(segment.get("length_km_geodesic") or 0.0), 6),
            "official_part_count": int(segment.get("official_part_count") or 0),
        }
        for segment in sorted(segments, key=_component_sort_key)
    ]
    return json.dumps(payload, ensure_ascii=True)


def _aggregate_resolved_segments(
    segments: list[dict[str, Any]],
    corridor_length_km_override: float | None,
) -> dict[str, Any]:
    representative = _choose_representative_component(segments)
    representative_length = (
        float(representative["length_km_geodesic"])
        if representative is not None
        else None
    )
    corridor_length = (
        corridor_length_km_override
        if corridor_length_km_override is not None
        else representative_length
    )
    total_component_length = (
        sum(float(segment["length_km_geodesic"]) for segment in segments)
        if segments
        else None
    )
    counts_by_type = {
        "official_troncal_component_count": 0,
        "official_paralelo_component_count": 0,
        "official_loop_component_count": 0,
    }
    for segment in segments:
        tipo_key = _normalize_text(segment.get("tipo_tramo"))
        if tipo_key == "troncal":
            counts_by_type["official_troncal_component_count"] += 1
        elif tipo_key == "paralelo":
            counts_by_type["official_paralelo_component_count"] += 1
        elif tipo_key == "loop":
            counts_by_type["official_loop_component_count"] += 1

    return {
        "official_object_ids": "|".join(str(int(segment["object_id"])) for segment in segments),
        "official_component_count": len(segments),
        "official_tramos": "|".join(sorted({str(segment["tramo"]) for segment in segments if segment.get("tramo")})),
        "official_gasoductos": "|".join(
            sorted({str(segment["gasoducto"]) for segment in segments if segment.get("gasoducto")})
        ),
        "official_tipos": "|".join(
            sorted({str(segment["tipo_tramo"]) for segment in segments if segment.get("tipo_tramo")})
        ),
        "official_representative_object_id": (
            int(representative["object_id"]) if representative is not None else None
        ),
        "official_representative_tipo": (
            str(representative.get("tipo_tramo") or "") if representative is not None else None
        ),
        "official_representative_gasoducto": (
            str(representative.get("gasoducto") or "") if representative is not None else None
        ),
        "official_corridor_length_km": corridor_length,
        "official_total_component_length_km": total_component_length,
        "official_length_km": corridor_length,
        "official_component_summary": _format_component_summary(segments),
        "official_components_json": _serialize_components(segments) if segments else "[]",
        "official_physical_pipe_count_assumed": len(segments),
        **counts_by_type,
    }


def _build_diagnostic(official_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    modeled_payload = _load_modeled_network()
    if modeled_payload is None:
        return pd.DataFrame(), pd.DataFrame()

    modeled_source_name, modeled_df = modeled_payload
    lookup = _build_match_lookup(official_df)
    official_by_id = {int(row["object_id"]): row for row in official_df.to_dict(orient="records")}
    manual_crosswalk = _load_manual_crosswalk()
    manual_component_specs = _load_manual_component_specs()
    manual_lookup = {
        str(row["edge_id"]): _parse_object_ids(row["official_object_ids"])
        for _, row in manual_crosswalk.iterrows()
        if str(row["edge_id"]).strip()
    }
    manual_length_lookup = {
        str(row["edge_id"]): _parse_float(row["corridor_length_km_override"])
        for _, row in manual_crosswalk.iterrows()
        if str(row["edge_id"]).strip()
    }
    manual_notes = {
        str(row["edge_id"]): None if pd.isna(row["notes"]) else str(row["notes"]).strip()
        for _, row in manual_crosswalk.iterrows()
        if str(row["edge_id"]).strip()
    }
    component_specs_lookup = {
        (str(row["edge_id"]), int(row["official_object_id"])): row
        for _, row in manual_component_specs.iterrows()
        if str(row["edge_id"]).strip() and not pd.isna(row["official_object_id"])
    }
    diagnostic_rows: list[dict[str, Any]] = []
    component_rows: list[dict[str, Any]] = []

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

        resolved_segments = sorted(
            [official_by_id[object_id] for object_id in resolved_object_ids],
            key=_component_sort_key,
        )
        aggregate = _aggregate_resolved_segments(
            resolved_segments,
            corridor_length_km_override=manual_length_lookup.get(str(row.get("edge_id"))),
        )
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
                **aggregate,
                "match_strategy": match_strategy,
                "match_status": match_status,
                "notes": manual_notes.get(str(row.get("edge_id"))),
                "source": "crosswalk_red_tramos_vs_enargas_gis",
            }
        )
        representative_object_id = aggregate.get("official_representative_object_id")
        total_component_length = aggregate.get("official_total_component_length_km")
        for component_rank, segment in enumerate(resolved_segments, start=1):
            object_id = int(segment["object_id"])
            spec = component_specs_lookup.get((str(row.get("edge_id")), object_id))
            component_length = float(segment.get("length_km_geodesic") or 0.0)
            component_rows.append(
                {
                    "modeled_source_table": modeled_source_name,
                    "edge_id": row.get("edge_id"),
                    "ruta": row.get("ruta"),
                    "gasoducto": row.get("gasoducto"),
                    "origen": row.get("origen"),
                    "destino": row.get("destino"),
                    "match_strategy": match_strategy,
                    "match_status": match_status,
                    "official_object_id": object_id,
                    "official_tramo": segment.get("tramo"),
                    "official_gasoducto": segment.get("gasoducto"),
                    "official_tipo": segment.get("tipo_tramo"),
                    "official_empresa": segment.get("empresa"),
                    "official_length_km": component_length,
                    "official_part_count": int(segment.get("official_part_count") or 0),
                    "component_rank": component_rank,
                    "is_representative_component": object_id == representative_object_id,
                    "official_length_share": (
                        component_length / total_component_length
                        if total_component_length and total_component_length > 0
                        else None
                    ),
                    "component_name": (
                        None
                        if spec is None or pd.isna(spec["component_name"])
                        else str(spec["component_name"]).strip()
                    ),
                    "pipe_count_override": (
                        None
                        if spec is None or pd.isna(spec["pipe_count_override"])
                        else float(spec["pipe_count_override"])
                    ),
                    "diameter_in_override": (
                        None
                        if spec is None or pd.isna(spec["diameter_in_override"])
                        else float(spec["diameter_in_override"])
                    ),
                    "diameter_m_override": (
                        None
                        if spec is None or pd.isna(spec["diameter_m_override"])
                        else float(spec["diameter_m_override"])
                    ),
                    "capacity_mm3_dia_override": (
                        None
                        if spec is None or pd.isna(spec["capacity_mm3_dia_override"])
                        else float(spec["capacity_mm3_dia_override"])
                    ),
                    "notes": (
                        None
                        if spec is None or pd.isna(spec["notes"])
                        else str(spec["notes"]).strip()
                    ),
                    "source": "crosswalk_red_tramos_vs_enargas_gis",
                }
            )

    diagnostic_df = pd.DataFrame(diagnostic_rows).sort_values(
        ["match_status", "gasoducto", "ruta"]
    ).reset_index(drop=True)
    components_df = pd.DataFrame(component_rows)
    if not components_df.empty:
        components_df = components_df.sort_values(
            ["gasoducto", "ruta", "component_rank", "official_object_id"]
        ).reset_index(drop=True)
    return diagnostic_df, components_df


def _save_snapshot(df: pd.DataFrame, table_name: str) -> Path:
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:8]
    path = SNAPSHOTS_DIR / f"{table_name}_{timestamp}_{digest}.parquet"
    df.to_parquet(path, index=False)
    log.info("Snapshot: %s (%s rows)", path, len(df))
    return path


def run() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    diagnostic_df, components_df = _build_diagnostic(official_segments_df)
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
        if not components_df.empty:
            components_df.to_parquet(COMPONENTS_PATH, index=False)
            log.info(
                "Saved processed: %s (%s rows, %s matched components)",
                COMPONENTS_PATH,
                len(components_df),
                len(components_df["edge_id"].drop_duplicates()),
            )
            _save_snapshot(components_df, "red_tramos_enargas_componentes")
    else:
        log.info("Modeled network not found yet. Skipping match diagnostic.")

    return official_df, diagnostic_df, components_df


if __name__ == "__main__":
    run()
