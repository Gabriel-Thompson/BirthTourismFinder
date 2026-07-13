from __future__ import annotations

from datetime import datetime, timezone
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from src.connectors.source_manifest import MANIFEST_PATH

API_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "api_sources.json"
PROVENANCE_FIELDS = [
    "source_name",
    "source_type",
    "source_record_id",
    "connector_name",
    "import_batch_id",
    "imported_at",
    "jurisdiction",
    "is_synthetic",
]

SYNTHETIC_SOURCE_LABELS = {"synthetic", "business_entities", "properties", "known_patterns"}
LOCAL_SOURCE_ALIASES = {
    "sunbiz": "sunbiz_local_file",
    "sunbiz_local_file": "sunbiz_local_file",
    "sunbiz_daily": "sunbiz_daily_api",
    "sunbiz_daily_api": "sunbiz_daily_api",
    "county_property": "county_property_local_file",
    "county_property_local_file": "county_property_local_file",
    "county_clerk": "county_clerk_local_file",
    "county_clerk_local_file": "county_clerk_local_file",
    "manual": "manual_csv",
    "manual_csv": "manual_csv",
}
SOURCE_TYPE_ALIASES = {
    "generated_dataset": "synthetic",
    "local_generator": "synthetic",
    "synthetic": "synthetic",
    "manual_import": "manual",
    "manual": "manual",
    "manual_csv": "manual",
    "connector": "connector",
    "local_registry_extract": "connector",
    "local_property_export": "connector",
    "local_clerk_export": "connector",
    "official_api_demo": "api",
    "official_api": "api",
    "official_authenticated_api": "api",
    "api": "api",
    "official_arcgis_demo": "arcgis",
    "official_arcgis_candidate": "arcgis",
    "official_arcgis_public_parcels": "arcgis",
    "arcgis_api": "arcgis",
    "arcgis": "arcgis",
    "unknown": "unknown",
}


@lru_cache(maxsize=8)
def load_manifest_sources(path: Path | str = MANIFEST_PATH) -> Dict[str, Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


@lru_cache(maxsize=8)
def load_api_sources(path: Path | str = API_CONFIG_PATH) -> Dict[str, Dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    return data if isinstance(data, dict) else {}


def infer_source_metadata(source_hint: str | None) -> Dict[str, str]:
    hint = (source_hint or "").strip()
    if not hint:
        return {"source_name": "unknown", "source_type": "unknown"}

    lowered = hint.lower()
    if lowered in SYNTHETIC_SOURCE_LABELS:
        return {"source_name": "synthetic", "source_type": "synthetic"}

    canonical_local = LOCAL_SOURCE_ALIASES.get(lowered)
    if canonical_local == "sunbiz_daily_api":
        return {"source_name": canonical_local, "source_type": "api"}
    if canonical_local == "manual_csv":
        return {"source_name": canonical_local, "source_type": "manual"}
    if canonical_local is not None:
        return {"source_name": canonical_local, "source_type": "connector"}

    api_sources = load_api_sources()
    if hint in api_sources:
        config = api_sources[hint]
        endpoint = str(config.get("endpoint", "")).lower()
        base_url = str(config.get("base_url", "")).lower()
        combined = f"{base_url} {endpoint} {config.get('source_type', '')}".lower()
        source_type = "arcgis" if any(token in combined for token in ("arcgis", "featureserver", "mapserver")) else "api"
        return {"source_name": hint, "source_type": source_type}

    manifest_sources = load_manifest_sources()
    if hint in manifest_sources:
        source_config = manifest_sources[hint]
        access_method = str(source_config.get("access_method", "")).lower()
        if "manual" in access_method:
            source_type = "manual"
        elif "arcgis" in access_method:
            source_type = "arcgis"
        elif "api" in access_method:
            source_type = "api"
        elif source_config.get("live_access_allowed") is False:
            source_type = "connector"
        else:
            source_type = "unknown"
        return {"source_name": hint, "source_type": source_type}

    return {"source_name": hint, "source_type": "unknown"}


def standardize_source_type(value: str | None) -> str:
    lowered = (value or "").strip().lower()
    if not lowered:
        return "unknown"
    if lowered in SOURCE_TYPE_ALIASES:
        return SOURCE_TYPE_ALIASES[lowered]
    if lowered in {"api", "arcgis", "connector", "manual", "synthetic"}:
        return lowered
    if "arcgis" in lowered or "featureserver" in lowered or "mapserver" in lowered:
        return "arcgis"
    if "api" in lowered:
        return "api"
    if "manual" in lowered:
        return "manual"
    if "local" in lowered or "file" in lowered or "connector" in lowered:
        return "connector"
    if "synthetic" in lowered or "generated" in lowered:
        return "synthetic"
    return "unknown"


def _infer_manifest_entry(source_name: str) -> Dict[str, Any]:
    manifest_sources = load_manifest_sources()
    if source_name in manifest_sources:
        return manifest_sources[source_name]
    api_sources = load_api_sources()
    if source_name in api_sources:
        return api_sources[source_name]
    return {}


def build_import_batch_id(source_name: str, imported_at: str | None = None) -> str:
    timestamp = imported_at or datetime.now(timezone.utc).strftime("%Y%m%d")
    safe_source = (source_name or "unknown").replace(":", "_").replace("/", "_")
    return f"{safe_source}:{timestamp}"


def build_provenance(
    source_name_hint: str | None,
    *,
    source_type_hint: str | None = None,
    source_record_id: str | None = None,
    connector_name: str | None = None,
    imported_at: str | None = None,
    jurisdiction: str | None = None,
    is_synthetic: bool | None = None,
) -> Dict[str, str]:
    inferred = infer_source_metadata(source_name_hint)
    source_name = inferred["source_name"]
    manifest_entry = _infer_manifest_entry(source_name)
    resolved_source_type = standardize_source_type(
        source_type_hint or str(manifest_entry.get("source_type", "")) or inferred["source_type"]
    )
    imported_at_value = (
        imported_at
        or str(manifest_entry.get("imported_at", "")).strip()
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    jurisdiction_value = (
        jurisdiction
        or str(manifest_entry.get("jurisdiction", "")).strip()
        or str(manifest_entry.get("county", "")).strip()
        or str(manifest_entry.get("state", "")).strip()
        or ""
    )
    synthetic_value = is_synthetic if is_synthetic is not None else resolved_source_type == "synthetic"
    return {
        "source_name": source_name,
        "source_type": resolved_source_type,
        "source_record_id": str(source_record_id or "").strip(),
        "connector_name": str(connector_name or source_name).strip(),
        "import_batch_id": build_import_batch_id(source_name, imported_at_value[:10].replace("-", "")),
        "imported_at": imported_at_value,
        "jurisdiction": jurisdiction_value,
        "is_synthetic": "true" if synthetic_value else "false",
    }


def apply_provenance(
    row: Dict[str, Any],
    source_name_hint: str | None,
    *,
    source_type_hint: str | None = None,
    source_record_id: str | None = None,
    connector_name: str | None = None,
    imported_at: str | None = None,
    jurisdiction: str | None = None,
    is_synthetic: bool | None = None,
) -> Dict[str, Any]:
    enriched = dict(row)
    provenance = build_provenance(
        source_name_hint,
        source_type_hint=source_type_hint or str(row.get("source_type", "")),
        source_record_id=source_record_id or str(row.get("source_record_id", "")),
        connector_name=connector_name or str(row.get("connector_name", "")),
        imported_at=imported_at or str(row.get("imported_at", "")),
        jurisdiction=jurisdiction or str(row.get("jurisdiction", "")),
        is_synthetic=is_synthetic,
    )
    enriched.update(provenance)
    return enriched


def merge_source_values(*values: str) -> str:
    parts: list[str] = []
    for value in values:
        for token in str(value or "").split("|"):
            cleaned = token.strip()
            if cleaned and cleaned not in parts:
                parts.append(cleaned)
    return "|".join(parts)


def is_real_source_type(value: str | None) -> bool:
    real_types = {"connector", "api", "arcgis", "manual"}
    return any(standardize_source_type(token.strip()) in real_types for token in str(value or "").split("|"))
