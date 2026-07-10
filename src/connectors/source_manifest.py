from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

MANIFEST_PATH = Path(__file__).resolve().parents[2] / "config" / "sources.json"
REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIRED_FIELDS = {
    "source_name",
    "source_type",
    "access_method",
    "live_access_allowed",
    "terms_review_required",
    "review_document",
    "data_path",
    "processed_outputs",
    "notes",
}


def load_sources() -> Dict[str, Dict[str, Any]]:
    with MANIFEST_PATH.open("r", encoding="utf-8") as handle:
        sources = json.load(handle)
    if not isinstance(sources, dict):
        raise ValueError("Source manifest must be a JSON object keyed by source name.")
    for source_name, config in sources.items():
        if not isinstance(config, dict):
            raise ValueError(f"Source manifest entry '{source_name}' must be an object.")
        missing_fields = REQUIRED_FIELDS - set(config)
        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(f"Source manifest entry '{source_name}' is missing required fields: {missing}")
        if config["source_name"] != source_name:
            raise ValueError(f"Source manifest entry '{source_name}' must have matching source_name.")
        if not isinstance(config["processed_outputs"], list):
            raise ValueError(f"Source manifest entry '{source_name}' processed_outputs must be a list.")
    return sources


def validate_source(source_name: str, require_live_access: bool = False) -> Dict[str, Any]:
    sources = load_sources()
    if source_name not in sources:
        raise ValueError(f"Unknown source '{source_name}' in source manifest.")
    source = sources[source_name]
    if not isinstance(source["terms_review_required"], bool):
        raise ValueError(f"Source manifest entry '{source_name}' terms_review_required must be a boolean.")
    if not isinstance(source["live_access_allowed"], bool):
        raise ValueError(f"Source manifest entry '{source_name}' live_access_allowed must be a boolean.")

    review_document = Path(source["review_document"])
    if not review_document.is_absolute():
        candidate_paths = [
            MANIFEST_PATH.parent / review_document,
            REPO_ROOT / review_document,
        ]
        review_document = next((path for path in candidate_paths if path.exists()), candidate_paths[0])
    if not review_document.exists():
        raise ValueError(f"Source '{source_name}' is missing review document: {review_document}")

    if source["live_access_allowed"] and not source["terms_review_required"]:
        raise ValueError(
            f"Source '{source_name}' cannot allow live access without terms review requirements enabled."
        )
    if require_live_access and not source["live_access_allowed"]:
        raise ValueError(f"Source '{source_name}' is not approved for live access.")
    return source


def list_sources() -> list[str]:
    return sorted(load_sources())


def is_live_access_allowed(source_name: str) -> bool:
    source = validate_source(source_name)
    return bool(source["live_access_allowed"])


def ensure_local_only_path(source_name: str, path_value: Path | str) -> Path:
    candidate = Path(path_value)
    if is_live_access_allowed(source_name):
        return candidate

    raw_value = str(path_value).strip()
    is_windows_drive_path = len(raw_value) >= 2 and raw_value[1] == ":"
    parsed = urlparse(raw_value)
    if parsed.scheme and parsed.scheme not in {"file"} and not is_windows_drive_path:
        raise ValueError(f"Source '{source_name}' does not permit live or remote access: {raw_value}")
    if raw_value.startswith("\\\\"):
        raise ValueError(f"Source '{source_name}' does not permit network paths: {raw_value}")
    return candidate
