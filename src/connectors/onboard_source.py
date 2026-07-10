from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.open_data_api import API_CONFIG_PATH
from src.connectors.source_manifest import MANIFEST_PATH, REPO_ROOT

READY_FOR_SAMPLE_TEST = "READY_FOR_SAMPLE_TEST"
NEEDS_SOURCE_REVIEW = "NEEDS_SOURCE_REVIEW"
LIVE_ACCESS_DISABLED = "LIVE_ACCESS_DISABLED"
CONFIG_INCOMPLETE = "CONFIG_INCOMPLETE"


def load_manifest_sources(path: Path | str | None = None) -> Dict[str, Dict[str, Any]]:
    manifest_path = Path(path) if path is not None else MANIFEST_PATH
    with manifest_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Source manifest must be a JSON object keyed by source name.")
    return data


def load_api_source_config(path: Path | str | None = None) -> Dict[str, Dict[str, Any]]:
    api_config_path = Path(path) if path is not None else API_CONFIG_PATH
    with api_config_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("API source config must be a JSON object keyed by source name.")
    return data


def resolve_review_document(path_value: str) -> Path:
    review_document = Path(path_value)
    if review_document.is_absolute():
        return review_document
    candidate_paths = [
        MANIFEST_PATH.parent / review_document,
        REPO_ROOT / review_document,
    ]
    return next((path for path in candidate_paths if path.exists()), candidate_paths[0])


def build_safe_test_command(source_name: str, source_config: Dict[str, Any], api_config: Dict[str, Any] | None) -> str:
    if api_config is not None:
        endpoint = str(api_config.get("endpoint", "")).lower()
        base_url = str(api_config.get("base_url", "")).lower()
        combined = f"{base_url} {endpoint}"
        if "arcgis" in combined or "featureserver" in combined or "mapserver" in combined:
            return f"python src/connectors/arcgis/arcgis_connector.py --source {source_name}"
        return f"python src/connectors/open_data_api.py --source {source_name}"

    access_method = str(source_config.get("access_method", "")).lower()
    data_path = str(source_config.get("data_path", "")).lower()
    if "sunbiz" in source_name:
        return "python src/connectors/sunbiz/local_file_connector.py"
    if "county_property" in source_name or "property" in data_path:
        return "python src/connectors/county_property/local_file_connector.py"
    if "county_clerk" in source_name or "clerk" in data_path:
        return "python src/connectors/county_clerk/local_file_connector.py"
    if "manual" in access_method:
        return "python src/connectors/manual_csv.py"
    return f"Review source config for {source_name} before running a connector."


def assess_source(source_name: str) -> Dict[str, Any]:
    sources = load_manifest_sources()
    api_sources = load_api_source_config()
    source_config = sources.get(source_name)
    api_config = api_sources.get(source_name)
    is_api_source = api_config is not None

    checks: list[dict[str, str]] = []
    config_failures = 0
    review_failures = 0

    def add_check(ok: bool, label: str, detail: str, *, category: str = "config") -> None:
        nonlocal config_failures, review_failures
        checks.append({"status": "PASS" if ok else "FAIL", "label": label, "detail": detail})
        if ok:
            return
        if category == "review":
            review_failures += 1
        else:
            config_failures += 1

    add_check(
        source_config is not None or api_config is not None,
        "source_exists",
        "Source found in config/sources.json or config/api_sources.json.",
    )

    if source_config is None:
        add_check(False, "manifest_entry", "Source is missing from config/sources.json.")
    else:
        add_check(True, "manifest_entry", "Source is documented in config/sources.json.")

    if is_api_source:
        add_check(True, "api_config_entry", "Source is documented in config/api_sources.json.")
    else:
        add_check(True, "api_config_entry", "API config is not required for this local-only source.")

    review_exists = False
    if source_config is not None and source_config.get("review_document"):
        review_path = resolve_review_document(str(source_config["review_document"]))
        review_exists = review_path.exists()
        add_check(review_exists, "review_document", f"Review document path: {review_path}", category="review")
    else:
        add_check(False, "review_document", "No review document is configured.", category="review")

    live_access: Any = None
    if source_config is not None:
        live_access = source_config.get("live_access_allowed")
        add_check(isinstance(live_access, bool), "live_access_allowed", f"live_access_allowed={live_access!r}")

        terms_required = source_config.get("terms_review_required")
        add_check(
            isinstance(terms_required, bool),
            "terms_review_required",
            f"terms_review_required={terms_required!r}",
            category="review",
        )

        access_method = str(source_config.get("access_method", "")).strip()
        add_check(bool(access_method), "access_method", f"access_method={access_method or '<missing>'}")

        outputs = source_config.get("processed_outputs", [])
        add_check(
            isinstance(outputs, list) and len(outputs) > 0,
            "processed_outputs",
            f"processed_outputs={outputs!r}",
        )
    else:
        add_check(False, "live_access_allowed", "No manifest entry to inspect.")
        add_check(False, "terms_review_required", "No manifest entry to inspect.", category="review")
        add_check(False, "access_method", "No manifest entry to inspect.")
        add_check(False, "processed_outputs", "No manifest entry to inspect.")

    if is_api_source:
        field_map = api_config.get("field_map", {})
        add_check(
            isinstance(field_map, dict) and len(field_map) > 0,
            "field_map",
            f"field_map keys={sorted(field_map.keys()) if isinstance(field_map, dict) else field_map!r}",
        )
    else:
        add_check(True, "field_map", "Field mapping is not required for this local-only source.")

    safe_test_command = build_safe_test_command(source_name, source_config or {}, api_config)

    if config_failures > 0:
        readiness = CONFIG_INCOMPLETE
    elif review_failures > 0:
        readiness = NEEDS_SOURCE_REVIEW
    elif live_access is False:
        readiness = LIVE_ACCESS_DISABLED
    else:
        readiness = READY_FOR_SAMPLE_TEST

    return {
        "source_name": source_name,
        "readiness_status": readiness,
        "safe_test_command": safe_test_command,
        "checks": checks,
    }


def print_report(report: Dict[str, Any]) -> None:
    print(f"Source Onboarding Check: {report['source_name']}")
    print("Checks:")
    for check in report["checks"]:
        print(f"- {check['status']}: {check['label']} - {check['detail']}")
    print("")
    print(f"Recommended safe test command: {report['safe_test_command']}")
    print(f"Final readiness status: {report['readiness_status']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate onboarding readiness for a configured public data source.")
    parser.add_argument("--source", required=True, help="Source name from config/sources.json or config/api_sources.json")
    args = parser.parse_args()

    report = assess_source(args.source)
    print_report(report)


if __name__ == "__main__":
    main()
