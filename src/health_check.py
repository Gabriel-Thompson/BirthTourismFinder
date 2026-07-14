from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

REQUIRED_FILES = [Path("local_osint.duckdb")]
REQUIRED_CSV_FILES = [
    Path("data/processed/anomaly_report.csv"),
    Path("data/processed/entities.csv"),
    Path("data/processed/relationships.csv"),
    Path("data/processed/canonical_entities.csv"),
    Path("data/processed/entity_aliases.csv"),
    Path("data/processed/entity_resolution_matches.csv"),
    Path("data/processed/canonical_relationships.csv"),
    Path("data/processed/fraud_markers.csv"),
    Path("data/processed/fraud_marker_summary.csv"),
    Path("data/processed/entity_risk.csv"),
    Path("data/processed/investigation_leads.csv"),
    Path("data/processed/entity_timelines.csv"),
    Path("data/processed/evidence_packets.csv"),
    Path("data/processed/network_clusters.csv"),
    Path("data/processed/network_summary.csv"),
    Path("data/processed/network_members.csv"),
    Path("data/processed/network_edges.csv"),
    Path("data/processed/cross_source_matches.csv"),
    Path("data/processed/cross_source_diagnostics.csv"),
    Path("data/processed/statistical_baselines.csv"),
    Path("data/processed/statistical_rarity.csv"),
    Path("data/processed/contextual_risk_adjustments.csv"),
    Path("data/processed/statistical_calibration_report.csv"),
    Path("data/processed/prioritized_leads.csv"),
    Path("data/processed/investigation_summary.csv"),
    Path("data/processed/lead_evidence_index.csv"),
    Path("data/processed/review_recommendations.csv"),
]
REQUIRED_JSON_FILES = [
    Path("data/processed/cross_source_diagnostic_summary.json"),
    Path("data/processed/statistical_marker_summary.json"),
    Path("data/processed/pipeline_profile.json"),
    Path("data/processed/pipeline_resume_state.json"),
]
CONFIG_FILES = [
    Path("config/entity_scoring.json"),
    Path("config/rules.json"),
    Path("config/sources.json"),
    Path("config/api_sources.json"),
    Path("config/entity_resolution.json"),
    Path("config/fraud_markers.json"),
    Path("config/network_detection.json"),
    Path("config/investigation_engine.json"),
    Path("config/cross_source.json"),
    Path("config/statistical_risk.json"),
    Path("config/dashboard.json"),
    Path("config/correlation_scoring.json"),
]
EXPORT_FILES = [
    Path("exports/high_risk_entities.csv"),
    Path("exports/lead_summary.csv"),
    Path("exports/lead_summary.json"),
    Path("exports/lead_summary.md"),
    Path("exports/lead_summary.html"),
]

REQUIRED_COLUMNS = {
    Path("data/processed/prioritized_leads.csv"): {
        "lead_id",
        "lead_type",
        "primary_entity_id",
        "risk_score",
        "confidence",
        "priority",
        "recommended_review",
        "source_names",
        "rarity_score",
        "highest_rarity_level",
        "rare_marker_count",
        "comparison_group",
    },
    Path("data/processed/investigation_summary.csv"): {
        "total_leads",
        "critical_leads",
        "average_risk",
        "average_confidence",
        "average_evidence_completeness",
    },
    Path("data/processed/lead_evidence_index.csv"): {
        "lead_id",
        "evidence_id",
        "evidence_type",
        "source_name",
        "evidence_summary",
        "confidence",
    },
    Path("data/processed/review_recommendations.csv"): {
        "lead_id",
        "priority",
        "confidence",
        "recommended_review",
        "status",
    },
    Path("data/processed/cross_source_matches.csv"): {
        "cross_source_match_id",
        "canonical_entity_id",
        "entity_type",
        "left_source_name",
        "right_source_name",
        "match_method",
        "decision",
        "contains_real_data",
    },
    Path("data/processed/cross_source_diagnostics.csv"): {
        "metric",
        "value",
    },
    Path("data/processed/statistical_baselines.csv"): {
        "marker_id",
        "comparison_group",
        "comparison_group_size",
        "observed_mean",
    },
    Path("data/processed/statistical_rarity.csv"): {
        "marker_id",
        "entity_id",
        "observed_value",
        "expected_value",
        "rarity_level",
        "comparison_group",
    },
    Path("data/processed/contextual_risk_adjustments.csv"): {
        "marker_id",
        "entity_id",
        "original_marker_score",
        "contextual_adjustment",
        "adjusted_marker_score",
    },
    Path("data/processed/statistical_calibration_report.csv"): {
        "metric",
        "value",
    },
}


def _check_csv_has_rows(path: Path) -> tuple[bool, Optional[str]]:
    if not path.exists():
        return False, f"Missing file: {path}"
    if path.stat().st_size == 0:
        return False, f"Empty file: {path}"
    try:
        frame = pd.read_csv(path)
        if len(frame) == 0:
            return False, f"CSV has header only and no data rows: {path}"
        return True, None
    except pd.errors.EmptyDataError:
        return False, f"CSV has no rows: {path}"
    except Exception as exc:
        return False, f"Unable to read CSV {path}: {exc}"


def _check_duplicate_ids(path: Path, id_column: str) -> tuple[bool, Optional[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return False, f"Missing or empty file for duplicate check: {path}"
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        return False, f"Unable to read duplicate-check file {path}: {exc}"
    if id_column not in frame.columns:
        return True, None
    duplicate_count = int(frame[id_column].astype(str).duplicated().sum())
    if duplicate_count:
        return False, f"Duplicate {id_column} values found in {path}: {duplicate_count}"
    return True, None


def _check_orphan_relationships(entities_path: Path, relationships_path: Path) -> tuple[bool, Optional[str]]:
    try:
        entities = pd.read_csv(entities_path)
        relationships = pd.read_csv(relationships_path)
    except Exception as exc:
        return False, f"Unable to validate orphan relationships: {exc}"
    if "entity_id" not in entities.columns:
        return True, None
    required_relationship_columns = {"source_entity_id", "target_entity_id"}
    if not required_relationship_columns.issubset(relationships.columns):
        return True, None
    entity_ids = set(entities["entity_id"].astype(str))
    missing = relationships[
        ~relationships["source_entity_id"].astype(str).isin(entity_ids)
        | ~relationships["target_entity_id"].astype(str).isin(entity_ids)
    ]
    if not missing.empty:
        return False, f"Orphan relationships found: {len(missing)}"
    return True, None


def _check_address_quality(entities_path: Path) -> tuple[bool, Optional[str]]:
    try:
        frame = pd.read_csv(entities_path)
    except Exception as exc:
        return False, f"Unable to validate address quality: {exc}"
    if "entity_type" not in frame.columns or "display_name" not in frame.columns:
        return True, None
    address_rows = frame[frame["entity_type"].astype(str) == "address"].copy()
    if address_rows.empty:
        return True, None
    malformed = address_rows["display_name"].astype(str).str.strip().eq("")
    if malformed.any():
        return False, f"Malformed addresses found: {int(malformed.sum())}"
    return True, None


def _check_required_columns(path: Path, required_columns: set[str]) -> tuple[bool, Optional[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return False, f"Missing or empty file for column validation: {path}"
    try:
        frame = pd.read_csv(path, nrows=5)
    except Exception as exc:
        return False, f"Unable to read CSV {path} for column validation: {exc}"
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        return False, f"Missing required columns in {path}: {', '.join(missing)}"
    return True, None


def _check_file_exists(path: Path) -> tuple[bool, Optional[str]]:
    if not path.exists():
        return False, f"Missing file: {path}"
    if path.stat().st_size == 0:
        return False, f"Empty file: {path}"
    return True, None


def check_project_health() -> tuple[bool, list[str]]:
    messages: list[str] = []
    failures: list[str] = []

    for path in REQUIRED_FILES:
        ok, message = _check_file_exists(path)
        if not ok and message:
            messages.append(f"FAIL: {message}")
            failures.append(message)
        else:
            messages.append(f"PASS: Found required file {path}")

    for path in REQUIRED_CSV_FILES:
        ok, message = _check_csv_has_rows(path)
        if not ok and message:
            messages.append(f"FAIL: {message}")
            failures.append(message)
        else:
            messages.append(f"PASS: CSV has rows {path}")
        if ok and path in REQUIRED_COLUMNS:
            columns_ok, columns_message = _check_required_columns(path, REQUIRED_COLUMNS[path])
            if not columns_ok and columns_message:
                messages.append(f"FAIL: {columns_message}")
                failures.append(columns_message)
            else:
                messages.append(f"PASS: Required columns present {path}")

    for path in REQUIRED_JSON_FILES:
        ok, message = _check_file_exists(path)
        if not ok and message:
            messages.append(f"FAIL: {message}")
            failures.append(message)
        else:
            messages.append(f"PASS: Found JSON file {path}")

    for path in CONFIG_FILES:
        ok, message = _check_file_exists(path)
        if not ok and message:
            messages.append(f"FAIL: {message}")
            failures.append(message)
        else:
            messages.append(f"PASS: Found config file {path}")

    for path in EXPORT_FILES:
        if path.exists():
            if path.suffix.lower() == ".csv":
                ok, message = _check_csv_has_rows(path)
                if not ok and message:
                    messages.append(f"FAIL: {message}")
                    failures.append(message)
                else:
                    messages.append(f"PASS: Optional export has rows {path}")
            else:
                ok, message = _check_file_exists(path)
                if not ok and message:
                    messages.append(f"FAIL: {message}")
                    failures.append(message)
                else:
                    messages.append(f"PASS: Optional export exists {path}")
        else:
            messages.append(f"PASS: Optional export not present {path}")

    duplicate_checks = [
        (Path("data/processed/entities.csv"), "entity_id"),
        (Path("data/processed/relationships.csv"), "relationship_id"),
        (Path("data/processed/canonical_entities.csv"), "canonical_entity_id"),
        (Path("data/processed/canonical_relationships.csv"), "relationship_id"),
        (Path("data/processed/cross_source_matches.csv"), "cross_source_match_id"),
    ]
    for path, column in duplicate_checks:
        ok, message = _check_duplicate_ids(path, column)
        if not ok and message:
            messages.append(f"FAIL: {message}")
            failures.append(message)
        else:
            messages.append(f"PASS: No duplicate {column} values in {path}")

    orphan_ok, orphan_message = _check_orphan_relationships(Path("data/processed/entities.csv"), Path("data/processed/relationships.csv"))
    if not orphan_ok and orphan_message:
        messages.append(f"FAIL: {orphan_message}")
        failures.append(orphan_message)
    else:
        messages.append("PASS: No orphan relationships in data/processed/relationships.csv")

    address_ok, address_message = _check_address_quality(Path("data/processed/entities.csv"))
    if not address_ok and address_message:
        messages.append(f"FAIL: {address_message}")
        failures.append(address_message)
    else:
        messages.append("PASS: No malformed addresses in data/processed/entities.csv")

    if failures:
        messages.append(f"SUMMARY: {len(failures)} checks failed.")
        messages.append("RECOMMENDED NEXT ACTION: Run `python src/run_pipeline.py --include-connectors --health-check` and fix missing inputs or configs.")
        return False, messages
    messages.append("SUMMARY: All required project health checks passed.")
    return True, messages


def main() -> None:
    passed, messages = check_project_health()
    print("Project Health Check")
    print("====================")
    for message in messages:
        print(f"- {message}")
    print("")
    if passed:
        print("PASS: Project health check succeeded.")
        sys.exit(0)
    print("FAIL: Project health check failed.")
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate local project health by checking generated artifacts.")
    return parser.parse_args()


if __name__ == "__main__":
    parse_args()
    main()
