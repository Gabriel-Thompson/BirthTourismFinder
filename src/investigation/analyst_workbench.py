from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.connectors.source_manifest import REPO_ROOT

DASHBOARD_CONFIG_PATH = REPO_ROOT / "config" / "dashboard.json"
SAVED_SEARCHES_PATH = REPO_ROOT / "data" / "processed" / "dashboard_saved_searches.json"
ANALYST_STATE_PATH = REPO_ROOT / "data" / "processed" / "analyst_lead_state.csv"
ANALYST_HISTORY_PATH = REPO_ROOT / "data" / "processed" / "analyst_history.csv"

DEFAULT_DASHBOARD_CONFIG: dict[str, Any] = {
    "theme": "local_workbench",
    "page_size": 25,
    "real_data_only": False,
    "default_navigation": "Overview",
    "default_filters": {
        "priority": "All",
        "status": "All",
        "source_scope": "all",
    },
    "default_views": {
        "queue_sort": "priority",
        "reports_format": "markdown",
    },
}

ANALYST_STATE_COLUMNS = [
    "lead_id",
    "status",
    "analyst_notes",
    "reviewer",
    "review_date",
    "disposition",
    "follow_up_needed",
    "bookmark",
    "priority_override",
    "needs_review",
    "last_reviewed_at",
    "last_exported_at",
    "updated_at",
]

ANALYST_HISTORY_COLUMNS = [
    "lead_id",
    "event_type",
    "previous_status",
    "new_status",
    "reviewer",
    "note",
    "occurred_at",
    "details",
]


def utc_now_string() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def load_dashboard_config(path: Path | str = DASHBOARD_CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists() or config_path.stat().st_size == 0:
        return dict(DEFAULT_DASHBOARD_CONFIG)
    with config_path.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        return dict(DEFAULT_DASHBOARD_CONFIG)
    merged = dict(DEFAULT_DASHBOARD_CONFIG)
    for key in ["default_filters", "default_views"]:
        merged[key] = dict(DEFAULT_DASHBOARD_CONFIG.get(key, {}))
        merged[key].update(raw.get(key, {}) if isinstance(raw.get(key), dict) else {})
    for key, value in raw.items():
        if key not in {"default_filters", "default_views"}:
            merged[key] = value
    return merged


def load_saved_searches(path: Path | str = SAVED_SEARCHES_PATH) -> list[dict[str, Any]]:
    saved_searches_path = Path(path)
    if not saved_searches_path.exists() or saved_searches_path.stat().st_size == 0:
        return []
    with saved_searches_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def save_saved_searches(searches: list[dict[str, Any]], path: Path | str = SAVED_SEARCHES_PATH) -> None:
    saved_searches_path = Path(path)
    saved_searches_path.parent.mkdir(parents=True, exist_ok=True)
    with saved_searches_path.open("w", encoding="utf-8") as handle:
        json.dump(searches, handle, indent=2)


def _normalize_state_frame(state_df: pd.DataFrame) -> pd.DataFrame:
    normalized = state_df.copy() if not state_df.empty else pd.DataFrame(columns=ANALYST_STATE_COLUMNS)
    for column in ANALYST_STATE_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = ""
    return normalized[ANALYST_STATE_COLUMNS]


def load_analyst_state(path: Path | str = ANALYST_STATE_PATH) -> pd.DataFrame:
    state_path = Path(path)
    if not state_path.exists() or state_path.stat().st_size == 0:
        return pd.DataFrame(columns=ANALYST_STATE_COLUMNS)
    return _normalize_state_frame(pd.read_csv(state_path))


def load_analyst_history(path: Path | str = ANALYST_HISTORY_PATH) -> pd.DataFrame:
    history_path = Path(path)
    if not history_path.exists() or history_path.stat().st_size == 0:
        return pd.DataFrame(columns=ANALYST_HISTORY_COLUMNS)
    history_df = pd.read_csv(history_path)
    for column in ANALYST_HISTORY_COLUMNS:
        if column not in history_df.columns:
            history_df[column] = ""
    return history_df[ANALYST_HISTORY_COLUMNS]


def build_history_entry(
    lead_id: str,
    event_type: str,
    *,
    previous_status: str = "",
    new_status: str = "",
    reviewer: str = "",
    note: str = "",
    details: str = "",
    occurred_at: str | None = None,
) -> dict[str, str]:
    return {
        "lead_id": str(lead_id),
        "event_type": str(event_type),
        "previous_status": str(previous_status),
        "new_status": str(new_status),
        "reviewer": str(reviewer),
        "note": str(note),
        "occurred_at": occurred_at or utc_now_string(),
        "details": str(details),
    }


def merge_analyst_state_with_leads(
    prioritized_leads_df: pd.DataFrame,
    analyst_state_df: pd.DataFrame,
    analyst_history_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    normalized_state = _normalize_state_frame(analyst_state_df)
    history_df = load_analyst_history() if analyst_history_df is None else load_analyst_history_from_df(analyst_history_df)
    existing_lookup = normalized_state.set_index("lead_id", drop=False) if not normalized_state.empty else pd.DataFrame()
    merged_rows: list[dict[str, Any]] = []
    state_rows: list[dict[str, Any]] = []
    new_history_rows: list[dict[str, str]] = []
    known_created = set()
    if not history_df.empty:
        known_created = set(
            history_df[history_df["event_type"].astype(str) == "CREATED"]["lead_id"].astype(str).tolist()
        )

    for _, lead in prioritized_leads_df.fillna("").iterrows():
        lead_id = str(lead.get("lead_id", "")).strip()
        if not lead_id:
            continue
        existing = None
        if not existing_lookup.empty and lead_id in existing_lookup.index:
            existing = existing_lookup.loc[lead_id]
            if isinstance(existing, pd.DataFrame):
                existing = existing.iloc[0]
        merged = lead.to_dict()
        merged["status"] = str(existing.get("status", "NEW")) if existing is not None else "NEW"
        merged["analyst_notes"] = str(existing.get("analyst_notes", "")) if existing is not None else ""
        merged["reviewer"] = str(existing.get("reviewer", "")) if existing is not None else ""
        merged["review_date"] = str(existing.get("review_date", "")) if existing is not None else ""
        merged["disposition"] = str(existing.get("disposition", "")) if existing is not None else ""
        merged["follow_up_needed"] = str(existing.get("follow_up_needed", "")) if existing is not None else ""
        merged["bookmark"] = str(existing.get("bookmark", "false")) if existing is not None else "false"
        merged["priority_override"] = str(existing.get("priority_override", "")) if existing is not None else ""
        merged["needs_review"] = str(existing.get("needs_review", "true")) if existing is not None else "true"
        merged["last_reviewed_at"] = str(existing.get("last_reviewed_at", "")) if existing is not None else ""
        merged["last_exported_at"] = str(existing.get("last_exported_at", "")) if existing is not None else ""
        merged["updated_at"] = str(existing.get("updated_at", utc_now_string())) if existing is not None else utc_now_string()
        merged_rows.append(merged)
        state_rows.append({column: merged.get(column, "") for column in ANALYST_STATE_COLUMNS})
        if lead_id not in known_created:
            new_history_rows.append(
                build_history_entry(
                    lead_id,
                    "CREATED",
                    new_status=str(merged.get("status", "NEW")),
                    reviewer=str(merged.get("reviewer", "")),
                    note=str(merged.get("analyst_notes", "")),
                    details=str(merged.get("title", merged.get("primary_entity_id", ""))),
                )
            )

    merged_df = pd.DataFrame(merged_rows)
    state_df = pd.DataFrame(state_rows, columns=ANALYST_STATE_COLUMNS)
    combined_history = pd.concat([history_df, pd.DataFrame(new_history_rows, columns=ANALYST_HISTORY_COLUMNS)], ignore_index=True)
    return merged_df, state_df, combined_history


def load_analyst_history_from_df(history_df: pd.DataFrame) -> pd.DataFrame:
    normalized = history_df.copy() if not history_df.empty else pd.DataFrame(columns=ANALYST_HISTORY_COLUMNS)
    for column in ANALYST_HISTORY_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = ""
    return normalized[ANALYST_HISTORY_COLUMNS]


def persist_analyst_state(
    state_df: pd.DataFrame,
    history_df: pd.DataFrame,
    *,
    state_path: Path | str = ANALYST_STATE_PATH,
    history_path: Path | str = ANALYST_HISTORY_PATH,
) -> None:
    analyst_state_path = Path(state_path)
    analyst_history_path = Path(history_path)
    analyst_state_path.parent.mkdir(parents=True, exist_ok=True)
    analyst_history_path.parent.mkdir(parents=True, exist_ok=True)
    _normalize_state_frame(state_df).to_csv(analyst_state_path, index=False)
    load_analyst_history_from_df(history_df).to_csv(analyst_history_path, index=False)


def record_export_history(
    lead_ids: list[str],
    *,
    analyst_state_df: pd.DataFrame,
    analyst_history_df: pd.DataFrame,
    reviewer: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    state_df = _normalize_state_frame(analyst_state_df)
    history_df = load_analyst_history_from_df(analyst_history_df)
    state_lookup = state_df.set_index("lead_id", drop=False) if not state_df.empty else pd.DataFrame()
    event_rows: list[dict[str, str]] = []
    for lead_id in [str(value).strip() for value in lead_ids if str(value).strip()]:
        if not state_lookup.empty and lead_id in state_lookup.index:
            row_index = state_lookup.index.get_loc(lead_id)
            if isinstance(row_index, slice):
                row_index = row_index.start
            state_df.loc[row_index, "last_exported_at"] = utc_now_string()
            state_df.loc[row_index, "updated_at"] = utc_now_string()
        event_rows.append(build_history_entry(lead_id, "EXPORTED", reviewer=reviewer))
    if event_rows:
        history_df = pd.concat([history_df, pd.DataFrame(event_rows, columns=ANALYST_HISTORY_COLUMNS)], ignore_index=True)
    return state_df, history_df


def update_analyst_record(
    state_df: pd.DataFrame,
    history_df: pd.DataFrame,
    *,
    lead_id: str,
    updates: dict[str, Any],
    reviewer: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized_state = _normalize_state_frame(state_df)
    normalized_history = load_analyst_history_from_df(history_df)
    lead_id = str(lead_id).strip()
    if not lead_id:
        return normalized_state, normalized_history
    if normalized_state.empty or lead_id not in normalized_state["lead_id"].astype(str).tolist():
        new_row = {column: "" for column in ANALYST_STATE_COLUMNS}
        new_row["lead_id"] = lead_id
        new_row["status"] = "NEW"
        normalized_state = pd.concat([normalized_state, pd.DataFrame([new_row])], ignore_index=True)
    row_index = normalized_state.index[normalized_state["lead_id"].astype(str) == lead_id][0]
    previous_status = str(normalized_state.loc[row_index, "status"])
    for key, value in updates.items():
        if key in ANALYST_STATE_COLUMNS and key != "lead_id":
            normalized_state.loc[row_index, key] = value
    normalized_state.loc[row_index, "updated_at"] = utc_now_string()
    if updates.get("status") and str(updates.get("status")) != previous_status:
        normalized_history = pd.concat(
            [
                normalized_history,
                pd.DataFrame(
                    [
                        build_history_entry(
                            lead_id,
                            "STATUS_CHANGED",
                            previous_status=previous_status,
                            new_status=str(updates.get("status", "")),
                            reviewer=reviewer or str(updates.get("reviewer", "")),
                            note=str(updates.get("analyst_notes", "")),
                        )
                    ],
                    columns=ANALYST_HISTORY_COLUMNS,
                ),
            ],
            ignore_index=True,
        )
    if updates.get("analyst_notes"):
        normalized_history = pd.concat(
            [
                normalized_history,
                pd.DataFrame(
                    [
                        build_history_entry(
                            lead_id,
                            "NOTE_ADDED",
                            previous_status=previous_status,
                            new_status=str(normalized_state.loc[row_index, "status"]),
                            reviewer=reviewer or str(updates.get("reviewer", "")),
                            note=str(updates.get("analyst_notes", "")),
                        )
                    ],
                    columns=ANALYST_HISTORY_COLUMNS,
                ),
            ],
            ignore_index=True,
        )
    return normalized_state, normalized_history


def build_source_health_report(
    manifest_sources: dict[str, dict[str, Any]],
    api_sources: dict[str, dict[str, Any]],
    processed_dir: Path | str,
) -> pd.DataFrame:
    processed_path = Path(processed_dir)
    rows: list[dict[str, Any]] = []
    combined_sources = dict(manifest_sources)
    for source_name, api_config in api_sources.items():
        combined_sources.setdefault(source_name, {"source_name": source_name, **api_config})
    for source_name, source in sorted(combined_sources.items()):
        outputs = source.get("processed_outputs", [])
        if not isinstance(outputs, list):
            outputs = []
        row_count = 0
        existing_outputs = 0
        duplicates = 0
        last_import = str(source.get("imported_at", ""))
        for output in outputs:
            output_path = Path(output)
            if not output_path.is_absolute():
                output_path = REPO_ROOT / output_path
            if output_path.exists() and output_path.stat().st_size > 0:
                existing_outputs += 1
                try:
                    frame = pd.read_csv(output_path)
                    row_count += int(len(frame))
                    if "source_record_id" in frame.columns:
                        duplicates += int(frame["source_record_id"].astype(str).duplicated().sum())
                except Exception:
                    pass
        review_document = str(source.get("review_document", "")).strip()
        review_path = REPO_ROOT / review_document if review_document and not Path(review_document).is_absolute() else Path(review_document)
        terms_required = source.get("terms_review_required", False)
        pending_review = bool(terms_required and (not review_document or not review_path.exists()))
        rows.append(
            {
                "source_name": source_name,
                "source_type": str(source.get("source_type", "")),
                "access_method": str(source.get("access_method", source.get("endpoint", ""))),
                "connector_status": "READY" if existing_outputs else "PENDING_IMPORT",
                "records_imported": row_count,
                "last_import": last_import,
                "coverage": f"{existing_outputs}/{len(outputs)} outputs",
                "duplicates": duplicates,
                "failures": max(len(outputs) - existing_outputs, 0),
                "pending_review": pending_review,
                "expected_outputs": "|".join(str(value) for value in outputs),
                "live_access_allowed": str(source.get("live_access_allowed", "")),
            }
        )
    return pd.DataFrame(rows)


def compare_records(
    left_record: dict[str, Any],
    right_record: dict[str, Any],
    fields: list[str],
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for field in fields:
        left_value = str(left_record.get(field, ""))
        right_value = str(right_record.get(field, ""))
        rows.append(
            {
                "field": field,
                "left_value": left_value,
                "right_value": right_value,
                "match": "same" if left_value == right_value else "different",
            }
        )
    return pd.DataFrame(rows)


def build_queue_view(
    prioritized_leads_df: pd.DataFrame,
    *,
    priority: str = "All",
    confidence: str = "All",
    evidence_mode: str = "All",
    jurisdiction: str = "All",
    source_name: str = "All",
    marker: str = "All",
    network_mode: str = "All",
    entity_type: str = "All",
    status: str = "All",
    reviewed_mode: str = "All",
) -> pd.DataFrame:
    if prioritized_leads_df.empty:
        return prioritized_leads_df
    filtered = prioritized_leads_df.copy()
    if priority != "All":
        filtered = filtered[filtered["priority"].astype(str) == priority]
    if confidence != "All":
        filtered = filtered[filtered["confidence"].astype(str) == confidence]
    if evidence_mode == "Needs Validation":
        filtered = filtered[pd.to_numeric(filtered["evidence_completeness_score"], errors="coerce").fillna(0) < 60]
    elif evidence_mode == "Sufficient":
        filtered = filtered[pd.to_numeric(filtered["evidence_completeness_score"], errors="coerce").fillna(0) >= 60]
    if jurisdiction != "All" and "jurisdiction" in filtered.columns:
        filtered = filtered[filtered["jurisdiction"].astype(str) == jurisdiction]
    if source_name != "All":
        filtered = filtered[filtered["source_names"].astype(str).str.contains(source_name, na=False)]
    if marker != "All":
        filtered = filtered[filtered["fraud_markers"].astype(str).str.contains(marker, na=False)]
    if network_mode == "With Network":
        filtered = filtered[filtered["network_id"].astype(str).str.strip() != ""]
    elif network_mode == "Without Network":
        filtered = filtered[filtered["network_id"].astype(str).str.strip() == ""]
    if entity_type != "All":
        filtered = filtered[filtered["primary_entity_type"].astype(str) == entity_type]
    if status != "All":
        filtered = filtered[filtered["status"].astype(str) == status]
    if reviewed_mode == "Reviewed":
        filtered = filtered[filtered["review_date"].astype(str).str.strip() != ""]
    elif reviewed_mode == "Needs Review":
        filtered = filtered[
            (filtered["review_date"].astype(str).str.strip() == "")
            | (filtered["needs_review"].astype(str).str.lower() == "true")
        ]
    return filtered
