from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.investigation.analyst_workbench import (
    build_queue_view,
    build_source_health_report,
    compare_records,
    load_dashboard_config,
    load_saved_searches,
    merge_analyst_state_with_leads,
    save_saved_searches,
    update_analyst_record,
)


def test_load_dashboard_config_merges_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "dashboard.json"
    config_path.write_text(
        '{"real_data_only": true, "default_views": {"queue_sort": "recently_updated"}}',
        encoding="utf-8",
    )

    config = load_dashboard_config(config_path)

    assert config["real_data_only"] is True
    assert config["default_views"]["queue_sort"] == "recently_updated"
    assert config["page_size"] == 25


def test_saved_searches_round_trip(tmp_path: Path) -> None:
    search_path = tmp_path / "saved_searches.json"
    searches = [{"name": "High Confidence Leads", "confidence": "High"}]

    save_saved_searches(searches, search_path)
    loaded = load_saved_searches(search_path)

    assert loaded == searches


def test_merge_analyst_state_preserves_sidecar_fields() -> None:
    leads_df = pd.DataFrame(
        [
            {
                "lead_id": "lead:1",
                "title": "Lead One",
                "primary_entity_id": "entity:1",
            }
        ]
    )
    analyst_state_df = pd.DataFrame(
        [
            {
                "lead_id": "lead:1",
                "status": "IN_REVIEW",
                "analyst_notes": "keep note",
                "reviewer": "A1",
                "review_date": "2026-07-10",
                "disposition": "Pending",
                "follow_up_needed": "Yes",
                "bookmark": "true",
                "priority_override": "CRITICAL",
                "needs_review": "false",
                "last_reviewed_at": "2026-07-10T10:00:00Z",
                "last_exported_at": "",
                "updated_at": "2026-07-10T10:00:00Z",
            }
        ]
    )
    history_df = pd.DataFrame(columns=["lead_id", "event_type", "previous_status", "new_status", "reviewer", "note", "occurred_at", "details"])

    merged_df, state_df, merged_history = merge_analyst_state_with_leads(leads_df, analyst_state_df, history_df)

    assert merged_df.iloc[0]["analyst_notes"] == "keep note"
    assert merged_df.iloc[0]["priority_override"] == "CRITICAL"
    assert state_df.iloc[0]["bookmark"] == "true"
    assert "CREATED" in set(merged_history["event_type"])


def test_update_analyst_record_tracks_status_change_and_note() -> None:
    state_df = pd.DataFrame([{"lead_id": "lead:1", "status": "NEW"}])
    history_df = pd.DataFrame(columns=["lead_id", "event_type", "previous_status", "new_status", "reviewer", "note", "occurred_at", "details"])

    next_state, next_history = update_analyst_record(
        state_df,
        history_df,
        lead_id="lead:1",
        reviewer="Analyst",
        updates={"status": "REVIEWED", "analyst_notes": "Checked supporting evidence."},
    )

    assert next_state.iloc[0]["status"] == "REVIEWED"
    assert set(next_history["event_type"]) == {"STATUS_CHANGED", "NOTE_ADDED"}


def test_build_queue_view_filters_expected_rows() -> None:
    leads_df = pd.DataFrame(
        [
            {
                "lead_id": "lead:1",
                "priority": "CRITICAL",
                "confidence": "High",
                "source_names": "sample_api",
                "fraud_markers": "Shared Address",
                "network_id": "network:1",
                "primary_entity_type": "business",
                "status": "NEW",
                "review_date": "",
                "needs_review": "true",
                "evidence_completeness_score": 90,
            },
            {
                "lead_id": "lead:2",
                "priority": "LOW",
                "confidence": "Low",
                "source_names": "synthetic",
                "fraud_markers": "Shared Phone",
                "network_id": "",
                "primary_entity_type": "person",
                "status": "REVIEWED",
                "review_date": "2026-07-10",
                "needs_review": "false",
                "evidence_completeness_score": 40,
            },
        ]
    )

    filtered = build_queue_view(
        leads_df,
        priority="CRITICAL",
        confidence="High",
        source_name="sample_api",
        marker="Shared Address",
        network_mode="With Network",
        entity_type="business",
        status="NEW",
        reviewed_mode="Needs Review",
    )

    assert filtered["lead_id"].tolist() == ["lead:1"]


def test_compare_records_marks_differences() -> None:
    comparison = compare_records({"risk": "90", "source": "api"}, {"risk": "70", "source": "api"}, ["risk", "source"])

    assert comparison.iloc[0]["match"] == "different"
    assert comparison.iloc[1]["match"] == "same"


def test_build_source_health_report_counts_outputs(tmp_path: Path) -> None:
    processed_output = tmp_path / "entities.csv"
    pd.DataFrame([{"source_record_id": "1"}, {"source_record_id": "1"}]).to_csv(processed_output, index=False)

    report = build_source_health_report(
        {
            "sample_source": {
                "source_name": "sample_source",
                "source_type": "connector",
                "access_method": "local_file_only",
                "processed_outputs": [str(processed_output)],
                "terms_review_required": False,
                "imported_at": "2026-07-10T00:00:00Z",
                "live_access_allowed": False,
            }
        },
        {},
        tmp_path,
    )

    assert report.iloc[0]["connector_status"] == "READY"
    assert report.iloc[0]["records_imported"] == 2
    assert report.iloc[0]["duplicates"] == 1
