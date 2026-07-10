from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.health_check import check_project_health, main


def write_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"a": 1}]).to_csv(path, index=False)


def write_json(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")


def test_health_check_passes_when_required_files_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local_osint.duckdb").write_bytes(b"duckdb")
    write_csv(tmp_path / "config/entity_scoring.json")
    write_csv(tmp_path / "config/rules.json")
    write_csv(tmp_path / "config/sources.json")
    write_csv(tmp_path / "config/api_sources.json")
    write_csv(tmp_path / "config/entity_resolution.json")
    write_csv(tmp_path / "config/fraud_markers.json")
    write_csv(tmp_path / "config/network_detection.json")
    write_csv(tmp_path / "config/investigation_engine.json")
    write_csv(tmp_path / "config/cross_source.json")
    write_csv(tmp_path / "config/statistical_risk.json")
    write_csv(tmp_path / "config/dashboard.json")
    write_csv(tmp_path / "data/processed/anomaly_report.csv")
    write_csv(tmp_path / "data/processed/fraud_markers.csv")
    write_csv(tmp_path / "data/processed/fraud_marker_summary.csv")
    write_csv(tmp_path / "data/processed/entities.csv")
    write_csv(tmp_path / "data/processed/relationships.csv")
    write_csv(tmp_path / "data/processed/canonical_entities.csv")
    write_csv(tmp_path / "data/processed/entity_aliases.csv")
    write_csv(tmp_path / "data/processed/entity_resolution_matches.csv")
    write_csv(tmp_path / "data/processed/canonical_relationships.csv")
    write_csv(tmp_path / "data/processed/entity_risk.csv")
    write_csv(tmp_path / "data/processed/investigation_leads.csv")
    write_csv(tmp_path / "data/processed/entity_timelines.csv")
    write_csv(tmp_path / "data/processed/evidence_packets.csv")
    write_csv(tmp_path / "data/processed/network_clusters.csv")
    write_csv(tmp_path / "data/processed/network_summary.csv")
    write_csv(tmp_path / "data/processed/network_members.csv")
    write_csv(tmp_path / "data/processed/network_edges.csv")
    pd.DataFrame([{
        "cross_source_match_id": "cross:1",
        "canonical_entity_id": "canonical:address:1",
        "entity_type": "address",
        "left_source_name": "sample_api",
        "right_source_name": "florida_county_arcgis_parcels",
        "match_method": "property_situs_matches_business_address",
        "decision": "AUTO_MATCH",
        "contains_real_data": True,
    }]).to_csv(tmp_path / "data/processed/cross_source_matches.csv", index=False)
    pd.DataFrame([{"metric": "root_cause_notes", "value": "[]"}]).to_csv(tmp_path / "data/processed/cross_source_diagnostics.csv", index=False)
    write_json(tmp_path / "data/processed/cross_source_diagnostic_summary.json")
    pd.DataFrame([{"marker_id": "shared_address_businesses", "comparison_group": "g1", "comparison_group_size": 10, "observed_mean": 2}]).to_csv(tmp_path / "data/processed/statistical_baselines.csv", index=False)
    pd.DataFrame([{"marker_id": "shared_address_businesses", "entity_id": "entity:1", "observed_value": 3, "expected_value": 1.2, "rarity_level": "ROUTINE_REVIEW", "comparison_group": "g1"}]).to_csv(tmp_path / "data/processed/statistical_rarity.csv", index=False)
    pd.DataFrame([{"marker_id": "shared_address_businesses", "entity_id": "entity:1", "original_marker_score": 18, "contextual_adjustment": 2, "adjusted_marker_score": 20}]).to_csv(tmp_path / "data/processed/contextual_risk_adjustments.csv", index=False)
    pd.DataFrame([{"metric": "marker_counts_before_adjustment", "value": 1}]).to_csv(tmp_path / "data/processed/statistical_calibration_report.csv", index=False)
    write_json(tmp_path / "data/processed/statistical_marker_summary.json")
    pd.DataFrame([{
        "lead_id": "lead:1",
        "lead_type": "ENTITY",
        "primary_entity_id": "entity:1",
        "risk_score": 80,
        "confidence": "HIGH",
        "priority": "HIGH",
        "recommended_review": "Review",
        "source_names": "sample_api",
        "rarity_score": 40,
        "highest_rarity_level": "ROUTINE_REVIEW",
        "rare_marker_count": 1,
        "comparison_group": "g1",
    }]).to_csv(tmp_path / "data/processed/prioritized_leads.csv", index=False)
    pd.DataFrame([{
        "total_leads": 1,
        "critical_leads": 0,
        "average_risk": 80,
        "average_confidence": 0.8,
        "average_evidence_completeness": 75,
    }]).to_csv(tmp_path / "data/processed/investigation_summary.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "evidence_id": "evidence:1",
        "evidence_type": "FRAUD_MARKER",
        "source_name": "sample_api",
        "evidence_summary": "summary",
        "confidence": "HIGH",
    }]).to_csv(tmp_path / "data/processed/lead_evidence_index.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "priority": "HIGH",
        "confidence": "HIGH",
        "recommended_review": "Review",
        "status": "NEW",
    }]).to_csv(tmp_path / "data/processed/review_recommendations.csv", index=False)

    passed, messages = check_project_health()

    assert passed is True
    assert any("SUMMARY: All required project health checks passed." in message for message in messages)


def test_health_check_validates_exports_when_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local_osint.duckdb").write_bytes(b"duckdb")
    write_csv(tmp_path / "config/entity_scoring.json")
    write_csv(tmp_path / "config/rules.json")
    write_csv(tmp_path / "config/sources.json")
    write_csv(tmp_path / "config/api_sources.json")
    write_csv(tmp_path / "config/entity_resolution.json")
    write_csv(tmp_path / "config/fraud_markers.json")
    write_csv(tmp_path / "config/network_detection.json")
    write_csv(tmp_path / "config/investigation_engine.json")
    write_csv(tmp_path / "config/cross_source.json")
    write_csv(tmp_path / "config/statistical_risk.json")
    write_csv(tmp_path / "config/dashboard.json")
    write_csv(tmp_path / "data/processed/anomaly_report.csv")
    write_csv(tmp_path / "data/processed/fraud_markers.csv")
    write_csv(tmp_path / "data/processed/fraud_marker_summary.csv")
    write_csv(tmp_path / "data/processed/entities.csv")
    write_csv(tmp_path / "data/processed/relationships.csv")
    write_csv(tmp_path / "data/processed/canonical_entities.csv")
    write_csv(tmp_path / "data/processed/entity_aliases.csv")
    write_csv(tmp_path / "data/processed/entity_resolution_matches.csv")
    write_csv(tmp_path / "data/processed/canonical_relationships.csv")
    write_csv(tmp_path / "data/processed/entity_risk.csv")
    write_csv(tmp_path / "data/processed/investigation_leads.csv")
    write_csv(tmp_path / "data/processed/entity_timelines.csv")
    write_csv(tmp_path / "data/processed/evidence_packets.csv")
    write_csv(tmp_path / "data/processed/network_clusters.csv")
    write_csv(tmp_path / "data/processed/network_summary.csv")
    write_csv(tmp_path / "data/processed/network_members.csv")
    write_csv(tmp_path / "data/processed/network_edges.csv")
    pd.DataFrame([{
        "cross_source_match_id": "cross:1",
        "canonical_entity_id": "canonical:address:1",
        "entity_type": "address",
        "left_source_name": "sample_api",
        "right_source_name": "florida_county_arcgis_parcels",
        "match_method": "property_situs_matches_business_address",
        "decision": "AUTO_MATCH",
        "contains_real_data": True,
    }]).to_csv(tmp_path / "data/processed/cross_source_matches.csv", index=False)
    pd.DataFrame([{"metric": "root_cause_notes", "value": "[]"}]).to_csv(tmp_path / "data/processed/cross_source_diagnostics.csv", index=False)
    write_json(tmp_path / "data/processed/cross_source_diagnostic_summary.json")
    pd.DataFrame([{"marker_id": "shared_address_businesses", "comparison_group": "g1", "comparison_group_size": 10, "observed_mean": 2}]).to_csv(tmp_path / "data/processed/statistical_baselines.csv", index=False)
    pd.DataFrame([{"marker_id": "shared_address_businesses", "entity_id": "entity:1", "observed_value": 3, "expected_value": 1.2, "rarity_level": "ROUTINE_REVIEW", "comparison_group": "g1"}]).to_csv(tmp_path / "data/processed/statistical_rarity.csv", index=False)
    pd.DataFrame([{"marker_id": "shared_address_businesses", "entity_id": "entity:1", "original_marker_score": 18, "contextual_adjustment": 2, "adjusted_marker_score": 20}]).to_csv(tmp_path / "data/processed/contextual_risk_adjustments.csv", index=False)
    pd.DataFrame([{"metric": "marker_counts_before_adjustment", "value": 1}]).to_csv(tmp_path / "data/processed/statistical_calibration_report.csv", index=False)
    write_json(tmp_path / "data/processed/statistical_marker_summary.json")
    pd.DataFrame([{
        "lead_id": "lead:1",
        "lead_type": "ENTITY",
        "primary_entity_id": "entity:1",
        "risk_score": 80,
        "confidence": "HIGH",
        "priority": "HIGH",
        "recommended_review": "Review",
        "source_names": "sample_api",
        "rarity_score": 40,
        "highest_rarity_level": "ROUTINE_REVIEW",
        "rare_marker_count": 1,
        "comparison_group": "g1",
    }]).to_csv(tmp_path / "data/processed/prioritized_leads.csv", index=False)
    pd.DataFrame([{
        "total_leads": 1,
        "critical_leads": 0,
        "average_risk": 80,
        "average_confidence": 0.8,
        "average_evidence_completeness": 75,
    }]).to_csv(tmp_path / "data/processed/investigation_summary.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "evidence_id": "evidence:1",
        "evidence_type": "FRAUD_MARKER",
        "source_name": "sample_api",
        "evidence_summary": "summary",
        "confidence": "HIGH",
    }]).to_csv(tmp_path / "data/processed/lead_evidence_index.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "priority": "HIGH",
        "confidence": "HIGH",
        "recommended_review": "Review",
        "status": "NEW",
    }]).to_csv(tmp_path / "data/processed/review_recommendations.csv", index=False)
    write_csv(tmp_path / "exports/high_risk_entities.csv")
    write_csv(tmp_path / "exports/lead_summary.csv")

    passed, messages = check_project_health()

    assert passed is True
    assert any("PASS: Optional export has rows" in message for message in messages)


def test_health_check_fails_when_required_csv_is_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local_osint.duckdb").write_bytes(b"duckdb")
    write_csv(tmp_path / "config/entity_scoring.json")
    write_csv(tmp_path / "config/rules.json")
    write_csv(tmp_path / "config/sources.json")
    write_csv(tmp_path / "config/api_sources.json")
    write_csv(tmp_path / "config/entity_resolution.json")
    write_csv(tmp_path / "config/fraud_markers.json")
    write_csv(tmp_path / "config/network_detection.json")
    write_csv(tmp_path / "config/investigation_engine.json")
    write_csv(tmp_path / "config/cross_source.json")
    write_csv(tmp_path / "config/statistical_risk.json")
    write_csv(tmp_path / "config/dashboard.json")
    (tmp_path / "data/processed").mkdir(parents=True)
    (tmp_path / "data/processed/anomaly_report.csv").write_text("", encoding="utf-8")
    write_csv(tmp_path / "data/processed/fraud_markers.csv")
    write_csv(tmp_path / "data/processed/fraud_marker_summary.csv")
    write_csv(tmp_path / "data/processed/entities.csv")
    write_csv(tmp_path / "data/processed/relationships.csv")
    write_csv(tmp_path / "data/processed/canonical_entities.csv")
    write_csv(tmp_path / "data/processed/entity_aliases.csv")
    write_csv(tmp_path / "data/processed/entity_resolution_matches.csv")
    write_csv(tmp_path / "data/processed/canonical_relationships.csv")
    write_csv(tmp_path / "data/processed/entity_risk.csv")
    write_csv(tmp_path / "data/processed/investigation_leads.csv")
    write_csv(tmp_path / "data/processed/entity_timelines.csv")
    write_csv(tmp_path / "data/processed/evidence_packets.csv")
    write_csv(tmp_path / "data/processed/network_clusters.csv")
    write_csv(tmp_path / "data/processed/network_summary.csv")
    write_csv(tmp_path / "data/processed/network_members.csv")
    write_csv(tmp_path / "data/processed/network_edges.csv")
    write_csv(tmp_path / "data/processed/cross_source_matches.csv")
    write_csv(tmp_path / "data/processed/cross_source_diagnostics.csv")
    write_json(tmp_path / "data/processed/cross_source_diagnostic_summary.json")
    write_csv(tmp_path / "data/processed/statistical_baselines.csv")
    write_csv(tmp_path / "data/processed/statistical_rarity.csv")
    write_csv(tmp_path / "data/processed/contextual_risk_adjustments.csv")
    write_csv(tmp_path / "data/processed/statistical_calibration_report.csv")
    write_json(tmp_path / "data/processed/statistical_marker_summary.json")
    pd.DataFrame([{
        "lead_id": "lead:1",
        "lead_type": "ENTITY",
        "primary_entity_id": "entity:1",
        "risk_score": 80,
        "confidence": "HIGH",
        "priority": "HIGH",
        "recommended_review": "Review",
        "source_names": "sample_api",
        "rarity_score": 40,
        "highest_rarity_level": "ROUTINE_REVIEW",
        "rare_marker_count": 1,
        "comparison_group": "g1",
    }]).to_csv(tmp_path / "data/processed/prioritized_leads.csv", index=False)
    pd.DataFrame([{
        "total_leads": 1,
        "critical_leads": 0,
        "average_risk": 80,
        "average_confidence": 0.8,
        "average_evidence_completeness": 75,
    }]).to_csv(tmp_path / "data/processed/investigation_summary.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "evidence_id": "evidence:1",
        "evidence_type": "FRAUD_MARKER",
        "source_name": "sample_api",
        "evidence_summary": "summary",
        "confidence": "HIGH",
    }]).to_csv(tmp_path / "data/processed/lead_evidence_index.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "priority": "HIGH",
        "confidence": "HIGH",
        "recommended_review": "Review",
        "status": "NEW",
    }]).to_csv(tmp_path / "data/processed/review_recommendations.csv", index=False)

    passed, messages = check_project_health()

    assert passed is False
    assert any("FAIL:" in message for message in messages)
    assert any("RECOMMENDED NEXT ACTION" in message for message in messages)


def test_main_exits_with_failure_for_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local_osint.duckdb").write_bytes(b"duckdb")
    write_csv(tmp_path / "config/entity_scoring.json")
    write_csv(tmp_path / "config/rules.json")
    write_csv(tmp_path / "config/sources.json")
    write_csv(tmp_path / "config/api_sources.json")
    write_csv(tmp_path / "config/entity_resolution.json")
    write_csv(tmp_path / "config/fraud_markers.json")
    write_csv(tmp_path / "config/network_detection.json")
    write_csv(tmp_path / "config/investigation_engine.json")
    write_csv(tmp_path / "config/cross_source.json")
    write_csv(tmp_path / "config/statistical_risk.json")
    write_csv(tmp_path / "config/dashboard.json")
    write_csv(tmp_path / "data/processed/anomaly_report.csv")
    write_csv(tmp_path / "data/processed/fraud_markers.csv")
    write_csv(tmp_path / "data/processed/fraud_marker_summary.csv")
    write_csv(tmp_path / "data/processed/entities.csv")
    write_csv(tmp_path / "data/processed/relationships.csv")
    write_csv(tmp_path / "data/processed/canonical_entities.csv")
    write_csv(tmp_path / "data/processed/entity_aliases.csv")
    write_csv(tmp_path / "data/processed/entity_resolution_matches.csv")
    write_csv(tmp_path / "data/processed/canonical_relationships.csv")
    write_csv(tmp_path / "data/processed/investigation_leads.csv")
    write_csv(tmp_path / "data/processed/entity_timelines.csv")
    write_csv(tmp_path / "data/processed/evidence_packets.csv")
    write_csv(tmp_path / "data/processed/network_clusters.csv")
    write_csv(tmp_path / "data/processed/network_summary.csv")
    write_csv(tmp_path / "data/processed/network_members.csv")
    write_csv(tmp_path / "data/processed/network_edges.csv")
    write_csv(tmp_path / "data/processed/cross_source_matches.csv")
    write_csv(tmp_path / "data/processed/cross_source_diagnostics.csv")
    write_json(tmp_path / "data/processed/cross_source_diagnostic_summary.json")
    write_csv(tmp_path / "data/processed/statistical_baselines.csv")
    write_csv(tmp_path / "data/processed/statistical_rarity.csv")
    write_csv(tmp_path / "data/processed/contextual_risk_adjustments.csv")
    write_csv(tmp_path / "data/processed/statistical_calibration_report.csv")
    write_json(tmp_path / "data/processed/statistical_marker_summary.json")
    pd.DataFrame([{
        "lead_id": "lead:1",
        "lead_type": "ENTITY",
        "primary_entity_id": "entity:1",
        "risk_score": 80,
        "confidence": "HIGH",
        "priority": "HIGH",
        "recommended_review": "Review",
        "source_names": "sample_api",
        "rarity_score": 40,
        "highest_rarity_level": "ROUTINE_REVIEW",
        "rare_marker_count": 1,
        "comparison_group": "g1",
    }]).to_csv(tmp_path / "data/processed/prioritized_leads.csv", index=False)
    pd.DataFrame([{
        "total_leads": 1,
        "critical_leads": 0,
        "average_risk": 80,
        "average_confidence": 0.8,
        "average_evidence_completeness": 75,
    }]).to_csv(tmp_path / "data/processed/investigation_summary.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "evidence_id": "evidence:1",
        "evidence_type": "FRAUD_MARKER",
        "source_name": "sample_api",
        "evidence_summary": "summary",
        "confidence": "HIGH",
    }]).to_csv(tmp_path / "data/processed/lead_evidence_index.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "priority": "HIGH",
        "confidence": "HIGH",
        "recommended_review": "Review",
        "status": "NEW",
    }]).to_csv(tmp_path / "data/processed/review_recommendations.csv", index=False)
    # entity_risk missing

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1


def test_health_check_fails_when_required_config_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "local_osint.duckdb").write_bytes(b"duckdb")
    write_csv(tmp_path / "config/entity_scoring.json")
    write_csv(tmp_path / "config/rules.json")
    write_csv(tmp_path / "data/processed/anomaly_report.csv")
    write_csv(tmp_path / "data/processed/fraud_markers.csv")
    write_csv(tmp_path / "data/processed/fraud_marker_summary.csv")
    write_csv(tmp_path / "data/processed/entities.csv")
    write_csv(tmp_path / "data/processed/relationships.csv")
    write_csv(tmp_path / "data/processed/canonical_entities.csv")
    write_csv(tmp_path / "data/processed/entity_aliases.csv")
    write_csv(tmp_path / "data/processed/entity_resolution_matches.csv")
    write_csv(tmp_path / "data/processed/canonical_relationships.csv")
    write_csv(tmp_path / "data/processed/entity_risk.csv")
    write_csv(tmp_path / "data/processed/investigation_leads.csv")
    write_csv(tmp_path / "data/processed/entity_timelines.csv")
    write_csv(tmp_path / "data/processed/evidence_packets.csv")
    write_csv(tmp_path / "data/processed/network_clusters.csv")
    write_csv(tmp_path / "data/processed/network_summary.csv")
    write_csv(tmp_path / "data/processed/network_members.csv")
    write_csv(tmp_path / "data/processed/network_edges.csv")
    pd.DataFrame([{
        "lead_id": "lead:1",
        "lead_type": "ENTITY",
        "primary_entity_id": "entity:1",
        "risk_score": 80,
        "confidence": "HIGH",
        "priority": "HIGH",
        "recommended_review": "Review",
        "source_names": "sample_api",
    }]).to_csv(tmp_path / "data/processed/prioritized_leads.csv", index=False)
    pd.DataFrame([{
        "total_leads": 1,
        "critical_leads": 0,
        "average_risk": 80,
        "average_confidence": 0.8,
        "average_evidence_completeness": 75,
    }]).to_csv(tmp_path / "data/processed/investigation_summary.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "evidence_id": "evidence:1",
        "evidence_type": "FRAUD_MARKER",
        "source_name": "sample_api",
        "evidence_summary": "summary",
        "confidence": "HIGH",
    }]).to_csv(tmp_path / "data/processed/lead_evidence_index.csv", index=False)
    pd.DataFrame([{
        "lead_id": "lead:1",
        "priority": "HIGH",
        "confidence": "HIGH",
        "recommended_review": "Review",
        "status": "NEW",
    }]).to_csv(tmp_path / "data/processed/review_recommendations.csv", index=False)

    passed, messages = check_project_health()

    assert passed is False
    assert any("sources.json" in message for message in messages)
