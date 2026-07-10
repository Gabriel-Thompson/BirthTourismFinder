from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.analytics.fraud_markers.engine import FraudMarkerEngine
from src.analytics.statistical_risk.context import classify_address_context, split_building_and_unit
from src.analytics.statistical_risk.engine import run_statistical_risk
from src.analytics.statistical_risk.rarity import (
    conservative_compound_score,
    empirical_percentile,
    poisson_probability_at_least,
    robust_z_score,
    rolling_window_peak,
)
from src.investigation.investigation_engine import run_investigation_engine


def test_empirical_percentile_calculation() -> None:
    assert empirical_percentile([1, 2, 3, 4, 5], 4) == 0.8


def test_poisson_probability_tail() -> None:
    probability = poisson_probability_at_least(8, 0.7)
    assert probability is not None
    assert probability < 0.01


def test_robust_z_score_calculation() -> None:
    score = robust_z_score([1, 1, 2, 2, 2, 3, 20], 20)
    assert score is not None
    assert score > 3


def test_rolling_window_counts() -> None:
    dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-02-10"]
    assert rolling_window_peak(dates, 3) == 3


def test_building_and_unit_level_address_distinction() -> None:
    building, unit = split_building_and_unit("123 Main St Apt 4B, Tampa, FL 33602")
    assert "APT 4B" not in building.upper()
    assert unit.upper() == "APT 4B"


def test_residential_vs_commercial_adjustment_context() -> None:
    residential = classify_address_context("123 Main St Apt 2A, Tampa, FL 33602", property_use="Single Family Residential")
    commercial = classify_address_context("900 Commerce Plaza, Tampa, FL 33602", property_use="Commercial Office")
    assert residential["address_context"] == "EXACT_APARTMENT_OR_UNIT"
    assert commercial["address_context"] == "COMMERCIAL_PROPERTY"


def test_real_vs_synthetic_baseline_separation_and_insufficient_baseline(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True)
    entities = pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Alpha", "normalized_value": "ALPHA", "entity_type": "business", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"entity_id": "addr-1", "display_name": "123 Main St Apt 2A, Tampa, FL 33602", "normalized_value": "123 MAIN ST APT 2A TAMPA FL 33602", "entity_type": "address", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"entity_id": "biz-s", "display_name": "Synthetic Biz", "normalized_value": "SYN", "entity_type": "business", "source_name": "synthetic", "source_type": "synthetic", "imported_at": "2026-07-01"},
            {"entity_id": "addr-s", "display_name": "500 Demo Ln", "normalized_value": "500 DEMO LN", "entity_type": "address", "source_name": "synthetic", "source_type": "synthetic", "imported_at": "2026-07-01"},
        ]
    )
    relationships = pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"relationship_id": "r2", "source_entity_id": "biz-s", "target_entity_id": "addr-s", "relationship_type": "LOCATED_AT", "source_name": "synthetic", "source_type": "synthetic", "imported_at": "2026-07-01"},
        ]
    )
    entities.to_csv(processed / "canonical_entities.csv", index=False)
    relationships.to_csv(processed / "canonical_relationships.csv", index=False)
    pd.DataFrame(columns=["canonical_entity_id"]).to_csv(processed / "cross_source_matches.csv", index=False)

    summary = run_statistical_risk(
        canonical_entities_path=processed / "canonical_entities.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        baselines_path=processed / "statistical_baselines.csv",
        rarity_path=processed / "statistical_rarity.csv",
        adjustments_path=processed / "contextual_risk_adjustments.csv",
        summary_path=processed / "statistical_marker_summary.json",
        calibration_report_path=processed / "statistical_calibration_report.csv",
    )

    rarity_df = pd.read_csv(processed / "statistical_rarity.csv")
    assert set(rarity_df["source_scope"]) == {"REAL_ONLY"}
    assert "synthetic" not in set(rarity_df["source_name"])
    assert summary["insufficient_baseline_count"] >= 1


def test_compound_marker_conservative_scoring() -> None:
    value = conservative_compound_score([0.01, 0.02, 0.25])
    assert value is not None
    assert value >= 0.01
    assert value > 0.0002


def test_fraud_marker_and_investigation_integration_with_statistical_fields(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True)
    entities = pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Alpha", "normalized_value": "ALPHA", "entity_type": "business", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"entity_id": "biz-2", "display_name": "Beta", "normalized_value": "BETA", "entity_type": "business", "source_name": "sunbiz_local_file", "source_type": "connector", "imported_at": "2026-07-02"},
            {"entity_id": "addr-1", "display_name": "123 Main St Apt 2A, Tampa, FL 33602", "normalized_value": "123 MAIN ST APT 2A TAMPA FL 33602", "entity_type": "address", "source_name": "sample_api|sunbiz_local_file", "source_type": "api|connector", "imported_at": "2026-07-02"},
        ]
    )
    relationships = pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"relationship_id": "r2", "source_entity_id": "biz-2", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "sunbiz_local_file", "source_type": "connector", "imported_at": "2026-07-02"},
        ]
    )
    aliases = pd.DataFrame(columns=["canonical_entity_id", "original_entity_id"])
    entities.to_csv(processed / "canonical_entities.csv", index=False)
    relationships.to_csv(processed / "canonical_relationships.csv", index=False)
    aliases.to_csv(processed / "entity_aliases.csv", index=False)
    pd.DataFrame(columns=["cross_source_match_id"]).to_csv(processed / "cross_source_matches.csv", index=False)

    run_statistical_risk(
        canonical_entities_path=processed / "canonical_entities.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        baselines_path=processed / "statistical_baselines.csv",
        rarity_path=processed / "statistical_rarity.csv",
        adjustments_path=processed / "contextual_risk_adjustments.csv",
        summary_path=processed / "statistical_marker_summary.json",
        calibration_report_path=processed / "statistical_calibration_report.csv",
    )

    engine = FraudMarkerEngine(
        entities_path=processed / "canonical_entities.csv",
        relationships_path=processed / "canonical_relationships.csv",
        aliases_path=processed / "entity_aliases.csv",
        output_path=processed / "fraud_markers.csv",
        summary_path=processed / "fraud_marker_summary.csv",
        compatibility_output_path=processed / "anomaly_report.csv",
        statistical_rarity_path=processed / "statistical_rarity.csv",
        statistical_adjustments_path=processed / "contextual_risk_adjustments.csv",
    )
    engine.run()
    fraud_markers = pd.read_csv(processed / "fraud_markers.csv")
    assert "rarity_score" in fraud_markers.columns
    assert "adjusted_risk_contribution" in fraud_markers.columns

    pd.DataFrame(
        [
            {
                "entity_id": "biz-1",
                "entity_type": "business",
                "display_name": "Alpha",
                "risk_score": 55,
                "marker_count": 1,
                "relationship_count": 1,
                "source_name": "sample_api",
                "source_type": "api",
                "average_marker_confidence": 0.8,
            }
        ]
    ).to_csv(processed / "entity_risk.csv", index=False)
    pd.DataFrame(
        [
            {
                "lead_id": "lead:seed",
                "entity_id": "biz-1",
                "Lead Title": "Business lead",
                "Lead Summary": "Summary",
                "Risk Score": 55,
                "Fraud Marker Count": 1,
                "Supporting Source Count": 1,
                "Relationship Count": 1,
                "source_name": "sample_api",
                "source_type": "api",
                "Cross-Source Correlation": "No",
                "Entity Resolution Confidence": 0.95,
                "Risk Explanation": "Shared address",
                "Recommended Review": "Review",
                "Fraud Markers": "Shared Address",
            }
        ]
    ).to_csv(processed / "investigation_leads.csv", index=False)
    pd.DataFrame(columns=["lead_id", "entity_id"]).to_csv(processed / "evidence_packets.csv", index=False)
    pd.DataFrame(columns=["lead_id", "entity_id"]).to_csv(processed / "entity_timelines.csv", index=False)
    pd.DataFrame(columns=["network_id", "entity_id"]).to_csv(processed / "network_members.csv", index=False)
    pd.DataFrame(columns=["network_id"]).to_csv(processed / "network_clusters.csv", index=False)

    run_investigation_engine(
        investigation_leads_path=processed / "investigation_leads.csv",
        network_clusters_path=processed / "network_clusters.csv",
        entity_risk_path=processed / "entity_risk.csv",
        fraud_markers_path=processed / "fraud_markers.csv",
        canonical_entities_path=processed / "canonical_entities.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        aliases_path=processed / "entity_aliases.csv",
        evidence_packets_path=processed / "evidence_packets.csv",
        entity_timelines_path=processed / "entity_timelines.csv",
        network_members_path=processed / "network_members.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        prioritized_leads_path=processed / "prioritized_leads.csv",
        investigation_summary_path=processed / "investigation_summary.csv",
        lead_evidence_index_path=processed / "lead_evidence_index.csv",
        review_recommendations_path=processed / "review_recommendations.csv",
        analyst_state_path=processed / "analyst_lead_state.csv",
        package_root=tmp_path / "exports" / "leads",
    )

    prioritized = pd.read_csv(processed / "prioritized_leads.csv")
    assert "rarity_score" in prioritized.columns
    assert "statistical_review_reason" in prioritized.columns
    assert "contextual_adjustment_summary" in prioritized.columns


def test_repeat_run_stability(tmp_path: Path) -> None:
    processed = tmp_path / "processed"
    processed.mkdir(parents=True)
    entities = pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Alpha", "normalized_value": "ALPHA", "entity_type": "business", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"entity_id": "biz-2", "display_name": "Beta", "normalized_value": "BETA", "entity_type": "business", "source_name": "sunbiz_local_file", "source_type": "connector", "imported_at": "2026-07-02"},
            {"entity_id": "addr-1", "display_name": "123 Main St Apt 2A, Tampa, FL 33602", "normalized_value": "123 MAIN ST APT 2A TAMPA FL 33602", "entity_type": "address", "source_name": "sample_api|sunbiz_local_file", "source_type": "api|connector", "imported_at": "2026-07-02"},
        ]
    )
    relationships = pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "sample_api", "source_type": "api", "imported_at": "2026-07-01"},
            {"relationship_id": "r2", "source_entity_id": "biz-2", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "sunbiz_local_file", "source_type": "connector", "imported_at": "2026-07-02"},
        ]
    )
    entities.to_csv(processed / "canonical_entities.csv", index=False)
    relationships.to_csv(processed / "canonical_relationships.csv", index=False)
    pd.DataFrame(columns=["cross_source_match_id"]).to_csv(processed / "cross_source_matches.csv", index=False)

    first = run_statistical_risk(
        canonical_entities_path=processed / "canonical_entities.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        baselines_path=processed / "statistical_baselines.csv",
        rarity_path=processed / "statistical_rarity.csv",
        adjustments_path=processed / "contextual_risk_adjustments.csv",
        summary_path=processed / "statistical_marker_summary.json",
        calibration_report_path=processed / "statistical_calibration_report.csv",
    )
    second = run_statistical_risk(
        canonical_entities_path=processed / "canonical_entities.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        baselines_path=processed / "statistical_baselines.csv",
        rarity_path=processed / "statistical_rarity.csv",
        adjustments_path=processed / "contextual_risk_adjustments.csv",
        summary_path=processed / "statistical_marker_summary.json",
        calibration_report_path=processed / "statistical_calibration_report.csv",
    )

    assert first["markers_evaluated"] == second["markers_evaluated"]
    assert json.loads((processed / "statistical_marker_summary.json").read_text(encoding="utf-8"))["markers_evaluated"] == first["markers_evaluated"]
