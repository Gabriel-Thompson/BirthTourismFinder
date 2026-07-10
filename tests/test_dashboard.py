from pathlib import Path

import pandas as pd
import pytest

from src.app import dashboard
from src.app.dashboard import (
    build_dashboard_metrics,
    build_resolution_metrics,
    build_relationship_explorer_data,
    filter_dataframe_by_source_scope,
    load_cross_source_matches,
    load_report,
    load_statistical_rarity,
)


def test_load_report_reads_csv_and_adds_risk_level(tmp_path: Path) -> None:
    report_path = tmp_path / "anomaly_report.csv"
    pd.DataFrame(
        [
            {
                "Risk Score": 30,
                "Rule Triggered": "Multiple businesses sharing one address",
                "Supporting Evidence": "Example evidence",
                "Entity IDs": "1,2",
                "Addresses": "123 Main St",
                "Phone Numbers": "555-0100",
                "Source Table": "business_entities",
                "source_name": "synthetic",
                "source_type": "synthetic",
            }
        ]
    ).to_csv(report_path, index=False)

    df = load_report(report_path)

    assert len(df) == 1
    assert df.iloc[0]["Risk Level"] == "High"
    assert df.iloc[0]["Risk Score"] == 30


def test_build_relationship_explorer_data_returns_connected_entities() -> None:
    entities_df = pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Acme", "entity_type": "business", "source": "business_entities"},
            {"entity_id": "addr-1", "display_name": "123 Main St", "entity_type": "address", "source": "properties"},
        ]
    )
    relationships_df = pd.DataFrame(
        [{"source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "confidence": 1.0}]
    )

    explorer_df = build_relationship_explorer_data(entities_df, relationships_df, selected_entity_id="biz-1")

    assert len(explorer_df) == 1
    assert explorer_df.iloc[0]["relationship_type"] == "LOCATED_AT"
    assert explorer_df.iloc[0]["connected_entity_id"] == "addr-1"
    assert explorer_df.iloc[0]["connected_entity_type"] == "address"
    assert explorer_df.iloc[0]["direction"] == "outgoing"


def test_load_report_returns_empty_dataframe_and_warning_for_missing_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(dashboard.st, "warning", lambda message: warnings.append(str(message)))

    df = dashboard.load_report(tmp_path / "missing.csv")

    assert df.empty
    assert any("Anomaly report not found" in message for message in warnings)


def test_build_dashboard_metrics_counts_expected_values() -> None:
    report_df = pd.DataFrame([{"Risk Score": 10}, {"Risk Score": 20}])
    entities_df = pd.DataFrame([{"entity_id": "e1"}, {"entity_id": "e2"}, {"entity_id": "e3"}])
    relationships_df = pd.DataFrame([{"source_entity_id": "e1"}, {"source_entity_id": "e2"}])
    entity_risk_df = pd.DataFrame([{"risk_level": "High"}, {"risk_level": "Medium"}, {"risk_level": "Low"}])
    leads_df = pd.DataFrame(
        [
            {"Priority": "Critical", "Confidence": "High", "Supporting Source Count": 2},
            {"Priority": "High", "Confidence": "Very High", "Supporting Source Count": 1},
        ]
    )

    metrics = build_dashboard_metrics(report_df, entities_df, relationships_df, entity_risk_df, leads_df)

    assert metrics["total_findings"] == 2
    assert metrics["total_entities"] == 3
    assert metrics["total_relationships"] == 2
    assert metrics["high_risk_entities"] == 1
    assert metrics["medium_risk_entities"] == 1
    assert metrics["critical_leads"] == 1
    assert metrics["high_priority_leads"] == 2
    assert metrics["high_confidence_leads"] == 2


def test_filter_dataframe_by_source_scope_supports_real_and_synthetic_modes() -> None:
    df = pd.DataFrame(
        [
            {"entity_id": "e1", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "e2", "source_name": "sample_api", "source_type": "api"},
            {"entity_id": "e3", "source_name": "county_property_local_file", "source_type": "connector"},
            {"entity_id": "e4", "source_name": "synthetic|sample_api", "source_type": "synthetic|api"},
        ]
    )

    real_only = filter_dataframe_by_source_scope(df, "real_only")
    synthetic_only = filter_dataframe_by_source_scope(df, "synthetic_only")
    filtered_source = filter_dataframe_by_source_scope(df, "all", ["sample_api"])

    assert set(real_only["entity_id"]) == {"e2", "e3", "e4"}
    assert set(synthetic_only["entity_id"]) == {"e1"}
    assert set(filtered_source["entity_id"]) == {"e2", "e4"}


def test_filter_dataframe_by_source_scope_supports_network_frames() -> None:
    df = pd.DataFrame(
        [
            {"network_id": "n1", "source_name": "synthetic", "source_type": "synthetic"},
            {"network_id": "n2", "source_name": "sample_api", "source_type": "api"},
        ]
    )

    filtered = filter_dataframe_by_source_scope(df, "real_only")

    assert set(filtered["network_id"]) == {"n2"}


def test_build_resolution_metrics_counts_expected_values() -> None:
    raw_entities = pd.DataFrame([{"entity_id": "a"}, {"entity_id": "b"}, {"entity_id": "c"}])
    canonical_entities = pd.DataFrame(
        [
            {"canonical_entity_id": "c1", "source_name": "synthetic"},
            {"canonical_entity_id": "c2", "source_name": "sample_api|sunbiz_local_file"},
        ]
    )
    matches = pd.DataFrame([{"decision": "REVIEW"}, {"decision": "AUTO_MERGE"}])

    metrics = build_resolution_metrics(raw_entities, canonical_entities, matches)

    assert metrics["raw_entities"] == 3
    assert metrics["canonical_entities"] == 2
    assert metrics["entities_merged"] == 1
    assert metrics["review_candidates"] == 1
    assert metrics["cross_source_canonical_entities"] == 1


def test_load_cross_source_matches_adds_source_scope_columns(tmp_path: Path) -> None:
    path = tmp_path / "cross_source_matches.csv"
    pd.DataFrame(
        [
            {
                "cross_source_match_id": "cross:1",
                "canonical_entity_id": "canonical:address:1",
                "entity_type": "address",
                "left_source_name": "sample_api",
                "right_source_name": "florida_county_arcgis_parcels",
                "left_source_type": "api",
                "right_source_type": "arcgis",
                "left_source_record_id": "api:1",
                "right_source_record_id": "parcel:1",
                "source_pair": "florida_county_arcgis_parcels|sample_api",
                "match_method": "property_situs_matches_business_address",
                "confidence": 0.88,
                "evidence": "Shared address",
                "decision": "AUTO_MATCH",
                "independent_real_source_count": 2,
                "contains_real_data": True,
                "contains_synthetic_data": False,
                "why_sources_independent": "Different source_name values",
            }
        ]
    ).to_csv(path, index=False)

    df = load_cross_source_matches(path)

    assert df.iloc[0]["source_name"] == "sample_api|florida_county_arcgis_parcels"
    assert df.iloc[0]["source_type"] == "api|arcgis"


def test_load_statistical_rarity_reads_expected_columns(tmp_path: Path) -> None:
    path = tmp_path / "statistical_rarity.csv"
    pd.DataFrame(
        [
            {
                "marker_id": "shared_address_businesses",
                "marker_name": "Shared Address",
                "entity_id": "biz-1",
                "entity_type": "business",
                "source_name": "sample_api",
                "source_type": "api",
                "jurisdiction": "FL",
                "source_scope": "REAL_ONLY",
                "address_context": "EXACT_APARTMENT_OR_UNIT",
                "base_building_address": "123 MAIN ST, TAMPA, FL 33602",
                "unit_level_address": "APT 2A",
                "classification_confidence": 0.9,
                "observed_value": 3,
                "expected_value": 0.7,
                "comparison_group": "g1",
                "comparison_group_size": 12,
                "percentile": 0.98,
                "probability_or_p_value": 0.004,
                "rarity_score": 60,
                "rarity_level": "IMMEDIATE_REVIEW",
                "model_used": "poisson_tail + empirical_percentile",
                "assumptions": "Count-based",
                "explanation": "Observed vs expected",
            }
        ]
    ).to_csv(path, index=False)

    df = load_statistical_rarity(path)

    assert df.iloc[0]["rarity_level"] == "IMMEDIATE_REVIEW"
    assert df.iloc[0]["observed_value"] == 3
    assert df.iloc[0]["comparison_group_size"] == 12
