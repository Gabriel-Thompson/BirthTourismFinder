from pathlib import Path

import json
import pandas as pd

from src.analytics.entity_intelligence import build_entity_risk
from src.analytics.fraud_markers.engine import FraudMarkerEngine
from src.analytics.fraud_markers.marker_registry import get_registered_markers


def _write_marker_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    entities_path = tmp_path / "canonical_entities.csv"
    relationships_path = tmp_path / "canonical_relationships.csv"
    aliases_path = tmp_path / "entity_aliases.csv"

    pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Alpha Care", "normalized_value": "ALPHA CARE", "entity_type": "business", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "biz-2", "display_name": "Alpha Travel", "normalized_value": "ALPHA TRAVEL", "entity_type": "business", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "addr-1", "display_name": "123 Main St", "normalized_value": "123 MAIN ST", "entity_type": "address", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "phone-1", "display_name": "5550100", "normalized_value": "5550100", "entity_type": "phone", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "email-1", "display_name": "info@example.com", "normalized_value": "info@example.com", "entity_type": "email", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "owner-1", "display_name": "Sample Owner LLC", "normalized_value": "SAMPLE OWNER", "entity_type": "owner", "source_name": "florida_county_arcgis_parcels", "source_type": "arcgis"},
            {"entity_id": "business-3", "display_name": "Sample Owner LLC", "normalized_value": "SAMPLE OWNER", "entity_type": "business", "source_name": "sunbiz_local_file", "source_type": "connector"},
        ]
    ).to_csv(entities_path, index=False)
    pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r2", "source_entity_id": "biz-2", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r3", "source_entity_id": "biz-1", "target_entity_id": "phone-1", "relationship_type": "USES_PHONE", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r4", "source_entity_id": "biz-2", "target_entity_id": "phone-1", "relationship_type": "USES_PHONE", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r5", "source_entity_id": "biz-1", "target_entity_id": "email-1", "relationship_type": "USES_EMAIL", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r6", "source_entity_id": "biz-2", "target_entity_id": "email-1", "relationship_type": "USES_EMAIL", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
        ]
    ).to_csv(relationships_path, index=False)
    pd.DataFrame(columns=["canonical_entity_id", "original_entity_id"]).to_csv(aliases_path, index=False)
    return entities_path, relationships_path, aliases_path


def test_marker_registry_contains_expected_markers() -> None:
    registry = get_registered_markers()
    assert "shared_address_businesses" in registry
    assert "business_cluster_compound" in registry
    assert "bridge_entity" in registry


def test_engine_generates_individual_and_compound_markers(tmp_path: Path) -> None:
    entities_path, relationships_path, aliases_path = _write_marker_inputs(tmp_path)
    output_path = tmp_path / "fraud_markers.csv"
    summary_path = tmp_path / "fraud_marker_summary.csv"
    compatibility_path = tmp_path / "anomaly_report.csv"

    engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=output_path,
        summary_path=summary_path,
        compatibility_output_path=compatibility_path,
    )
    engine.run()

    fraud_markers = pd.read_csv(output_path)
    summary = pd.read_csv(summary_path)
    compatibility = pd.read_csv(compatibility_path)

    assert "Shared Address" in set(fraud_markers["marker_name"])
    assert "Shared Phone" in set(fraud_markers["marker_name"])
    assert "Business Cluster Compound Marker" in set(fraud_markers["marker_name"])
    assert not summary.empty
    assert not compatibility.empty


def test_engine_respects_disabled_markers(tmp_path: Path) -> None:
    entities_path, relationships_path, aliases_path = _write_marker_inputs(tmp_path)
    config_path = tmp_path / "fraud_markers.json"
    config = {
        "risk_bands": {"high": 70, "medium": 35, "low": 1},
        "confidence_bands": {"very_high": 0.9, "high": 0.75, "medium": 0.55, "low": 0.35},
        "markers": {
            "shared_address_businesses": {"enabled": False, "weight": 18, "minimum_confidence": 0.65, "minimum_support": 2, "minimum_sources": 1},
            "shared_phone": {"enabled": True, "weight": 14, "minimum_confidence": 0.65, "minimum_support": 2, "minimum_sources": 1},
        },
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")

    output_path = tmp_path / "fraud_markers.csv"
    engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=output_path,
        summary_path=tmp_path / "fraud_marker_summary.csv",
        compatibility_output_path=tmp_path / "anomaly_report.csv",
        config_path=config_path,
    )
    engine.run()
    fraud_markers = pd.read_csv(output_path)
    assert "Shared Address" not in set(fraud_markers["marker_name"])
    assert "Shared Phone" in set(fraud_markers["marker_name"])


def test_cross_source_marker_and_entity_intelligence_integration(tmp_path: Path) -> None:
    entities_path, relationships_path, aliases_path = _write_marker_inputs(tmp_path)
    output_path = tmp_path / "fraud_markers.csv"
    engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=output_path,
        summary_path=tmp_path / "fraud_marker_summary.csv",
        compatibility_output_path=tmp_path / "anomaly_report.csv",
    )
    engine.run()

    entities_df = pd.read_csv(entities_path)
    relationships_df = pd.read_csv(relationships_path)
    markers_df = pd.read_csv(output_path)
    risk_df = build_entity_risk(entities_df, relationships_df, markers_df)

    assert "ArcGIS Owner Appears in Business Records" in set(markers_df["marker_name"])
    assert "confidence" in risk_df.columns
    assert "recommended_review" in risk_df.columns
    owner_row = risk_df[risk_df["entity_id"] == "owner-1"].iloc[0]
    assert owner_row["risk_score"] > 0
