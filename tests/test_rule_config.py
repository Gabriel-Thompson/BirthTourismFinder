from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.analytics.fraud_markers.engine import FraudMarkerEngine, load_fraud_marker_config


def _write_marker_dataset(tmp_path: Path) -> tuple[Path, Path, Path]:
    entities_path = tmp_path / "canonical_entities.csv"
    relationships_path = tmp_path / "canonical_relationships.csv"
    aliases_path = tmp_path / "entity_aliases.csv"

    pd.DataFrame(
        [
            {"entity_id": "biz-1", "display_name": "Alpha Care", "normalized_value": "ALPHA CARE", "entity_type": "business", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "biz-2", "display_name": "Beta Care", "normalized_value": "BETA CARE", "entity_type": "business", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "addr-1", "display_name": "123 Main St", "normalized_value": "123 MAIN ST", "entity_type": "address", "source_name": "synthetic", "source_type": "synthetic"},
            {"entity_id": "phone-1", "display_name": "5550100", "normalized_value": "5550100", "entity_type": "phone", "source_name": "synthetic", "source_type": "synthetic"},
        ]
    ).to_csv(entities_path, index=False)
    pd.DataFrame(
        [
            {"relationship_id": "r1", "source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r2", "source_entity_id": "biz-2", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r3", "source_entity_id": "biz-1", "target_entity_id": "phone-1", "relationship_type": "USES_PHONE", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
            {"relationship_id": "r4", "source_entity_id": "biz-2", "target_entity_id": "phone-1", "relationship_type": "USES_PHONE", "source_name": "synthetic", "source_type": "synthetic", "confidence": 1.0},
        ]
    ).to_csv(relationships_path, index=False)
    pd.DataFrame(columns=["canonical_entity_id", "original_entity_id"]).to_csv(aliases_path, index=False)
    return entities_path, relationships_path, aliases_path


def test_config_loading_uses_defaults_when_missing(tmp_path: Path) -> None:
    config = load_fraud_marker_config(tmp_path / "missing.json")
    assert "markers" in config
    assert config["risk_bands"]["high"] == 70


def test_engine_respects_disabled_marker_config(tmp_path: Path) -> None:
    entities_path, relationships_path, aliases_path = _write_marker_dataset(tmp_path)
    config_path = tmp_path / "fraud_markers.json"
    config_path.write_text(
        json.dumps(
            {
                "risk_bands": {"high": 70, "medium": 35, "low": 1},
                "confidence_bands": {"very_high": 0.9, "high": 0.75, "medium": 0.55, "low": 0.35},
                "markers": {
                    "shared_address_businesses": {"enabled": False, "weight": 18, "minimum_confidence": 0.65, "minimum_support": 2, "minimum_sources": 1},
                    "shared_phone": {"enabled": True, "weight": 14, "minimum_confidence": 0.65, "minimum_support": 2, "minimum_sources": 1},
                },
            }
        ),
        encoding="utf-8",
    )

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


def test_threshold_changes_reduce_findings(tmp_path: Path) -> None:
    entities_path, relationships_path, aliases_path = _write_marker_dataset(tmp_path)
    permissive_path = tmp_path / "permissive.json"
    strict_path = tmp_path / "strict.json"

    base = {
        "risk_bands": {"high": 70, "medium": 35, "low": 1},
        "confidence_bands": {"very_high": 0.9, "high": 0.75, "medium": 0.55, "low": 0.35},
        "markers": {
            "shared_address_businesses": {"enabled": True, "weight": 18, "minimum_confidence": 0.5, "minimum_support": 2, "minimum_sources": 1},
            "shared_phone": {"enabled": True, "weight": 14, "minimum_confidence": 0.5, "minimum_support": 2, "minimum_sources": 1},
        },
    }
    permissive_path.write_text(json.dumps(base), encoding="utf-8")
    base["markers"]["shared_address_businesses"]["minimum_support"] = 3
    base["markers"]["shared_phone"]["minimum_support"] = 3
    strict_path.write_text(json.dumps(base), encoding="utf-8")

    permissive_engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=tmp_path / "fraud_markers_permissive.csv",
        summary_path=tmp_path / "summary_permissive.csv",
        compatibility_output_path=tmp_path / "anomaly_permissive.csv",
        config_path=permissive_path,
    )
    strict_engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=tmp_path / "fraud_markers_strict.csv",
        summary_path=tmp_path / "summary_strict.csv",
        compatibility_output_path=tmp_path / "anomaly_strict.csv",
        config_path=strict_path,
    )
    permissive_engine.run()
    strict_engine.run()

    permissive_rows = pd.read_csv(tmp_path / "fraud_markers_permissive.csv")
    strict_rows = pd.read_csv(tmp_path / "fraud_markers_strict.csv")
    assert len(permissive_rows) > len(strict_rows)
