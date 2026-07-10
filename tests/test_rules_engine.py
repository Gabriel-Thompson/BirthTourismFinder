from pathlib import Path

import pandas as pd

from src.analytics.engine import AnomalyEngine
from src.analytics.fraud_markers.engine import FraudMarkerEngine


def test_engine_writes_report_and_summarizes(tmp_path: Path) -> None:
    entities_path = tmp_path / "canonical_entities.csv"
    relationships_path = tmp_path / "canonical_relationships.csv"
    aliases_path = tmp_path / "entity_aliases.csv"
    output_path = tmp_path / "anomaly_report.csv"
    fraud_markers_path = tmp_path / "fraud_markers.csv"
    summary_path = tmp_path / "fraud_marker_summary.csv"

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

    engine = FraudMarkerEngine(
        entities_path=entities_path,
        relationships_path=relationships_path,
        aliases_path=aliases_path,
        output_path=fraud_markers_path,
        summary_path=summary_path,
        compatibility_output_path=output_path,
    )
    findings = engine.run()

    assert output_path.exists()
    assert fraud_markers_path.exists()
    assert summary_path.exists()
    assert len(findings) >= 2
    assert all("Rule Triggered" in finding for finding in findings)

    wrapper = AnomalyEngine(output_path=output_path)
    summary = wrapper.summarize(findings)
    assert summary["Low"] + summary["Medium"] + summary["High"] >= 2
