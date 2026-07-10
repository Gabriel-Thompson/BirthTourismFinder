from pathlib import Path

import pandas as pd

from src.investigation.workspace import build_investigation_workspace


def test_build_investigation_workspace_writes_outputs(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "entity_type": "business",
                "display_name": "Acme Group",
                "risk_score": 88,
                "risk_level": "High",
                "confidence": "High",
                "relationship_count": 3,
                "source_count": 2,
                "source_name": "synthetic|sunbiz_local_file",
                "source_type": "synthetic|connector",
                "contributing_rules": "Shared Address|Shared Phone",
                "recommended_review": "Review linked businesses.",
                "marker_count": 2,
                "average_marker_confidence": 0.86,
            }
        ]
    ).to_csv(processed_dir / "entity_risk.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "marker_id": "shared_address_businesses",
                "marker_name": "Shared Address",
                "marker_category": "address",
                "risk_contribution": 18,
                "confidence": "High",
                "confidence_score": 0.82,
                "support": 3,
                "sources": "synthetic|sunbiz_local_file",
                "source_types": "synthetic|connector",
                "supporting_entities": "canonical:business:1|canonical:address:1",
                "supporting_relationships": "r1|r2",
                "recommended_review": "Review the address cluster.",
                "explanation": "Three businesses share an address.",
            }
        ]
    ).to_csv(processed_dir / "fraud_markers.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "canonical_entity_id": "canonical:business:1",
                "entity_type": "business",
                "display_name": "Acme Group",
                "resolution_confidence": 0.97,
                "source_name": "synthetic|sunbiz_local_file",
                "source_type": "synthetic|connector",
            },
            {
                "entity_id": "canonical:address:1",
                "canonical_entity_id": "canonical:address:1",
                "entity_type": "address",
                "display_name": "123 Main St",
                "resolution_confidence": 1.0,
                "source_name": "synthetic",
                "source_type": "synthetic",
            },
        ]
    ).to_csv(processed_dir / "canonical_entities.csv", index=False)
    pd.DataFrame(
        [
            {
                "relationship_id": "r1",
                "source_entity_id": "canonical:business:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "LOCATED_AT",
                "source_name": "synthetic",
                "source_type": "synthetic",
                "evidence": "Resolved from raw relationship",
                "confidence": 1.0,
            }
        ]
    ).to_csv(processed_dir / "canonical_relationships.csv", index=False)
    pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:business:1",
                "original_entity_id": "biz-1",
                "alias_value": "ACME Group LLC",
                "normalized_alias": "ACME GROUP LLC",
                "source_name": "sunbiz_local_file",
                "source_type": "connector",
                "source_record_id": "SB1",
            }
        ]
    ).to_csv(processed_dir / "entity_aliases.csv", index=False)

    summary = build_investigation_workspace(
        entity_risk_path=processed_dir / "entity_risk.csv",
        fraud_markers_path=processed_dir / "fraud_markers.csv",
        canonical_entities_path=processed_dir / "canonical_entities.csv",
        canonical_relationships_path=processed_dir / "canonical_relationships.csv",
        aliases_path=processed_dir / "entity_aliases.csv",
        leads_output_path=processed_dir / "investigation_leads.csv",
        timelines_output_path=processed_dir / "entity_timelines.csv",
        evidence_output_path=processed_dir / "evidence_packets.csv",
    )

    leads = pd.read_csv(processed_dir / "investigation_leads.csv")
    timelines = pd.read_csv(processed_dir / "entity_timelines.csv")
    evidence = pd.read_csv(processed_dir / "evidence_packets.csv")

    assert summary["lead_count"] == 1
    assert not leads.empty
    assert not timelines.empty
    assert not evidence.empty
    assert leads.iloc[0]["Priority"] in {"Critical", "High", "Medium", "Low"}
    assert "Lead Notes" in leads.columns
    assert "Fraud Markers" in evidence.columns
