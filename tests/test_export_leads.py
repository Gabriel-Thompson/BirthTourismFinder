from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.reports.export_leads import build_lead_exports, load_dataframe, main


def test_build_lead_exports_creates_expected_columns(tmp_path: Path) -> None:
    entity_risk = pd.DataFrame([
        {
            "entity_id": "1",
            "entity_type": "business",
            "display_name": "ACME Corp",
            "risk_score": 85,
            "risk_level": "High",
            "relationship_count": 5,
            "contributing_rules": "Shared Address|Shared Phone",
            "supporting_evidence": "123 Main St|555-1212",
        },
        {
            "entity_id": "2",
            "entity_type": "property",
            "display_name": "123 Main St",
            "risk_score": 20,
            "risk_level": "Low",
            "relationship_count": 1,
            "contributing_rules": "",
            "supporting_evidence": "",
        },
    ])
    relationships = pd.DataFrame([
        {"source_entity_id": "1", "target_entity_id": "address:123 Main St", "relationship_type": "LOCATED_AT", "confidence": 1.0},
    ])
    anomaly = pd.DataFrame([
        {"Entity IDs": "1", "Rule Triggered": "Shared Address", "Supporting Evidence": "123 Main St"},
    ])

    high_risk_export, lead_summary = build_lead_exports(entity_risk, relationships, anomaly)

    assert list(high_risk_export.columns) == [
        "entity_id",
        "entity_type",
        "display_name",
        "risk_score",
        "risk_level",
        "relationship_count",
        "contributing_rules",
        "supporting_evidence",
    ]
    assert len(high_risk_export) == 1
    assert high_risk_export.iloc[0]["display_name"] == "ACME Corp"

    assert list(lead_summary.columns) == [
        "display_name",
        "risk_score",
        "why_flagged",
        "connected_entity_count",
        "recommended_review_action",
    ]
    assert len(lead_summary) == 1
    assert "Rules: Shared Address|Shared Phone" in lead_summary.iloc[0]["why_flagged"]
    assert lead_summary.iloc[0]["connected_entity_count"] == 5


def test_main_writes_export_files(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    exports_dir = tmp_path / "exports"
    processed_dir.mkdir(parents=True)
    exports_dir.mkdir(parents=True)

    entity_risk_path = processed_dir / "entity_risk.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    high_risk_export = exports_dir / "high_risk_entities.csv"
    lead_summary_export = exports_dir / "lead_summary.csv"

    pd.DataFrame([
        {
            "entity_id": "1",
            "entity_type": "business",
            "display_name": "ACME Corp",
            "risk_score": 95,
            "risk_level": "High",
            "relationship_count": 3,
            "contributing_rules": "Shared Website",
            "supporting_evidence": "acme.com",
        }
    ]).to_csv(entity_risk_path, index=False)
    pd.DataFrame([
        {"source_entity_id": "1", "target_entity_id": "website:acme.com", "relationship_type": "HAS_WEBSITE", "confidence": 1.0},
    ]).to_csv(relationships_path, index=False)
    pd.DataFrame([
        {"Entity IDs": "1", "Rule Triggered": "Shared Website", "Supporting Evidence": "acme.com"},
    ]).to_csv(anomaly_path, index=False)

    main(
        entity_risk_path=entity_risk_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        high_risk_export_path=high_risk_export,
        summary_export_path=lead_summary_export,
    )

    assert high_risk_export.exists()
    assert lead_summary_export.exists()

    high_risk_df = pd.read_csv(high_risk_export)
    summary_df = pd.read_csv(lead_summary_export)

    assert len(high_risk_df) == 1
    assert len(summary_df) == 1
    assert high_risk_df.iloc[0]["risk_level"] == "High"
    assert summary_df.iloc[0]["recommended_review_action"] == "Inspect website registration and linked entity relationships."


def test_build_lead_exports_prefers_investigation_workspace_outputs() -> None:
    entity_risk = pd.DataFrame([{"entity_id": "1", "risk_level": "High"}])
    relationships = pd.DataFrame()
    anomaly = pd.DataFrame()
    investigation_leads = pd.DataFrame(
        [
            {
                "lead_id": "lead:1",
                "entity_id": "1",
                "Primary Entity": "Acme Group",
                "Lead Summary": "Acme Group scored 90 with multiple markers.",
                "Risk Score": 90,
                "Confidence": "High",
                "Priority": "Critical",
                "Status": "Open",
                "Fraud Marker Count": 3,
                "Supporting Source Count": 2,
                "Relationship Count": 4,
                "source_name": "synthetic|sample_api",
                "source_type": "synthetic|api",
                "Fraud Markers": "Shared Address|Shared Website",
                "Recommended Review": "Review all linked records.",
            }
        ]
    )
    timelines = pd.DataFrame([{"lead_id": "lead:1", "Event": "Fraud marker: Shared Address"}])
    evidence = pd.DataFrame([{"lead_id": "lead:1", "Supporting Evidence": "Shared office", "Recommended Review": "Review all linked records."}])

    high_risk_export, lead_summary = build_lead_exports(
        entity_risk,
        relationships,
        anomaly,
        investigation_leads_df=investigation_leads,
        entity_timelines_df=timelines,
        evidence_packets_df=evidence,
    )

    assert "lead_id" in high_risk_export.columns
    assert high_risk_export.iloc[0]["Priority"] == "Critical"
    assert lead_summary.iloc[0]["timeline_event_count"] == 1
    assert "Shared office" in lead_summary.iloc[0]["evidence"]
