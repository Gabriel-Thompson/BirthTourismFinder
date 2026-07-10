from pathlib import Path

import pandas as pd

from src.investigation.investigation_engine import run_investigation_engine
from src.investigation.prioritization import (
    assign_priority_score,
    compute_confidence_score,
    consolidate_duplicate_leads,
    deterministic_lead_id,
    evidence_completeness,
    load_investigation_engine_config,
)
from src.investigation.recommendations import review_steps


def test_deterministic_lead_id_is_stable() -> None:
    assert deterministic_lead_id("ENTITY", "canonical:business:1") == deterministic_lead_id("ENTITY", "canonical:business:1")
    assert deterministic_lead_id("ENTITY", "canonical:business:1") != deterministic_lead_id("NETWORK", "canonical:business:1")


def test_risk_and_confidence_are_separate() -> None:
    high_risk_low_confidence = compute_confidence_score(1, 0.4, 2, 0.3, 0.3, 40, False)
    high_risk_priority = assign_priority_score(
        {
            "risk_score": 90,
            "confidence_score": high_risk_low_confidence,
            "fraud_marker_count": 4,
            "independent_source_count": 1,
            "cross_source_match_count": 0,
            "relationship_density": 0.8,
            "network_member_count": 2,
            "bridge_entity_count": 0,
            "entity_resolution_confidence": 0.4,
            "evidence_completeness_score": 40,
            "temporal_concentration": 0.1,
            "contains_real_data": False,
        },
        load_investigation_engine_config(),
    )
    assert high_risk_priority > 0
    assert high_risk_low_confidence < 0.6


def test_evidence_completeness_reports_missing_fields() -> None:
    score, missing, evidence_count = evidence_completeness(
        {
            "primary_entity_id": "canonical:business:1",
            "fraud_marker_count": 0,
            "relationship_count": 0,
            "source_names": "",
            "timeline_event_count": 0,
            "alias_count": 0,
            "entity_resolution_confidence": 0,
            "network_id": "",
            "recommended_review": "",
        },
        load_investigation_engine_config()["evidence_completeness_weights"],
    )
    assert score < 100
    assert "marker_evidence" in missing
    assert evidence_count >= 1


def test_consolidate_duplicate_leads_preserves_strongest() -> None:
    df = pd.DataFrame(
        [
            {
                "lead_id": "lead:1",
                "lead_type": "ENTITY",
                "primary_entity_id": "canonical:business:1",
                "network_id": "",
                "priority_score": 80,
                "risk_score": 70,
                "confidence_score": 0.7,
                "fraud_marker_count": 2,
                "independent_source_count": 1,
                "relationship_count": 2,
                "cross_source_match_count": 0,
                "network_member_count": 0,
                "bridge_entity_count": 0,
                "source_names": "synthetic",
                "source_types": "synthetic",
                "contains_real_data": False,
                "contains_synthetic_data": True,
            },
            {
                "lead_id": "lead:2",
                "lead_type": "ENTITY",
                "primary_entity_id": "canonical:business:1",
                "network_id": "",
                "priority_score": 90,
                "risk_score": 80,
                "confidence_score": 0.8,
                "fraud_marker_count": 3,
                "independent_source_count": 2,
                "relationship_count": 4,
                "cross_source_match_count": 1,
                "network_member_count": 5,
                "bridge_entity_count": 1,
                "source_names": "synthetic|sample_api",
                "source_types": "synthetic|api",
                "contains_real_data": True,
                "contains_synthetic_data": True,
            },
        ]
    )

    deduped = consolidate_duplicate_leads(df)

    assert len(deduped) == 1
    assert deduped.iloc[0]["lead_id"] == "lead:2"
    assert "lead:1" in deduped.iloc[0]["related_lead_ids"]


def test_review_recommendations_are_explainable() -> None:
    text = review_steps(
        {
            "lead_type": "NETWORK",
            "primary_entity_type": "network",
            "fraud_markers": "Shared Address",
            "evidence_completeness_score": 55,
        }
    )
    assert "bridge entities" in text.lower()
    assert "requires evidence validation" in text.lower()


def test_run_investigation_engine_preserves_analyst_state_and_exports_packages(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    exports_dir = tmp_path / "exports" / "leads"
    processed_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "lead_id": "lead:seed",
                "entity_id": "canonical:business:1",
                "Primary Entity": "Acme Group",
                "Lead Title": "Acme lead",
                "Lead Summary": "Acme flagged",
                "Risk Score": 82,
                "Confidence": "High",
                "Priority": "High",
                "Status": "Open",
                "Date Generated": "2026-07-09",
                "Fraud Marker Count": 2,
                "Supporting Source Count": 2,
                "Relationship Count": 3,
                "source_name": "sample_api|synthetic",
                "source_type": "api|synthetic",
                "Cross-Source Correlation": "Yes",
                "Entity Resolution Confidence": 0.95,
                "Risk Explanation": "Shared Address",
                "Recommended Review": "Review linked records.",
                "Fraud Markers": "Shared Address|Shared Website",
                "Lead Notes": "keep me",
                "Reviewer": "Analyst",
                "Review Date": "2026-07-08",
                "Disposition": "",
                "Review Status": "",
                "Follow-up Needed": "",
            }
        ]
    ).to_csv(processed_dir / "investigation_leads.csv", index=False)
    pd.DataFrame(
        [
            {
                "network_id": "network:0001",
                "network_size": 5,
                "business_count": 2,
                "address_count": 1,
                "property_count": 0,
                "owner_count": 1,
                "registered_agent_count": 0,
                "officer_count": 0,
                "relationship_count": 4,
                "fraud_marker_count": 2,
                "independent_source_count": 2,
                "cross_source_matches": 1,
                "entity_resolution_confidence": 0.96,
                "network_risk_score": 88,
                "network_confidence_score": 0.82,
                "network_confidence": "High",
                "network_priority": "Critical",
                "relationship_density": 0.6,
                "bridge_entity_count": 1,
                "community_count": 2,
                "source_name": "sample_api|synthetic",
                "source_type": "api|synthetic",
                "explanation": "2 businesses | 1 address",
                "top_markers": "Shared Address",
                "latest_activity_date": "2026-07-09",
                "timeline_event_count": 4,
                "fast_growth_score": 0.8,
            }
        ]
    ).to_csv(processed_dir / "network_clusters.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "entity_type": "business",
                "display_name": "Acme Group",
                "risk_score": 82,
                "risk_level": "High",
                "confidence": "High",
                "relationship_count": 3,
                "source_count": 2,
                "source_name": "sample_api|synthetic",
                "source_type": "api|synthetic",
                "average_marker_confidence": 0.82,
            }
        ]
    ).to_csv(processed_dir / "entity_risk.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "marker_id": "shared_address_businesses",
                "marker_name": "Shared Address",
                "confidence": "High",
                "confidence_score": 0.82,
                "sources": "sample_api|synthetic",
                "source_types": "api|synthetic",
                "explanation": "Shared office",
            }
        ]
    ).to_csv(processed_dir / "fraud_markers.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "display_name": "Acme Group",
                "entity_type": "business",
                "source_name": "sample_api|synthetic",
                "source_type": "api|synthetic",
                "resolution_confidence": 0.95,
            }
        ]
    ).to_csv(processed_dir / "canonical_entities.csv", index=False)
    pd.DataFrame(
        [
            {
                "relationship_id": "r1",
                "source_entity_id": "canonical:business:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "LOCATED_AT",
                "source_name": "sample_api",
                "source_type": "api",
                "evidence": "located at",
                "confidence": 1.0,
            }
        ]
    ).to_csv(processed_dir / "canonical_relationships.csv", index=False)
    pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:business:1",
                "alias_value": "ACME GROUP LLC",
                "normalized_alias": "ACME GROUP LLC",
                "source_name": "sample_api",
                "source_type": "api",
            }
        ]
    ).to_csv(processed_dir / "entity_aliases.csv", index=False)
    pd.DataFrame(
        [
            {
                "lead_id": "lead:seed",
                "entity_id": "canonical:business:1",
                "Primary Entity": "Acme Group",
                "Supporting Evidence": "Shared office",
                "Source": "sample_api",
                "source_name": "sample_api",
                "source_type": "api",
                "Record ID": "RID-1",
                "Import Date": "2026-07-09",
                "Fraud Markers": "Shared Address",
                "Relationships": "LOCATED_AT",
            }
        ]
    ).to_csv(processed_dir / "evidence_packets.csv", index=False)
    pd.DataFrame(
        [
            {
                "lead_id": "lead:seed",
                "entity_id": "canonical:business:1",
                "Date": "2026-07-09",
                "Event": "Fraud marker: Shared Address",
                "Entity": "Acme Group",
                "Source": "sample_api",
                "source_name": "sample_api",
                "source_type": "api",
                "Evidence": "Shared office",
            }
        ]
    ).to_csv(processed_dir / "entity_timelines.csv", index=False)
    pd.DataFrame(
        [
            {
                "network_id": "network:0001",
                "entity_id": "canonical:business:1",
                "display_name": "Acme Group",
                "entity_type": "business",
                "bridge_flag": "Yes",
                "community_id": "community:0001",
            }
        ]
    ).to_csv(processed_dir / "network_members.csv", index=False)
    pd.DataFrame(
        [
            {
                "lead_id": deterministic_lead_id("CROSS_SOURCE_CLUSTER", "canonical:business:1", "network:0001"),
                "status": "IN_REVIEW",
                "analyst_notes": "preserve this note",
                "reviewer": "Analyst A",
                "review_date": "2026-07-08",
                "disposition": "Pending",
                "follow_up_needed": "Yes",
                "updated_at": "2026-07-08",
            }
        ]
    ).to_csv(processed_dir / "analyst_lead_state.csv", index=False)

    summary = run_investigation_engine(
        investigation_leads_path=processed_dir / "investigation_leads.csv",
        network_clusters_path=processed_dir / "network_clusters.csv",
        entity_risk_path=processed_dir / "entity_risk.csv",
        fraud_markers_path=processed_dir / "fraud_markers.csv",
        canonical_entities_path=processed_dir / "canonical_entities.csv",
        canonical_relationships_path=processed_dir / "canonical_relationships.csv",
        aliases_path=processed_dir / "entity_aliases.csv",
        evidence_packets_path=processed_dir / "evidence_packets.csv",
        entity_timelines_path=processed_dir / "entity_timelines.csv",
        network_members_path=processed_dir / "network_members.csv",
        prioritized_leads_path=processed_dir / "prioritized_leads.csv",
        investigation_summary_path=processed_dir / "investigation_summary.csv",
        lead_evidence_index_path=processed_dir / "lead_evidence_index.csv",
        review_recommendations_path=processed_dir / "review_recommendations.csv",
        analyst_state_path=processed_dir / "analyst_lead_state.csv",
        package_root=exports_dir,
    )

    prioritized = pd.read_csv(processed_dir / "prioritized_leads.csv")
    evidence_index = pd.read_csv(processed_dir / "lead_evidence_index.csv")
    recommendations = pd.read_csv(processed_dir / "review_recommendations.csv")

    assert summary["total_prioritized_leads"] >= 2
    assert not prioritized.empty
    assert not evidence_index.empty
    assert not recommendations.empty
    assert "evidence_completeness_score" in prioritized.columns
    assert "status" in prioritized.columns
    assert "preserve this note" in prioritized["analyst_notes"].astype(str).tolist()
    assert any(path.name == "lead_summary.csv" for path in exports_dir.rglob("lead_summary.csv"))
