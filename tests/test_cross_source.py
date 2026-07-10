from pathlib import Path

import pandas as pd

from src.analytics.cross_source import build_cross_source_matches, load_cross_source_config, run_cross_source_correlation
from src.connectors.source_metadata import apply_provenance
from src.investigation.investigation_engine import run_investigation_engine


def test_apply_provenance_standardizes_source_type() -> None:
    row = apply_provenance({"entity_id": "e1"}, "florida_county_arcgis_parcels", source_type_hint="arcgis_api", source_record_id="RID-1")

    assert row["source_name"] == "florida_county_arcgis_parcels"
    assert row["source_type"] == "arcgis"
    assert row["source_record_id"] == "RID-1"
    assert row["connector_name"] == "florida_county_arcgis_parcels"


def test_build_cross_source_matches_generates_exact_and_compound_real_matches() -> None:
    canonical_entities = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "entity_id": "canonical:address:1",
                "entity_type": "address",
                "display_name": "123 MAIN ST PENSACOLA FL 32501",
                "normalized_value": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "sample_api|florida_county_arcgis_parcels",
                "source_type": "api|arcgis",
                "resolution_confidence": 0.99,
                "resolution_method": "exact_address",
            },
            {
                "canonical_entity_id": "canonical:property:1",
                "entity_id": "canonical:property:1",
                "entity_type": "property",
                "display_name": "PARCEL-1",
                "normalized_value": "PARCEL1",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "resolution_confidence": 1.0,
                "resolution_method": "exact_property_parcel",
            },
            {
                "canonical_entity_id": "canonical:business:1",
                "entity_id": "canonical:business:1",
                "entity_type": "business",
                "display_name": "ACME LLC",
                "normalized_value": "ACME",
                "source_name": "sample_api",
                "source_type": "api",
                "resolution_confidence": 0.97,
                "resolution_method": "exact_name_plus_secondary",
            },
        ]
    )
    aliases = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:123",
                "alias_value": "123 Main St, Pensacola, FL 32501",
                "normalized_alias": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "sample_api",
                "source_type": "api",
                "source_record_id": "API-1",
            },
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:123-arcgis",
                "alias_value": "123 Main St, Pensacola, FL 32501",
                "normalized_alias": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "source_record_id": "PARCEL-1",
            },
        ]
    )
    relationships = pd.DataFrame(
        [
            {
                "relationship_id": "r1",
                "source_entity_id": "canonical:property:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "source_record_id": "PARCEL-1",
            },
            {
                "relationship_id": "r2",
                "source_entity_id": "canonical:business:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "LOCATED_AT",
                "source_name": "sample_api",
                "source_type": "api",
                "source_record_id": "API-1",
            },
        ]
    )

    matches = build_cross_source_matches(canonical_entities, aliases, relationships, load_cross_source_config())

    assert "exact_canonical_address" in set(matches["match_method"])
    assert "property_situs_matches_business_address" in set(matches["match_method"])
    assert "AUTO_MATCH" in set(matches["decision"])


def test_build_cross_source_matches_excludes_synthetic_from_real_support() -> None:
    canonical_entities = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "entity_id": "canonical:address:1",
                "entity_type": "address",
                "display_name": "123 MAIN",
                "normalized_value": "123 MAIN",
                "source_name": "synthetic|sample_api",
                "source_type": "synthetic|api",
                "resolution_confidence": 0.99,
                "resolution_method": "exact_address",
            }
        ]
    )
    aliases = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:synthetic",
                "alias_value": "123 Main",
                "normalized_alias": "123 MAIN",
                "source_name": "synthetic",
                "source_type": "synthetic",
                "source_record_id": "SYN-1",
            },
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:api",
                "alias_value": "123 Main",
                "normalized_alias": "123 MAIN",
                "source_name": "sample_api",
                "source_type": "api",
                "source_record_id": "API-1",
            },
        ]
    )

    matches = build_cross_source_matches(canonical_entities, aliases, pd.DataFrame(), load_cross_source_config())

    assert "REJECTED_SYNTHETIC_OR_SINGLE_REAL" in set(matches["decision"])


def test_person_matching_requires_secondary_evidence() -> None:
    canonical_entities = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:owner:1",
                "entity_id": "canonical:owner:1",
                "entity_type": "owner",
                "display_name": "JANE DOE",
                "normalized_value": "JANE DOE",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "resolution_confidence": 0.95,
                "resolution_method": "exact_name",
            },
            {
                "canonical_entity_id": "canonical:person:1",
                "entity_id": "canonical:person:1",
                "entity_type": "person",
                "display_name": "JANE DOE",
                "normalized_value": "JANE DOE",
                "source_name": "county_clerk_local_file",
                "source_type": "connector",
                "resolution_confidence": 0.95,
                "resolution_method": "exact_name",
            },
        ]
    )

    matches = build_cross_source_matches(canonical_entities, pd.DataFrame(), pd.DataFrame(), load_cross_source_config())

    assert "REJECTED_NO_SECONDARY_EVIDENCE" in set(matches["decision"])


def test_run_cross_source_correlation_writes_outputs_and_investigation_engine_generates_cross_source_lead(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    canonical_entities = pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "entity_id": "canonical:address:1",
                "entity_type": "address",
                "display_name": "123 MAIN ST PENSACOLA FL 32501",
                "normalized_value": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "sample_api|florida_county_arcgis_parcels",
                "source_type": "api|arcgis",
                "resolution_confidence": 0.99,
                "resolution_method": "exact_address",
            },
            {
                "canonical_entity_id": "canonical:property:1",
                "entity_id": "canonical:property:1",
                "entity_type": "property",
                "display_name": "PARCEL-1",
                "normalized_value": "PARCEL1",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "resolution_confidence": 1.0,
                "resolution_method": "exact_property_parcel",
            },
            {
                "canonical_entity_id": "canonical:business:1",
                "entity_id": "canonical:business:1",
                "entity_type": "business",
                "display_name": "ACME LLC",
                "normalized_value": "ACME",
                "source_name": "sample_api",
                "source_type": "api",
                "resolution_confidence": 0.97,
                "resolution_method": "exact_name_plus_secondary",
            },
        ]
    )
    canonical_entities.to_csv(processed / "canonical_entities.csv", index=False)
    pd.DataFrame(
        [
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:123",
                "alias_value": "123 Main St, Pensacola, FL 32501",
                "normalized_alias": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "sample_api",
                "source_type": "api",
                "source_record_id": "API-1",
            },
            {
                "canonical_entity_id": "canonical:address:1",
                "original_entity_id": "address:123-arcgis",
                "alias_value": "123 Main St, Pensacola, FL 32501",
                "normalized_alias": "123 MAIN ST PENSACOLA FL 32501",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "source_record_id": "PARCEL-1",
            },
        ]
    ).to_csv(processed / "entity_aliases.csv", index=False)
    pd.DataFrame(
        [
            {
                "relationship_id": "r1",
                "source_entity_id": "canonical:property:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS",
                "source_name": "florida_county_arcgis_parcels",
                "source_type": "arcgis",
                "source_record_id": "PARCEL-1",
            },
            {
                "relationship_id": "r2",
                "source_entity_id": "canonical:business:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "LOCATED_AT",
                "source_name": "sample_api",
                "source_type": "api",
                "source_record_id": "API-1",
            },
        ]
    ).to_csv(processed / "canonical_relationships.csv", index=False)
    pd.DataFrame(columns=["match_id", "left_entity_id", "right_entity_id", "entity_type", "match_method", "confidence_score", "decision", "evidence", "source_names"]).to_csv(processed / "entity_resolution_matches.csv", index=False)
    pd.DataFrame([{"entity_id": "canonical:address:1", "entity_type": "address", "display_name": "123 MAIN ST", "risk_score": 45, "marker_count": 1, "relationship_count": 2, "source_name": "sample_api|florida_county_arcgis_parcels", "source_type": "api|arcgis", "average_marker_confidence": 0.9}]).to_csv(processed / "entity_risk.csv", index=False)
    pd.DataFrame([{"entity_id": "canonical:address:1", "marker_id": "cross_source_multi_source_address", "marker_name": "Cross-Source Address Support", "marker_category": "cross_source", "risk_contribution": 18, "confidence": "High", "confidence_score": 0.88, "support": 2, "sources": "sample_api|florida_county_arcgis_parcels", "source_types": "api|arcgis", "supporting_entities": "canonical:address:1", "supporting_relationships": "", "recommended_review": "Review", "explanation": "Shared address"}]).to_csv(processed / "fraud_markers.csv", index=False)
    pd.DataFrame([{"lead_id": "lead:seed", "entity_id": "canonical:address:1", "Lead Title": "Address lead", "Lead Summary": "Summary", "Risk Score": 45, "Fraud Marker Count": 1, "Supporting Source Count": 2, "Relationship Count": 2, "source_name": "sample_api|florida_county_arcgis_parcels", "source_type": "api|arcgis", "Cross-Source Correlation": "Yes", "Entity Resolution Confidence": 0.99, "Risk Explanation": "Shared address", "Recommended Review": "Review", "Fraud Markers": "Cross-Source Address Support"}]).to_csv(processed / "investigation_leads.csv", index=False)
    pd.DataFrame(columns=["lead_id", "entity_id"]).to_csv(processed / "evidence_packets.csv", index=False)
    pd.DataFrame(columns=["lead_id", "entity_id"]).to_csv(processed / "entity_timelines.csv", index=False)
    pd.DataFrame(columns=["network_id", "entity_id"]).to_csv(processed / "network_members.csv", index=False)
    pd.DataFrame(columns=["network_id"]).to_csv(processed / "network_clusters.csv", index=False)

    summary = run_cross_source_correlation(
        canonical_entities_path=processed / "canonical_entities.csv",
        aliases_path=processed / "entity_aliases.csv",
        entity_resolution_matches_path=processed / "entity_resolution_matches.csv",
        canonical_relationships_path=processed / "canonical_relationships.csv",
        fraud_markers_path=processed / "fraud_markers.csv",
        prioritized_leads_path=processed / "prioritized_leads.csv",
        cross_source_matches_path=processed / "cross_source_matches.csv",
        diagnostics_path=processed / "cross_source_diagnostics.csv",
        diagnostic_summary_path=processed / "cross_source_diagnostic_summary.json",
    )

    assert summary["auto_match_count"] >= 1

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
    assert "CROSS_SOURCE_CLUSTER" in set(prioritized["lead_type"])
