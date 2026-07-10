from pathlib import Path

import os
import pandas as pd
import pytest

from src.run_pipeline import clear_lead_packages, reset_generated_artifacts, run_pipeline


def test_run_pipeline_creates_all_steps(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"
    fraud_markers_path = processed_dir / "fraud_markers.csv"
    fraud_marker_summary_path = processed_dir / "fraud_marker_summary.csv"
    canonical_entities_path = processed_dir / "canonical_entities.csv"
    canonical_relationships_path = processed_dir / "canonical_relationships.csv"
    aliases_path = processed_dir / "entity_aliases.csv"
    matches_path = processed_dir / "entity_resolution_matches.csv"
    investigation_leads_path = processed_dir / "investigation_leads.csv"
    entity_timelines_path = processed_dir / "entity_timelines.csv"
    evidence_packets_path = processed_dir / "evidence_packets.csv"
    network_clusters_path = processed_dir / "network_clusters.csv"
    network_summary_path = processed_dir / "network_summary.csv"
    network_members_path = processed_dir / "network_members.csv"
    network_edges_path = processed_dir / "network_edges.csv"
    prioritized_leads_path = processed_dir / "prioritized_leads.csv"
    investigation_summary_path = processed_dir / "investigation_summary.csv"
    lead_evidence_index_path = processed_dir / "lead_evidence_index.csv"
    review_recommendations_path = processed_dir / "review_recommendations.csv"
    cross_source_matches_path = processed_dir / "cross_source_matches.csv"
    cross_source_diagnostics_path = processed_dir / "cross_source_diagnostics.csv"
    cross_source_summary_path = processed_dir / "cross_source_diagnostic_summary.json"
    statistical_baselines_path = processed_dir / "statistical_baselines.csv"
    statistical_rarity_path = processed_dir / "statistical_rarity.csv"
    contextual_adjustments_path = processed_dir / "contextual_risk_adjustments.csv"
    statistical_summary_path = processed_dir / "statistical_marker_summary.json"
    statistical_calibration_report_path = processed_dir / "statistical_calibration_report.csv"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
    )

    assert source_dir.exists()
    assert (source_dir / "business_entities.csv").exists()
    assert entities_path.exists()
    assert relationships_path.exists()
    assert anomaly_path.exists()
    assert entity_risk_path.exists()
    assert fraud_markers_path.exists()
    assert fraud_marker_summary_path.exists()
    assert canonical_entities_path.exists()
    assert canonical_relationships_path.exists()
    assert aliases_path.exists()
    assert matches_path.exists()
    assert investigation_leads_path.exists()
    assert entity_timelines_path.exists()
    assert evidence_packets_path.exists()
    assert network_clusters_path.exists()
    assert network_summary_path.exists()
    assert network_members_path.exists()
    assert network_edges_path.exists()
    assert prioritized_leads_path.exists()
    assert investigation_summary_path.exists()
    assert lead_evidence_index_path.exists()
    assert review_recommendations_path.exists()
    assert cross_source_matches_path.exists()
    assert cross_source_diagnostics_path.exists()
    assert cross_source_summary_path.exists()
    assert statistical_baselines_path.exists()
    assert statistical_rarity_path.exists()
    assert contextual_adjustments_path.exists()
    assert statistical_summary_path.exists()
    assert statistical_calibration_report_path.exists()

    entity_risk = pd.read_csv(entity_risk_path)
    assert "entity_id" in entity_risk.columns
    assert "risk_score" in entity_risk.columns
    assert "source_name" in entity_risk.columns
    assert "source_type" in entity_risk.columns
    canonical_entities = pd.read_csv(canonical_entities_path)
    assert "canonical_entity_id" in canonical_entities.columns
    fraud_markers = pd.read_csv(fraud_markers_path)
    assert "marker_name" in fraud_markers.columns
    investigation_leads = pd.read_csv(investigation_leads_path)
    assert "lead_id" in investigation_leads.columns
    assert "Priority" in investigation_leads.columns
    network_clusters = pd.read_csv(network_clusters_path)
    assert "network_id" in network_clusters.columns
    network_members = pd.read_csv(network_members_path)
    assert "community_id" in network_members.columns
    prioritized = pd.read_csv(prioritized_leads_path)
    assert "lead_type" in prioritized.columns
    assert "priority" in prioritized.columns
    cross_source_matches = pd.read_csv(cross_source_matches_path)
    assert "cross_source_match_id" in cross_source_matches.columns
    assert "decision" in cross_source_matches.columns
    statistical_rarity = pd.read_csv(statistical_rarity_path)
    assert "rarity_level" in statistical_rarity.columns
    assert "comparison_group" in statistical_rarity.columns


def test_run_pipeline_includes_sunbiz_connector(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"
    sunbiz_input_dir = tmp_path / "data" / "raw" / "sunbiz"
    sunbiz_input_dir.mkdir(parents=True)
    sunbiz_input = sunbiz_input_dir / "sunbiz_entities.csv"
    sunbiz_input.write_text(
        "record_id,business_name,address,phone,email,owner_name\n"
        "SB1,Sunbiz Sample,100 Commerce Blvd,555-1234,sample@sunbiz.gov,Sunbiz Owner\n",
        encoding="utf-8",
    )

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        include_connectors=True,
    )

    sunbiz_entities = processed_dir / "sunbiz_entities.csv"
    sunbiz_relationships = processed_dir / "sunbiz_relationships.csv"
    assert sunbiz_entities.exists()
    assert sunbiz_relationships.exists()

    entities = pd.read_csv(entities_path)
    assert "sunbiz:SB1" in set(entities["entity_id"])
    assert "source_name" in entities.columns
    assert "source_type" in entities.columns
    relationships = pd.read_csv(relationships_path)
    assert "sunbiz:SB1" in set(relationships["source_entity_id"])
    assert "source_name" in relationships.columns
    assert "source_type" in relationships.columns


def test_run_pipeline_skips_when_default_sunbiz_file_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        include_connectors=True,
    )

    captured = capsys.readouterr()
    assert "No default Sunbiz connector input found" in captured.out
    assert "Sample input is supported only when you run the connector explicitly with --input" in captured.out


def test_run_pipeline_includes_county_property_connector(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"
    county_property_input_dir = tmp_path / "data" / "raw" / "county_property"
    county_property_input_dir.mkdir(parents=True)
    county_property_input = county_property_input_dir / "property_records.csv"
    county_property_input.write_text(
        "parcel_id,owner_name,situs_address,mailing_address,property_use,land_use,assessed_value,sale_date,sale_price\n"
        "EC-3,County Owner,\"789 Pine Rd, Pensacola, FL 32505\",\"PO Box 700, Pensacola, FL 32506\",Residential,Single Family,275000,2024-08-01,260000\n",
        encoding="utf-8",
    )

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        include_connectors=True,
    )

    county_property_entities = processed_dir / "county_property_entities.csv"
    county_property_relationships = processed_dir / "county_property_relationships.csv"
    assert county_property_entities.exists()
    assert county_property_relationships.exists()

    entities = pd.read_csv(entities_path)
    assert "property:EC-3" in set(entities["entity_id"])
    assert "owner:County Owner" in set(entities["entity_id"])
    relationships = pd.read_csv(relationships_path)
    assert "property:EC-3" in set(relationships["source_entity_id"])


def test_run_pipeline_skips_when_default_county_property_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        include_connectors=True,
    )

    captured = capsys.readouterr()
    assert "No default county property connector input found" in captured.out
    assert "data/raw/county_property/property_records.csv" in captured.out


def test_run_pipeline_includes_county_clerk_connector(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"
    county_clerk_input_dir = tmp_path / "data" / "raw" / "county_clerk"
    county_clerk_input_dir.mkdir(parents=True)
    county_clerk_input = county_clerk_input_dir / "clerk_records.csv"
    county_clerk_input.write_text(
        "case_number,filing_date,record_type,party_name,party_role,business_name,address,document_type,status\n"
        "2025-CA-9,2025-07-01,Civil,Clerk Person,Plaintiff,Clerk Business LLC,\"321 Bay St, Pensacola, FL 32507\",Complaint,Open\n",
        encoding="utf-8",
    )

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        include_connectors=True,
    )

    county_clerk_entities = processed_dir / "county_clerk_entities.csv"
    county_clerk_relationships = processed_dir / "county_clerk_relationships.csv"
    assert county_clerk_entities.exists()
    assert county_clerk_relationships.exists()

    entities = pd.read_csv(entities_path)
    assert "case:2025-CA-9" in set(entities["entity_id"])
    assert "business:Clerk Business LLC" in set(entities["entity_id"])
    relationships = pd.read_csv(relationships_path)
    assert "case:2025-CA-9" in set(relationships["source_entity_id"])


def test_run_pipeline_skips_when_default_county_clerk_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        include_connectors=True,
    )

    captured = capsys.readouterr()
    assert "No default county clerk connector input found" in captured.out
    assert "data/raw/county_clerk/clerk_records.csv" in captured.out


def test_run_pipeline_includes_api_connector_outputs(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        include_connectors=True,
    )

    api_entities = processed_dir / "api_entities.csv"
    api_relationships = processed_dir / "api_relationships.csv"
    assert api_entities.exists()
    assert api_relationships.exists()

    entities = pd.read_csv(entities_path)
    assert "api:sample_api:API-001" in set(entities["entity_id"])
    relationships = pd.read_csv(relationships_path)
    assert "api:sample_api:API-001" in set(relationships["source_entity_id"])


def test_run_pipeline_includes_arcgis_connector_outputs(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        include_connectors=True,
    )

    arcgis_entities = processed_dir / "arcgis_entities.csv"
    arcgis_relationships = processed_dir / "arcgis_relationships.csv"
    assert arcgis_entities.exists()
    assert arcgis_relationships.exists()

    arcgis_entities_frame = pd.read_csv(arcgis_entities)
    arcgis_relationships_frame = pd.read_csv(arcgis_relationships)
    assert not arcgis_entities_frame.empty
    assert not arcgis_relationships_frame.empty
    assert set(arcgis_entities_frame["source"]) == {"florida_county_arcgis_parcels"}
    assert set(arcgis_entities_frame["source_type"]) == {"arcgis"}
    assert set(arcgis_relationships_frame["source_type"]) == {"arcgis"}
    assert "PROPERTY_OWNED_BY" in set(arcgis_relationships_frame["relationship_type"])

    entities = pd.read_csv(entities_path)
    assert any(str(entity_id).startswith("property:") for entity_id in entities["entity_id"])
    assert any(str(entity_id).startswith("owner:") for entity_id in entities["entity_id"])
    relationships = pd.read_csv(relationships_path)
    assert "PROPERTY_OWNED_BY" in set(relationships["relationship_type"])


def test_run_pipeline_rejects_remote_synthetic_source() -> None:
    with pytest.raises(ValueError, match="does not permit live or remote access"):
        run_pipeline(source_dir="https://example.com/synthetic")


def test_run_pipeline_runs_health_check_when_requested(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "entity_scoring.json").write_text("{}", encoding="utf-8")
    (config_dir / "rules.json").write_text("{}", encoding="utf-8")
    (config_dir / "sources.json").write_text("{}", encoding="utf-8")
    (config_dir / "api_sources.json").write_text("{}", encoding="utf-8")
    (config_dir / "entity_resolution.json").write_text("{}", encoding="utf-8")
    (config_dir / "fraud_markers.json").write_text("{}", encoding="utf-8")
    (config_dir / "network_detection.json").write_text("{}", encoding="utf-8")
    (config_dir / "investigation_engine.json").write_text("{}", encoding="utf-8")
    (config_dir / "cross_source.json").write_text("{}", encoding="utf-8")
    (config_dir / "statistical_risk.json").write_text("{}", encoding="utf-8")

    original_cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        run_pipeline(
            records=10,
            source_dir=source_dir,
            output_db=output_db,
            processed_dir=processed_dir,
            run_health_check=True,
        )
    finally:
        os.chdir(original_cwd)

    captured = capsys.readouterr()
    assert "Health check passed." in captured.out


def test_reset_generated_artifacts_deletes_only_generated_outputs(tmp_path: Path) -> None:
    output_db = tmp_path / "local_osint.duckdb"
    processed_dir = tmp_path / "data" / "processed"
    exports_dir = tmp_path / "exports"
    raw_dir = tmp_path / "data" / "raw"
    config_dir = tmp_path / "config"
    docs_dir = tmp_path / "docs"

    processed_dir.mkdir(parents=True)
    exports_dir.mkdir(parents=True)
    raw_dir.mkdir(parents=True)
    config_dir.mkdir(parents=True)
    docs_dir.mkdir(parents=True)

    output_db.write_bytes(b"duckdb")
    (processed_dir / "entities.csv").write_text("a\n1\n", encoding="utf-8")
    (processed_dir / "entities.parquet").write_text("parquet", encoding="utf-8")
    (processed_dir / "report.json").write_text("{}", encoding="utf-8")
    (exports_dir / "lead_summary.csv").write_text("a\n1\n", encoding="utf-8")
    raw_file = raw_dir / "keep.csv"
    config_file = config_dir / "keep.json"
    docs_file = docs_dir / "keep.md"
    raw_file.write_text("raw", encoding="utf-8")
    config_file.write_text("{}", encoding="utf-8")
    docs_file.write_text("# docs", encoding="utf-8")

    deleted = reset_generated_artifacts(output_db=output_db, processed_dir=processed_dir, exports_dir=exports_dir, workspace_root=tmp_path)

    assert output_db.resolve() in deleted
    assert not output_db.exists()
    assert not (processed_dir / "entities.csv").exists()
    assert not (processed_dir / "entities.parquet").exists()
    assert not (processed_dir / "report.json").exists()
    assert not (exports_dir / "lead_summary.csv").exists()
    assert raw_file.exists()
    assert config_file.exists()
    assert docs_file.exists()


def test_clear_lead_packages_deletes_generated_package_dirs_only(tmp_path: Path) -> None:
    package_root = tmp_path / "exports" / "leads"
    package_root.mkdir(parents=True)
    package_dir = package_root / "lead_1"
    package_dir.mkdir()
    (package_dir / "lead_summary.csv").write_text("a\n1\n", encoding="utf-8")
    keep_state = tmp_path / "data" / "processed" / "analyst_lead_state.csv"
    keep_state.parent.mkdir(parents=True)
    keep_state.write_text("lead_id,status\nlead:1,NEW\n", encoding="utf-8")

    deleted = clear_lead_packages(package_root=package_root, workspace_root=tmp_path)

    assert package_dir.resolve() in deleted
    assert not package_dir.exists()
    assert keep_state.exists()


def test_run_pipeline_reset_rebuilds_outputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"
    processed_dir.mkdir(parents=True)
    output_db.write_bytes(b"old-db")
    stale_csv = processed_dir / "stale.csv"
    stale_csv.write_text("stale", encoding="utf-8")
    lead_package_dir = tmp_path / "exports" / "leads" / "lead_1"
    lead_package_dir.mkdir(parents=True)
    (lead_package_dir / "lead_summary.csv").write_text("a\n1\n", encoding="utf-8")

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
        reset=True,
        clear_lead_packages_flag=True,
    )

    captured = capsys.readouterr()
    assert "Reset generated artifacts" in captured.out
    assert "Deleted" in captured.out
    assert output_db.exists()
    assert not stale_csv.exists()
    assert entities_path.exists()
    assert not lead_package_dir.exists()
