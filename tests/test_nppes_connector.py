from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.app.utils.dashboard_io import load_cross_source_matches
from src.app.utils.dashboard_filters import parse_bool_series
from src.connectors.nppes.api_connector import NPPESAPIConnector
from src.connectors.nppes.bulk_file_connector import NPPESBulkFileConnector
from src.connectors.nppes.correlation import generate_nppes_correlations


def test_nppes_api_connector_mock_generates_expected_rows() -> None:
    connector = NPPESAPIConnector(mock=True, state="FL", city="Tampa", max_records=10)

    provider_rows, entity_rows, relationship_rows, taxonomy_rows, summary, manifest = connector.run()

    assert len(provider_rows) == 3
    assert any(row["entity_id"] == "provider:nppes:1013012345" for row in entity_rows)
    assert any(row["relationship_type"] == "PROVIDER_PRACTICES_AT" for row in relationship_rows)
    assert any(row["taxonomy_code"] == "261QF0400X" for row in taxonomy_rows)
    assert summary["mode"] == "mock"
    assert manifest["result_status"] == "SUCCESS"


def test_nppes_bulk_connector_filters_and_deduplicates_fixture() -> None:
    fixture_path = Path("tests/fixtures/nppes/nppes_sample.csv")
    connector = NPPESBulkFileConnector(
        input_path=fixture_path,
        state="FL",
        postal_prefix="336",
        max_records=10,
    )

    provider_rows, entity_rows, relationship_rows, taxonomy_rows, summary, _ = connector.run()

    assert len(provider_rows) == 3
    assert summary["duplicate_records_removed"] == 1
    assert any(row["relationship_type"] == "PROVIDER_MAILS_TO" for row in relationship_rows)
    assert any(row["address_match_scope"] == "MAILING_ONLY" for row in entity_rows if row["entity_type"] == "address")
    assert len(taxonomy_rows) >= 2


def test_generate_nppes_correlations_writes_three_source_outputs(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)

    connector = NPPESAPIConnector(mock=True, state="FL", city="Tampa", max_records=10, output_dir=processed_dir)
    provider_rows, entity_rows, relationship_rows, taxonomy_rows, _, _ = connector.run()
    pd.DataFrame(provider_rows).to_csv(processed_dir / "nppes_providers.csv", index=False)
    pd.DataFrame(entity_rows).to_csv(processed_dir / "nppes_entities.csv", index=False)
    pd.DataFrame(relationship_rows).to_csv(processed_dir / "nppes_relationships.csv", index=False)
    pd.DataFrame(taxonomy_rows).to_csv(processed_dir / "nppes_taxonomies.csv", index=False)
    practice_address = next(
        row["display_name"]
        for row in entity_rows
        if row["entity_type"] == "address" and row.get("address_role") == "practice" and row.get("npi") == "1417654321"
    )

    pd.DataFrame(
        [
            {
                "entity_id": "sunbiz-address:1",
                "display_name": practice_address,
                "entity_type": "address",
                "source_name": "sunbiz_daily",
                "source_type": "api",
                "source_record_id": "L26000010001",
                "corporation_number": "L26000010001",
            }
        ]
    ).to_csv(processed_dir / "sunbiz_entities.csv", index=False)

    pd.DataFrame(
        [
            {
                "entity_id": "property:HC-1",
                "display_name": "HC-1",
                "entity_type": "property",
                "source_name": "county_property_local_file",
                "source_type": "connector",
                "source_record_id": "HC-1",
            },
                {
                    "entity_id": "address:HC-1:situs",
                    "display_name": practice_address,
                    "entity_type": "address",
                    "source_name": "county_property_local_file",
                "source_type": "connector",
                "source_record_id": "HC-1:situs",
            },
        ]
    ).to_csv(processed_dir / "county_property_entities.csv", index=False)
    pd.DataFrame(
        [
            {
                "relationship_id": "property:HC-1:situs",
                "source_entity_id": "property:HC-1",
                "target_entity_id": "address:HC-1:situs",
                "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS",
                "source_record_id": "HC-1",
                "source_name": "county_property_local_file",
                "source_type": "connector",
            }
        ]
    ).to_csv(processed_dir / "county_property_relationships.csv", index=False)

    report = generate_nppes_correlations(processed_dir=processed_dir, append_to_cross_source=True)

    assert report["npi_to_sunbiz_candidates"] >= 1
    assert report["npi_to_parcel_candidates"] >= 1
    assert report["three_source_paths"] >= 1
    assert (processed_dir / "nppes_sunbiz_matches.csv").exists()
    assert (processed_dir / "nppes_parcel_matches.csv").exists()
    assert (processed_dir / "nppes_sunbiz_parcel_paths.csv").exists()
    assert (processed_dir / "nppes_match_quality_report.json").exists()

    cross_source_df = load_cross_source_matches(processed_dir / "cross_source_matches.csv")
    assert "npi" in cross_source_df.columns
    assert "enumeration_type" in cross_source_df.columns
    assert parse_bool_series(cross_source_df["three_source_only"]).any()


def test_nppes_match_quality_report_is_json_object(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)
    (processed_dir / "nppes_entities.csv").write_text("", encoding="utf-8")

    report = generate_nppes_correlations(processed_dir=processed_dir, append_to_cross_source=False)

    assert report["providers_processed"] == 0
    payload = json.loads((processed_dir / "nppes_match_quality_report.json").read_text(encoding="utf-8"))
    assert payload["providers_processed"] == 0
