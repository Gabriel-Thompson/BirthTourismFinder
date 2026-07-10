from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connectors import source_manifest
from src.connectors.arcgis.arcgis_connector import ArcGISRESTConnector
from src.connectors.open_data_api import write_csv


def test_arcgis_connector_uses_mock_response_and_exports(tmp_path: Path) -> None:
    connector = ArcGISRESTConnector("sample_arcgis_parcels")

    entities, relationships = connector.run()

    assert any(row["entity_id"] == "property:ARC-001" for row in entities)
    assert any(row["entity_id"] == "owner:ArcGIS Owner LLC" for row in entities)
    assert any(row["entity_id"] == "address:700 MAP AVE, PENSACOLA, FL 32501" for row in entities)
    assert any(row["relationship_type"] == "PROPERTY_OWNED_BY" for row in relationships)
    assert any(row["relationship_type"] == "PROPERTY_HAS_SITUS_ADDRESS" for row in relationships)
    assert any(row["relationship_type"] == "PROPERTY_HAS_MAILING_ADDRESS" for row in relationships)

    entities_output = tmp_path / "arcgis_entities.csv"
    relationships_output = tmp_path / "arcgis_relationships.csv"
    write_csv(entities_output, entities)
    write_csv(relationships_output, relationships)

    assert entities_output.exists()
    assert relationships_output.exists()


def test_arcgis_connector_parses_feature_payload_with_custom_fields(tmp_path: Path) -> None:
    mock_json = tmp_path / "arcgis.json"
    mock_json.write_text(
        json.dumps(
            {
                "features": [
                    {
                        "attributes": {
                            "PID": "ARC-9",
                            "OWNER": "ArcGIS Example",
                            "SITE_ADDR": "800 River Rd, Pensacola, FL 32502",
                            "MAIL_ADDR": "PO Box 800, Pensacola, FL 32503",
                            "USE_CODE": "Residential",
                            "VALUE": 410000,
                            "SALE_DT": "2025-02-01",
                            "SALE_AMT": 395000
                        },
                        "geometry": {"x": -87.3, "y": 30.5}
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "api_sources.json"
    config_path.write_text(
        json.dumps(
            {
                "sample_arcgis_parcels": {
                    "source_name": "sample_arcgis_parcels",
                    "base_url": "https://sampleserver.example.invalid",
                    "endpoint": "/query",
                    "query_params": {"where": "1=1"},
                    "response_format": "json",
                    "timeout_seconds": 5,
                    "retry_attempts": 1,
                    "retry_backoff_seconds": 0.0,
                    "rate_limit_per_minute": 60,
                    "mock_response_path": str(mock_json),
                    "field_map": {
                        "parcel_id": "PID",
                        "owner_name": "OWNER",
                        "situs_address": "SITE_ADDR",
                        "mailing_address": "MAIL_ADDR",
                        "land_use": "USE_CODE",
                        "assessed_value": "VALUE",
                        "sale_date": "SALE_DT",
                        "sale_price": "SALE_AMT",
                        "latitude": "y",
                        "longitude": "x"
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    connector = ArcGISRESTConnector("sample_arcgis_parcels", config_path=config_path)
    entities, relationships = connector.run()

    assert any(row["entity_id"] == "property:ARC-9" for row in entities)
    assert any(row["target_entity_id"] == "address:800 RIVER RD, PENSACOLA, FL 32502" for row in relationships)


def test_arcgis_connector_refuses_source_without_live_access(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = tmp_path / "sources.json"
    review_doc = tmp_path / "sample_arcgis.md"
    review_doc.write_text("# review", encoding="utf-8")
    manifest.write_text(
        json.dumps(
            {
                "sample_arcgis_parcels": {
                    "source_name": "sample_arcgis_parcels",
                    "source_type": "official_arcgis_demo",
                    "access_method": "official_arcgis_rest_or_mocked_response",
                    "live_access_allowed": False,
                    "terms_review_required": True,
                    "review_document": str(review_doc),
                    "imported_at": "2026-07-08T00:00:00Z",
                    "data_path": "data/raw/api/sample_arcgis_parcels_response.json",
                    "processed_outputs": ["data/processed/arcgis_entities.csv", "data/processed/arcgis_relationships.csv"],
                    "notes": "test"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    with pytest.raises(ValueError, match="not approved for live access"):
        ArcGISRESTConnector("sample_arcgis_parcels")


def test_arcgis_connector_applies_limit_override() -> None:
    connector = ArcGISRESTConnector("sample_arcgis_parcels", limit=100)

    assert connector.query_params["resultRecordCount"] == 100


def test_arcgis_connector_refuses_documented_escambia_source_until_live_access_is_approved() -> None:
    with pytest.raises(ValueError, match="not approved for live access"):
        ArcGISRESTConnector("escambia_arcgis_parcels")


def test_arcgis_connector_builds_query_url_from_layer_endpoint_and_default_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    review_doc = tmp_path / "review.md"
    review_doc.write_text("# review", encoding="utf-8")
    manifest = tmp_path / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "source_type": "official_arcgis_public_parcels",
                    "access_method": "official_arcgis_rest_query",
                    "live_access_allowed": True,
                    "terms_review_required": True,
                    "review_document": str(review_doc),
                    "imported_at": "2026-07-09T00:00:00Z",
                    "data_path": "https://example.invalid/arcgis/rest/services/Parcels/FeatureServer/0",
                    "processed_outputs": ["data/processed/arcgis_entities.csv", "data/processed/arcgis_relationships.csv"],
                    "notes": "test",
                }
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "api_sources.json"
    config_path.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "base_url": "https://example.invalid",
                    "endpoint": "/arcgis/rest/services/Parcels/FeatureServer/0",
                    "default_limit": 100,
                    "query_params": {"where": "1=1"},
                    "response_format": "json",
                    "timeout_seconds": 5,
                    "retry_attempts": 1,
                    "retry_backoff_seconds": 0.0,
                    "rate_limit_per_minute": 60,
                    "mock_response_path": str(tmp_path / "arcgis.json"),
                    "field_map": {"parcel_id": "PID"},
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "arcgis.json").write_text(json.dumps({"features": []}), encoding="utf-8")
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    connector = ArcGISRESTConnector("florida_county_arcgis_parcels", config_path=config_path)

    assert connector.query_params["resultRecordCount"] == 100
    assert connector._build_metadata_url() == "https://example.invalid/arcgis/rest/services/Parcels/FeatureServer/0?f=json"
    assert connector._build_request_url().startswith(
        "https://example.invalid/arcgis/rest/services/Parcels/FeatureServer/0/query?"
    )


def test_arcgis_connector_joins_multi_field_addresses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    review_doc = tmp_path / "review.md"
    review_doc.write_text("# review", encoding="utf-8")
    manifest = tmp_path / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "source_type": "official_arcgis_public_parcels",
                    "access_method": "official_arcgis_rest_query",
                    "live_access_allowed": True,
                    "terms_review_required": True,
                    "review_document": str(review_doc),
                    "imported_at": "2026-07-09T00:00:00Z",
                    "data_path": "https://example.invalid/arcgis/rest/services/Parcels/FeatureServer/0",
                    "processed_outputs": ["data/processed/arcgis_entities.csv", "data/processed/arcgis_relationships.csv"],
                    "notes": "test",
                }
            }
        ),
        encoding="utf-8",
    )
    mock_json = tmp_path / "arcgis.json"
    mock_json.write_text(
        json.dumps(
            {
                "features": [
                    {
                        "attributes": {
                            "STRAP": "15-1",
                            "OWNER": "County Example",
                            "SITE_ADDR": "123 Main St",
                            "SITE_CITY": "Tampa",
                            "SITE_ZIP": "33602",
                            "ADDR_1": "PO Box 55",
                            "ADDR_2": "Suite 4",
                            "CITY": "Tampa",
                            "STATE": "FL",
                            "ZIP": "33601",
                            "DOR_CODE": "0100",
                            "ASD_VAL": 123456,
                            "S_DATE": "2026-01-01"
                        },
                        "geometry": {}
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "api_sources.json"
    config_path.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "base_url": "https://example.invalid",
                    "endpoint": "/arcgis/rest/services/Parcels/FeatureServer/0",
                    "query_params": {"where": "1=1"},
                    "response_format": "json",
                    "timeout_seconds": 5,
                    "retry_attempts": 1,
                    "retry_backoff_seconds": 0.0,
                    "rate_limit_per_minute": 60,
                    "mock_response_path": str(mock_json),
                    "field_map": {
                        "parcel_id": "STRAP",
                        "owner_name": "OWNER",
                        "situs_address": ["SITE_ADDR", "SITE_CITY", "SITE_ZIP"],
                        "mailing_address": ["ADDR_1", "ADDR_2", "CITY", "STATE", "ZIP"],
                        "land_use": "DOR_CODE",
                        "assessed_value": "ASD_VAL",
                        "sale_date": "S_DATE",
                        "sale_price": ""
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    connector = ArcGISRESTConnector("florida_county_arcgis_parcels", config_path=config_path)
    entities, relationships = connector.run()

    assert any(row["entity_id"] == "property:15-1" for row in entities)
    assert any(row["entity_id"] == "address:123 MAIN ST, TAMPA, 33602" for row in entities)
    assert any(row["entity_id"] == "address:PO BOX 55, SUITE 4, TAMPA, FL, 33601" for row in entities)
    assert any(row["relationship_type"] == "PROPERTY_HAS_MAILING_ADDRESS" for row in relationships)
