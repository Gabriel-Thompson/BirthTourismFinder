from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connectors import source_manifest
from src.connectors.open_data_api import OpenDataAPIConnector, load_api_sources, write_csv


def test_load_api_sources_returns_sample_source() -> None:
    sources = load_api_sources()

    assert "sample_api" in sources
    assert sources["sample_api"]["response_format"] == "json"


def test_open_data_api_uses_mock_response_and_exports(tmp_path: Path) -> None:
    connector = OpenDataAPIConnector("sample_api")

    entities, relationships = connector.run()

    assert any(row["entity_id"] == "api:sample_api:API-001" for row in entities)
    assert any(row["entity_id"] == "address:500 DEMO BLVD, PENSACOLA, FL 32501" for row in entities)
    assert any(row["relationship_type"] == "LOCATED_AT" for row in relationships)

    entities_output = tmp_path / "api_entities.csv"
    relationships_output = tmp_path / "api_relationships.csv"
    write_csv(entities_output, entities)
    write_csv(relationships_output, relationships)

    assert entities_output.exists()
    assert relationships_output.exists()


def test_open_data_api_parses_csv_payload(tmp_path: Path) -> None:
    mock_csv = tmp_path / "sample.csv"
    mock_csv.write_text(
        "id,name,address,website,category\n"
        "CSV-1,CSV Entity,\"600 River Rd, Pensacola, FL 32502\",https://csv.example.org,demo\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "api_sources.json"
    config_path.write_text(
        json.dumps(
            {
                "sample_api": {
                    "source_name": "sample_api",
                    "base_url": "https://demo.example.invalid",
                    "endpoint": "/csv",
                    "query_params": {},
                    "response_format": "csv",
                    "timeout_seconds": 5,
                    "retry_attempts": 1,
                    "retry_backoff_seconds": 0.0,
                    "rate_limit_per_minute": 60,
                    "mock_response_path": str(mock_csv),
                    "entity_type": "business",
                    "field_map": {
                        "record_id": "id",
                        "display_name": "name",
                        "address": "address",
                        "website": "website",
                        "category": "category"
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    connector = OpenDataAPIConnector("sample_api", config_path=config_path)
    entities, relationships = connector.run()

    assert any(row["entity_id"] == "api:sample_api:CSV-1" for row in entities)
    assert any(row["target_entity_id"] == "address:600 RIVER RD, PENSACOLA, FL 32502" for row in relationships)


def test_open_data_api_refuses_source_without_live_access(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = tmp_path / "sources.json"
    review_doc = tmp_path / "sample_api.md"
    review_doc.write_text("# review", encoding="utf-8")
    manifest.write_text(
        json.dumps(
            {
                "sample_api": {
                    "source_name": "sample_api",
                    "source_type": "official_api_demo",
                    "access_method": "official_api_or_mocked_response",
                    "live_access_allowed": False,
                    "terms_review_required": True,
                    "review_document": str(review_doc),
                    "imported_at": "2026-07-08T00:00:00Z",
                    "data_path": "data/raw/api/sample.json",
                    "processed_outputs": ["data/processed/api_entities.csv", "data/processed/api_relationships.csv"],
                    "notes": "test"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    with pytest.raises(ValueError, match="not approved for live access"):
        OpenDataAPIConnector("sample_api")
