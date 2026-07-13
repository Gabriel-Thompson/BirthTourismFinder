from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.connectors.sunbiz_daily_connector import MissingAPIKeyError, SunbizDailyConnector
from src.run_pipeline import run_pipeline


class _FakeHeaders:
    def get_content_charset(self) -> str:
        return "utf-8"


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.headers = _FakeHeaders()

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _write_config(tmp_path: Path, *, mock_response_path: Path | None = None, prefer_mock_response: bool = False, default_page_size: int = 100) -> Path:
    config_path = tmp_path / "sunbiz_daily.json"
    config = {
        "source_name": "sunbiz_daily_api",
        "base_url": "https://api.sunbizdaily.example.invalid",
        "endpoint": "/v1/business-filings",
        "default_page_size": default_page_size,
        "max_requests_per_hour": 3600,
        "retry_attempts": 3,
        "retry_backoff": 0.01,
        "timeout": 5,
        "county_filter": "Hillsborough",
        "entity_types": ["LLC", "CORPORATION"],
        "enabled": True,
        "api_version": "v1",
        "api_key_env_var": "SUNBIZ_DAILY_API_KEY",
        "auth_header": "X-API-Key",
        "auth_prefix": "",
        "accept_header": "application/json",
        "page_param": "page",
        "page_size_param": "page_size",
        "limit_param": "limit",
        "county_param": "county",
        "city_param": "city",
        "zip_param": "zip",
        "from_date_param": "from_date",
        "to_date_param": "to_date",
        "entity_type_param": "entity_type",
        "response_root": "results",
        "next_page_path": "pagination.next_page",
        "prefer_mock_response": prefer_mock_response,
        "mock_response_path": str(mock_response_path) if mock_response_path is not None else "",
        "field_map": {
            "document_number": "document_number",
            "business_name": "business_name",
            "entity_type": "entity_type",
            "status": "status",
            "filing_date": "filing_date",
            "principal_address": "principal_address",
            "mailing_address": "mailing_address",
            "registered_agent_name": "registered_agent.name",
            "registered_agent_address": "registered_agent.address",
            "officers": "officers",
            "county": "county",
            "city": "city",
            "zip": "zip",
        },
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def _sample_record(document_number: str, business_name: str, principal_address: str, mailing_address: str) -> dict[str, Any]:
    return {
        "document_number": document_number,
        "business_name": business_name,
        "entity_type": "LLC",
        "status": "ACTIVE",
        "filing_date": "2026-07-01",
        "principal_address": principal_address,
        "mailing_address": mailing_address,
        "registered_agent": {"name": "JANE AGENT", "address": principal_address},
        "officers": [
            {"name": "ROBERT OWNER", "address": principal_address},
        ],
        "county": "Hillsborough",
        "city": "Tampa",
        "zip": "33602",
    }


def test_sunbiz_daily_connector_requires_api_key_for_live_fetch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.delenv("SUNBIZ_DAILY_API_KEY", raising=False)

    connector = SunbizDailyConnector(config_path=config_path, limit=1)

    with pytest.raises(MissingAPIKeyError):
        connector.fetch()


def test_sunbiz_daily_connector_fetches_paginated_results(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, default_page_size=1)
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")

    payloads = {
        "1": {
            "results": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")],
            "pagination": {"next_page": 2},
        },
        "2": {
            "results": [_sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", "PO Box 501, Tampa, FL 33601")],
            "pagination": {"next_page": None},
        },
    }

    def requester(request, timeout=0):
        from urllib.parse import parse_qs, urlparse

        page = parse_qs(urlparse(request.full_url).query)["page"][0]
        return _FakeResponse(payloads[page])

    connector = SunbizDailyConnector(config_path=config_path, requester=requester, limit=2)
    rows = connector.normalize(connector.parse(connector.fetch()))

    assert [row["document_number"] for row in rows] == ["L26000010001", "L26000010002"]


def test_sunbiz_daily_connector_retries_and_rate_limits(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")
    sleeps: list[float] = []
    attempts = {"count": 0}

    monkeypatch.setattr("src.connectors.sunbiz_daily_connector.time.sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr("src.connectors.sunbiz_daily_connector.time.time", lambda: 1000.0)

    def requester(request, timeout=0):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError("temporary failure")
        return _FakeResponse({"results": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")], "pagination": {"next_page": None}})

    connector = SunbizDailyConnector(config_path=config_path, requester=requester, limit=1)
    connector.retry_attempts = 2
    connector.retry_backoff = 0.25

    rows = connector.normalize(connector.parse(connector.fetch()))

    assert attempts["count"] == 2
    assert rows[0]["document_number"] == "L26000010001"
    assert 0.25 in sleeps


def test_sunbiz_daily_connector_builds_entities_and_relationships(tmp_path: Path) -> None:
    mock_path = tmp_path / "sample.json"
    mock_path.write_text(
        json.dumps(
            {
                "results": [
                    _sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path = _write_config(tmp_path, mock_response_path=mock_path, prefer_mock_response=True)

    connector = SunbizDailyConnector(config_path=config_path, limit=2)
    entities, relationships, status = connector.run()

    entity_types = {row["entity_type"] for row in entities}
    relationship_types = {row["relationship_type"] for row in relationships}
    assert {"business", "officer", "registered_agent", "address"} <= entity_types
    assert {"OFFICER_OF", "REGISTERED_AGENT_FOR", "BUSINESS_LOCATED_AT", "BUSINESS_MAILING_ADDRESS", "OFFICER_AT_ADDRESS", "REGISTERED_AGENT_AT_ADDRESS"} <= relationship_types
    assert status["businesses_imported"] == 2
    assert status["registered_agents_imported"] == 1


def test_run_pipeline_includes_sunbiz_daily_and_generates_cross_source_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    property_input_dir = tmp_path / "data" / "raw" / "county_property"
    property_input_dir.mkdir(parents=True)
    (property_input_dir / "property_records.csv").write_text(
        "parcel_id,owner_name,situs_address,mailing_address,property_use,land_use,assessed_value,sale_date,sale_price\n"
        "HC-1,ROBERT OWNER,\"100 Commerce Blvd, Tampa, FL 33602\",\"PO Box 500, Tampa, FL 33601\",Residential,Single Family,350000,2025-01-01,320000\n",
        encoding="utf-8",
    )

    mock_path = tmp_path / "sunbiz_daily_mock.json"
    mock_path.write_text(
        json.dumps(
            {
                "results": [
                    _sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010003", "THREE LLC", "300 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                ]
            }
        ),
        encoding="utf-8",
    )
    config_path = _write_config(tmp_path, mock_response_path=mock_path, prefer_mock_response=True)
    monkeypatch.setenv("OPENFRAUD_SUNBIZ_DAILY_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")

    run_pipeline(
        records=10,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        include_connectors=True,
        include_sunbiz=True,
    )

    sunbiz_entities = pd.read_csv(processed_dir / "sunbiz_entities.csv")
    fraud_markers = pd.read_csv(processed_dir / "fraud_markers.csv")
    cross_source_matches = pd.read_csv(processed_dir / "cross_source_matches.csv")
    status_payload = json.loads((processed_dir / "sunbiz_daily_status.json").read_text(encoding="utf-8"))

    assert not sunbiz_entities.empty
    assert set(sunbiz_entities["source_name"]) == {"sunbiz_daily_api"}
    assert not cross_source_matches.empty
    assert "property_situs_matches_business_address" in set(cross_source_matches["match_method"]) or "parcel_owner_matches_person_with_secondary" in set(cross_source_matches["match_method"])
    assert "Many Companies Sharing One Mailing Address" in set(fraud_markers["marker_name"])
    assert "One Officer Controlling Many Businesses" in set(fraud_markers["marker_name"])
    assert "One Registered Agent Connected to Unusually Dense Networks" in set(fraud_markers["marker_name"])
    assert status_payload["api_status"] == "SUCCESS"
