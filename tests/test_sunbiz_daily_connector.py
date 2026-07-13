from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.connectors.sunbiz_daily_connector import MissingAPIKeyError, SunbizDailyConnector
from src.run_pipeline import run_pipeline


class _FakeHeaders(dict):
    def get_content_charset(self) -> str:
        return "utf-8"


class _FakeResponse:
    def __init__(self, payload: Any, *, status: int = 200, headers: dict[str, Any] | None = None) -> None:
        self.payload = payload
        self.status = status
        self.headers = _FakeHeaders(headers or {})

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _write_config(
    tmp_path: Path,
    *,
    mock_response_path: Path | None = None,
    prefer_mock_response: bool = False,
    enabled: bool = True,
    default_page_size: int = 100,
) -> Path:
    config_path = tmp_path / "sunbiz_daily.json"
    config = {
        "source_name": "sunbiz_daily",
        "source_type": "official_api",
        "enabled": enabled,
        "base_url": "https://sunbizdaily.example.invalid",
        "list_endpoint": "/api/v2/filings/",
        "detail_endpoint_template": "/api/v2/filings/{corporation_number}/",
        "api_key_env": "SUNBIZ_DAILY_API_KEY",
        "default_county": "Hillsborough",
        "default_state": "FL",
        "default_status": "active",
        "default_page_size": default_page_size,
        "max_pages": 10,
        "max_records": 1000,
        "timeout_seconds": 5,
        "retry_attempts": 2,
        "retry_backoff_seconds": 0.01,
        "poll_interval_seconds": 0.01,
        "max_poll_attempts": 5,
        "rate_limit_per_hour": 3600,
        "prefer_mock_response": prefer_mock_response,
        "mock_response_path": str(mock_response_path) if mock_response_path is not None else "",
        "raw_snapshot_dir": str(tmp_path / "raw_snapshots"),
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def _sample_record(
    corporation_number: str,
    corporation_name: str,
    principal_address: str,
    mailing_address: str | None,
    *,
    officers: list[dict[str, Any]] | None = None,
    privacy_redacted: bool = False,
) -> dict[str, Any]:
    return {
        "corporation_number": corporation_number,
        "corporation_name": corporation_name,
        "filing_type": "LLC",
        "filing_type_display": "Florida Limited Liability Company",
        "status": "ACTIVE",
        "file_date": "2026-07-01",
        "fei_number": "12-3456789",
        "principal_address": {
            "address_1": principal_address.split(",")[0],
            "city": "Tampa",
            "state": "FL",
            "zip": "33602",
            "country": "US",
        },
        "mailing_address": None
        if mailing_address is None
        else {
            "address_1": mailing_address.split(",")[0],
            "city": "Tampa",
            "state": "FL",
            "zip": "33601",
            "country": "US",
        },
        "registered_agent": {
            "name": "JANE AGENT",
            "address": {
                "address_1": principal_address.split(",")[0],
                "city": "Tampa",
                "state": "FL",
                "zip": "33602",
                "country": "US",
            },
            "agent_type": "INDIVIDUAL",
        },
        "officers": officers
        if officers is not None
        else [
            {
                "name": "ROBERT OWNER",
                "title": "MGR",
                "officer_type": "MANAGER",
                "position": "MANAGER",
                "address": {
                    "address_1": principal_address.split(",")[0],
                    "city": "Tampa",
                    "state": "FL",
                    "zip": "33602",
                    "country": "US",
                },
            }
        ],
        "county": "Hillsborough",
        "city": "Tampa",
        "state": "FL",
        "zip": "33602",
        "privacy_redacted": privacy_redacted,
    }


def test_sunbiz_daily_connector_requires_api_key_for_live_fetch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.delenv("SUNBIZ_DAILY_API_KEY", raising=False)

    connector = SunbizDailyConnector(config_path=config_path, max_records=1)

    with pytest.raises(MissingAPIKeyError):
        connector.fetch()


def test_sunbiz_daily_connector_adds_key_header_and_paginates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, default_page_size=1)
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")
    seen_headers: list[dict[str, Any]] = []

    payloads = {
        "1": _FakeResponse(
            {
                "filings": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")],
                "pagination": {"page": 1, "per_page": 1, "total_pages": 2},
            },
            headers={"X-RateLimit-Limit": "1000", "X-RateLimit-Remaining": "999"},
        ),
        "2": _FakeResponse(
            {
                "filings": [_sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", "PO Box 501, Tampa, FL 33601")],
                "pagination": {"page": 2, "per_page": 1, "total_pages": 2},
            }
        ),
    }

    def requester(request, timeout=0):
        from urllib.parse import parse_qs, urlparse

        seen_headers.append(dict(request.headers))
        page = parse_qs(urlparse(request.full_url).query)["page"][0]
        return payloads[page]

    connector = SunbizDailyConnector(config_path=config_path, requester=requester, max_records=2)
    rows = connector.normalize(connector.parse(connector.fetch()))

    assert [row["corporation_number"] for row in rows] == ["L26000010001", "L26000010002"]
    assert any(headers.get("X-api-key") == "dummy-key" or headers.get("X-API-Key") == "dummy-key" for headers in seen_headers)


def test_sunbiz_daily_connector_handles_async_job_polling(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, default_page_size=1)
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")
    calls: list[str] = []

    def requester(request, timeout=0):
        calls.append(request.full_url)
        if request.full_url.endswith("/api/v2/filings/?sort=file_date&order=desc&county=Hillsborough&state=FL&status=active&page=1&per_page=1"):
            return _FakeResponse({"job_id": "job-1", "status": "queued", "poll_url": "/api/v2/jobs/job-1/"}, status=202)
        if request.full_url.endswith("/api/v2/jobs/job-1/") and len([url for url in calls if url.endswith("/api/v2/jobs/job-1/")]) == 1:
            return _FakeResponse({"job_id": "job-1", "status": "running"}, status=200)
        return _FakeResponse(
            {
                "status": "done",
                "filings": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")],
                "pagination": {"page": 1, "per_page": 1, "total_pages": 1},
                "truncated": True
            },
            status=200,
        )

    connector = SunbizDailyConnector(config_path=config_path, requester=requester, page_size=1, max_records=1)
    entities, relationships, summary = connector.run()

    assert entities
    assert relationships
    assert summary["asynchronous_jobs"] >= 1
    assert summary["truncated_results"] is True


def test_sunbiz_daily_connector_mock_mode_works_without_key_and_preserves_redaction(tmp_path: Path) -> None:
    mock_path = tmp_path / "sample.json"
    mock_path.write_text(
        json.dumps(
            {
                "filings": [
                    _sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", None, officers=[], privacy_redacted=True),
                ],
                "pagination": {"page": 1, "per_page": 100, "total_pages": 1},
            }
        ),
        encoding="utf-8",
    )
    config_path = _write_config(tmp_path, mock_response_path=mock_path, prefer_mock_response=True)

    connector = SunbizDailyConnector(config_path=config_path, max_records=10, use_mock=True)
    entities, relationships, summary = connector.run()

    assert summary["api_status"] == "SUCCESS"
    assert summary["privacy_redacted_records"] == 1
    assert summary["redacted_or_incomplete_records"] >= 1
    assert {"business", "officer", "registered_agent", "address"} <= {row["entity_type"] for row in entities}
    assert {"OFFICER_OF", "REGISTERED_AGENT_FOR", "BUSINESS_PRINCIPAL_ADDRESS", "BUSINESS_MAILING_ADDRESS"} <= {
        row["relationship_type"] for row in relationships
    }


def test_sunbiz_daily_connector_deduplicates_duplicate_filings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, default_page_size=1)
    monkeypatch.setenv("SUNBIZ_DAILY_API_KEY", "dummy-key")

    payloads = {
        "1": _FakeResponse(
            {
                "filings": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")],
                "pagination": {"page": 1, "per_page": 1, "total_pages": 2},
            }
        ),
        "2": _FakeResponse(
            {
                "filings": [_sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601")],
                "pagination": {"page": 2, "per_page": 1, "total_pages": 2},
            }
        ),
    }

    def requester(request, timeout=0):
        from urllib.parse import parse_qs, urlparse

        page = parse_qs(urlparse(request.full_url).query)["page"][0]
        return payloads[page]

    connector = SunbizDailyConnector(config_path=config_path, requester=requester, max_records=10)
    rows = connector.parse(connector.fetch())

    assert len(rows) == 1


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
                "filings": [
                    _sample_record("L26000010001", "ONE LLC", "100 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010002", "TWO LLC", "200 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                    _sample_record("L26000010003", "THREE LLC", "300 Commerce Blvd, Tampa, FL 33602", "PO Box 500, Tampa, FL 33601"),
                ],
                "pagination": {"page": 1, "per_page": 100, "total_pages": 1},
            }
        ),
        encoding="utf-8",
    )
    config_path = _write_config(tmp_path, mock_response_path=mock_path, prefer_mock_response=True, enabled=True)
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
    sunbiz_daily_summary = json.loads((processed_dir / "sunbiz_daily_import_summary.json").read_text(encoding="utf-8"))
    cross_source_matches = pd.read_csv(processed_dir / "cross_source_matches.csv")

    assert not sunbiz_entities.empty
    assert set(sunbiz_entities["source_name"]) == {"sunbiz_daily"}
    assert not cross_source_matches.empty
    assert (processed_dir / "sunbiz_parcel_matches.csv").exists()
    assert sunbiz_daily_summary["api_status"] == "SUCCESS"
