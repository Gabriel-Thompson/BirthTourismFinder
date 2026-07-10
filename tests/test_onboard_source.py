from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connectors import onboard_source


def test_assess_source_reports_ready_for_sample_api() -> None:
    report = onboard_source.assess_source("sample_arcgis_parcels")

    assert report["readiness_status"] == onboard_source.READY_FOR_SAMPLE_TEST
    assert "arcgis_connector.py --source sample_arcgis_parcels" in report["safe_test_command"]
    assert any(check["status"] == "PASS" and check["label"] == "field_map" for check in report["checks"])


def test_assess_source_reports_live_access_disabled_for_documented_blocked_source() -> None:
    report = onboard_source.assess_source("escambia_arcgis_parcels")

    assert report["readiness_status"] == onboard_source.LIVE_ACCESS_DISABLED
    assert any(check["status"] == "PASS" and check["label"] == "live_access_allowed" for check in report["checks"])


def test_assess_source_reports_ready_for_first_live_florida_arcgis_source() -> None:
    report = onboard_source.assess_source("florida_county_arcgis_parcels")

    assert report["readiness_status"] == onboard_source.READY_FOR_SAMPLE_TEST
    assert "arcgis_connector.py --source florida_county_arcgis_parcels" in report["safe_test_command"]


def test_assess_source_reports_config_incomplete_when_manifest_entry_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = tmp_path / "sources.json"
    api_config = tmp_path / "api_sources.json"
    manifest.write_text("{}", encoding="utf-8")
    api_config.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "base_url": "https://example.invalid",
                    "endpoint": "/arcgis/rest/services/Parcels/FeatureServer/0/query",
                    "field_map": {"parcel_id": "PID"},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(onboard_source, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(onboard_source, "API_CONFIG_PATH", api_config)

    report = onboard_source.assess_source("florida_county_arcgis_parcels")

    assert report["readiness_status"] == onboard_source.CONFIG_INCOMPLETE
    assert any(check["status"] == "FAIL" and check["label"] == "manifest_entry" for check in report["checks"])


def test_assess_source_reports_needs_source_review_when_review_doc_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = tmp_path / "sources.json"
    api_config = tmp_path / "api_sources.json"
    manifest.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "source_type": "official_arcgis_candidate",
                    "access_method": "official_arcgis_rest",
                    "live_access_allowed": True,
                    "terms_review_required": True,
                    "review_document": "docs/source_reviews/missing.md",
                    "imported_at": "2026-07-09T00:00:00Z",
                    "data_path": "data/raw/api/mock.json",
                    "processed_outputs": ["data/processed/arcgis_entities.csv"],
                    "notes": "test",
                }
            }
        ),
        encoding="utf-8",
    )
    api_config.write_text(
        json.dumps(
            {
                "florida_county_arcgis_parcels": {
                    "source_name": "florida_county_arcgis_parcels",
                    "base_url": "https://example.invalid",
                    "endpoint": "/arcgis/rest/services/Parcels/FeatureServer/0/query",
                    "field_map": {"parcel_id": "PID"},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(onboard_source, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(onboard_source, "API_CONFIG_PATH", api_config)
    monkeypatch.setattr(onboard_source, "REPO_ROOT", tmp_path)

    report = onboard_source.assess_source("florida_county_arcgis_parcels")

    assert report["readiness_status"] == onboard_source.NEEDS_SOURCE_REVIEW
    assert any(check["status"] == "FAIL" and check["label"] == "review_document" for check in report["checks"])


def test_assess_source_local_source_does_not_require_field_map() -> None:
    report = onboard_source.assess_source("county_property_local_file")

    assert report["readiness_status"] == onboard_source.LIVE_ACCESS_DISABLED
    assert any(check["status"] == "PASS" and check["label"] == "field_map" for check in report["checks"])
