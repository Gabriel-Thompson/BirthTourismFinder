from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connectors import source_manifest
from src.connectors.manual_csv import ManualCSVConnector
from src.connectors.source_manifest import (
    ensure_local_only_path,
    is_live_access_allowed,
    list_sources,
    load_sources,
    validate_source,
)
from src.connectors.sunbiz.local_file_connector import SunbizLocalFileConnector


def test_load_sources_contains_expected_entries() -> None:
    sources = load_sources()

    assert set(sources) >= {
        "synthetic",
        "manual_csv",
        "sunbiz_local_file",
        "sunbiz_daily_api",
        "county_property_local_file",
        "county_clerk_local_file",
        "sample_api",
        "sample_arcgis_parcels",
        "escambia_arcgis_parcels",
        "florida_county_arcgis_parcels",
    }
    assert sources["manual_csv"]["source_name"] == "manual_csv"
    assert isinstance(sources["sunbiz_local_file"]["processed_outputs"], list)
    assert "review_document" in sources["manual_csv"]


def test_list_sources_returns_sorted_names() -> None:
    assert list_sources() == sorted(list_sources())


def test_validate_source_raises_for_unknown_source() -> None:
    with pytest.raises(ValueError, match="Unknown source"):
        validate_source("missing_source")


def test_is_live_access_allowed_is_false_for_local_sources() -> None:
    assert is_live_access_allowed("synthetic") is False
    assert is_live_access_allowed("manual_csv") is False
    assert is_live_access_allowed("sunbiz_local_file") is False
    assert is_live_access_allowed("sunbiz_daily_api") is True
    assert is_live_access_allowed("county_property_local_file") is False
    assert is_live_access_allowed("county_clerk_local_file") is False
    assert is_live_access_allowed("sample_api") is True
    assert is_live_access_allowed("sample_arcgis_parcels") is True
    assert is_live_access_allowed("escambia_arcgis_parcels") is False
    assert is_live_access_allowed("florida_county_arcgis_parcels") is True


def test_ensure_local_only_path_rejects_remote_urls() -> None:
    with pytest.raises(ValueError, match="does not permit live or remote access"):
        ensure_local_only_path("manual_csv", "https://example.com/data.csv")


def test_manual_connector_rejects_remote_input_dir() -> None:
    with pytest.raises(ValueError, match="does not permit live or remote access"):
        ManualCSVConnector(input_dir="https://example.com/manual")


def test_sunbiz_connector_rejects_remote_input_path() -> None:
    with pytest.raises(ValueError, match="does not permit live or remote access"):
        SunbizLocalFileConnector(input_path="https://example.com/sunbiz.csv")


def test_load_sources_validates_required_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bad_manifest = tmp_path / "sources.json"
    bad_manifest.write_text(
        json.dumps(
            {
                "synthetic": {
                    "source_name": "synthetic",
                    "source_type": "generated_dataset"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", bad_manifest)

    with pytest.raises(ValueError, match="missing required fields"):
        source_manifest.load_sources()


def test_validate_source_requires_review_document_to_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = tmp_path / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "manual_csv": {
                    "source_name": "manual_csv",
                    "source_type": "manual_import",
                    "access_method": "local_file_drop",
                    "live_access_allowed": False,
                    "terms_review_required": False,
                    "review_document": "docs/source_reviews/missing.md",
                    "data_path": "data/raw/manual",
                    "processed_outputs": ["data/processed/manual_entities.csv"],
                    "notes": "test"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    with pytest.raises(ValueError, match="missing review document"):
        source_manifest.validate_source("manual_csv")


def test_validate_source_rejects_live_access_without_review_requirement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    review_dir = tmp_path / "docs" / "source_reviews"
    review_dir.mkdir(parents=True)
    (review_dir / "live.md").write_text("# Live Source", encoding="utf-8")
    manifest = tmp_path / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "live_source": {
                    "source_name": "live_source",
                    "source_type": "api",
                    "access_method": "api",
                    "live_access_allowed": True,
                    "terms_review_required": False,
                    "review_document": "docs/source_reviews/live.md",
                    "data_path": "data/raw/live",
                    "processed_outputs": ["data/processed/live.csv"],
                    "notes": "test"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    with pytest.raises(ValueError, match="cannot allow live access without terms review requirements enabled"):
        source_manifest.validate_source("live_source")


def test_validate_source_rejects_non_boolean_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    review_dir = tmp_path / "docs" / "source_reviews"
    review_dir.mkdir(parents=True)
    (review_dir / "manual.md").write_text("# Manual", encoding="utf-8")
    manifest = tmp_path / "sources.json"
    manifest.write_text(
        json.dumps(
            {
                "manual_csv": {
                    "source_name": "manual_csv",
                    "source_type": "manual_import",
                    "access_method": "local_file_drop",
                    "live_access_allowed": "no",
                    "terms_review_required": False,
                    "review_document": "docs/source_reviews/manual.md",
                    "data_path": "data/raw/manual",
                    "processed_outputs": ["data/processed/manual_entities.csv"],
                    "notes": "test"
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(source_manifest, "MANIFEST_PATH", manifest)

    with pytest.raises(ValueError, match="live_access_allowed must be a boolean"):
        source_manifest.validate_source("manual_csv")


def test_validate_source_requires_live_access_when_requested() -> None:
    with pytest.raises(ValueError, match="not approved for live access"):
        source_manifest.validate_source("manual_csv", require_live_access=True)
