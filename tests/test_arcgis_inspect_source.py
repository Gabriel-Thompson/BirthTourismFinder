from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.connectors.arcgis.inspect_source import build_inspection_report, save_inspection_report


def test_build_inspection_report_uses_mock_metadata_and_sample_rows() -> None:
    report = build_inspection_report(source_name="sample_arcgis_parcels", limit=1)

    assert report["source_name"] == "sample_arcgis_parcels"
    assert len(report["fields"]) >= 3
    assert report["fields"][0]["name"] == "PARCEL_ID"
    assert len(report["sample_rows"]) == 1
    assert report["sample_rows"][0]["attributes"]["PARCEL_ID"] == "ARC-001"


def test_save_inspection_report_writes_json(tmp_path: Path) -> None:
    report = {
        "source_name": "sample_arcgis_parcels",
        "fields": [{"name": "PARCEL_ID", "alias": "Parcel ID", "type": "esriFieldTypeString"}],
        "sample_rows": [{"attributes": {"PARCEL_ID": "ARC-001"}, "geometry": {"x": 1, "y": 2}}],
    }

    output_path = save_inspection_report(report, output_dir=tmp_path)

    assert output_path.exists()
    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved["source_name"] == "sample_arcgis_parcels"


def test_build_inspection_report_refuses_disallowed_live_source() -> None:
    with pytest.raises(ValueError, match="not approved for live access"):
        build_inspection_report(source_name="escambia_arcgis_parcels", limit=5)
