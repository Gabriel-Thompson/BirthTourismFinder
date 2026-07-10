from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.connectors.county_property.local_file_connector import (
    DEFAULT_COUNTY_PROPERTY_INPUT_PATH,
    SAMPLE_COUNTY_PROPERTY_INPUT_PATH,
    CountyPropertyLocalFileConnector,
    main,
    resolve_input_path,
)


def test_county_property_connector_ingests_and_exports(tmp_path: Path) -> None:
    input_file = tmp_path / "data" / "raw" / "county_property" / "property_records.csv"
    input_file.parent.mkdir(parents=True)
    input_file.write_text(
        "parcel_id,owner_name,situs_address,mailing_address,property_use,land_use,assessed_value,sale_date,sale_price\n"
        "EC-1,Owner LLC,\"123 Main St, Pensacola, FL 32501\",\"PO Box 50, Pensacola, FL 32502\",Residential,Single Family,300000,2025-01-01,280000\n",
        encoding="utf-8",
    )

    connector = CountyPropertyLocalFileConnector(input_path=input_file)
    rows = connector.ingest()

    assert len(rows) == 1
    assert rows[0]["parcel_id"] == "EC-1"
    assert rows[0]["situs_address"] == "123 MAIN ST, PENSACOLA, FL 32501"

    entities_output = tmp_path / "data" / "processed" / "county_property_entities.csv"
    relationships_output = tmp_path / "data" / "processed" / "county_property_relationships.csv"
    connector.export_entities(rows, entities_output)
    connector.export_relationships(rows, relationships_output)

    with entities_output.open("r", encoding="utf-8", newline="") as handle:
        entity_rows = list(csv.DictReader(handle))
    assert any(row["entity_id"] == "property:EC-1" and row["entity_type"] == "property" for row in entity_rows)
    assert any(row["entity_id"] == "owner:Owner LLC" and row["entity_type"] == "owner" for row in entity_rows)
    assert any(row["entity_id"] == "address:123 MAIN ST, PENSACOLA, FL 32501" for row in entity_rows)

    with relationships_output.open("r", encoding="utf-8", newline="") as handle:
        relationship_rows = list(csv.DictReader(handle))
    assert any(row["relationship_type"] == "PROPERTY_HAS_SITUS_ADDRESS" for row in relationship_rows)
    assert any(row["relationship_type"] == "PROPERTY_HAS_MAILING_ADDRESS" for row in relationship_rows)
    assert any(row["relationship_type"] == "PROPERTY_OWNED_BY" for row in relationship_rows)


def test_county_property_connector_supports_flexible_column_mapping(tmp_path: Path) -> None:
    input_file = tmp_path / "data" / "raw" / "county_property" / "property_records.csv"
    input_file.parent.mkdir(parents=True)
    input_file.write_text(
        "Parcel Number,Owner,Site Address,Mail Address,Use Description,Land Use Desc,Market Value,Last Sale Date,Price Paid\n"
        "EC-2,Example Owner,\"456 Oak Ave, Pensacola, FL 32503\",\"PO Box 90, Pensacola, FL 32504\",Commercial,Retail,450000,2024-05-01,420000\n",
        encoding="utf-8",
    )

    connector = CountyPropertyLocalFileConnector(input_path=input_file)
    rows = connector.ingest()

    assert rows[0]["parcel_id"] == "EC-2"
    assert rows[0]["owner_name"] == "Example Owner"
    assert rows[0]["property_use"] == "Commercial"
    assert rows[0]["land_use"] == "Retail"


def test_resolve_input_path_returns_default_when_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    default_input = tmp_path / "data" / "raw" / "county_property" / "property_records.csv"
    default_input.parent.mkdir(parents=True)
    default_input.write_text("parcel_id,owner_name,situs_address\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert resolve_input_path(None) == DEFAULT_COUNTY_PROPERTY_INPUT_PATH


def test_resolve_input_path_returns_none_when_default_missing(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = resolve_input_path(None)

    captured = capsys.readouterr()
    assert resolved is None
    assert "No default county property input found" in captured.out
    assert str(SAMPLE_COUNTY_PROPERTY_INPUT_PATH) in captured.out


def test_main_with_missing_default_prints_helpful_message(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["local_file_connector.py"])

    main()

    captured = capsys.readouterr()
    assert "No default county property input found" in captured.out
