from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.connectors.county_clerk.local_file_connector import (
    DEFAULT_COUNTY_CLERK_INPUT_PATH,
    SAMPLE_COUNTY_CLERK_INPUT_PATH,
    CountyClerkLocalFileConnector,
    main,
    resolve_input_path,
)


def test_county_clerk_connector_ingests_and_exports(tmp_path: Path) -> None:
    input_file = tmp_path / "data" / "raw" / "county_clerk" / "clerk_records.csv"
    input_file.parent.mkdir(parents=True)
    input_file.write_text(
        "case_number,filing_date,record_type,party_name,party_role,business_name,address,document_type,status\n"
        "2025-CA-1,2025-05-02,Civil,Jane Example,Plaintiff,Example Ventures LLC,\"123 Main St, Pensacola, FL 32501\",Complaint,Open\n",
        encoding="utf-8",
    )

    connector = CountyClerkLocalFileConnector(input_path=input_file)
    rows = connector.ingest()

    assert len(rows) == 1
    assert rows[0]["case_number"] == "2025-CA-1"
    assert rows[0]["address"] == "123 MAIN ST, PENSACOLA, FL 32501"

    entities_output = tmp_path / "data" / "processed" / "county_clerk_entities.csv"
    relationships_output = tmp_path / "data" / "processed" / "county_clerk_relationships.csv"
    connector.export_entities(rows, entities_output)
    connector.export_relationships(rows, relationships_output)

    with entities_output.open("r", encoding="utf-8", newline="") as handle:
        entity_rows = list(csv.DictReader(handle))
    assert any(row["entity_id"] == "case:2025-CA-1" and row["entity_type"] == "case" for row in entity_rows)
    assert any(row["entity_id"] == "person:Jane Example" and row["entity_type"] == "person" for row in entity_rows)
    assert any(row["entity_id"] == "business:Example Ventures LLC" and row["entity_type"] == "business" for row in entity_rows)
    assert any(row["entity_id"] == "document:2025-CA-1|Complaint" and row["entity_type"] == "document" for row in entity_rows)

    with relationships_output.open("r", encoding="utf-8", newline="") as handle:
        relationship_rows = list(csv.DictReader(handle))
    assert any(row["relationship_type"] == "CASE_HAS_PARTY" for row in relationship_rows)
    assert any(row["relationship_type"] == "PARTY_HAS_ADDRESS" for row in relationship_rows)
    assert any(row["relationship_type"] == "CASE_HAS_DOCUMENT" for row in relationship_rows)
    assert any(row["relationship_type"] == "BUSINESS_LINKED_TO_CASE" for row in relationship_rows)


def test_county_clerk_connector_supports_flexible_column_mapping(tmp_path: Path) -> None:
    input_file = tmp_path / "data" / "raw" / "county_clerk" / "clerk_records.csv"
    input_file.parent.mkdir(parents=True)
    input_file.write_text(
        "Case No,Filed Date,Case Type,Plaintiff,Role,Entity Name,Mailing Address,Instrument Type,Case Status\n"
        "2025-CA-2,2025-06-01,Probate,John Example,Petitioner,Example Holdings,\"456 Oak Ave, Pensacola, FL 32503\",Petition,Closed\n",
        encoding="utf-8",
    )

    connector = CountyClerkLocalFileConnector(input_path=input_file)
    rows = connector.ingest()

    assert rows[0]["case_number"] == "2025-CA-2"
    assert rows[0]["record_type"] == "Probate"
    assert rows[0]["business_name"] == "Example Holdings"
    assert rows[0]["document_type"] == "Petition"


def test_resolve_input_path_returns_default_when_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    default_input = tmp_path / "data" / "raw" / "county_clerk" / "clerk_records.csv"
    default_input.parent.mkdir(parents=True)
    default_input.write_text("case_number,party_name,address\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert resolve_input_path(None) == DEFAULT_COUNTY_CLERK_INPUT_PATH


def test_resolve_input_path_returns_none_when_default_missing(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = resolve_input_path(None)

    captured = capsys.readouterr()
    assert resolved is None
    assert "No default county clerk input found" in captured.out
    assert str(SAMPLE_COUNTY_CLERK_INPUT_PATH) in captured.out


def test_main_with_missing_default_prints_helpful_message(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["local_file_connector.py"])

    main()

    captured = capsys.readouterr()
    assert "No default county clerk input found" in captured.out
