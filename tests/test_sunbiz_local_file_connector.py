from __future__ import annotations

import csv
from pathlib import Path

import pytest

from src.connectors.sunbiz.local_file_connector import (
    DEFAULT_SUNBIZ_INPUT_PATH,
    SAMPLE_SUNBIZ_INPUT_PATH,
    SunbizLocalFileConnector,
    main,
    resolve_input_path,
)


def test_sunbiz_connector_ingests_and_exports(tmp_path: Path) -> None:
    input_file = tmp_path / "data" / "raw" / "sunbiz" / "sunbiz_entities.csv"
    input_file.parent.mkdir(parents=True)
    input_file.write_text(
        "record_id,business_name,address,phone,email,owner_name\n"
        "SB1,Sunbiz Sample,100 Commerce Blvd,555-1234,sample@sunbiz.gov,Sunbiz Owner\n",
        encoding="utf-8",
    )

    connector = SunbizLocalFileConnector(input_path=input_file)
    rows = connector.ingest()

    assert len(rows) == 1
    assert rows[0]["record_id"] == "SB1"

    entities_output = tmp_path / "data" / "processed" / "sunbiz_entities.csv"
    relationships_output = tmp_path / "data" / "processed" / "sunbiz_relationships.csv"
    connector.export_entities(rows, entities_output)
    connector.export_relationships(rows, relationships_output)

    with entities_output.open("r", encoding="utf-8", newline="") as handle:
        entity_rows = list(csv.DictReader(handle))
    assert entity_rows[0]["entity_id"] == "sunbiz:SB1"

    with relationships_output.open("r", encoding="utf-8", newline="") as handle:
        relationship_rows = list(csv.DictReader(handle))
    assert any(row["relationship_type"] == "OWNED_BY" for row in relationship_rows)


def test_resolve_input_path_returns_default_when_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    default_input = tmp_path / "data" / "raw" / "sunbiz" / "sunbiz_entities.csv"
    default_input.parent.mkdir(parents=True)
    default_input.write_text("record_id,business_name,address,phone,email,owner_name\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    assert resolve_input_path(None) == DEFAULT_SUNBIZ_INPUT_PATH


def test_resolve_input_path_returns_none_when_default_missing(capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    resolved = resolve_input_path(None)

    captured = capsys.readouterr()
    assert resolved is None
    assert "No default Sunbiz input found" in captured.out
    assert str(SAMPLE_SUNBIZ_INPUT_PATH) in captured.out


def test_main_with_missing_default_prints_helpful_message(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["local_file_connector.py"])

    main()

    captured = capsys.readouterr()
    assert "No default Sunbiz input found" in captured.out
