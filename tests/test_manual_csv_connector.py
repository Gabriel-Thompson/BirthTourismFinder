from __future__ import annotations

import csv
from pathlib import Path

from src.connectors.manual_csv import ManualCSVConnector


def test_manual_csv_connector_ingests_and_exports(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "manual"
    input_dir.mkdir(parents=True)
    sample_file = input_dir / "sample_public_records.csv"
    sample_rows = [
        {"record_id": "R1", "name": "Global Holdings", "address": "123 Main St", "phone": "555-0101", "email": "contact@global.com"},
        {"record_id": "R2", "name": "Sunrise Properties", "address": "456 Oak Ave", "phone": "", "email": "info@sunrise.com"},
    ]
    with sample_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["record_id", "name", "address", "phone", "email"])
        writer.writeheader()
        writer.writerows(sample_rows)

    connector = ManualCSVConnector(input_dir=input_dir)
    discovered = connector.discover_inputs()
    assert discovered == [sample_file]

    rows = connector.ingest(sample_file)
    assert len(rows) == 2
    assert rows[0]["record_id"] == "R1"
    assert rows[1]["name"] == "Sunrise Properties"

    entities_output = tmp_path / "data" / "processed" / "manual_entities.csv"
    relationships_output = tmp_path / "data" / "processed" / "manual_relationships.csv"
    connector.export_entities(rows, entities_output)
    connector.export_relationships(rows, relationships_output)

    assert entities_output.exists()
    assert relationships_output.exists()

    with entities_output.open("r", encoding="utf-8") as handle:
        contents = handle.read()
    assert "Global Holdings" in contents
    assert "public_record" in contents
    assert "manual:R1" in contents

    with relationships_output.open("r", encoding="utf-8") as handle:
        contents = handle.read()
    assert "LOCATED_AT" in contents
    assert "USES_EMAIL" in contents
    assert "manual:R1" in contents


def test_manual_csv_connector_ignores_blank_record_ids(tmp_path: Path) -> None:
    input_dir = tmp_path / "data" / "raw" / "manual"
    input_dir.mkdir(parents=True)
    sample_file = input_dir / "sample_public_records.csv"
    sample_file.write_text(
        "record_id,name,address,phone,email\n"
        ",Missing Id,123 Main St,555-0101,missing@example.com\n"
        "R2,Valid Record,456 Oak Ave,555-0102,valid@example.com\n",
        encoding="utf-8",
    )

    connector = ManualCSVConnector(input_dir=input_dir)
    rows = connector.ingest(sample_file)

    assert len(rows) == 1
    assert rows[0]["record_id"] == "R2"
