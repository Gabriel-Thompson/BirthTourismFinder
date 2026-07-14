from __future__ import annotations

from pathlib import Path

from src.utils.csv_io import write_csv_rows


def test_write_csv_rows_writes_header_for_empty_rows(tmp_path: Path) -> None:
    output = tmp_path / "empty.csv"

    write_csv_rows(output, [], fieldnames=["entity_id", "display_name"])

    contents = output.read_text(encoding="utf-8").strip()
    assert contents == "entity_id,display_name"


def test_write_csv_rows_preserves_union_of_fields(tmp_path: Path) -> None:
    output = tmp_path / "rows.csv"

    write_csv_rows(
        output,
        [{"entity_id": "e1", "display_name": "Alpha"}, {"entity_id": "e2", "extra": "x"}],
        fieldnames=["entity_id"],
    )

    contents = output.read_text(encoding="utf-8")
    assert "display_name" in contents
    assert "extra" in contents
