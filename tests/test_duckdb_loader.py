from pathlib import Path

import duckdb

from src.ingest.load_to_duckdb import load_synthetic_data, validate_csv_file


def test_validate_csv_file_passes_for_expected_columns(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("record_id,name\n1,Alice\n", encoding="utf-8")

    validated = validate_csv_file(csv_path, required_columns=["record_id", "name"])

    assert validated == csv_path


def test_load_synthetic_data_creates_tables_and_parquet(tmp_path: Path) -> None:
    source_dir = tmp_path / "synthetic"
    output_db = tmp_path / "local_osint.duckdb"
    processed_dir = tmp_path / "processed"
    source_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    (source_dir / "business_entities.csv").write_text(
        "record_id,business_name,address,phone\n1,Alpha,123 Main St,555-0100\n",
        encoding="utf-8",
    )
    (source_dir / "properties.csv").write_text(
        "record_id,property_address,owner_name\n1,123 Main St,Alpha LLC\n",
        encoding="utf-8",
    )
    (source_dir / "web_leads.csv").write_text(
        "record_id,lead_name,phone,email\n1,Alice,555-0100,alice@example.com\n",
        encoding="utf-8",
    )
    (source_dir / "known_patterns.csv").write_text(
        "record_id,entity_name,keyword\n1,Alpha,maternity\n",
        encoding="utf-8",
    )

    manifest = load_synthetic_data(source_dir=source_dir, output_db=output_db, processed_dir=processed_dir)

    assert set(manifest) == {"business_entities", "properties", "web_leads", "known_patterns"}
    assert output_db.exists()
    assert (processed_dir / "business_entities.parquet").exists()

    with duckdb.connect(output_db) as conn:
        for table_name in manifest:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count == 1
