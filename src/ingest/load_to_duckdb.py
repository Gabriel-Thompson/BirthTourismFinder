from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List

import duckdb

DEFAULT_SOURCE_DIR = Path("data/raw/synthetic")
DEFAULT_OUTPUT_DB = Path("local_osint.duckdb")
DEFAULT_PROCESSED_DIR = Path("data/processed")
TABLE_FILES = {
    "business_entities": "business_entities.csv",
    "properties": "properties.csv",
    "web_leads": "web_leads.csv",
    "known_patterns": "known_patterns.csv",
}


class ValidationError(Exception):
    """Raised when an input CSV does not satisfy basic requirements."""


def validate_csv_file(csv_path: Path, required_columns: List[str]) -> Path:
    if not csv_path.exists():
        raise ValidationError(f"Missing input file: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValidationError(f"CSV file is missing a header row: {csv_path}")

        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            raise ValidationError(f"CSV file {csv_path} is missing columns: {', '.join(missing_columns)}")

        rows = list(reader)
        if len(rows) == 0:
            raise ValidationError(f"CSV file has no rows: {csv_path}")

    return csv_path


def load_synthetic_data(source_dir: Path | str | None = None, output_db: Path | str | None = None, processed_dir: Path | str | None = None) -> Dict[str, Path]:
    source_path = Path(source_dir or DEFAULT_SOURCE_DIR)
    db_path = Path(output_db or DEFAULT_OUTPUT_DB)
    processed_path = Path(processed_dir or DEFAULT_PROCESSED_DIR)

    source_path.mkdir(parents=True, exist_ok=True)
    processed_path.mkdir(parents=True, exist_ok=True)

    manifest: Dict[str, Path] = {}

    for table_name, filename in TABLE_FILES.items():
        csv_path = source_path / filename
        required_columns = []
        if table_name == "business_entities":
            required_columns = ["record_id", "business_name", "address", "phone"]
        elif table_name == "properties":
            required_columns = ["record_id", "property_address", "owner_name"]
        elif table_name == "web_leads":
            required_columns = ["record_id", "lead_name", "phone", "email"]
        elif table_name == "known_patterns":
            required_columns = ["record_id", "entity_name", "keyword"]

        validate_csv_file(csv_path, required_columns)
        manifest[table_name] = csv_path

    with duckdb.connect(db_path) as conn:
        for table_name, csv_path in manifest.items():
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)", [str(csv_path)])
            parquet_path = processed_path / f"{table_name}.parquet"
            conn.execute(f"COPY {table_name} TO ? (FORMAT PARQUET)", [str(parquet_path)])

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Load synthetic CSV data into a local DuckDB database and export Parquet copies.")
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR), help="Directory containing synthetic CSV files")
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB), help="Path to the DuckDB database file")
    parser.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR), help="Directory where Parquet files will be written")
    args = parser.parse_args()

    manifest = load_synthetic_data(source_dir=args.source_dir, output_db=args.output_db, processed_dir=args.processed_dir)
    print("Loaded synthetic CSV files into DuckDB:")
    for name, path in manifest.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
