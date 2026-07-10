from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List

from .base import ConnectorBase
from .source_metadata import infer_source_metadata
from .source_manifest import ensure_local_only_path, validate_source


class ManualCSVConnector(ConnectorBase):
    """Connector for manual CSV ingestion of public record-style rows."""

    def __init__(self, input_dir: Path | str = Path("data/raw/manual")) -> None:
        self.source_config = validate_source("manual_csv")
        self.input_dir = ensure_local_only_path("manual_csv", input_dir)
        self.input_dir.mkdir(parents=True, exist_ok=True)

    def discover_inputs(self) -> List[Path]:
        if not self.input_dir.exists():
            return []
        return sorted(self.input_dir.glob("*.csv"))

    def ingest(self, source_path: Path) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        with source_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                normalized = {
                    "record_id": row.get("record_id", "").strip(),
                    "name": row.get("name", "").strip(),
                    "address": row.get("address", "").strip(),
                    "phone": row.get("phone", "").strip(),
                    "email": row.get("email", "").strip(),
                    "source": "manual_csv",
                }
                if normalized["record_id"]:
                    rows.append(normalized)
        return rows

    def export_entities(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("manual_csv")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "entity_id": f"manual:{row['record_id']}",
                        "display_name": row["name"],
                        "entity_type": "public_record",
                        "source": row["source"],
                        "source_name": source_metadata["source_name"],
                        "source_type": source_metadata["source_type"],
                    }
                )

    def export_relationships(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("manual_csv")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                entity_id = f"manual:{row['record_id']}"
                if row["address"]:
                    writer.writerow(
                        {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"address:{row['address']}",
                            "relationship_type": "LOCATED_AT",
                            "confidence": 1.0,
                            "source_name": source_metadata["source_name"],
                            "source_type": source_metadata["source_type"],
                        }
                    )
                if row["phone"]:
                    writer.writerow(
                        {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"phone:{row['phone']}",
                            "relationship_type": "USES_PHONE",
                            "confidence": 1.0,
                            "source_name": source_metadata["source_name"],
                            "source_type": source_metadata["source_type"],
                        }
                    )
                if row["email"]:
                    writer.writerow(
                        {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"email:{row['email']}",
                            "relationship_type": "USES_EMAIL",
                            "confidence": 1.0,
                            "source_name": source_metadata["source_name"],
                            "source_type": source_metadata["source_type"],
                        }
                    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest manual CSV files and export entity/relationship CSVs")
    parser.add_argument("--input-dir", default="data/raw/manual", help="Directory containing manual CSV files")
    parser.add_argument("--entities-path", default="data/processed/manual_entities.csv", help="Path to write entities CSV")
    parser.add_argument("--relationships-path", default="data/processed/manual_relationships.csv", help="Path to write relationships CSV")
    args = parser.parse_args()

    connector = ManualCSVConnector(input_dir=Path(args.input_dir))
    source_files = connector.discover_inputs()
    if not source_files:
        print(f"No manual CSV files found in {connector.input_dir}")
        return

    all_rows: List[Dict[str, str]] = []
    for source in source_files:
        print(f"Loading {source}")
        all_rows.extend(connector.ingest(source))

    connector.export_entities(all_rows, Path(args.entities_path))
    connector.export_relationships(all_rows, Path(args.relationships_path))
    print(f"Wrote {len(all_rows)} manual records")


if __name__ == "__main__":
    main()
