from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.source_manifest import ensure_local_only_path, validate_source
from src.connectors.source_metadata import apply_provenance, infer_source_metadata

DEFAULT_SUNBIZ_INPUT_PATH = Path("data/raw/sunbiz/sunbiz_entities.csv")
SAMPLE_SUNBIZ_INPUT_PATH = Path("data/raw/sunbiz/sample_sunbiz.csv")


class SunbizLocalFileConnector:
    """Local Sunbiz connector that converts a CSV file into entity/relationship exports."""

    def __init__(self, input_path: Path | str, output_dir: Path | str = Path("data/processed")) -> None:
        self.source_config = validate_source("sunbiz_local_file")
        self.input_path = ensure_local_only_path("sunbiz_local_file", input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def ingest(self) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        if not self.input_path.exists():
            return rows
        with self.input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                record_id = (row.get("record_id") or "").strip()
                if not record_id:
                    continue
                rows.append(
                    {
                        "record_id": record_id,
                        "business_name": (row.get("business_name") or "").strip(),
                        "address": (row.get("address") or "").strip(),
                        "phone": (row.get("phone") or "").strip(),
                        "email": (row.get("email") or "").strip(),
                        "owner_name": (row.get("owner_name") or "").strip(),
                    }
                )
        return rows

    def export_entities(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("sunbiz")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    apply_provenance(
                        {
                        "entity_id": f"sunbiz:{row['record_id']}",
                        "display_name": row["business_name"],
                        "entity_type": "business",
                        "source": "sunbiz",
                        },
                        "sunbiz",
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["record_id"],
                    )
                )

    def export_relationships(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("sunbiz")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                entity_id = f"sunbiz:{row['record_id']}"
                if row["address"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"address:{row['address']}",
                            "relationship_type": "LOCATED_AT",
                            "confidence": 1.0,
                            },
                            "sunbiz",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["record_id"],
                        )
                    )
                if row["phone"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"phone:{row['phone']}",
                            "relationship_type": "USES_PHONE",
                            "confidence": 1.0,
                            },
                            "sunbiz",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["record_id"],
                        )
                    )
                if row["email"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"email:{row['email']}",
                            "relationship_type": "USES_EMAIL",
                            "confidence": 1.0,
                            },
                            "sunbiz",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["record_id"],
                        )
                    )
                if row["owner_name"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": entity_id,
                            "target_entity_id": f"business:{row['owner_name']}",
                            "relationship_type": "OWNED_BY",
                            "confidence": 0.9,
                            },
                            "sunbiz",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["record_id"],
                        )
                    )


def resolve_input_path(input_arg: Path | str | None) -> Path | None:
    if input_arg is not None:
        return Path(input_arg)

    if DEFAULT_SUNBIZ_INPUT_PATH.exists():
        return DEFAULT_SUNBIZ_INPUT_PATH

    print(
        "No default Sunbiz input found at "
        f"{DEFAULT_SUNBIZ_INPUT_PATH}. Manually download public Sunbiz data, "
        "save it locally, and rerun this connector. To use a sample file, pass "
        f"--input {SAMPLE_SUNBIZ_INPUT_PATH} explicitly."
    )
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a local Sunbiz CSV file and export entity/relationship CSVs")
    parser.add_argument(
        "--input",
        default=None,
        help=(
            "Path to the Sunbiz CSV input file. Defaults to "
            f"{DEFAULT_SUNBIZ_INPUT_PATH}"
        ),
    )
    parser.add_argument("--entities-path", default="data/processed/sunbiz_entities.csv", help="Path to write Sunbiz entities CSV")
    parser.add_argument("--relationships-path", default="data/processed/sunbiz_relationships.csv", help="Path to write Sunbiz relationships CSV")
    args = parser.parse_args()

    input_path = resolve_input_path(args.input)
    if input_path is None:
        return
    if not input_path.exists():
        print(f"Sunbiz input file not found: {input_path}")
        return

    connector = SunbizLocalFileConnector(input_path=input_path)
    rows = connector.ingest()
    connector.export_entities(rows, Path(args.entities_path))
    connector.export_relationships(rows, Path(args.relationships_path))
    print(f"Wrote {len(rows)} sunbiz records")


if __name__ == "__main__":
    main()
