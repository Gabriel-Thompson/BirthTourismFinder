from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.source_manifest import ensure_local_only_path, validate_source
from src.connectors.source_metadata import apply_provenance, infer_source_metadata
from src.normalize.address_normalizer import normalize_address

DEFAULT_COUNTY_CLERK_INPUT_PATH = Path("data/raw/county_clerk/clerk_records.csv")
SAMPLE_COUNTY_CLERK_INPUT_PATH = Path("data/raw/county_clerk/sample_clerk_records.csv")

FIELD_ALIASES = {
    "case_number": ["case_number", "case", "case_no", "case_num", "instrument_number", "record_number"],
    "filing_date": ["filing_date", "filed_date", "filing", "recorded_date"],
    "record_type": ["record_type", "record", "case_type", "record_category"],
    "party_name": ["party_name", "party", "name", "grantor", "grantee", "defendant", "plaintiff"],
    "party_role": ["party_role", "role", "party_type", "grantor_grantee"],
    "business_name": ["business_name", "business", "entity_name", "company_name"],
    "address": ["address", "mailing_address", "party_address", "property_address"],
    "document_type": ["document_type", "document", "doc_type", "instrument_type"],
    "status": ["status", "case_status", "record_status"],
}


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _build_column_map(fieldnames: List[str] | None) -> Dict[str, str]:
    available = {_normalize_header(name): name for name in fieldnames or []}
    mapping: Dict[str, str] = {}
    for target_field, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            matched_name = available.get(_normalize_header(alias))
            if matched_name:
                mapping[target_field] = matched_name
                break
    return mapping


class CountyClerkLocalFileConnector:
    """Connector for county clerk/court/official-record style local CSV files."""

    def __init__(self, input_path: Path | str, output_dir: Path | str = Path("data/processed")) -> None:
        self.source_config = validate_source("county_clerk_local_file")
        self.input_path = ensure_local_only_path("county_clerk_local_file", input_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def ingest(self) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        if not self.input_path.exists():
            return rows
        with self.input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            column_map = _build_column_map(reader.fieldnames)
            for row in reader:
                case_number = (row.get(column_map.get("case_number", ""), "") or "").strip()
                if not case_number:
                    continue
                normalized = {
                    "case_number": case_number,
                    "filing_date": (row.get(column_map.get("filing_date", ""), "") or "").strip(),
                    "record_type": (row.get(column_map.get("record_type", ""), "") or "").strip(),
                    "party_name": (row.get(column_map.get("party_name", ""), "") or "").strip(),
                    "party_role": (row.get(column_map.get("party_role", ""), "") or "").strip(),
                    "business_name": (row.get(column_map.get("business_name", ""), "") or "").strip(),
                    "address": normalize_address((row.get(column_map.get("address", ""), "") or "").strip()),
                    "document_type": (row.get(column_map.get("document_type", ""), "") or "").strip(),
                    "status": (row.get(column_map.get("status", ""), "") or "").strip(),
                }
                rows.append(normalized)
        return rows

    def export_entities(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        seen_entity_ids: set[str] = set()
        source_metadata = infer_source_metadata("county_clerk")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                entities = [
                    apply_provenance(
                        {
                        "entity_id": f"case:{row['case_number']}",
                        "display_name": row["case_number"],
                        "entity_type": "case",
                        "source": "county_clerk",
                        },
                        "county_clerk",
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["case_number"],
                    )
                ]
                if row["party_name"]:
                    entities.append(
                        apply_provenance(
                            {
                            "entity_id": f"person:{row['party_name']}",
                            "display_name": row["party_name"],
                            "entity_type": "person",
                            "source": "county_clerk",
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if row["business_name"]:
                    entities.append(
                        apply_provenance(
                            {
                            "entity_id": f"business:{row['business_name']}",
                            "display_name": row["business_name"],
                            "entity_type": "business",
                            "source": "county_clerk",
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if row["address"]:
                    entities.append(
                        apply_provenance(
                            {
                            "entity_id": f"address:{row['address']}",
                            "display_name": row["address"],
                            "entity_type": "address",
                            "source": "county_clerk",
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if row["document_type"]:
                    document_key = f"{row['case_number']}|{row['document_type']}"
                    entities.append(
                        apply_provenance(
                            {
                            "entity_id": f"document:{document_key}",
                            "display_name": row["document_type"],
                            "entity_type": "document",
                            "source": "county_clerk",
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )

                for entity in entities:
                    if entity["entity_id"] not in seen_entity_ids:
                        seen_entity_ids.add(entity["entity_id"])
                        writer.writerow(entity)

    def export_relationships(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("county_clerk")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                case_entity_id = f"case:{row['case_number']}"
                party_entity_id = f"person:{row['party_name']}" if row["party_name"] else ""
                if party_entity_id:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": case_entity_id,
                            "target_entity_id": party_entity_id,
                            "relationship_type": "CASE_HAS_PARTY",
                            "confidence": 1.0,
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if party_entity_id and row["address"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": party_entity_id,
                            "target_entity_id": f"address:{row['address']}",
                            "relationship_type": "PARTY_HAS_ADDRESS",
                            "confidence": 1.0,
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if row["document_type"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": case_entity_id,
                            "target_entity_id": f"document:{row['case_number']}|{row['document_type']}",
                            "relationship_type": "CASE_HAS_DOCUMENT",
                            "confidence": 1.0,
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )
                if row["business_name"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": f"business:{row['business_name']}",
                            "target_entity_id": case_entity_id,
                            "relationship_type": "BUSINESS_LINKED_TO_CASE",
                            "confidence": 1.0,
                            },
                            "county_clerk",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["case_number"],
                        )
                    )


def resolve_input_path(input_arg: Path | str | None) -> Path | None:
    if input_arg is not None:
        return Path(input_arg)
    if DEFAULT_COUNTY_CLERK_INPUT_PATH.exists():
        return DEFAULT_COUNTY_CLERK_INPUT_PATH
    print(
        "No default county clerk input found at "
        f"{DEFAULT_COUNTY_CLERK_INPUT_PATH}. Manually download public clerk or "
        "official record data, save it locally, and rerun this connector. To use "
        f"the local sample file, pass --input {SAMPLE_COUNTY_CLERK_INPUT_PATH} explicitly."
    )
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a local county clerk CSV file and export entity/relationship CSVs")
    parser.add_argument(
        "--input",
        default=None,
        help=f"Path to the county clerk CSV input file. Defaults to {DEFAULT_COUNTY_CLERK_INPUT_PATH}",
    )
    parser.add_argument(
        "--entities-path",
        default="data/processed/county_clerk_entities.csv",
        help="Path to write county clerk entities CSV",
    )
    parser.add_argument(
        "--relationships-path",
        default="data/processed/county_clerk_relationships.csv",
        help="Path to write county clerk relationships CSV",
    )
    args = parser.parse_args()

    input_path = resolve_input_path(args.input)
    if input_path is None:
        return
    if not input_path.exists():
        print(f"County clerk input file not found: {input_path}")
        return

    connector = CountyClerkLocalFileConnector(input_path=input_path)
    rows = connector.ingest()
    connector.export_entities(rows, Path(args.entities_path))
    connector.export_relationships(rows, Path(args.relationships_path))
    print(f"Wrote {len(rows)} county clerk records")


if __name__ == "__main__":
    main()
