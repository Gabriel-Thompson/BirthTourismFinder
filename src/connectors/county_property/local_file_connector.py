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

DEFAULT_COUNTY_PROPERTY_INPUT_PATH = Path("data/raw/county_property/property_records.csv")
SAMPLE_COUNTY_PROPERTY_INPUT_PATH = Path("data/raw/county_property/sample_property_records.csv")

FIELD_ALIASES = {
    "parcel_id": ["parcel_id", "parcel", "parcel_number", "parcel_no", "parcelid", "property_id"],
    "owner_name": ["owner_name", "owner", "owner1", "owner_name_1", "owner full name"],
    "situs_address": ["situs_address", "property_address", "site_address", "situs", "physical_address"],
    "mailing_address": ["mailing_address", "owner_address", "mail_address", "mailing"],
    "property_use": ["property_use", "use_code", "use_description", "propertyuse"],
    "land_use": ["land_use", "landuse", "land_use_code", "land_use_desc"],
    "assessed_value": ["assessed_value", "assessed", "market_value", "just_value"],
    "sale_date": ["sale_date", "last_sale_date", "deed_date"],
    "sale_price": ["sale_price", "last_sale_price", "price_paid"],
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


class CountyPropertyLocalFileConnector:
    """Connector for county property records imported from local CSV files."""

    def __init__(self, input_path: Path | str, output_dir: Path | str = Path("data/processed")) -> None:
        self.source_config = validate_source("county_property_local_file")
        self.input_path = ensure_local_only_path("county_property_local_file", input_path)
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
                parcel_id = (row.get(column_map.get("parcel_id", ""), "") or "").strip()
                if not parcel_id:
                    continue
                normalized = {
                    "parcel_id": parcel_id,
                    "owner_name": (row.get(column_map.get("owner_name", ""), "") or "").strip(),
                    "situs_address": normalize_address((row.get(column_map.get("situs_address", ""), "") or "").strip()),
                    "mailing_address": normalize_address((row.get(column_map.get("mailing_address", ""), "") or "").strip()),
                    "property_use": (row.get(column_map.get("property_use", ""), "") or "").strip(),
                    "land_use": (row.get(column_map.get("land_use", ""), "") or "").strip(),
                    "assessed_value": (row.get(column_map.get("assessed_value", ""), "") or "").strip(),
                    "sale_date": (row.get(column_map.get("sale_date", ""), "") or "").strip(),
                    "sale_price": (row.get(column_map.get("sale_price", ""), "") or "").strip(),
                }
                rows.append(normalized)
        return rows

    def export_entities(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        seen_entity_ids: set[str] = set()
        source_metadata = infer_source_metadata("county_property")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                property_entity = apply_provenance(
                    {
                    "entity_id": f"property:{row['parcel_id']}",
                    "display_name": row["situs_address"] or row["parcel_id"],
                    "entity_type": "property",
                    "source": "county_property",
                    },
                    "county_property",
                    source_type_hint=source_metadata["source_type"],
                    source_record_id=row["parcel_id"],
                )
                for entity in [property_entity]:
                    if entity["entity_id"] not in seen_entity_ids:
                        seen_entity_ids.add(entity["entity_id"])
                        writer.writerow(entity)
                for address_field in ["situs_address", "mailing_address"]:
                    address = row[address_field]
                    if address:
                        entity = apply_provenance(
                            {
                            "entity_id": f"address:{address}",
                            "display_name": address,
                            "entity_type": "address",
                            "source": "county_property",
                            },
                            "county_property",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["parcel_id"],
                        )
                        if entity["entity_id"] not in seen_entity_ids:
                            seen_entity_ids.add(entity["entity_id"])
                            writer.writerow(entity)
                if row["owner_name"]:
                    owner_entity = apply_provenance(
                        {
                        "entity_id": f"owner:{row['owner_name']}",
                        "display_name": row["owner_name"],
                        "entity_type": "owner",
                        "source": "county_property",
                        },
                        "county_property",
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["parcel_id"],
                    )
                    if owner_entity["entity_id"] not in seen_entity_ids:
                        seen_entity_ids.add(owner_entity["entity_id"])
                        writer.writerow(owner_entity)

    def export_relationships(self, rows: List[Dict[str, str]], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_metadata = infer_source_metadata("county_property")
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            fieldnames = ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type", "source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                property_entity_id = f"property:{row['parcel_id']}"
                if row["situs_address"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": property_entity_id,
                            "target_entity_id": f"address:{row['situs_address']}",
                            "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS",
                            "confidence": 1.0,
                            },
                            "county_property",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["parcel_id"],
                        )
                    )
                if row["mailing_address"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": property_entity_id,
                            "target_entity_id": f"address:{row['mailing_address']}",
                            "relationship_type": "PROPERTY_HAS_MAILING_ADDRESS",
                            "confidence": 1.0,
                            },
                            "county_property",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["parcel_id"],
                        )
                    )
                if row["owner_name"]:
                    writer.writerow(
                        apply_provenance(
                            {
                            "source_entity_id": property_entity_id,
                            "target_entity_id": f"owner:{row['owner_name']}",
                            "relationship_type": "PROPERTY_OWNED_BY",
                            "confidence": 1.0,
                            },
                            "county_property",
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["parcel_id"],
                        )
                    )


def resolve_input_path(input_arg: Path | str | None) -> Path | None:
    if input_arg is not None:
        return Path(input_arg)
    if DEFAULT_COUNTY_PROPERTY_INPUT_PATH.exists():
        return DEFAULT_COUNTY_PROPERTY_INPUT_PATH
    print(
        "No default county property input found at "
        f"{DEFAULT_COUNTY_PROPERTY_INPUT_PATH}. Manually download public parcel data, "
        "save it locally, and rerun this connector. To use the local sample file, pass "
        f"--input {SAMPLE_COUNTY_PROPERTY_INPUT_PATH} explicitly."
    )
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a local county property CSV file and export entity/relationship CSVs")
    parser.add_argument(
        "--input",
        default=None,
        help=(
            "Path to the county property CSV input file. Defaults to "
            f"{DEFAULT_COUNTY_PROPERTY_INPUT_PATH}"
        ),
    )
    parser.add_argument(
        "--entities-path",
        default="data/processed/county_property_entities.csv",
        help="Path to write county property entities CSV",
    )
    parser.add_argument(
        "--relationships-path",
        default="data/processed/county_property_relationships.csv",
        help="Path to write county property relationships CSV",
    )
    args = parser.parse_args()

    input_path = resolve_input_path(args.input)
    if input_path is None:
        return
    if not input_path.exists():
        print(f"County property input file not found: {input_path}")
        return

    connector = CountyPropertyLocalFileConnector(input_path=input_path)
    rows = connector.ingest()
    connector.export_entities(rows, Path(args.entities_path))
    connector.export_relationships(rows, Path(args.relationships_path))
    print(f"Wrote {len(rows)} county property records")


if __name__ == "__main__":
    main()
