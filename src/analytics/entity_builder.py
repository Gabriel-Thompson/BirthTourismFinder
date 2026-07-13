from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from src.connectors.source_metadata import PROVENANCE_FIELDS, apply_provenance, infer_source_metadata, merge_source_values

DB_PATH = Path("local_osint.duckdb")
ENTITIES_PATH = Path("data/processed/entities.csv")
RELATIONSHIPS_PATH = Path("data/processed/relationships.csv")


def build_entity_graph(
    db_path: Path | str = DB_PATH,
    entities_path: Path | str = ENTITIES_PATH,
    relationships_path: Path | str = RELATIONSHIPS_PATH,
    additional_entity_paths: Optional[List[Path | str]] = None,
    additional_relationship_paths: Optional[List[Path | str]] = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    start_time = time.time()
    db = Path(db_path)
    entities_output = Path(entities_path)
    relationships_output = Path(relationships_path)
    entities_output.parent.mkdir(parents=True, exist_ok=True)
    relationships_output.parent.mkdir(parents=True, exist_ok=True)
    print("Entity Builder: started")
    print(f"Entity Builder: input DuckDB {db}")
    if additional_entity_paths:
        print(f"Entity Builder: additional entity inputs {[str(Path(path)) for path in additional_entity_paths]}")
    if additional_relationship_paths:
        print(f"Entity Builder: additional relationship inputs {[str(Path(path)) for path in additional_relationship_paths]}")

    conn = duckdb.connect(str(db))
    try:
        business_rows = conn.execute(
            """
            SELECT record_id AS entity_id, business_name AS display_name, 'business' AS entity_type, address, phone, website, email, llc_name
            FROM business_entities
            """
        ).fetchall()
        property_rows = conn.execute(
            """
            SELECT record_id AS entity_id, property_address AS display_name, 'property' AS entity_type, property_address AS address, '' AS phone, '' AS website, '' AS email, owner_name AS llc_name
            FROM properties
            """
        ).fetchall()
        pattern_rows = conn.execute(
            """
            SELECT record_id AS entity_id, entity_name AS display_name, 'pattern' AS entity_type, '' AS address, '' AS phone, '' AS website, '' AS email, keyword AS llc_name
            FROM known_patterns
            """
        ).fetchall()
    finally:
        conn.close()
    print(f"Entity Builder: loaded {len(business_rows)} business rows, {len(property_rows)} property rows, {len(pattern_rows)} pattern rows from DuckDB")

    def read_csv_rows(path: Path, required_fields: List[str]) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        if not path.exists():
            return rows
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not any((row.get(field) or "").strip() for field in required_fields):
                    continue
                cleaned = {str(field): (value or "").strip() for field, value in row.items()}
                rows.append(cleaned)
        return rows

    def merge_entities(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
        merged: Dict[str, Dict[str, object]] = {}
        for row in rows:
            entity_id = str(row.get("entity_id", "")).strip()
            if not entity_id:
                continue
            if entity_id not in merged:
                merged[entity_id] = row
                continue
            existing = merged[entity_id]
            existing["source"] = merge_source_values(str(existing.get("source", "")), str(row.get("source", "")))
            existing["source_name"] = merge_source_values(str(existing.get("source_name", "")), str(row.get("source_name", "")))
            existing["source_type"] = merge_source_values(str(existing.get("source_type", "")), str(row.get("source_type", "")))
            for field in ["source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]:
                existing[field] = merge_source_values(str(existing.get(field, "")), str(row.get(field, "")))
        return list(merged.values())

    def dedupe_relationships(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
        seen: set[tuple[str, str, str]] = set()
        unique_rows: Dict[tuple[str, str, str], Dict[str, object]] = {}
        for row in rows:
            source_entity_id = str(row.get("source_entity_id", "")).strip()
            target_entity_id = str(row.get("target_entity_id", "")).strip()
            relationship_type = str(row.get("relationship_type", "")).strip()
            if not source_entity_id or not target_entity_id:
                continue
            key = (source_entity_id, target_entity_id, relationship_type)
            if key in seen:
                existing = unique_rows[key]
                existing["source_name"] = merge_source_values(str(existing.get("source_name", "")), str(row.get("source_name", "")))
                existing["source_type"] = merge_source_values(str(existing.get("source_type", "")), str(row.get("source_type", "")))
                existing["source"] = merge_source_values(str(existing.get("source", "")), str(row.get("source", "")))
                for field in ["source_record_id", "connector_name", "import_batch_id", "imported_at", "jurisdiction", "is_synthetic"]:
                    existing[field] = merge_source_values(str(existing.get(field, "")), str(row.get(field, "")))
                continue
            seen.add(key)
            unique_rows[key] = row
        return list(unique_rows.values())

    entities: List[Dict[str, object]] = []
    relationships: List[Dict[str, object]] = []
    synthetic_metadata = infer_source_metadata("synthetic")

    for row in business_rows:
        entity_id, display_name, entity_type, address, phone, website, email, llc_name = row
        entities.append(
            apply_provenance(
                {
                "entity_id": entity_id,
                "display_name": display_name,
                "entity_type": entity_type,
                "source": "business_entities",
                },
                "synthetic",
                source_type_hint=synthetic_metadata["source_type"],
                source_record_id=str(entity_id),
            )
        )
        if address:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"address:{address}",
                    "relationship_type": "LOCATED_AT",
                    "confidence": 1.0,
                    "source": "business_entities",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )
        if phone:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"phone:{phone}",
                    "relationship_type": "USES_PHONE",
                    "confidence": 1.0,
                    "source": "business_entities",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )
        if website:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"website:{website}",
                    "relationship_type": "HAS_WEBSITE",
                    "confidence": 1.0,
                    "source": "business_entities",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )
        if email:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"email:{email}",
                    "relationship_type": "USES_EMAIL",
                    "confidence": 1.0,
                    "source": "business_entities",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )
        if llc_name:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"business:{llc_name}",
                    "relationship_type": "ASSOCIATED_WITH",
                    "confidence": 0.8,
                    "source": "business_entities",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )

    for row in property_rows:
        entity_id, display_name, entity_type, address, phone, website, email, llc_name = row
        entities.append(
            apply_provenance(
                {
                "entity_id": entity_id,
                "display_name": display_name,
                "entity_type": entity_type,
                "source": "properties",
                },
                "synthetic",
                source_type_hint=synthetic_metadata["source_type"],
                source_record_id=str(entity_id),
            )
        )
        if address:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"address:{address}",
                    "relationship_type": "LOCATED_AT",
                    "confidence": 1.0,
                    "source": "properties",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )
        if llc_name:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"business:{llc_name}",
                    "relationship_type": "OWNED_BY",
                    "confidence": 0.9,
                    "source": "properties",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )

    for row in pattern_rows:
        entity_id, display_name, entity_type, address, phone, website, email, keyword = row
        entities.append(
            apply_provenance(
                {
                "entity_id": entity_id,
                "display_name": display_name,
                "entity_type": entity_type,
                "source": "known_patterns",
                },
                "synthetic",
                source_type_hint=synthetic_metadata["source_type"],
                source_record_id=str(entity_id),
            )
        )
        if keyword:
            relationships.append(
                apply_provenance(
                    {
                    "source_entity_id": entity_id,
                    "target_entity_id": f"pattern:{keyword}",
                    "relationship_type": "CONTAINS_KEYWORD",
                    "confidence": 1.0,
                    "source": "known_patterns",
                    },
                    "synthetic",
                    source_type_hint=synthetic_metadata["source_type"],
                    source_record_id=str(entity_id),
                )
            )

    if additional_entity_paths:
        for path in additional_entity_paths:
            additional_rows = read_csv_rows(Path(path), ["entity_id", "display_name", "entity_type"])
            for row in additional_rows:
                source_hint = str(row.get("source_name") or row.get("source") or "").strip()
                if not str(row.get("source_name", "")).strip() or not str(row.get("source_type", "")).strip():
                    source_metadata = infer_source_metadata(source_hint)
                    row["source_name"] = row.get("source_name") or source_metadata["source_name"]
                    row["source_type"] = row.get("source_type") or source_metadata["source_type"]
                entities.append(apply_provenance(row, source_hint or str(row.get("source_name", "")), source_record_id=str(row.get("source_record_id", "")) or str(row.get("entity_id", ""))))

    if additional_relationship_paths:
        for path in additional_relationship_paths:
            raw_rows = read_csv_rows(Path(path), ["source_entity_id", "target_entity_id", "relationship_type", "confidence"])
            for row in raw_rows:
                try:
                    row["confidence"] = float(row.get("confidence", 1.0))
                except ValueError:
                    row["confidence"] = 1.0
                source_hint = str(row.get("source_name") or row.get("source") or "").strip()
                if not str(row.get("source_name", "")).strip() or not str(row.get("source_type", "")).strip():
                    source_metadata = infer_source_metadata(source_hint)
                    row["source_name"] = row.get("source_name") or source_metadata["source_name"]
                    row["source_type"] = row.get("source_type") or source_metadata["source_type"]
                relationships.append(
                    apply_provenance(
                        row,
                        source_hint or str(row.get("source_name", "")),
                        source_record_id=str(row.get("source_record_id", "")) or str(row.get("source_entity_id", "")),
                    )
                )

    entities = merge_entities(entities)
    relationships = dedupe_relationships(relationships)

    # write entity and relationship rows to DuckDB tables
    with duckdb.connect(str(db)) as conn:
        conn.execute("DROP TABLE IF EXISTS entities")
        conn.execute("DROP TABLE IF EXISTS relationships")
        if entities:
            conn.register("entities_tmp", pd.DataFrame(entities))
            conn.execute("CREATE TABLE entities AS SELECT * FROM entities_tmp")
        else:
            conn.execute(
                "CREATE TABLE entities AS SELECT CAST(NULL AS VARCHAR) AS entity_id, CAST(NULL AS VARCHAR) AS display_name, CAST(NULL AS VARCHAR) AS entity_type, CAST(NULL AS VARCHAR) AS source, CAST(NULL AS VARCHAR) AS source_name, CAST(NULL AS VARCHAR) AS source_type"
            )
        if relationships:
            conn.register("relationships_tmp", pd.DataFrame(relationships))
            conn.execute("CREATE TABLE relationships AS SELECT * FROM relationships_tmp")
        else:
            conn.execute(
                "CREATE TABLE relationships AS SELECT CAST(NULL AS VARCHAR) AS source_entity_id, CAST(NULL AS VARCHAR) AS target_entity_id, CAST(NULL AS VARCHAR) AS relationship_type, CAST(NULL AS DOUBLE) AS confidence, CAST(NULL AS VARCHAR) AS source, CAST(NULL AS VARCHAR) AS source_name, CAST(NULL AS VARCHAR) AS source_type"
            )

    write_csv(entities_output, entities)
    write_csv(relationships_output, relationships)
    duration = time.time() - start_time
    print(f"Entity Builder: wrote {len(entities)} entities to {entities_output}")
    print(f"Entity Builder: wrote {len(relationships)} relationships to {relationships_output}")
    print(f"Entity Builder: completed in {duration:.2f}s")
    return entities, relationships


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build normalized entity and relationship tables from the local DuckDB database")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to the DuckDB database")
    parser.add_argument("--entities-path", default=str(ENTITIES_PATH), help="Path to the entities CSV export")
    parser.add_argument("--relationships-path", default=str(RELATIONSHIPS_PATH), help="Path to the relationships CSV export")
    args = parser.parse_args()

    build_entity_graph(db_path=args.db_path, entities_path=args.entities_path, relationships_path=args.relationships_path)
    print("Entity Builder: PASS")


if __name__ == "__main__":
    main()
