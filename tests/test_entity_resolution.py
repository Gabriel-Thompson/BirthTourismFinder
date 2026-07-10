from __future__ import annotations
from pathlib import Path

import pandas as pd

from src.analytics.entity_resolution.normalizers import (
    normalize_address_value,
    normalize_business_name,
    normalize_email,
    normalize_phone,
)
from src.analytics.entity_resolution.resolver import resolve_entities


def _write_inputs(tmp_path: Path, entities: list[dict], relationships: list[dict]) -> tuple[Path, Path, Path]:
    entities_path = tmp_path / "entities.csv"
    relationships_path = tmp_path / "relationships.csv"
    db_path = tmp_path / "local_osint.duckdb"
    pd.DataFrame(entities).to_csv(entities_path, index=False)
    pd.DataFrame(relationships).to_csv(relationships_path, index=False)
    return entities_path, relationships_path, db_path


def test_normalizers_handle_address_phone_email_and_business_suffixes() -> None:
    address_a = normalize_address_value("123 Main Street Apt 2, Pensacola, FL 32502")
    address_b = normalize_address_value("123 MAIN ST #2 Pensacola FL 32502")
    address_c = normalize_address_value("123 Main St Apt 3, Pensacola, FL 32502")

    assert address_a["normalized_value"] == address_b["normalized_value"]
    assert address_a["normalized_value"] != address_c["normalized_value"]
    assert address_a["building_key"] == address_c["building_key"]

    assert normalize_phone("+1 (850) 555-1234")["normalized_value"] == "8505551234"
    assert normalize_email(" Example@SunBiz.gov ")["normalized_value"] == "example@sunbiz.gov"
    assert normalize_business_name("Acme, L.L.C.")["normalized_value"] == "ACME"


def test_resolver_creates_deterministic_canonical_ids_and_aliases(tmp_path: Path) -> None:
    entities = [
        {"entity_id": "phone:a", "display_name": "(850) 555-1212", "entity_type": "phone", "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
        {"entity_id": "phone:b", "display_name": "8505551212", "entity_type": "phone", "source": "manual", "source_name": "manual_csv", "source_type": "manual"},
        {"entity_id": "email:a", "display_name": "Hello@Example.com", "entity_type": "email", "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
        {"entity_id": "email:b", "display_name": "hello@example.com", "entity_type": "email", "source": "api", "source_name": "sample_api", "source_type": "api"},
    ]
    relationships: list[dict] = []
    entities_path, relationships_path, db_path = _write_inputs(tmp_path, entities, relationships)

    summary_a = resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=tmp_path / "canonical_entities.csv",
        aliases_path=tmp_path / "entity_aliases.csv",
        matches_path=tmp_path / "entity_resolution_matches.csv",
        canonical_relationships_path=tmp_path / "canonical_relationships.csv",
        db_path=db_path,
    )
    summary_b = resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=tmp_path / "canonical_entities_second.csv",
        aliases_path=tmp_path / "entity_aliases_second.csv",
        matches_path=tmp_path / "entity_resolution_matches_second.csv",
        canonical_relationships_path=tmp_path / "canonical_relationships_second.csv",
        db_path=db_path,
    )

    canonical_a = pd.read_csv(tmp_path / "canonical_entities.csv")
    canonical_b = pd.read_csv(tmp_path / "canonical_entities_second.csv")
    aliases = pd.read_csv(tmp_path / "entity_aliases.csv")

    assert summary_a["canonical_entity_count"] == 2
    assert summary_b["canonical_entity_count"] == 2
    assert canonical_a["canonical_entity_id"].tolist() == canonical_b["canonical_entity_id"].tolist()
    assert len(aliases) == 4
    assert set(aliases["source_name"]) == {"sunbiz_local_file", "manual_csv", "sample_api"}


def test_resolver_requires_secondary_evidence_for_person_and_blocks_fuzzy_name_only(tmp_path: Path) -> None:
    entities = [
        {"entity_id": "person:1", "display_name": "Jane Q. Doe", "entity_type": "person", "source": "clerk", "source_name": "county_clerk_local_file", "source_type": "connector"},
        {"entity_id": "person:2", "display_name": "Jane Doe", "entity_type": "person", "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
        {"entity_id": "person:3", "display_name": "John Smyth", "entity_type": "person", "source": "manual", "source_name": "manual_csv", "source_type": "manual"},
        {"entity_id": "person:4", "display_name": "Jon Smith", "entity_type": "person", "source": "manual", "source_name": "manual_csv", "source_type": "manual"},
        {"entity_id": "phone:1", "display_name": "850-555-9999", "entity_type": "phone", "source": "clerk", "source_name": "county_clerk_local_file", "source_type": "connector"},
        {"entity_id": "phone:2", "display_name": "(850) 555-9999", "entity_type": "phone", "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
    ]
    relationships = [
        {"source_entity_id": "person:1", "target_entity_id": "phone:1", "relationship_type": "USES_PHONE", "confidence": 1.0, "source": "clerk", "source_name": "county_clerk_local_file", "source_type": "connector"},
        {"source_entity_id": "person:2", "target_entity_id": "phone:2", "relationship_type": "USES_PHONE", "confidence": 1.0, "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
    ]
    entities_path, relationships_path, db_path = _write_inputs(tmp_path, entities, relationships)

    resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=tmp_path / "canonical_entities.csv",
        aliases_path=tmp_path / "entity_aliases.csv",
        matches_path=tmp_path / "entity_resolution_matches.csv",
        canonical_relationships_path=tmp_path / "canonical_relationships.csv",
        db_path=db_path,
    )

    canonical = pd.read_csv(tmp_path / "canonical_entities.csv")
    matches = pd.read_csv(tmp_path / "entity_resolution_matches.csv")

    person_clusters = canonical[canonical["entity_type"] == "person"]
    assert len(person_clusters) == 3
    assert "AUTO_MERGE" in set(matches["decision"])
    fuzzy_only = matches[matches["match_method"] == "fuzzy_name_only"]
    assert not fuzzy_only.empty
    assert set(fuzzy_only["decision"]) == {"NO_MERGE"}


def test_resolver_preserves_cross_source_provenance_and_deduplicates_canonical_relationships(tmp_path: Path) -> None:
    entities = [
        {"entity_id": "property:01-1111", "display_name": "Parcel 01-1111", "entity_type": "property", "source": "arcgis", "source_name": "florida_county_arcgis_parcels", "source_type": "arcgis"},
        {"entity_id": "property:011111", "display_name": "Parcel 01 1111", "entity_type": "property", "source": "county_property", "source_name": "county_property_local_file", "source_type": "connector"},
        {"entity_id": "address:1", "display_name": "123 Main Street, Pensacola, FL 32502", "entity_type": "address", "source": "arcgis", "source_name": "florida_county_arcgis_parcels", "source_type": "arcgis"},
        {"entity_id": "address:2", "display_name": "123 MAIN ST, Pensacola FL 32502", "entity_type": "address", "source": "county_property", "source_name": "county_property_local_file", "source_type": "connector"},
    ]
    relationships = [
        {"source_entity_id": "property:01-1111", "target_entity_id": "address:1", "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS", "confidence": 0.9, "source": "arcgis", "source_name": "florida_county_arcgis_parcels", "source_type": "arcgis"},
        {"source_entity_id": "property:011111", "target_entity_id": "address:2", "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS", "confidence": 1.0, "source": "county_property", "source_name": "county_property_local_file", "source_type": "connector"},
    ]
    entities_path, relationships_path, db_path = _write_inputs(tmp_path, entities, relationships)

    summary = resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=tmp_path / "canonical_entities.csv",
        aliases_path=tmp_path / "entity_aliases.csv",
        matches_path=tmp_path / "entity_resolution_matches.csv",
        canonical_relationships_path=tmp_path / "canonical_relationships.csv",
        db_path=db_path,
    )

    canonical = pd.read_csv(tmp_path / "canonical_entities.csv")
    canonical_relationships = pd.read_csv(tmp_path / "canonical_relationships.csv")
    matches = pd.read_csv(tmp_path / "entity_resolution_matches.csv")

    property_rows = canonical[canonical["entity_type"] == "property"]
    assert len(property_rows) == 1
    assert "florida_county_arcgis_parcels" in property_rows.iloc[0]["source_name"]
    assert "county_property_local_file" in property_rows.iloc[0]["source_name"]
    assert len(canonical_relationships) == 1
    assert "|" in canonical_relationships.iloc[0]["original_relationship_ids"]
    assert summary["cross_source_canonical_entities"] >= 1
    assert "exact_property_parcel" in set(matches["match_method"])


def test_resolver_marks_exact_name_without_secondary_business_evidence_for_review(tmp_path: Path) -> None:
    entities = [
        {"entity_id": "business:1", "display_name": "Acme LLC", "entity_type": "business", "source": "sunbiz", "source_name": "sunbiz_local_file", "source_type": "connector"},
        {"entity_id": "business:2", "display_name": "ACME, L.L.C.", "entity_type": "business", "source": "manual", "source_name": "manual_csv", "source_type": "manual"},
    ]
    relationships: list[dict] = []
    entities_path, relationships_path, db_path = _write_inputs(tmp_path, entities, relationships)

    resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=tmp_path / "canonical_entities.csv",
        aliases_path=tmp_path / "entity_aliases.csv",
        matches_path=tmp_path / "entity_resolution_matches.csv",
        canonical_relationships_path=tmp_path / "canonical_relationships.csv",
        db_path=db_path,
    )

    matches = pd.read_csv(tmp_path / "entity_resolution_matches.csv")
    assert set(matches["decision"]) == {"REVIEW"}
    assert set(matches["match_method"]) == {"exact_name_requires_secondary_review"}
