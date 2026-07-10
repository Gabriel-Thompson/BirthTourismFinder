from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
from pandas.errors import EmptyDataError

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.entity_resolution.matchers import build_candidate_pairs, build_secondary_index, evaluate_match
from src.analytics.entity_resolution.models import CanonicalEntity, CanonicalRelationship, MatchResult, NormalizedEntity
from src.analytics.entity_resolution.normalizers import normalize_entity_row
from src.connectors.source_metadata import is_real_source_type, merge_source_values

ENTITIES_PATH = Path("data/processed/entities.csv")
RELATIONSHIPS_PATH = Path("data/processed/relationships.csv")
CANONICAL_ENTITIES_PATH = Path("data/processed/canonical_entities.csv")
ALIASES_PATH = Path("data/processed/entity_aliases.csv")
MATCHES_PATH = Path("data/processed/entity_resolution_matches.csv")
CANONICAL_RELATIONSHIPS_PATH = Path("data/processed/canonical_relationships.csv")
CONFIG_PATH = Path("config/entity_resolution.json")
DB_PATH = Path("local_osint.duckdb")
DEFAULT_CONFIG = {
    "auto_merge_threshold": 0.95,
    "review_threshold": 0.75,
    "fuzzy_name_threshold": 0.9,
    "address_similarity_threshold": 0.95,
    "business_similarity_threshold": 0.9,
    "person_similarity_threshold": 0.92,
    "exact_phone_enabled": True,
    "exact_email_enabled": True,
    "exact_website_enabled": True,
    "exact_parcel_enabled": True,
    "require_secondary_evidence_for_person": True,
    "require_secondary_evidence_for_business": True,
    "maximum_block_size": 200,
    "enabled_entity_types": [
        "address",
        "business",
        "person",
        "owner",
        "registered_agent",
        "officer",
        "phone",
        "email",
        "website",
        "property",
    ],
}


class UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if left_root < right_root:
            self.parent[right_root] = left_root
        else:
            self.parent[left_root] = right_root


def load_resolution_config(path: Path | str = CONFIG_PATH) -> Dict[str, object]:
    config_path = Path(path)
    if not config_path.exists():
        return dict(DEFAULT_CONFIG)
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception:
        return dict(DEFAULT_CONFIG)
    config = dict(DEFAULT_CONFIG)
    config.update(loaded)
    return config


def stable_canonical_id(entity_type: str, canonical_basis: str) -> str:
    digest = hashlib.sha1(f"{entity_type}|{canonical_basis}".encode("utf-8")).hexdigest()[:16]
    return f"canonical:{entity_type}:{digest}"


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame()


def _write_frame(path: Path, rows: List[dict], columns: List[str]) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=columns)
    frame.to_csv(path, index=False)
    return frame


def _canonical_basis_for_cluster(cluster: List[NormalizedEntity]) -> str:
    ordered = sorted(
        cluster,
        key=lambda entity: (
            entity.canonical_basis or "~",
            entity.normalized_value or "~",
            entity.display_name.upper(),
            entity.source_name,
            entity.entity_id,
        ),
    )
    representative = ordered[0]
    if representative.entity_type == "address" and representative.normalized_value:
        return representative.normalized_value
    return representative.canonical_basis or representative.normalized_value or representative.display_name.upper()


def _relationship_id(
    source_canonical_entity_id: str,
    target_canonical_entity_id: str,
    relationship_type: str,
) -> str:
    digest = hashlib.sha1(f"{source_canonical_entity_id}|{target_canonical_entity_id}|{relationship_type}".encode("utf-8")).hexdigest()[:16]
    return f"canonical-rel:{digest}"


def _match_method_for_cluster(cluster_matches: List[MatchResult]) -> str:
    if not cluster_matches:
        return "identity"
    methods = sorted({match.match_method for match in cluster_matches if match.decision == "AUTO_MERGE"})
    return "|".join(methods) if methods else "identity"


def _match_confidence_for_cluster(cluster_matches: List[MatchResult]) -> float:
    scores = [match.confidence_score for match in cluster_matches if match.decision == "AUTO_MERGE"]
    if not scores:
        return 1.0
    return round(max(scores), 4)


def resolve_entities(
    entities_path: Path | str = ENTITIES_PATH,
    relationships_path: Path | str = RELATIONSHIPS_PATH,
    canonical_entities_path: Path | str = CANONICAL_ENTITIES_PATH,
    aliases_path: Path | str = ALIASES_PATH,
    matches_path: Path | str = MATCHES_PATH,
    canonical_relationships_path: Path | str = CANONICAL_RELATIONSHIPS_PATH,
    config_path: Path | str = CONFIG_PATH,
    db_path: Optional[Path | str] = DB_PATH,
) -> dict[str, object]:
    start_time = time.time()
    entities_input = Path(entities_path)
    relationships_input = Path(relationships_path)
    canonical_entities_output = Path(canonical_entities_path)
    aliases_output = Path(aliases_path)
    matches_output = Path(matches_path)
    canonical_relationships_output = Path(canonical_relationships_path)
    config = load_resolution_config(config_path)

    print("Entity Resolution: started")
    print(f"Entity Resolution: entities input {entities_input}")
    print(f"Entity Resolution: relationships input {relationships_input}")
    print(f"Entity Resolution: config {Path(config_path)}")

    entities_df = _load_frame(entities_input)
    raw_entity_count = len(entities_df)
    relationships_df = _load_frame(relationships_input)
    implicit_rows: list[dict] = []
    known_entity_ids = set(entities_df.get("entity_id", pd.Series(dtype=str)).astype(str)) if not entities_df.empty else set()
    for relationship in relationships_df.fillna("").to_dict("records"):
        for endpoint_field in ["source_entity_id", "target_entity_id"]:
            endpoint_id = str(relationship.get(endpoint_field, "")).strip()
            if not endpoint_id or endpoint_id in known_entity_ids or ":" not in endpoint_id:
                continue
            entity_type, display_name = endpoint_id.split(":", 1)
            implicit_rows.append(
                {
                    "entity_id": endpoint_id,
                    "display_name": display_name,
                    "entity_type": entity_type,
                    "source": str(relationship.get("source", "")).strip(),
                    "source_name": str(relationship.get("source_name", "")).strip(),
                    "source_type": str(relationship.get("source_type", "")).strip(),
                }
            )
            known_entity_ids.add(endpoint_id)
    if implicit_rows:
        entities_df = pd.concat([entities_df, pd.DataFrame(implicit_rows)], ignore_index=True)
    print(f"Entity Resolution: raw entities loaded {raw_entity_count}")
    if implicit_rows:
        print(f"Entity Resolution: implicit relationship entities added {len(implicit_rows)}")
    print(f"Entity Resolution: raw relationships loaded {len(relationships_df)}")

    enabled_types = {str(value).lower() for value in config.get("enabled_entity_types", [])}
    normalized_entities: List[NormalizedEntity] = []
    passthrough_entities: List[NormalizedEntity] = []
    for row in entities_df.fillna("").to_dict("records"):
        normalized = normalize_entity_row({str(key): str(value) for key, value in row.items()})
        if normalized.entity_type in enabled_types:
            normalized_entities.append(normalized)
        else:
            passthrough_entities.append(normalized)

    entities_by_id = {entity.entity_id: entity for entity in [*normalized_entities, *passthrough_entities]}
    secondary_index = build_secondary_index(entities_by_id, relationships_df.fillna("").to_dict("records"))
    candidate_pairs, usable_blocks = build_candidate_pairs(normalized_entities, secondary_index, config)
    print(f"Entity Resolution: blocking groups created {usable_blocks}")
    print(f"Entity Resolution: candidate matches queued {len(candidate_pairs)}")

    matches: List[MatchResult] = []
    match_counter = Counter()
    by_id = {entity.entity_id: entity for entity in normalized_entities}
    for left_id, right_id in sorted(candidate_pairs):
        left = by_id[left_id]
        right = by_id[right_id]
        match = evaluate_match(left, right, secondary_index, config)
        matches.append(match)
        match_counter[match.decision] += 1

    print(f"Entity Resolution: candidate matches evaluated {len(matches)}")
    print(f"Entity Resolution: auto-merges {match_counter['AUTO_MERGE']}")
    print(f"Entity Resolution: review matches {match_counter['REVIEW']}")

    union_find = UnionFind(entity.entity_id for entity in normalized_entities)
    for match in matches:
        if match.decision == "AUTO_MERGE":
            union_find.union(match.left_entity_id, match.right_entity_id)

    clusters: Dict[str, List[NormalizedEntity]] = defaultdict(list)
    for entity in normalized_entities:
        clusters[union_find.find(entity.entity_id)].append(entity)
    for entity in passthrough_entities:
        clusters[entity.entity_id].append(entity)

    match_lookup: Dict[frozenset[str], MatchResult] = {}
    for match in matches:
        match_lookup[frozenset({match.left_entity_id, match.right_entity_id})] = match

    canonical_rows: List[dict] = []
    alias_rows: List[dict] = []
    canonical_id_by_entity_id: Dict[str, str] = {}
    cluster_match_methods: Dict[str, list[MatchResult]] = defaultdict(list)

    for cluster_id, cluster_entities in sorted(clusters.items(), key=lambda item: item[0]):
        entity_type = cluster_entities[0].entity_type
        basis = _canonical_basis_for_cluster(cluster_entities)
        canonical_entity_id = stable_canonical_id(entity_type, basis)
        source_names = merge_source_values(*[entity.source_name for entity in cluster_entities if entity.source_name])
        source_types = merge_source_values(*[entity.source_type for entity in cluster_entities if entity.source_type])
        source_record_ids = merge_source_values(*[entity.source_record_id for entity in cluster_entities if entity.source_record_id])
        connector_names = merge_source_values(*[str(entity.extra.get("connector_name", "")) for entity in cluster_entities if entity.extra.get("connector_name", "")])
        import_batch_ids = merge_source_values(*[str(entity.extra.get("import_batch_id", "")) for entity in cluster_entities if entity.extra.get("import_batch_id", "")])
        imported_at_values = merge_source_values(*[str(entity.extra.get("imported_at", "")) for entity in cluster_entities if entity.extra.get("imported_at", "")])
        jurisdictions = merge_source_values(*[entity.jurisdiction for entity in cluster_entities if entity.jurisdiction])
        is_synthetic = "true" if all(str(entity.extra.get("is_synthetic", "")).lower() == "true" for entity in cluster_entities) else "false"
        for left_entity in cluster_entities:
            canonical_id_by_entity_id[left_entity.entity_id] = canonical_entity_id
        for left_index, left_entity in enumerate(cluster_entities):
            for right_entity in cluster_entities[left_index + 1 :]:
                match = match_lookup.get(frozenset({left_entity.entity_id, right_entity.entity_id}))
                if match:
                    cluster_match_methods[canonical_entity_id].append(match)

        representative = sorted(
            cluster_entities,
            key=lambda entity: (
                entity.normalized_value or "~",
                -len(entity.display_name),
                entity.display_name.upper(),
                entity.entity_id,
            ),
        )[0]
        cluster_matches = cluster_match_methods.get(canonical_entity_id, [])
        canonical = CanonicalEntity(
            canonical_entity_id=canonical_entity_id,
            entity_type=entity_type,
            display_name=representative.display_name,
            normalized_value=representative.normalized_value,
            source_count=len({token for entity in cluster_entities for token in entity.source_name.split("|") if token}),
            record_count=len(cluster_entities),
            alias_count=len(cluster_entities),
            first_seen="",
            last_seen="",
            source_names=source_names,
            resolution_confidence=_match_confidence_for_cluster(cluster_matches),
            resolution_method=_match_method_for_cluster(cluster_matches),
            source_type=source_types,
        )
        canonical_rows.append(
            {
                **canonical.__dict__,
                "entity_id": canonical_entity_id,
                "source_name": source_names,
                "source": canonical.resolution_method,
                "source_record_ids": source_record_ids,
                "connector_name": connector_names,
                "import_batch_id": import_batch_ids,
                "imported_at": imported_at_values,
                "jurisdiction": jurisdictions,
                "is_synthetic": is_synthetic,
            }
        )
        confidence = canonical.resolution_confidence
        method = canonical.resolution_method
        for entity in sorted(cluster_entities, key=lambda value: value.entity_id):
            alias_rows.append(
                {
                    "canonical_entity_id": canonical_entity_id,
                    "original_entity_id": entity.entity_id,
                    "alias_value": entity.display_name,
                    "normalized_alias": entity.normalized_value,
                    "source_name": entity.source_name,
                    "source_type": entity.source_type,
                    "source_record_id": entity.source_record_id,
                    "connector_name": str(entity.extra.get("connector_name", "")),
                    "import_batch_id": str(entity.extra.get("import_batch_id", "")),
                    "imported_at": str(entity.extra.get("imported_at", "")),
                    "jurisdiction": entity.jurisdiction,
                    "is_synthetic": str(entity.extra.get("is_synthetic", "")),
                    "resolution_method": method,
                    "confidence_score": confidence,
                }
            )

    relationship_rows: List[dict] = []
    canonical_relationship_map: Dict[tuple[str, str, str], dict] = {}
    for index, relationship in enumerate(relationships_df.fillna("").to_dict("records"), start=1):
        source_entity_id = str(relationship.get("source_entity_id", "")).strip()
        target_entity_id = str(relationship.get("target_entity_id", "")).strip()
        source_canonical_entity_id = canonical_id_by_entity_id.get(source_entity_id)
        target_canonical_entity_id = canonical_id_by_entity_id.get(target_entity_id)
        if not source_canonical_entity_id or not target_canonical_entity_id:
            continue
        if source_canonical_entity_id == target_canonical_entity_id:
            continue
        relationship_type = str(relationship.get("relationship_type", "")).strip()
        confidence_value = float(relationship.get("confidence", 1.0) or 1.0)
        source_name = str(relationship.get("source_name", "")).strip()
        source_type = str(relationship.get("source_type", "")).strip()
        original_relationship_id = f"rel{index}:{source_entity_id}->{target_entity_id}:{relationship_type}"
        source_record_id = str(relationship.get("source_record_id", "")).strip()
        key = (source_canonical_entity_id, target_canonical_entity_id, relationship_type)
        if key not in canonical_relationship_map:
            canonical_relationship = CanonicalRelationship(
                relationship_id=_relationship_id(*key),
                source_canonical_entity_id=source_canonical_entity_id,
                target_canonical_entity_id=target_canonical_entity_id,
                relationship_type=relationship_type,
                source_name=source_name,
                source_type=source_type,
                evidence=f"Resolved from raw relationship {source_entity_id} -> {target_entity_id}",
                confidence_score=confidence_value,
                original_relationship_ids=original_relationship_id,
            )
            canonical_relationship_map[key] = {
                **canonical_relationship.__dict__,
                "source_entity_id": source_canonical_entity_id,
                "target_entity_id": target_canonical_entity_id,
                "confidence": confidence_value,
                "source_record_id": source_record_id,
                "connector_name": str(relationship.get("connector_name", "")).strip(),
                "import_batch_id": str(relationship.get("import_batch_id", "")).strip(),
                "imported_at": str(relationship.get("imported_at", "")).strip(),
                "jurisdiction": str(relationship.get("jurisdiction", "")).strip(),
                "is_synthetic": str(relationship.get("is_synthetic", "")).strip(),
            }
        else:
            existing = canonical_relationship_map[key]
            existing["source_name"] = merge_source_values(existing["source_name"], source_name)
            existing["source_type"] = merge_source_values(existing["source_type"], source_type)
            existing["confidence_score"] = max(float(existing["confidence_score"]), confidence_value)
            existing["confidence"] = max(float(existing["confidence"]), confidence_value)
            existing["original_relationship_ids"] = merge_source_values(existing["original_relationship_ids"], original_relationship_id)
            existing["source_record_id"] = merge_source_values(existing["source_record_id"], source_record_id)
            existing["connector_name"] = merge_source_values(existing["connector_name"], str(relationship.get("connector_name", "")).strip())
            existing["import_batch_id"] = merge_source_values(existing["import_batch_id"], str(relationship.get("import_batch_id", "")).strip())
            existing["imported_at"] = merge_source_values(existing["imported_at"], str(relationship.get("imported_at", "")).strip())
            existing["jurisdiction"] = merge_source_values(existing["jurisdiction"], str(relationship.get("jurisdiction", "")).strip())
            existing["is_synthetic"] = merge_source_values(existing["is_synthetic"], str(relationship.get("is_synthetic", "")).strip())
    relationship_rows = list(canonical_relationship_map.values())

    canonical_entities_frame = _write_frame(
        canonical_entities_output,
        canonical_rows,
        [
            "canonical_entity_id",
            "entity_id",
            "entity_type",
            "display_name",
            "normalized_value",
            "source_count",
            "record_count",
            "alias_count",
            "first_seen",
            "last_seen",
            "source_names",
            "source_name",
            "resolution_confidence",
            "resolution_method",
            "source",
            "source_type",
            "source_record_ids",
            "connector_name",
            "import_batch_id",
            "imported_at",
            "jurisdiction",
            "is_synthetic",
        ],
    )
    aliases_frame = _write_frame(
        aliases_output,
        alias_rows,
        [
            "canonical_entity_id",
            "original_entity_id",
            "alias_value",
            "normalized_alias",
            "source_name",
            "source_type",
            "source_record_id",
            "connector_name",
            "import_batch_id",
            "imported_at",
            "jurisdiction",
            "is_synthetic",
            "resolution_method",
            "confidence_score",
        ],
    )
    matches_frame = _write_frame(
        matches_output,
        [match.__dict__ for match in matches],
        [
            "match_id",
            "left_entity_id",
            "right_entity_id",
            "entity_type",
            "match_method",
            "confidence_score",
            "decision",
            "evidence",
            "source_names",
        ],
    )
    canonical_relationships_frame = _write_frame(
        canonical_relationships_output,
        relationship_rows,
        [
            "relationship_id",
            "source_canonical_entity_id",
            "target_canonical_entity_id",
            "source_entity_id",
            "target_entity_id",
            "relationship_type",
            "source_name",
            "source_type",
            "evidence",
            "confidence",
            "confidence_score",
            "original_relationship_ids",
            "source_record_id",
            "connector_name",
            "import_batch_id",
            "imported_at",
            "jurisdiction",
            "is_synthetic",
        ],
    )

    if db_path is not None:
        with duckdb.connect(str(Path(db_path))) as connection:
            connection.execute("DROP TABLE IF EXISTS canonical_entities")
            connection.execute("DROP TABLE IF EXISTS entity_aliases")
            connection.execute("DROP TABLE IF EXISTS entity_resolution_matches")
            connection.execute("DROP TABLE IF EXISTS canonical_relationships")
            if not canonical_entities_frame.empty:
                connection.register("canonical_entities_tmp", canonical_entities_frame)
                connection.execute("CREATE TABLE canonical_entities AS SELECT * FROM canonical_entities_tmp")
            if not aliases_frame.empty:
                connection.register("entity_aliases_tmp", aliases_frame)
                connection.execute("CREATE TABLE entity_aliases AS SELECT * FROM entity_aliases_tmp")
            if not matches_frame.empty:
                connection.register("entity_resolution_matches_tmp", matches_frame)
                connection.execute("CREATE TABLE entity_resolution_matches AS SELECT * FROM entity_resolution_matches_tmp")
            if not canonical_relationships_frame.empty:
                connection.register("canonical_relationships_tmp", canonical_relationships_frame)
                connection.execute("CREATE TABLE canonical_relationships AS SELECT * FROM canonical_relationships_tmp")

    cross_source_count = 0
    for row in canonical_rows:
        source_types = str(row.get("source_type", ""))
        if "|" in str(row.get("source_names", "")) or is_real_source_type(source_types):
            distinct_sources = [token for token in str(row.get("source_names", "")).split("|") if token]
            if len(set(distinct_sources)) > 1:
                cross_source_count += 1

    duration = time.time() - start_time
    print(f"Entity Resolution: canonical entities created {len(canonical_rows)}")
    print(f"Entity Resolution: canonical relationships created {len(relationship_rows)}")
    print(f"Entity Resolution: completed in {duration:.2f}s")

    return {
        "raw_entity_count": raw_entity_count,
        "canonical_entity_count": len(canonical_rows),
        "merged_entities": max(len(entities_df) - len(canonical_rows), 0),
        "review_candidates": int((matches_frame["decision"] == "REVIEW").sum()) if not matches_frame.empty else 0,
        "cross_source_canonical_entities": cross_source_count,
        "match_counts_by_entity_type": matches_frame["entity_type"].value_counts().to_dict() if not matches_frame.empty else {},
        "match_counts_by_method": matches_frame["match_method"].value_counts().to_dict() if not matches_frame.empty else {},
        "canonical_relationship_count": len(relationship_rows),
        "runtime_seconds": round(duration, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve raw entities into deterministic canonical entities.")
    parser.add_argument("--entities-path", default=str(ENTITIES_PATH), help="Path to the raw entities CSV")
    parser.add_argument("--relationships-path", default=str(RELATIONSHIPS_PATH), help="Path to the raw relationships CSV")
    parser.add_argument("--canonical-entities-path", default=str(CANONICAL_ENTITIES_PATH), help="Path to write canonical entities CSV")
    parser.add_argument("--aliases-path", default=str(ALIASES_PATH), help="Path to write alias CSV")
    parser.add_argument("--matches-path", default=str(MATCHES_PATH), help="Path to write match decisions CSV")
    parser.add_argument("--canonical-relationships-path", default=str(CANONICAL_RELATIONSHIPS_PATH), help="Path to write canonical relationships CSV")
    parser.add_argument("--config-path", default=str(CONFIG_PATH), help="Path to the entity-resolution config")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to the DuckDB database for canonical tables")
    args = parser.parse_args()

    summary = resolve_entities(
        entities_path=args.entities_path,
        relationships_path=args.relationships_path,
        canonical_entities_path=args.canonical_entities_path,
        aliases_path=args.aliases_path,
        matches_path=args.matches_path,
        canonical_relationships_path=args.canonical_relationships_path,
        config_path=args.config_path,
        db_path=args.db_path,
    )
    print("Entity Resolution: PASS")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
