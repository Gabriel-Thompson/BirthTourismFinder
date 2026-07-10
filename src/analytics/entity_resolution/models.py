from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class NormalizedEntity:
    entity_id: str
    entity_type: str
    display_name: str
    source: str
    source_name: str
    source_type: str
    source_record_id: str
    normalized_value: str
    canonical_basis: str
    match_key: str
    building_key: str = ""
    unit_key: str = ""
    zip_code: str = ""
    name_prefix: str = ""
    surname_prefix: str = ""
    domain: str = ""
    phone_prefix: str = ""
    parcel_key: str = ""
    jurisdiction: str = ""
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class MatchResult:
    match_id: str
    left_entity_id: str
    right_entity_id: str
    entity_type: str
    match_method: str
    confidence_score: float
    decision: str
    evidence: str
    source_names: str


@dataclass(frozen=True)
class CanonicalEntity:
    canonical_entity_id: str
    entity_type: str
    display_name: str
    normalized_value: str
    source_count: int
    record_count: int
    alias_count: int
    first_seen: str
    last_seen: str
    source_names: str
    resolution_confidence: float
    resolution_method: str
    source_type: str


@dataclass(frozen=True)
class CanonicalRelationship:
    relationship_id: str
    source_canonical_entity_id: str
    target_canonical_entity_id: str
    relationship_type: str
    source_name: str
    source_type: str
    evidence: str
    confidence_score: float
    original_relationship_ids: str
