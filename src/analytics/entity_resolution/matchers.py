from __future__ import annotations

import hashlib
from collections import defaultdict
from itertools import combinations
from typing import Dict, Iterable, List, Set, Tuple

from src.analytics.entity_resolution.models import MatchResult, NormalizedEntity
from src.analytics.entity_resolution.normalizers import similarity
from src.connectors.source_metadata import merge_source_values


def build_match_id(left_entity_id: str, right_entity_id: str, method: str) -> str:
    ordered = "|".join(sorted([left_entity_id, right_entity_id]))
    digest = hashlib.sha1(f"{ordered}|{method}".encode("utf-8")).hexdigest()[:16]
    return f"match:{digest}"


def build_secondary_index(
    entities_by_id: Dict[str, NormalizedEntity],
    relationships: Iterable[Dict[str, str]],
) -> Dict[str, Dict[str, Set[str]]]:
    secondary: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in relationships:
        source_id = str(row.get("source_entity_id", "")).strip()
        target_id = str(row.get("target_entity_id", "")).strip()
        if source_id not in entities_by_id or target_id not in entities_by_id:
            continue
        source_entity = entities_by_id[source_id]
        target_entity = entities_by_id[target_id]
        if target_entity.normalized_value:
            secondary[source_id][target_entity.entity_type].add(target_entity.normalized_value)
        if source_entity.normalized_value:
            secondary[target_id][source_entity.entity_type].add(source_entity.normalized_value)
    return secondary


def build_candidate_pairs(
    normalized_entities: List[NormalizedEntity],
    secondary_index: Dict[str, Dict[str, Set[str]]],
    config: Dict[str, object],
) -> tuple[Set[Tuple[str, str]], int]:
    maximum_block_size = int(config.get("maximum_block_size", 200))
    blocks: Dict[tuple[str, str], list[str]] = defaultdict(list)

    for entity in normalized_entities:
        entity_type = entity.entity_type
        if not entity.normalized_value:
            continue
        blocks[(entity_type, f"exact:{entity.match_key}")].append(entity.entity_id)
        if entity_type == "address" and entity.building_key:
            blocks[(entity_type, f"building:{entity.building_key}")].append(entity.entity_id)
            if entity.zip_code and entity.extra.get("address_number"):
                blocks[(entity_type, f"zipnum:{entity.zip_code}:{entity.extra['address_number']}")].append(entity.entity_id)
        if entity_type == "phone" and entity.phone_prefix:
            blocks[(entity_type, f"phone:{entity.phone_prefix}")].append(entity.entity_id)
        if entity_type == "email" and entity.domain:
            blocks[(entity_type, f"domain:{entity.domain}")].append(entity.entity_id)
        if entity_type in {"business", "owner"} and entity.name_prefix:
            blocks[(entity_type, f"name:{entity.name_prefix}")].append(entity.entity_id)
            blocks[(entity_type, f"fuzzy-name:{entity.normalized_value[:3]}")].append(entity.entity_id)
        if entity_type in {"person", "registered_agent", "officer"} and entity.surname_prefix:
            blocks[(entity_type, f"surname:{entity.surname_prefix}")].append(entity.entity_id)
            blocks[(entity_type, f"fuzzy-name:{entity.normalized_value[:3]}")].append(entity.entity_id)
            person_tokens = [token for token in entity.normalized_value.split() if token]
            if len(person_tokens) >= 2:
                blocks[(entity_type, f"fuzzy-person:{person_tokens[0][0]}:{person_tokens[-1][:2]}")].append(entity.entity_id)

        secondary = secondary_index.get(entity.entity_id, {})
        for related_type in ["address", "phone", "email", "website", "property"]:
            for normalized_value in sorted(secondary.get(related_type, set())):
                if entity_type in {"business", "owner"} and entity.name_prefix:
                    blocks[(entity_type, f"compound:{entity.name_prefix}:{related_type}:{normalized_value}")].append(entity.entity_id)
                if entity_type in {"person", "registered_agent", "officer", "owner"} and entity.surname_prefix:
                    blocks[(entity_type, f"compound:{entity.surname_prefix}:{related_type}:{normalized_value}")].append(entity.entity_id)

    candidate_pairs: Set[Tuple[str, str]] = set()
    usable_blocks = 0
    for _, entity_ids in blocks.items():
        unique_ids = sorted(set(entity_ids))
        if len(unique_ids) < 2 or len(unique_ids) > maximum_block_size:
            continue
        usable_blocks += 1
        for left_id, right_id in combinations(unique_ids, 2):
            candidate_pairs.add((left_id, right_id))
    return candidate_pairs, usable_blocks


def _shared_secondary_evidence(
    left_id: str,
    right_id: str,
    secondary_index: Dict[str, Dict[str, Set[str]]],
) -> list[str]:
    shared: list[str] = []
    left = secondary_index.get(left_id, {})
    right = secondary_index.get(right_id, {})
    for related_type in ["address", "phone", "email", "website", "property"]:
        overlap = sorted(left.get(related_type, set()) & right.get(related_type, set()))
        if overlap:
            shared.append(f"{related_type}={overlap[0]}")
    return shared


def evaluate_match(
    left: NormalizedEntity,
    right: NormalizedEntity,
    secondary_index: Dict[str, Dict[str, Set[str]]],
    config: Dict[str, object],
) -> MatchResult:
    if left.entity_type != right.entity_type:
        raise ValueError("Cross-type matching is not supported for canonical merges.")

    shared_secondary = _shared_secondary_evidence(left.entity_id, right.entity_id, secondary_index)
    source_names = merge_source_values(left.source_name, right.source_name)
    confidence = 0.0
    decision = "NO_MERGE"
    method = "candidate_only"
    evidence = "No deterministic or supported compound evidence."
    entity_type = left.entity_type

    if entity_type == "phone" and config.get("exact_phone_enabled", True) and left.normalized_value and left.normalized_value == right.normalized_value:
        confidence, decision, method = 1.0, "AUTO_MERGE", "exact_phone"
        evidence = f"Exact normalized phone match: {left.normalized_value}"
    elif entity_type == "email" and config.get("exact_email_enabled", True) and left.normalized_value and left.normalized_value == right.normalized_value:
        confidence, decision, method = 1.0, "AUTO_MERGE", "exact_email"
        evidence = f"Exact normalized email match: {left.normalized_value}"
    elif entity_type == "website" and config.get("exact_website_enabled", True) and left.normalized_value and left.normalized_value == right.normalized_value:
        confidence, decision, method = 1.0, "AUTO_MERGE", "exact_website"
        evidence = f"Exact normalized website match: {left.normalized_value}"
    elif entity_type == "property" and config.get("exact_parcel_enabled", True) and left.parcel_key and left.parcel_key == right.parcel_key:
        confidence, decision, method = 1.0, "AUTO_MERGE", "exact_property_parcel"
        evidence = f"Exact parcel identifier match: {left.parcel_key}"
    elif entity_type == "address":
        if left.normalized_value and left.normalized_value == right.normalized_value:
            confidence, decision, method = 0.99, "AUTO_MERGE", "exact_address"
            evidence = f"Exact normalized unit/building address match: {left.normalized_value}"
        elif left.building_key and left.building_key == right.building_key and left.unit_key and right.unit_key and left.unit_key != right.unit_key:
            confidence, decision, method = 0.3, "NO_MERGE", "shared_building_different_unit"
            evidence = f"Same building {left.building_key} but different units {left.unit_key} vs {right.unit_key}"
    else:
        exact_name = left.normalized_value and left.normalized_value == right.normalized_value
        ratio = similarity(left.normalized_value, right.normalized_value)
        fuzzy_threshold = float(config.get("fuzzy_name_threshold", 0.9))
        business_threshold = float(config.get("business_similarity_threshold", fuzzy_threshold))
        person_threshold = float(config.get("person_similarity_threshold", fuzzy_threshold))
        threshold = person_threshold if entity_type in {"person", "owner", "registered_agent", "officer"} else business_threshold
        require_person_secondary = bool(config.get("require_secondary_evidence_for_person", True))
        require_business_secondary = bool(config.get("require_secondary_evidence_for_business", True))
        requires_secondary = (
            entity_type in {"person", "registered_agent", "officer"} and require_person_secondary
        ) or (entity_type in {"business", "owner"} and require_business_secondary)

        if exact_name and shared_secondary:
            confidence, decision, method = 0.97, "AUTO_MERGE", "exact_name_plus_secondary"
            evidence = f"Exact normalized name plus secondary evidence: {', '.join(shared_secondary)}"
        elif exact_name and not requires_secondary:
            confidence, decision, method = 0.96, "AUTO_MERGE", "exact_name"
            evidence = f"Exact normalized name match: {left.normalized_value}"
        elif exact_name:
            confidence, decision, method = 0.82, "REVIEW", "exact_name_requires_secondary_review"
            evidence = f"Exact normalized name match without secondary evidence: {left.normalized_value}"
        elif ratio >= threshold and shared_secondary:
            confidence, decision, method = 0.86, "REVIEW", "fuzzy_name_plus_secondary"
            evidence = f"Fuzzy name match ({ratio:.2f}) supported by {', '.join(shared_secondary)}"
        elif ratio >= 0.75:
            confidence, decision, method = 0.45, "NO_MERGE", "fuzzy_name_only"
            evidence = f"Fuzzy name similarity {ratio:.2f} without secondary evidence"

    auto_threshold = float(config.get("auto_merge_threshold", 0.95))
    review_threshold = float(config.get("review_threshold", 0.75))
    if method == "fuzzy_name_only":
        decision = "NO_MERGE"
    elif confidence >= auto_threshold and decision != "NO_MERGE":
        decision = "AUTO_MERGE"
    elif confidence >= review_threshold and decision != "NO_MERGE":
        decision = "REVIEW" if method.startswith("fuzzy") or method.endswith("review") else decision
    else:
        decision = "NO_MERGE" if confidence < review_threshold else decision

    match_id = build_match_id(left.entity_id, right.entity_id, method)
    return MatchResult(
        match_id=match_id,
        left_entity_id=left.entity_id,
        right_entity_id=right.entity_id,
        entity_type=entity_type,
        match_method=method,
        confidence_score=round(confidence, 4),
        decision=decision,
        evidence=evidence,
        source_names=source_names,
    )
