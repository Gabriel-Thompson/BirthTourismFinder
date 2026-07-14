from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

import pandas as pd


def token_set(value: object) -> set[str]:
    return {token.strip() for token in str(value or "").split("|") if token.strip()}


def _normalized_text(value: object) -> str:
    return str(value or "").strip()


def build_value_index(
    records: Iterable[dict[str, Any]],
    *,
    fields: list[str],
    allow_multiple: bool = True,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        for field in fields:
            value = _normalized_text(record.get(field))
            if not value:
                continue
            key = (field, value)
            if allow_multiple or not index[key]:
                index[key].append(record)
    return dict(index)


def build_dataframe_index(df: pd.DataFrame, *, fields: list[str]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    if df.empty:
        return {}
    available = [field for field in fields if field in df.columns]
    if not available:
        return {}
    return build_value_index(df.fillna("").to_dict("records"), fields=available)


def build_group_index(df: pd.DataFrame, *, field: str) -> dict[str, list[dict[str, Any]]]:
    if df.empty or field not in df.columns:
        return {}
    groups: dict[str, list[dict[str, Any]]] = {}
    for value, group in df.fillna("").groupby(field):
        normalized = _normalized_text(value)
        if normalized:
            groups[normalized] = group.to_dict("records")
    return groups


def build_entity_id_index(df: pd.DataFrame, *, id_field: str = "entity_id") -> dict[str, dict[str, Any]]:
    if df.empty or id_field not in df.columns:
        return {}
    return {
        _normalized_text(row.get(id_field)): row
        for row in df.fillna("").to_dict("records")
        if _normalized_text(row.get(id_field))
    }


def build_relationship_endpoint_index(relationships_df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if relationships_df.empty:
        return {}
    endpoint_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in relationships_df.fillna("").to_dict("records"):
        source_entity_id = _normalized_text(row.get("source_entity_id"))
        target_entity_id = _normalized_text(row.get("target_entity_id"))
        if source_entity_id:
            endpoint_index[source_entity_id].append(row)
        if target_entity_id and target_entity_id != source_entity_id:
            endpoint_index[target_entity_id].append(row)
    return dict(endpoint_index)


def candidate_pairs_from_groups(
    left_groups: dict[str, list[dict[str, Any]]],
    right_groups: dict[str, list[dict[str, Any]]],
) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
    pairs: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for key in sorted(set(left_groups) & set(right_groups)):
        for left in left_groups[key]:
            for right in right_groups[key]:
                pairs.append((left, right, key))
    return pairs
