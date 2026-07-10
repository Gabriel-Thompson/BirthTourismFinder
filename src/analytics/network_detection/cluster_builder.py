from __future__ import annotations

from collections import defaultdict

import pandas as pd


def build_adjacency(entities_df: pd.DataFrame, relationships_df: pd.DataFrame) -> dict[str, set[str]]:
    adjacency: dict[str, set[str]] = defaultdict(set)
    for entity_id in entities_df.get("entity_id", pd.Series(dtype=str)).astype(str):
        if entity_id:
            adjacency[entity_id]
    for _, row in relationships_df.fillna("").iterrows():
        source_id = str(row.get("source_entity_id", "")).strip()
        target_id = str(row.get("target_entity_id", "")).strip()
        if not source_id or not target_id:
            continue
        adjacency[source_id].add(target_id)
        adjacency[target_id].add(source_id)
    return {entity_id: neighbors for entity_id, neighbors in adjacency.items()}


def connected_components(adjacency: dict[str, set[str]]) -> list[list[str]]:
    visited: set[str] = set()
    components: list[list[str]] = []

    for start in sorted(adjacency):
        if start in visited:
            continue
        stack = [start]
        component: list[str] = []
        visited.add(start)
        while stack:
            current = stack.pop()
            component.append(current)
            for neighbor in sorted(adjacency.get(current, set())):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                stack.append(neighbor)
        components.append(sorted(component))
    return components
