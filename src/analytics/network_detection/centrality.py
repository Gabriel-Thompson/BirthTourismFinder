from __future__ import annotations


def degree_centrality(component_members: list[str], adjacency: dict[str, set[str]]) -> dict[str, float]:
    if not component_members:
        return {}
    denominator = max(len(component_members) - 1, 1)
    centrality: dict[str, float] = {}
    component_set = set(component_members)
    for entity_id in component_members:
        degree = len([neighbor for neighbor in adjacency.get(entity_id, set()) if neighbor in component_set])
        centrality[entity_id] = round(degree / denominator, 4)
    return centrality
