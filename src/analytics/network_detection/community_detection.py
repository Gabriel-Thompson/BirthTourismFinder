from __future__ import annotations

from collections import Counter


def detect_communities(
    component_members: list[str],
    adjacency: dict[str, set[str]],
    max_iterations: int = 20,
) -> dict[str, str]:
    if not component_members:
        return {}
    labels = {entity_id: entity_id for entity_id in component_members}
    for _ in range(max_iterations):
        changed = False
        for entity_id in sorted(component_members):
            neighbors = [neighbor for neighbor in adjacency.get(entity_id, set()) if neighbor in labels]
            if not neighbors:
                continue
            counts = Counter(labels[neighbor] for neighbor in neighbors)
            best_label = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
            if labels[entity_id] != best_label:
                labels[entity_id] = best_label
                changed = True
        if not changed:
            break

    normalized: dict[str, str] = {}
    label_map: dict[str, str] = {}
    community_number = 1
    for entity_id in sorted(component_members):
        label = labels[entity_id]
        if label not in label_map:
            label_map[label] = f"community:{community_number:04d}"
            community_number += 1
        normalized[entity_id] = label_map[label]
    return normalized
