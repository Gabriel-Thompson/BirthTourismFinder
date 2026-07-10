from __future__ import annotations


def find_bridge_entities(component_members: list[str], adjacency: dict[str, set[str]]) -> dict[str, dict[str, int | bool]]:
    if not component_members:
        return {}

    disc: dict[str, int] = {}
    low: dict[str, int] = {}
    parent: dict[str, str | None] = {}
    articulation_children: dict[str, int] = {entity_id: 0 for entity_id in component_members}
    articulation_points: set[str] = set()
    time_counter = 0

    def dfs(node: str) -> None:
        nonlocal time_counter
        time_counter += 1
        disc[node] = time_counter
        low[node] = time_counter
        child_count = 0

        for neighbor in sorted(adjacency.get(node, set())):
            if neighbor not in component_members:
                continue
            if neighbor not in disc:
                parent[neighbor] = node
                child_count += 1
                dfs(neighbor)
                low[node] = min(low[node], low[neighbor])
                if parent.get(node) is None and child_count > 1:
                    articulation_points.add(node)
                    articulation_children[node] = child_count
                if parent.get(node) is not None and low[neighbor] >= disc[node]:
                    articulation_points.add(node)
                    articulation_children[node] += 1
            elif neighbor != parent.get(node):
                low[node] = min(low[node], disc[neighbor])

    for entity_id in sorted(component_members):
        if entity_id in disc:
            continue
        parent[entity_id] = None
        dfs(entity_id)

    results: dict[str, dict[str, int | bool]] = {}
    for entity_id in component_members:
        degree = len([neighbor for neighbor in adjacency.get(entity_id, set()) if neighbor in component_members])
        disconnected_groups = articulation_children.get(entity_id, 0) + (1 if entity_id in articulation_points and parent.get(entity_id) is not None else 0)
        bridge_score = max(disconnected_groups, 0) * 10 + degree
        results[entity_id] = {
            "is_bridge": entity_id in articulation_points,
            "bridge_score": bridge_score,
            "disconnected_groups": max(disconnected_groups, 0),
        }
    return results
