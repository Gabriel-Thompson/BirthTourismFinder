from pathlib import Path

import pandas as pd

from src.analytics.network_detection.bridge_analysis import find_bridge_entities
from src.analytics.network_detection.cluster_builder import build_adjacency, connected_components
from src.analytics.network_detection.community_detection import detect_communities
from src.analytics.network_detection.engine import build_network_intelligence


def test_connected_components_builds_expected_groups() -> None:
    entities = pd.DataFrame([{"entity_id": "a"}, {"entity_id": "b"}, {"entity_id": "c"}, {"entity_id": "d"}])
    relationships = pd.DataFrame(
        [
            {"source_entity_id": "a", "target_entity_id": "b"},
            {"source_entity_id": "c", "target_entity_id": "d"},
        ]
    )

    adjacency = build_adjacency(entities, relationships)
    components = connected_components(adjacency)

    assert components == [["a", "b"], ["c", "d"]]


def test_bridge_analysis_flags_articulation_points() -> None:
    component_members = ["a", "b", "c"]
    adjacency = {"a": {"b"}, "b": {"a", "c"}, "c": {"b"}}

    bridge_info = find_bridge_entities(component_members, adjacency)

    assert bridge_info["b"]["is_bridge"] is True
    assert bridge_info["a"]["is_bridge"] is False


def test_community_detection_groups_dense_neighbors() -> None:
    component_members = ["a", "b", "c", "d"]
    adjacency = {
        "a": {"b", "c"},
        "b": {"a", "c"},
        "c": {"a", "b", "d"},
        "d": {"c"},
    }

    communities = detect_communities(component_members, adjacency)

    assert set(communities.keys()) == set(component_members)
    assert len(set(communities.values())) >= 1


def test_build_network_intelligence_writes_outputs(tmp_path: Path) -> None:
    processed_dir = tmp_path / "data" / "processed"
    processed_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "display_name": "Acme One",
                "entity_type": "business",
                "source_name": "synthetic|sunbiz_local_file",
                "source_type": "synthetic|connector",
                "resolution_confidence": 0.97,
            },
            {
                "entity_id": "canonical:address:1",
                "display_name": "123 Main St",
                "entity_type": "address",
                "source_name": "synthetic",
                "source_type": "synthetic",
                "resolution_confidence": 1.0,
            },
            {
                "entity_id": "canonical:owner:1",
                "display_name": "Owner One",
                "entity_type": "owner",
                "source_name": "sample_api",
                "source_type": "api",
                "resolution_confidence": 1.0,
            },
        ]
    ).to_csv(processed_dir / "canonical_entities.csv", index=False)
    pd.DataFrame(
        [
            {
                "relationship_id": "r1",
                "source_entity_id": "canonical:business:1",
                "target_entity_id": "canonical:address:1",
                "relationship_type": "LOCATED_AT",
                "confidence": 1.0,
                "source_name": "synthetic",
                "source_type": "synthetic",
                "evidence": "shared address",
            },
            {
                "relationship_id": "r2",
                "source_entity_id": "canonical:address:1",
                "target_entity_id": "canonical:owner:1",
                "relationship_type": "PROPERTY_OWNED_BY",
                "confidence": 1.0,
                "source_name": "sample_api",
                "source_type": "api",
                "evidence": "owner link",
            },
        ]
    ).to_csv(processed_dir / "canonical_relationships.csv", index=False)
    pd.DataFrame(
        [
            {
                "entity_id": "canonical:business:1",
                "marker_name": "Shared Address",
                "confidence_score": 0.82,
                "sources": "synthetic|sunbiz_local_file",
                "source_types": "synthetic|connector",
            }
        ]
    ).to_csv(processed_dir / "fraud_markers.csv", index=False)
    pd.DataFrame(
        [
            {"entity_id": "canonical:business:1", "Date": "2026-07-01", "Event": "Fraud marker: Shared Address"},
            {"entity_id": "canonical:owner:1", "Date": "2026-07-02", "Event": "Property ownership link"},
        ]
    ).to_csv(processed_dir / "entity_timelines.csv", index=False)

    summary = build_network_intelligence(
        entities_path=processed_dir / "canonical_entities.csv",
        relationships_path=processed_dir / "canonical_relationships.csv",
        fraud_markers_path=processed_dir / "fraud_markers.csv",
        timelines_path=processed_dir / "entity_timelines.csv",
        cluster_output_path=processed_dir / "network_clusters.csv",
        summary_output_path=processed_dir / "network_summary.csv",
        members_output_path=processed_dir / "network_members.csv",
        edges_output_path=processed_dir / "network_edges.csv",
    )

    clusters = pd.read_csv(processed_dir / "network_clusters.csv")
    members = pd.read_csv(processed_dir / "network_members.csv")
    edges = pd.read_csv(processed_dir / "network_edges.csv")
    network_summary = pd.read_csv(processed_dir / "network_summary.csv")

    assert summary["network_count"] == 1
    assert not clusters.empty
    assert not members.empty
    assert not edges.empty
    assert not network_summary.empty
    assert "network_risk_score" in clusters.columns
    assert "bridge_flag" in members.columns
    assert int(network_summary.iloc[0]["cross_source_network_count"]) == 1
