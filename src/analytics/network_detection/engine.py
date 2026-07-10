from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.analytics.network_detection.bridge_analysis import find_bridge_entities
from src.analytics.network_detection.centrality import degree_centrality
from src.analytics.network_detection.cluster_builder import build_adjacency, connected_components
from src.analytics.network_detection.community_detection import detect_communities
from src.connectors.source_metadata import is_real_source_type, merge_source_values

DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_FRAUD_MARKERS_PATH = DEFAULT_PROCESSED_DIR / "fraud_markers.csv"
DEFAULT_TIMELINES_PATH = DEFAULT_PROCESSED_DIR / "entity_timelines.csv"
DEFAULT_CLUSTER_OUTPUT_PATH = DEFAULT_PROCESSED_DIR / "network_clusters.csv"
DEFAULT_SUMMARY_OUTPUT_PATH = DEFAULT_PROCESSED_DIR / "network_summary.csv"
DEFAULT_MEMBERS_OUTPUT_PATH = DEFAULT_PROCESSED_DIR / "network_members.csv"
DEFAULT_EDGES_OUTPUT_PATH = DEFAULT_PROCESSED_DIR / "network_edges.csv"
CONFIG_PATH = Path("config/network_detection.json")

DEFAULT_CONFIG = {
    "min_network_size": 2,
    "max_score": 100,
    "risk_weights": {
        "fraud_marker_count": 6,
        "relationship_density": 30,
        "independent_source_count": 5,
        "cross_source_matches": 4,
        "entity_diversity": 3,
        "bridge_entities": 5,
    },
    "priority_thresholds": {"critical": 85, "high": 65, "medium": 40},
    "confidence_thresholds": {"very_high": 0.9, "high": 0.75, "medium": 0.55, "low": 0.35},
}


def load_network_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return DEFAULT_CONFIG
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return DEFAULT_CONFIG
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in data.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def _confidence_label(score: float, thresholds: dict[str, float]) -> str:
    if score >= float(thresholds.get("very_high", 0.9)):
        return "Very High"
    if score >= float(thresholds.get("high", 0.75)):
        return "High"
    if score >= float(thresholds.get("medium", 0.55)):
        return "Medium"
    if score >= float(thresholds.get("low", 0.35)):
        return "Low"
    return "Unknown"


def _priority(score: float, thresholds: dict[str, float]) -> str:
    if score >= float(thresholds.get("critical", 85)):
        return "Critical"
    if score >= float(thresholds.get("high", 65)):
        return "High"
    if score >= float(thresholds.get("medium", 40)):
        return "Medium"
    return "Low"


def _latest_activity_for_members(timelines_df: pd.DataFrame, member_ids: set[str]) -> tuple[str, int]:
    if timelines_df.empty or "entity_id" not in timelines_df.columns:
        return "", 0
    subset = timelines_df[timelines_df["entity_id"].astype(str).isin(member_ids)].copy()
    if subset.empty:
        return "", 0
    dates = subset["Date"].fillna("").astype(str)
    latest = max([value for value in dates.tolist() if value], default="")
    return latest, int(len(subset))


def _network_reason(
    business_count: int,
    address_count: int,
    owner_count: int,
    fraud_marker_count: int,
    independent_source_count: int,
    top_markers: list[str],
) -> str:
    reasons = [
        f"{business_count} businesses",
        f"{address_count} addresses",
        f"{owner_count} owners",
        f"{fraud_marker_count} fraud markers",
        f"{independent_source_count} independent sources",
    ]
    if top_markers:
        reasons.append(f"Top markers: {', '.join(top_markers[:3])}")
    return " | ".join(reasons)


def build_network_intelligence(
    entities_path: Path | str = DEFAULT_ENTITIES_PATH,
    relationships_path: Path | str = DEFAULT_RELATIONSHIPS_PATH,
    fraud_markers_path: Path | str = DEFAULT_FRAUD_MARKERS_PATH,
    timelines_path: Path | str = DEFAULT_TIMELINES_PATH,
    cluster_output_path: Path | str = DEFAULT_CLUSTER_OUTPUT_PATH,
    summary_output_path: Path | str = DEFAULT_SUMMARY_OUTPUT_PATH,
    members_output_path: Path | str = DEFAULT_MEMBERS_OUTPUT_PATH,
    edges_output_path: Path | str = DEFAULT_EDGES_OUTPUT_PATH,
    config_path: Path | str = CONFIG_PATH,
) -> dict[str, int | float | str]:
    start_time = time.time()
    entities_path = Path(entities_path)
    relationships_path = Path(relationships_path)
    fraud_markers_path = Path(fraud_markers_path)
    timelines_path = Path(timelines_path)
    cluster_output_path = Path(cluster_output_path)
    summary_output_path = Path(summary_output_path)
    members_output_path = Path(members_output_path)
    edges_output_path = Path(edges_output_path)
    config = load_network_config(config_path)

    print("Network Detection: started")
    print(f"Network Detection: entities input {entities_path}")
    print(f"Network Detection: relationships input {relationships_path}")
    print(f"Network Detection: fraud markers input {fraud_markers_path}")
    print(f"Network Detection: timelines input {timelines_path}")
    print(f"Network Detection: config {Path(config_path)}")

    entities_df = pd.read_csv(entities_path) if entities_path.exists() and entities_path.stat().st_size > 0 else pd.DataFrame()
    relationships_df = pd.read_csv(relationships_path) if relationships_path.exists() and relationships_path.stat().st_size > 0 else pd.DataFrame()
    fraud_markers_df = pd.read_csv(fraud_markers_path) if fraud_markers_path.exists() and fraud_markers_path.stat().st_size > 0 else pd.DataFrame()
    timelines_df = pd.read_csv(timelines_path) if timelines_path.exists() and timelines_path.stat().st_size > 0 else pd.DataFrame()

    print(f"Network Detection: entities loaded {len(entities_df)}")
    print(f"Network Detection: relationships loaded {len(relationships_df)}")
    print(f"Network Detection: fraud markers loaded {len(fraud_markers_df)}")

    if entities_df.empty:
        for path, columns in [
            (cluster_output_path, ["network_id", "network_size"]),
            (summary_output_path, ["network_count"]),
            (members_output_path, ["network_id", "entity_id"]),
            (edges_output_path, ["network_id", "relationship_id"]),
        ]:
            path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(columns=columns).to_csv(path, index=False)
        return {"network_count": 0, "largest_network_size": 0, "highest_risk_network_id": "", "community_count": 0, "bridge_entity_count": 0}

    adjacency = build_adjacency(entities_df, relationships_df)
    components = connected_components(adjacency)
    entity_lookup = entities_df.set_index("entity_id", drop=False)
    markers_by_entity = fraud_markers_df.groupby("entity_id") if not fraud_markers_df.empty and "entity_id" in fraud_markers_df.columns else None
    min_network_size = int(config.get("min_network_size", 2))

    network_cluster_rows: list[dict[str, Any]] = []
    network_member_rows: list[dict[str, Any]] = []
    network_edge_rows: list[dict[str, Any]] = []
    all_community_ids: set[str] = set()
    bridge_entity_count = 0

    for component_index, component_members in enumerate(components, start=1):
        if len(component_members) < min_network_size:
            continue
        network_id = f"network:{component_index:04d}"
        component_set = set(component_members)
        component_entities = entities_df[entities_df["entity_id"].astype(str).isin(component_set)].copy()
        component_relationships = relationships_df[
            relationships_df["source_entity_id"].astype(str).isin(component_set)
            & relationships_df["target_entity_id"].astype(str).isin(component_set)
        ].copy()
        communities = detect_communities(component_members, adjacency)
        bridge_info = find_bridge_entities(component_members, adjacency)
        centrality = degree_centrality(component_members, adjacency)

        component_markers = fraud_markers_df[
            fraud_markers_df["entity_id"].astype(str).isin(component_set)
        ].copy() if not fraud_markers_df.empty and "entity_id" in fraud_markers_df.columns else pd.DataFrame()
        all_sources = merge_source_values(
            *component_entities.get("source_name", pd.Series(dtype=str)).astype(str).tolist(),
            *component_relationships.get("source_name", pd.Series(dtype=str)).astype(str).tolist(),
            *component_markers.get("sources", pd.Series(dtype=str)).astype(str).tolist(),
        )
        all_source_types = merge_source_values(
            *component_entities.get("source_type", pd.Series(dtype=str)).astype(str).tolist(),
            *component_relationships.get("source_type", pd.Series(dtype=str)).astype(str).tolist(),
            *component_markers.get("source_types", pd.Series(dtype=str)).astype(str).tolist(),
        )
        independent_source_count = len([token for token in all_sources.split("|") if token])
        cross_source_matches = int(
            component_entities.get("source_name", pd.Series(dtype=str)).fillna("").astype(str).apply(lambda value: len([token for token in value.split("|") if token]) > 1).sum()
        )
        resolution_confidence = round(
            pd.to_numeric(component_entities.get("resolution_confidence", pd.Series(dtype=float)), errors="coerce").fillna(1.0).mean(),
            4,
        ) if not component_entities.empty else 1.0
        average_marker_confidence = round(
            pd.to_numeric(component_markers.get("confidence_score", pd.Series(dtype=float)), errors="coerce").fillna(0).mean(),
            4,
        ) if not component_markers.empty else 0.0
        relationship_count = int(len(component_relationships))
        entity_count = int(len(component_entities))
        relationship_density = round((2 * relationship_count) / max(entity_count * max(entity_count - 1, 1), 1), 4)
        bridge_entities = [entity_id for entity_id, details in bridge_info.items() if bool(details["is_bridge"])]
        bridge_entity_count += len(bridge_entities)
        entity_type_counts = component_entities["entity_type"].fillna("").astype(str).value_counts().to_dict()
        top_markers = component_markers["marker_name"].value_counts().head(3).index.astype(str).tolist() if not component_markers.empty else []
        latest_activity_date, timeline_event_count = _latest_activity_for_members(timelines_df, component_set)

        weights = config.get("risk_weights", {})
        risk_score = 0.0
        risk_score += min(int(len(component_markers)), 10) * float(weights.get("fraud_marker_count", 6))
        risk_score += min(relationship_density, 1.0) * float(weights.get("relationship_density", 30))
        risk_score += min(independent_source_count, 5) * float(weights.get("independent_source_count", 5))
        risk_score += min(cross_source_matches, 5) * float(weights.get("cross_source_matches", 4))
        risk_score += min(len(entity_type_counts), 8) * float(weights.get("entity_diversity", 3))
        risk_score += min(len(bridge_entities), 5) * float(weights.get("bridge_entities", 5))
        risk_score = round(min(risk_score, float(config.get("max_score", 100))), 2)

        confidence_score = round(
            min(
                (average_marker_confidence * 0.5) + (resolution_confidence * 0.3) + (min(independent_source_count, 5) / 5.0 * 0.2),
                1.0,
            ),
            4,
        )
        confidence_label = _confidence_label(confidence_score, config.get("confidence_thresholds", {}))
        priority = _priority(risk_score, config.get("priority_thresholds", {}))
        reason = _network_reason(
            business_count=int(entity_type_counts.get("business", 0)),
            address_count=int(entity_type_counts.get("address", 0)),
            owner_count=int(entity_type_counts.get("owner", 0)),
            fraud_marker_count=int(len(component_markers)),
            independent_source_count=independent_source_count,
            top_markers=top_markers,
        )

        for _, relationship in component_relationships.fillna("").iterrows():
            source_id = str(relationship.get("source_entity_id", ""))
            target_id = str(relationship.get("target_entity_id", ""))
            network_edge_rows.append(
                {
                    "network_id": network_id,
                    "relationship_id": str(relationship.get("relationship_id", "")),
                    "source_entity_id": source_id,
                    "target_entity_id": target_id,
                    "relationship_type": str(relationship.get("relationship_type", "")),
                    "confidence": relationship.get("confidence", relationship.get("confidence_score", 0)),
                    "source_name": str(relationship.get("source_name", "")),
                    "source_type": str(relationship.get("source_type", "")),
                    "evidence": str(relationship.get("evidence", "")),
                    "source_community_id": communities.get(source_id, ""),
                    "target_community_id": communities.get(target_id, ""),
                }
            )

        for entity_id in component_members:
            entity_row = entity_lookup.loc[entity_id]
            if isinstance(entity_row, pd.DataFrame):
                entity_row = entity_row.iloc[0]
            entity_markers = component_markers[component_markers["entity_id"].astype(str) == entity_id].copy() if not component_markers.empty else pd.DataFrame()
            community_id = communities.get(entity_id, "")
            all_community_ids.add(community_id)
            network_member_rows.append(
                {
                    "network_id": network_id,
                    "community_id": community_id,
                    "entity_id": entity_id,
                    "display_name": str(entity_row.get("display_name", entity_id)),
                    "entity_type": str(entity_row.get("entity_type", "")),
                    "degree": len([neighbor for neighbor in adjacency.get(entity_id, set()) if neighbor in component_set]),
                    "degree_centrality": centrality.get(entity_id, 0.0),
                    "bridge_flag": "Yes" if bool(bridge_info.get(entity_id, {}).get("is_bridge")) else "No",
                    "bridge_score": int(bridge_info.get(entity_id, {}).get("bridge_score", 0)),
                    "disconnected_groups": int(bridge_info.get(entity_id, {}).get("disconnected_groups", 0)),
                    "marker_count": int(len(entity_markers)),
                    "marker_names": merge_source_values(*entity_markers.get("marker_name", pd.Series(dtype=str)).astype(str).tolist()),
                    "source_name": str(entity_row.get("source_name", "")),
                    "source_type": str(entity_row.get("source_type", "")),
                    "resolution_confidence": entity_row.get("resolution_confidence", 1.0),
                    "data_scope": "real" if is_real_source_type(str(entity_row.get("source_type", ""))) else "synthetic",
                }
            )

        network_cluster_rows.append(
            {
                "network_id": network_id,
                "network_size": entity_count,
                "business_count": int(entity_type_counts.get("business", 0)),
                "address_count": int(entity_type_counts.get("address", 0)),
                "property_count": int(entity_type_counts.get("property", 0)),
                "owner_count": int(entity_type_counts.get("owner", 0)),
                "registered_agent_count": int(entity_type_counts.get("registered_agent", 0)),
                "officer_count": int(entity_type_counts.get("officer", 0)),
                "relationship_count": relationship_count,
                "average_relationships_per_entity": round(relationship_count / max(entity_count, 1), 4),
                "fraud_marker_count": int(len(component_markers)),
                "independent_source_count": independent_source_count,
                "cross_source_matches": cross_source_matches,
                "entity_resolution_confidence": resolution_confidence,
                "network_risk_score": risk_score,
                "network_confidence_score": confidence_score,
                "network_confidence": confidence_label,
                "network_priority": priority,
                "network_size_label": "Large" if entity_count >= 25 else "Medium" if entity_count >= 10 else "Small",
                "relationship_density": relationship_density,
                "entity_diversity": len(entity_type_counts),
                "bridge_entity_count": len(bridge_entities),
                "community_count": len(set(communities.values())),
                "supporting_sources": all_sources,
                "source_name": all_sources,
                "source_type": all_source_types,
                "data_scope": "real" if is_real_source_type(all_source_types) else "synthetic",
                "explanation": reason,
                "top_markers": merge_source_values(*top_markers),
                "bridge_entities": merge_source_values(*bridge_entities),
                "latest_activity_date": latest_activity_date,
                "timeline_event_count": timeline_event_count,
                "fast_growth_score": round(timeline_event_count / max(entity_count, 1), 4),
            }
        )

    clusters_df = pd.DataFrame(network_cluster_rows)
    members_df = pd.DataFrame(network_member_rows)
    edges_df = pd.DataFrame(network_edge_rows)

    if clusters_df.empty:
        summary_df = pd.DataFrame(
            [
                {
                    "network_count": 0,
                    "largest_network_id": "",
                    "largest_network_size": 0,
                    "highest_risk_network_id": "",
                    "highest_risk_score": 0,
                    "average_network_size": 0,
                    "bridge_entity_count": 0,
                    "community_count": 0,
                    "cross_source_network_count": 0,
                }
            ]
        )
    else:
        largest_network = clusters_df.sort_values(["network_size", "network_risk_score"], ascending=[False, False]).iloc[0]
        highest_risk = clusters_df.sort_values(["network_risk_score", "network_size"], ascending=[False, False]).iloc[0]
        summary_df = pd.DataFrame(
            [
                {
                    "network_count": int(len(clusters_df)),
                    "largest_network_id": str(largest_network["network_id"]),
                    "largest_network_size": int(largest_network["network_size"]),
                    "highest_risk_network_id": str(highest_risk["network_id"]),
                    "highest_risk_score": float(highest_risk["network_risk_score"]),
                    "average_network_size": round(pd.to_numeric(clusters_df["network_size"], errors="coerce").fillna(0).mean(), 2),
                    "bridge_entity_count": int(bridge_entity_count),
                    "community_count": int(len(all_community_ids)),
                    "cross_source_network_count": int((clusters_df["independent_source_count"] > 1).sum()),
                    "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
                }
            ]
        )

    for path, frame in [
        (cluster_output_path, clusters_df),
        (summary_output_path, summary_df),
        (members_output_path, members_df),
        (edges_output_path, edges_df),
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        print(f"Network Detection: wrote {len(frame)} rows to {path}")

    duration = time.time() - start_time
    print(f"Network Detection: completed in {duration:.2f}s")
    print("Network Detection: PASS")

    return {
        "network_count": int(len(clusters_df)),
        "largest_network_size": int(summary_df.iloc[0]["largest_network_size"]) if not summary_df.empty else 0,
        "highest_risk_network_id": str(summary_df.iloc[0]["highest_risk_network_id"]) if not summary_df.empty else "",
        "community_count": int(summary_df.iloc[0]["community_count"]) if not summary_df.empty else 0,
        "bridge_entity_count": int(summary_df.iloc[0]["bridge_entity_count"]) if not summary_df.empty else 0,
        "runtime_seconds": round(duration, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build explainable network intelligence from canonical OpenFraud outputs.")
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH))
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH))
    parser.add_argument("--fraud-markers-path", default=str(DEFAULT_FRAUD_MARKERS_PATH))
    parser.add_argument("--timelines-path", default=str(DEFAULT_TIMELINES_PATH))
    parser.add_argument("--cluster-output-path", default=str(DEFAULT_CLUSTER_OUTPUT_PATH))
    parser.add_argument("--summary-output-path", default=str(DEFAULT_SUMMARY_OUTPUT_PATH))
    parser.add_argument("--members-output-path", default=str(DEFAULT_MEMBERS_OUTPUT_PATH))
    parser.add_argument("--edges-output-path", default=str(DEFAULT_EDGES_OUTPUT_PATH))
    parser.add_argument("--config-path", default=str(CONFIG_PATH))
    args = parser.parse_args()
    build_network_intelligence(
        entities_path=args.entities_path,
        relationships_path=args.relationships_path,
        fraud_markers_path=args.fraud_markers_path,
        timelines_path=args.timelines_path,
        cluster_output_path=args.cluster_output_path,
        summary_output_path=args.summary_output_path,
        members_output_path=args.members_output_path,
        edges_output_path=args.edges_output_path,
        config_path=args.config_path,
    )


if __name__ == "__main__":
    main()
