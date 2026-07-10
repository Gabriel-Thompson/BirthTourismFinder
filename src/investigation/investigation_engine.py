from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.connectors.source_metadata import merge_source_values
from src.investigation.prioritization import (
    CONFIG_PATH,
    assign_priority_score,
    classify_entity_lead_type,
    compute_confidence_score,
    confidence_label,
    consolidate_duplicate_leads,
    deterministic_lead_id,
    evidence_completeness,
    load_investigation_engine_config,
    priority_label,
)
from src.investigation.recommendations import review_steps
from src.investigation.report_builder import (
    build_investigation_summary,
    build_lead_evidence_index,
    export_lead_packages,
    preserve_analyst_state,
)

DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_INVESTIGATION_LEADS_PATH = DEFAULT_PROCESSED_DIR / "investigation_leads.csv"
DEFAULT_NETWORK_CLUSTERS_PATH = DEFAULT_PROCESSED_DIR / "network_clusters.csv"
DEFAULT_ENTITY_RISK_PATH = DEFAULT_PROCESSED_DIR / "entity_risk.csv"
DEFAULT_FRAUD_MARKERS_PATH = DEFAULT_PROCESSED_DIR / "fraud_markers.csv"
DEFAULT_CANONICAL_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_CANONICAL_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_ENTITY_ALIASES_PATH = DEFAULT_PROCESSED_DIR / "entity_aliases.csv"
DEFAULT_EVIDENCE_PACKETS_PATH = DEFAULT_PROCESSED_DIR / "evidence_packets.csv"
DEFAULT_ENTITY_TIMELINES_PATH = DEFAULT_PROCESSED_DIR / "entity_timelines.csv"
DEFAULT_NETWORK_MEMBERS_PATH = DEFAULT_PROCESSED_DIR / "network_members.csv"
DEFAULT_CROSS_SOURCE_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "cross_source_matches.csv"
DEFAULT_PRIORITIZED_LEADS_PATH = DEFAULT_PROCESSED_DIR / "prioritized_leads.csv"
DEFAULT_INVESTIGATION_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "investigation_summary.csv"
DEFAULT_LEAD_EVIDENCE_INDEX_PATH = DEFAULT_PROCESSED_DIR / "lead_evidence_index.csv"
DEFAULT_REVIEW_RECOMMENDATIONS_PATH = DEFAULT_PROCESSED_DIR / "review_recommendations.csv"
DEFAULT_ANALYST_STATE_PATH = DEFAULT_PROCESSED_DIR / "analyst_lead_state.csv"
DEFAULT_ANALYST_HISTORY_PATH = DEFAULT_PROCESSED_DIR / "analyst_history.csv"
DEFAULT_LEAD_PACKAGE_ROOT = Path("exports/leads")


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def _token_count(value: object) -> int:
    return len([token for token in str(value or "").split("|") if token.strip()])


def _marker_statistics_lookup(fraud_markers_df: pd.DataFrame) -> dict[str, dict[str, object]]:
    if fraud_markers_df.empty or "entity_id" not in fraud_markers_df.columns:
        return {}
    def _numeric_series(group: pd.DataFrame, column: str) -> pd.Series:
        if column not in group.columns:
            return pd.Series([0] * len(group), index=group.index, dtype=float)
        return pd.to_numeric(group[column], errors="coerce").fillna(0)

    level_order = {
        "EXTREME_OUTLIER": 4,
        "IMMEDIATE_REVIEW": 3,
        "ELEVATED_REVIEW": 2,
        "ROUTINE_REVIEW": 1,
        "COMMON": 0,
        "INSUFFICIENT_BASELINE": -1,
        "": -1,
    }
    lookup: dict[str, dict[str, object]] = {}
    for entity_id, group in fraud_markers_df.fillna("").groupby("entity_id"):
        rarity_scores = _numeric_series(group, "rarity_score")
        observed_values = _numeric_series(group, "observed_value")
        expected_values = _numeric_series(group, "expected_value")
        rarity_level_series = (
            group["rarity_level"].astype(str)
            if "rarity_level" in group.columns
            else pd.Series([""] * len(group), index=group.index, dtype=str)
        )
        rare_group = group[rarity_level_series.isin(["ROUTINE_REVIEW", "ELEVATED_REVIEW", "IMMEDIATE_REVIEW", "EXTREME_OUTLIER"])]
        adjustment_parts = [
            f"{row['marker_name']}:{int(pd.to_numeric(row.get('contextual_adjustment', 0), errors='coerce')):+d}"
            for _, row in group.iterrows()
            if str(row.get("contextual_adjustment", "")).strip() not in {"", "0", "0.0"}
        ]
        review_reasons = [str(value).strip() for value in group.get("statistical_explanation", pd.Series(dtype=str)).astype(str).tolist() if str(value).strip()]
        rarity_levels = group.get("rarity_level", pd.Series(dtype=str)).astype(str).tolist() if "rarity_level" in group.columns else []
        highest_rarity_level = ""
        if rarity_levels:
            highest_rarity_level = max(rarity_levels, key=lambda value: level_order.get(str(value), -1))
        lookup[str(entity_id)] = {
            "rarity_score": round(float(rarity_scores.max()), 2) if not rarity_scores.empty else 0.0,
            "highest_rarity_level": highest_rarity_level,
            "rare_marker_count": int(len(rare_group)),
            "expected_value": round(float(expected_values.max()), 4) if not expected_values.empty else 0.0,
            "observed_value": round(float(observed_values.max()), 4) if not observed_values.empty else 0.0,
            "comparison_group": merge_source_values(*group.get("comparison_group", pd.Series(dtype=str)).astype(str).tolist()),
            "contextual_adjustment_summary": merge_source_values(*adjustment_parts),
            "statistical_review_reason": " | ".join(review_reasons[:3]),
        }
    return lookup


def _entity_lead_candidates(
    investigation_leads_df: pd.DataFrame,
    entity_risk_df: pd.DataFrame,
    network_members_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    network_lookup = network_members_df.groupby("entity_id") if not network_members_df.empty and "entity_id" in network_members_df.columns else None
    risk_lookup = entity_risk_df.set_index("entity_id", drop=False) if not entity_risk_df.empty and "entity_id" in entity_risk_df.columns else pd.DataFrame()
    marker_stats_lookup = _marker_statistics_lookup(fraud_markers_df)
    enabled_types = set(config.get("enabled_lead_types", []))
    for _, row in investigation_leads_df.fillna("").iterrows():
        entity_id = str(row.get("entity_id", "")).strip()
        if not entity_id:
            continue
        network_rows = network_lookup.get_group(entity_id) if network_lookup is not None and entity_id in network_lookup.groups else pd.DataFrame()
        risk_row = None
        if not risk_lookup.empty and entity_id in risk_lookup.index:
            risk_row = risk_lookup.loc[entity_id]
            if isinstance(risk_row, pd.DataFrame):
                risk_row = risk_row.iloc[0]
        network_id = str(network_rows.iloc[0]["network_id"]) if not network_rows.empty else ""
        network_member_count = int(network_rows["network_id"].astype(str).count()) if not network_rows.empty else 0
        bridge_entity_count = int((network_rows.get("bridge_flag", pd.Series(dtype=str)).astype(str) == "Yes").sum()) if not network_rows.empty else 0
        lead_type = classify_entity_lead_type(
            primary_entity_type=str(risk_row.get("entity_type", row.get("primary_entity_type", "")) if risk_row is not None else ""),
            fraud_markers=str(row.get("Fraud Markers", "")),
            cross_source_match_count=1 if str(row.get("Cross-Source Correlation", "")).upper() == "YES" else 0,
            timeline_event_count=0,
            enabled_lead_types=list(enabled_types),
        )
        source_types = str(row.get("source_type", ""))
        contains_real_data = any(token.strip() for token in source_types.split("|") if token.strip() and token.strip() != "synthetic")
        contains_synthetic_data = "synthetic" in {token.strip() for token in source_types.split("|") if token.strip()}
        marker_stats = marker_stats_lookup.get(entity_id, {})
        candidate = {
            "lead_id": deterministic_lead_id(lead_type, entity_id, network_id),
            "lead_type": lead_type,
            "title": str(row.get("Lead Title", f"{row.get('Primary Entity', entity_id)} lead")),
            "primary_entity_id": entity_id,
            "primary_entity_type": str(risk_row.get("entity_type", "")) if risk_row is not None else "",
            "network_id": network_id,
            "risk_score": float(row.get("Risk Score", 0) or 0),
            "fraud_marker_count": int(row.get("Fraud Marker Count", 0) or 0),
            "independent_source_count": int(row.get("Supporting Source Count", 0) or _token_count(row.get("source_name", ""))),
            "relationship_count": int(row.get("Relationship Count", 0) or 0),
            "cross_source_match_count": 1 if str(row.get("Cross-Source Correlation", "")).upper() == "YES" else 0,
            "network_member_count": network_member_count,
            "first_seen": "",
            "last_seen": "",
            "status": "NEW",
            "recommended_review": str(row.get("Recommended Review", "")),
            "explanation": str(row.get("Risk Explanation", row.get("Lead Summary", ""))),
            "source_names": str(row.get("source_name", "")),
            "source_types": source_types,
            "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
            "fraud_markers": str(row.get("Fraud Markers", "")),
            "entity_resolution_confidence": float(row.get("Entity Resolution Confidence", 0) or 0),
            "marker_confidence": float(risk_row.get("average_marker_confidence", 0) if risk_row is not None else 0),
            "relationship_confidence": 1.0,
            "alias_count": 0,
            "bridge_entity_count": bridge_entity_count,
            "contains_real_data": contains_real_data,
            "contains_synthetic_data": contains_synthetic_data,
            "timeline_event_count": 0,
            "relationship_density": min(int(row.get("Relationship Count", 0) or 0) / max(network_member_count or 1, 1), 1.0),
            "temporal_concentration": 0.0,
            "rarity_score": float(marker_stats.get("rarity_score", 0) or 0),
            "highest_rarity_level": str(marker_stats.get("highest_rarity_level", "")),
            "rare_marker_count": int(marker_stats.get("rare_marker_count", 0) or 0),
            "expected_value": float(marker_stats.get("expected_value", 0) or 0),
            "observed_value": float(marker_stats.get("observed_value", 0) or 0),
            "comparison_group": str(marker_stats.get("comparison_group", "")),
            "contextual_adjustment_summary": str(marker_stats.get("contextual_adjustment_summary", "")),
            "statistical_review_reason": str(marker_stats.get("statistical_review_reason", "")),
        }
        candidates.append(candidate)
    return candidates


def _network_lead_candidates(
    network_clusters_df: pd.DataFrame,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    if network_clusters_df.empty:
        return candidates
    if "NETWORK" not in set(config.get("enabled_lead_types", [])):
        return candidates
    for _, row in network_clusters_df.fillna("").iterrows():
        network_id = str(row.get("network_id", "")).strip()
        if not network_id:
            continue
        source_types = str(row.get("source_type", ""))
        contains_real_data = any(token.strip() for token in source_types.split("|") if token.strip() and token.strip() != "synthetic")
        contains_synthetic_data = "synthetic" in {token.strip() for token in source_types.split("|") if token.strip()}
        candidates.append(
            {
                "lead_id": deterministic_lead_id("NETWORK", network_id, network_id),
                "lead_type": "NETWORK",
                "title": f"Network lead {network_id}",
                "primary_entity_id": network_id,
                "primary_entity_type": "network",
                "network_id": network_id,
                "risk_score": float(row.get("network_risk_score", 0) or 0),
                "fraud_marker_count": int(row.get("fraud_marker_count", 0) or 0),
                "independent_source_count": int(row.get("independent_source_count", 0) or 0),
                "relationship_count": int(row.get("relationship_count", 0) or 0),
                "cross_source_match_count": int(row.get("cross_source_matches", 0) or 0),
                "network_member_count": int(row.get("network_size", 0) or 0),
                "first_seen": "",
                "last_seen": str(row.get("latest_activity_date", "")),
                "status": "NEW",
                "recommended_review": "",
                "explanation": str(row.get("explanation", "")),
                "source_names": str(row.get("source_name", "")),
                "source_types": source_types,
                "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
                "fraud_markers": str(row.get("top_markers", "")),
                "entity_resolution_confidence": float(row.get("entity_resolution_confidence", 0) or 0),
                "marker_confidence": float(row.get("network_confidence_score", 0) or 0),
                "relationship_confidence": 1.0,
                "alias_count": 0,
                "bridge_entity_count": int(row.get("bridge_entity_count", 0) or 0),
                "contains_real_data": contains_real_data,
                "contains_synthetic_data": contains_synthetic_data,
                "timeline_event_count": int(row.get("timeline_event_count", 0) or 0),
                "relationship_density": float(row.get("relationship_density", 0) or 0),
                "temporal_concentration": min(float(row.get("fast_growth_score", 0) or 0), 1.0),
                "rarity_score": 0.0,
                "highest_rarity_level": "",
                "rare_marker_count": 0,
                "expected_value": 0.0,
                "observed_value": 0.0,
                "comparison_group": "",
                "contextual_adjustment_summary": "",
                "statistical_review_reason": "",
            }
        )
    return candidates


def _cross_source_lead_candidates(
    cross_source_matches_df: pd.DataFrame,
    entity_risk_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    if cross_source_matches_df.empty:
        return []
    thresholds = config.get("cross_source_lead_thresholds", {})
    minimum_matches = int(thresholds.get("minimum_matches", 1))
    minimum_real_matches = int(thresholds.get("minimum_real_matches", 1))
    risk_lookup = entity_risk_df.set_index("entity_id", drop=False) if not entity_risk_df.empty and "entity_id" in entity_risk_df.columns else pd.DataFrame()
    marker_stats_lookup = _marker_statistics_lookup(fraud_markers_df)
    candidates: list[dict[str, Any]] = []
    filtered = cross_source_matches_df[
        (cross_source_matches_df["decision"].astype(str) == "AUTO_MATCH")
        & (pd.to_numeric(cross_source_matches_df.get("independent_real_source_count", 0), errors="coerce").fillna(0) >= 2)
    ].copy()
    if filtered.empty:
        return candidates
    for canonical_entity_id, group in filtered.groupby("canonical_entity_id"):
        if len(group) < minimum_matches:
            continue
        real_match_count = int((pd.to_numeric(group.get("independent_real_source_count", 0), errors="coerce").fillna(0) >= 2).sum())
        if real_match_count < minimum_real_matches:
            continue
        entity_type = str(group["entity_type"].iloc[0])
        risk_row = None
        if not risk_lookup.empty and canonical_entity_id in risk_lookup.index:
            risk_row = risk_lookup.loc[canonical_entity_id]
            if isinstance(risk_row, pd.DataFrame):
                risk_row = risk_row.iloc[0]
        source_names = "|".join(sorted(set(group["left_source_name"].astype(str).tolist() + group["right_source_name"].astype(str).tolist())))
        source_types = "|".join(sorted(set(group["left_source_type"].astype(str).tolist() + group["right_source_type"].astype(str).tolist())))
        source_count = len({token for token in source_names.split("|") if token})
        evidence_text = " | ".join(group["evidence"].astype(str).head(3).tolist())
        marker_stats = marker_stats_lookup.get(str(canonical_entity_id), {})
        candidates.append(
            {
                "lead_id": deterministic_lead_id("CROSS_SOURCE_CLUSTER", str(canonical_entity_id)),
                "lead_type": "CROSS_SOURCE_CLUSTER",
                "title": f"Cross-source cluster for {canonical_entity_id}",
                "primary_entity_id": str(canonical_entity_id),
                "primary_entity_type": entity_type,
                "network_id": "",
                "risk_score": float(risk_row.get("risk_score", 0) if risk_row is not None else 0),
                "fraud_marker_count": int(risk_row.get("marker_count", 0) if risk_row is not None else 0),
                "independent_source_count": source_count,
                "relationship_count": int(risk_row.get("relationship_count", 0) if risk_row is not None else len(group)),
                "cross_source_match_count": int(len(group)),
                "network_member_count": 0,
                "first_seen": "",
                "last_seen": "",
                "status": "NEW",
                "recommended_review": (
                    f"Review side-by-side evidence across independent public sources: {source_names}. "
                    "Confirm why the sources are independent and verify the shared entity, address, or identifier manually."
                ),
                "explanation": evidence_text,
                "source_names": source_names,
                "source_types": source_types,
                "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
                "fraud_markers": merge_source_values(*group.get("match_method", pd.Series(dtype=str)).astype(str).tolist()),
                "entity_resolution_confidence": float(risk_row.get("average_marker_confidence", 0) if risk_row is not None else 0.9),
                "marker_confidence": float(pd.to_numeric(group.get("confidence", 0), errors="coerce").fillna(0).mean()),
                "relationship_confidence": float(pd.to_numeric(group.get("confidence", 0), errors="coerce").fillna(0).mean()),
                "alias_count": 0,
                "bridge_entity_count": 0,
                "contains_real_data": True,
                "contains_synthetic_data": bool(group["contains_synthetic_data"].astype(str).str.lower().eq("true").any()),
                "timeline_event_count": 0,
                "relationship_density": 0.0,
                "temporal_concentration": 0.0,
                "rarity_score": float(marker_stats.get("rarity_score", 0) or 0),
                "highest_rarity_level": str(marker_stats.get("highest_rarity_level", "")),
                "rare_marker_count": int(marker_stats.get("rare_marker_count", 0) or 0),
                "expected_value": float(marker_stats.get("expected_value", 0) or 0),
                "observed_value": float(marker_stats.get("observed_value", 0) or 0),
                "comparison_group": str(marker_stats.get("comparison_group", "")),
                "contextual_adjustment_summary": str(marker_stats.get("contextual_adjustment_summary", "")),
                "statistical_review_reason": str(marker_stats.get("statistical_review_reason", "")),
            }
        )
    return candidates


def run_investigation_engine(
    investigation_leads_path: Path | str = DEFAULT_INVESTIGATION_LEADS_PATH,
    network_clusters_path: Path | str = DEFAULT_NETWORK_CLUSTERS_PATH,
    entity_risk_path: Path | str = DEFAULT_ENTITY_RISK_PATH,
    fraud_markers_path: Path | str = DEFAULT_FRAUD_MARKERS_PATH,
    canonical_entities_path: Path | str = DEFAULT_CANONICAL_ENTITIES_PATH,
    canonical_relationships_path: Path | str = DEFAULT_CANONICAL_RELATIONSHIPS_PATH,
    aliases_path: Path | str = DEFAULT_ENTITY_ALIASES_PATH,
    evidence_packets_path: Path | str = DEFAULT_EVIDENCE_PACKETS_PATH,
    entity_timelines_path: Path | str = DEFAULT_ENTITY_TIMELINES_PATH,
    network_members_path: Path | str = DEFAULT_NETWORK_MEMBERS_PATH,
    cross_source_matches_path: Path | str = DEFAULT_CROSS_SOURCE_MATCHES_PATH,
    prioritized_leads_path: Path | str = DEFAULT_PRIORITIZED_LEADS_PATH,
    investigation_summary_path: Path | str = DEFAULT_INVESTIGATION_SUMMARY_PATH,
    lead_evidence_index_path: Path | str = DEFAULT_LEAD_EVIDENCE_INDEX_PATH,
    review_recommendations_path: Path | str = DEFAULT_REVIEW_RECOMMENDATIONS_PATH,
    analyst_state_path: Path | str = DEFAULT_ANALYST_STATE_PATH,
    analyst_history_path: Path | str = DEFAULT_ANALYST_HISTORY_PATH,
    package_root: Path | str = DEFAULT_LEAD_PACKAGE_ROOT,
    config_path: Path | str = CONFIG_PATH,
) -> dict[str, Any]:
    start_time = time.time()
    config = load_investigation_engine_config(config_path)
    paths = {
        "investigation_leads": Path(investigation_leads_path),
        "network_clusters": Path(network_clusters_path),
        "entity_risk": Path(entity_risk_path),
        "fraud_markers": Path(fraud_markers_path),
        "canonical_entities": Path(canonical_entities_path),
        "canonical_relationships": Path(canonical_relationships_path),
        "aliases": Path(aliases_path),
        "evidence_packets": Path(evidence_packets_path),
        "entity_timelines": Path(entity_timelines_path),
        "network_members": Path(network_members_path),
        "cross_source_matches": Path(cross_source_matches_path),
    }

    print("Investigation Engine: started")
    for label, path in paths.items():
        print(f"Investigation Engine: input {label} {path}")

    investigation_leads_df = _load_frame(paths["investigation_leads"])
    network_clusters_df = _load_frame(paths["network_clusters"])
    entity_risk_df = _load_frame(paths["entity_risk"])
    fraud_markers_df = _load_frame(paths["fraud_markers"])
    canonical_entities_df = _load_frame(paths["canonical_entities"])
    canonical_relationships_df = _load_frame(paths["canonical_relationships"])
    aliases_df = _load_frame(paths["aliases"])
    evidence_packets_df = _load_frame(paths["evidence_packets"])
    entity_timelines_df = _load_frame(paths["entity_timelines"])
    network_members_df = _load_frame(paths["network_members"])
    cross_source_matches_df = _load_frame(paths["cross_source_matches"])

    print(f"Investigation Engine: investigation leads loaded {len(investigation_leads_df)}")
    print(f"Investigation Engine: network clusters loaded {len(network_clusters_df)}")

    candidates = _entity_lead_candidates(investigation_leads_df, entity_risk_df, network_members_df, fraud_markers_df, config)
    candidates.extend(_network_lead_candidates(network_clusters_df, config))
    candidates.extend(_cross_source_lead_candidates(cross_source_matches_df, entity_risk_df, fraud_markers_df, config))
    print(f"Investigation Engine: candidate leads created {len(candidates)}")

    alias_counts = aliases_df.groupby("canonical_entity_id").size().to_dict() if not aliases_df.empty else {}
    timeline_counts = entity_timelines_df.groupby("entity_id").size().to_dict() if not entity_timelines_df.empty else {}
    evidence_counts = evidence_packets_df.groupby("entity_id").size().to_dict() if not evidence_packets_df.empty else {}
    network_member_counts = network_members_df.groupby("network_id").size().to_dict() if not network_members_df.empty else {}

    enriched_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        primary_entity_id = str(candidate.get("primary_entity_id", ""))
        network_id = str(candidate.get("network_id", ""))
        candidate["alias_count"] = int(alias_counts.get(primary_entity_id, 0))
        candidate["timeline_event_count"] = int(candidate.get("timeline_event_count", 0) or timeline_counts.get(primary_entity_id, 0))
        supporting_record_count = int(candidate.get("fraud_marker_count", 0) or 0) + int(candidate.get("relationship_count", 0) or 0) + int(evidence_counts.get(primary_entity_id, 0))
        candidate["network_member_count"] = max(int(candidate.get("network_member_count", 0) or 0), int(network_member_counts.get(network_id, 0)))
        completeness_score, missing_fields, evidence_count = evidence_completeness(candidate, config.get("evidence_completeness_weights", {}))
        candidate["evidence_completeness_score"] = completeness_score
        candidate["missing_evidence_fields"] = missing_fields
        candidate["evidence_count"] = evidence_count
        candidate["confidence_score"] = compute_confidence_score(
            independent_source_count=int(candidate.get("independent_source_count", 0) or 0),
            entity_resolution_confidence=float(candidate.get("entity_resolution_confidence", 0) or 0),
            supporting_record_count=supporting_record_count,
            relationship_confidence=float(candidate.get("relationship_confidence", 0) or 0),
            marker_confidence=float(candidate.get("marker_confidence", 0) or 0),
            evidence_completeness_score=completeness_score,
            contains_real_data=bool(candidate.get("contains_real_data", False)),
        )
        candidate["confidence"] = confidence_label(candidate["confidence_score"], config.get("confidence_thresholds", {}))
        candidate["priority_score"] = assign_priority_score(candidate, config)
        candidate["priority"] = priority_label(candidate["priority_score"], config.get("priority_thresholds", {}))
        candidate["recommended_review"] = review_steps(candidate)
        candidate["contains_real_data"] = bool(candidate.get("contains_real_data", False))
        candidate["contains_synthetic_data"] = bool(candidate.get("contains_synthetic_data", False))
        if candidate["contains_synthetic_data"] and not candidate["contains_real_data"]:
            candidate["title"] = f"DEMO {candidate['title']}"
        if candidate["lead_type"] == "CROSS_SOURCE_CLUSTER":
            if completeness_score < float(config.get("minimum_cross_source_evidence_completeness", 60)):
                continue
            if float(candidate.get("entity_resolution_confidence", 0) or 0) < float(config.get("minimum_cross_source_entity_resolution_confidence", 0.75)):
                continue
        enriched_rows.append(candidate)

    leads_df = pd.DataFrame(enriched_rows)
    print(f"Investigation Engine: priorities assigned {len(leads_df)}")
    if not leads_df.empty:
        leads_df = consolidate_duplicate_leads(leads_df)
    print(f"Investigation Engine: duplicate leads consolidated to {len(leads_df)}")

    analyst_state_file = Path(analyst_state_path)
    analyst_history_file = Path(analyst_history_path)
    leads_with_state_df, analyst_state_df, analyst_history_df = preserve_analyst_state(
        leads_df,
        analyst_state_file,
        analyst_history_file,
    )
    review_recommendations_df = leads_with_state_df[[
        "lead_id",
        "lead_type",
        "priority",
        "confidence",
        "recommended_review",
        "evidence_completeness_score",
        "missing_evidence_fields",
        "status",
    ]].copy() if not leads_with_state_df.empty else pd.DataFrame(columns=[
        "lead_id", "lead_type", "priority", "confidence", "recommended_review", "evidence_completeness_score", "missing_evidence_fields", "status"
    ])
    lead_evidence_index_df = build_lead_evidence_index(
        leads_with_state_df,
        fraud_markers_df,
        canonical_relationships_df,
        evidence_packets_df,
        entity_timelines_df,
    )
    print(f"Investigation Engine: evidence indexed {len(lead_evidence_index_df)}")

    summary_df = build_investigation_summary(leads_with_state_df)
    package_result = export_lead_packages(
        leads_with_state_df,
        lead_evidence_index_df,
        canonical_entities_df,
        canonical_relationships_df,
        fraud_markers_df,
        entity_timelines_df,
        Path(package_root),
        list(config.get("package_priorities", ["CRITICAL", "HIGH"])),
        analyst_state_df=analyst_state_df,
        analyst_history_df=analyst_history_df,
    )
    package_count, analyst_state_df, analyst_history_df = package_result
    print(f"Investigation Engine: packages exported {package_count}")

    output_paths = {
        "prioritized_leads": Path(prioritized_leads_path),
        "investigation_summary": Path(investigation_summary_path),
        "lead_evidence_index": Path(lead_evidence_index_path),
        "review_recommendations": Path(review_recommendations_path),
        "analyst_state": analyst_state_file,
        "analyst_history": analyst_history_file,
    }
    for label, path in output_paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
    leads_with_state_df.to_csv(output_paths["prioritized_leads"], index=False)
    summary_df.to_csv(output_paths["investigation_summary"], index=False)
    lead_evidence_index_df.to_csv(output_paths["lead_evidence_index"], index=False)
    review_recommendations_df.to_csv(output_paths["review_recommendations"], index=False)
    analyst_state_df.to_csv(output_paths["analyst_state"], index=False)
    analyst_history_df.to_csv(output_paths["analyst_history"], index=False)

    duration = time.time() - start_time
    print(f"Investigation Engine: runtime {duration:.2f}s")
    print("Investigation Engine: PASS")
    return {
        "total_prioritized_leads": int(len(leads_with_state_df)),
        "package_count": int(package_count),
        "runtime_seconds": round(duration, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the consolidated OpenFraud Investigation Engine v1.0 outputs.")
    parser.add_argument("--config-path", default=str(CONFIG_PATH))
    parser.add_argument("--prioritized-leads-path", default=str(DEFAULT_PRIORITIZED_LEADS_PATH))
    parser.add_argument("--investigation-summary-path", default=str(DEFAULT_INVESTIGATION_SUMMARY_PATH))
    parser.add_argument("--lead-evidence-index-path", default=str(DEFAULT_LEAD_EVIDENCE_INDEX_PATH))
    parser.add_argument("--review-recommendations-path", default=str(DEFAULT_REVIEW_RECOMMENDATIONS_PATH))
    parser.add_argument("--analyst-state-path", default=str(DEFAULT_ANALYST_STATE_PATH))
    parser.add_argument("--analyst-history-path", default=str(DEFAULT_ANALYST_HISTORY_PATH))
    parser.add_argument("--package-root", default=str(DEFAULT_LEAD_PACKAGE_ROOT))
    args = parser.parse_args()
    run_investigation_engine(
        prioritized_leads_path=args.prioritized_leads_path,
        investigation_summary_path=args.investigation_summary_path,
        lead_evidence_index_path=args.lead_evidence_index_path,
        review_recommendations_path=args.review_recommendations_path,
        analyst_state_path=args.analyst_state_path,
        analyst_history_path=args.analyst_history_path,
        package_root=args.package_root,
        config_path=args.config_path,
    )


if __name__ == "__main__":
    main()
