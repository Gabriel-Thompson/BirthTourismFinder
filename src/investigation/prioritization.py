from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.connectors.source_metadata import is_real_source_type, merge_source_values

CONFIG_PATH = Path("config/investigation_engine.json")

DEFAULT_CONFIG: dict[str, Any] = {
    "priority_weights": {
        "risk_score": 0.25,
        "rarity_score": 0.08,
        "confidence_score": 0.15,
        "independent_marker_count": 0.08,
        "independent_source_count": 0.08,
        "cross_source_support": 0.08,
        "relationship_density": 0.07,
        "network_size": 0.07,
        "bridge_entity_presence": 0.06,
        "entity_resolution_confidence": 0.06,
        "evidence_completeness": 0.05,
        "temporal_concentration": 0.03,
        "real_data_preference": 0.02,
    },
    "priority_thresholds": {
        "critical": 85,
        "high": 65,
        "medium": 45,
        "low": 20,
    },
    "confidence_thresholds": {
        "very_high": 0.9,
        "high": 0.75,
        "medium": 0.55,
        "low": 0.35,
    },
    "evidence_completeness_weights": {
        "primary_entity": 1.0,
        "marker_evidence": 1.0,
        "relationship_evidence": 1.0,
        "source_provenance": 1.0,
        "timeline_events": 1.0,
        "aliases": 1.0,
        "canonical_resolution": 1.0,
        "network_context": 1.0,
        "recommended_review": 1.0,
    },
    "lead_deduplication_threshold": 0.85,
    "real_data_preference_weight": 0.15,
    "cross_source_weight": 0.1,
    "network_weight": 0.1,
    "bridge_entity_weight": 0.1,
    "minimum_markers_for_lead_generation": 1,
    "minimum_sources_for_high_confidence_lead": 2,
    "cross_source_lead_thresholds": {
        "minimum_matches": 1,
        "minimum_real_matches": 1,
    },
    "minimum_cross_source_evidence_completeness": 60,
    "minimum_cross_source_entity_resolution_confidence": 0.75,
    "package_priorities": ["CRITICAL", "HIGH"],
    "enabled_lead_types": [
        "ENTITY",
        "NETWORK",
        "CROSS_SOURCE_CLUSTER",
        "ADDRESS_CLUSTER",
        "BUSINESS_CLUSTER",
        "PROPERTY_CLUSTER",
        "COMMUNICATION_CLUSTER",
        "TEMPORAL_CLUSTER",
    ],
    "default_dashboard_source_mode": "Real Data Only",
}

CONFIDENCE_LABEL_SCORES = {
    "VERY_HIGH": 1.0,
    "HIGH": 0.85,
    "MEDIUM": 0.65,
    "LOW": 0.45,
    "UNKNOWN": 0.25,
    "Very High": 1.0,
    "High": 0.85,
    "Medium": 0.65,
    "Low": 0.45,
    "Unknown": 0.25,
}


def load_investigation_engine_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in data.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def deterministic_lead_id(lead_type: str, primary_entity_id: str, network_id: str = "") -> str:
    basis = f"{lead_type}|{primary_entity_id}|{network_id}"
    digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
    return f"lead:{digest}"


def confidence_label(score: float, thresholds: dict[str, float]) -> str:
    if score >= float(thresholds.get("very_high", 0.9)):
        return "VERY_HIGH"
    if score >= float(thresholds.get("high", 0.75)):
        return "HIGH"
    if score >= float(thresholds.get("medium", 0.55)):
        return "MEDIUM"
    if score >= float(thresholds.get("low", 0.35)):
        return "LOW"
    return "UNKNOWN"


def priority_label(score: float, thresholds: dict[str, float]) -> str:
    if score >= float(thresholds.get("critical", 85)):
        return "CRITICAL"
    if score >= float(thresholds.get("high", 65)):
        return "HIGH"
    if score >= float(thresholds.get("medium", 45)):
        return "MEDIUM"
    if score >= float(thresholds.get("low", 20)):
        return "LOW"
    return "INFORMATIONAL"


def _token_count(value: object) -> int:
    return len([token for token in str(value or "").split("|") if token.strip()])


def evidence_completeness(
    lead_row: dict[str, Any],
    weights: dict[str, float],
) -> tuple[float, str, int]:
    checks = {
        "primary_entity": bool(str(lead_row.get("primary_entity_id", "")).strip()),
        "marker_evidence": int(lead_row.get("fraud_marker_count", 0) or 0) > 0,
        "relationship_evidence": int(lead_row.get("relationship_count", 0) or 0) > 0,
        "source_provenance": bool(str(lead_row.get("source_names", "")).strip()),
        "timeline_events": int(lead_row.get("timeline_event_count", 0) or 0) > 0,
        "aliases": int(lead_row.get("alias_count", 0) or 0) > 0,
        "canonical_resolution": float(lead_row.get("entity_resolution_confidence", 0) or 0) > 0,
        "network_context": bool(str(lead_row.get("network_id", "")).strip()) or int(lead_row.get("network_member_count", 0) or 0) > 0,
        "recommended_review": bool(str(lead_row.get("recommended_review", "")).strip()),
    }
    total_weight = sum(float(weights.get(key, 1.0)) for key in checks)
    earned = sum(float(weights.get(key, 1.0)) for key, passed in checks.items() if passed)
    missing = [key for key, passed in checks.items() if not passed]
    evidence_count = sum(1 for passed in checks.values() if passed)
    score = round((earned / total_weight) * 100, 2) if total_weight else 0.0
    return score, "|".join(missing), evidence_count


def compute_confidence_score(
    independent_source_count: int,
    entity_resolution_confidence: float,
    supporting_record_count: int,
    relationship_confidence: float,
    marker_confidence: float,
    evidence_completeness_score: float,
    contains_real_data: bool,
) -> float:
    source_component = min(independent_source_count, 5) / 5.0
    supporting_component = min(supporting_record_count, 10) / 10.0
    evidence_component = evidence_completeness_score / 100.0
    real_data_component = 1.0 if contains_real_data else 0.45
    score = (
        source_component * 0.2
        + float(entity_resolution_confidence) * 0.2
        + supporting_component * 0.15
        + float(relationship_confidence) * 0.15
        + float(marker_confidence) * 0.15
        + evidence_component * 0.1
        + real_data_component * 0.05
    )
    return round(min(score, 1.0), 4)


def assign_priority_score(
    lead_row: dict[str, Any],
    config: dict[str, Any],
) -> float:
    weights = config.get("priority_weights", {})
    score = 0.0
    score += float(lead_row.get("risk_score", 0) or 0) * float(weights.get("risk_score", 0.25))
    score += min(float(lead_row.get("rarity_score", 0) or 0), 100.0) * float(weights.get("rarity_score", 0.08))
    score += float(lead_row.get("confidence_score", 0) or 0) * 100.0 * float(weights.get("confidence_score", 0.15))
    score += min(int(lead_row.get("fraud_marker_count", 0) or 0), 10) * 10.0 * float(weights.get("independent_marker_count", 0.08))
    score += min(int(lead_row.get("independent_source_count", 0) or 0), 5) * 20.0 * float(weights.get("independent_source_count", 0.08))
    score += min(int(lead_row.get("cross_source_match_count", 0) or 0), 5) * 20.0 * float(weights.get("cross_source_support", 0.08))
    score += min(float(lead_row.get("relationship_density", 0) or 0), 1.0) * 100.0 * float(weights.get("relationship_density", 0.07))
    score += min(int(lead_row.get("network_member_count", 0) or 0), 25) * 4.0 * float(weights.get("network_size", 0.07))
    score += (100.0 if int(lead_row.get("bridge_entity_count", 0) or 0) > 0 else 0.0) * float(weights.get("bridge_entity_presence", 0.06))
    score += min(float(lead_row.get("entity_resolution_confidence", 0) or 0), 1.0) * 100.0 * float(weights.get("entity_resolution_confidence", 0.06))
    score += float(lead_row.get("evidence_completeness_score", 0) or 0) * float(weights.get("evidence_completeness", 0.05))
    score += min(float(lead_row.get("temporal_concentration", 0) or 0), 1.0) * 100.0 * float(weights.get("temporal_concentration", 0.03))
    if bool(lead_row.get("contains_real_data", False)):
        score += 100.0 * float(weights.get("real_data_preference", 0.02))
    return round(min(score, 100.0), 2)


def classify_entity_lead_type(
    primary_entity_type: str,
    fraud_markers: str,
    cross_source_match_count: int,
    timeline_event_count: int,
    enabled_lead_types: list[str],
) -> str:
    marker_set = {token.strip().lower() for token in str(fraud_markers or "").split("|") if token.strip()}
    entity_type = str(primary_entity_type or "").lower()
    if cross_source_match_count > 0 and "CROSS_SOURCE_CLUSTER" in enabled_lead_types:
        return "CROSS_SOURCE_CLUSTER"
    if entity_type == "address" and "ADDRESS_CLUSTER" in enabled_lead_types:
        return "ADDRESS_CLUSTER"
    if entity_type == "property" and "PROPERTY_CLUSTER" in enabled_lead_types:
        return "PROPERTY_CLUSTER"
    if entity_type in {"phone", "email", "website"} and "COMMUNICATION_CLUSTER" in enabled_lead_types:
        return "COMMUNICATION_CLUSTER"
    if entity_type == "business" and "BUSINESS_CLUSTER" in enabled_lead_types:
        return "BUSINESS_CLUSTER"
    if timeline_event_count >= 5 and "TEMPORAL_CLUSTER" in enabled_lead_types:
        return "TEMPORAL_CLUSTER"
    if marker_set and "ENTITY" in enabled_lead_types:
        return "ENTITY"
    return "ENTITY"


def consolidate_duplicate_leads(leads_df: pd.DataFrame) -> pd.DataFrame:
    if leads_df.empty:
        return leads_df
    grouped_rows: list[dict[str, Any]] = []
    for _, group in leads_df.groupby(["lead_type", "primary_entity_id", "network_id"], dropna=False):
        ordered = group.sort_values(["priority_score", "risk_score", "confidence_score"], ascending=[False, False, False]).reset_index(drop=True)
        winner = ordered.iloc[0].to_dict()
        related_ids = merge_source_values(*ordered["lead_id"].astype(str).tolist())
        winner["related_lead_ids"] = related_ids
        winner["fraud_marker_count"] = int(pd.to_numeric(ordered["fraud_marker_count"], errors="coerce").fillna(0).max())
        winner["independent_source_count"] = int(pd.to_numeric(ordered["independent_source_count"], errors="coerce").fillna(0).max())
        winner["relationship_count"] = int(pd.to_numeric(ordered["relationship_count"], errors="coerce").fillna(0).max())
        winner["cross_source_match_count"] = int(pd.to_numeric(ordered["cross_source_match_count"], errors="coerce").fillna(0).max())
        winner["network_member_count"] = int(pd.to_numeric(ordered["network_member_count"], errors="coerce").fillna(0).max())
        winner["bridge_entity_count"] = int(pd.to_numeric(ordered["bridge_entity_count"], errors="coerce").fillna(0).max())
        winner["rarity_score"] = float(pd.to_numeric(ordered["rarity_score"], errors="coerce").fillna(0).max()) if "rarity_score" in ordered.columns else 0.0
        winner["rare_marker_count"] = int(pd.to_numeric(ordered["rare_marker_count"], errors="coerce").fillna(0).max()) if "rare_marker_count" in ordered.columns else 0
        winner["source_names"] = merge_source_values(*ordered["source_names"].astype(str).tolist())
        winner["source_types"] = merge_source_values(*ordered["source_types"].astype(str).tolist())
        if "comparison_group" in ordered.columns:
            winner["comparison_group"] = merge_source_values(*ordered["comparison_group"].astype(str).tolist())
        if "contextual_adjustment_summary" in ordered.columns:
            winner["contextual_adjustment_summary"] = merge_source_values(*ordered["contextual_adjustment_summary"].astype(str).tolist())
        if "statistical_review_reason" in ordered.columns:
            winner["statistical_review_reason"] = " | ".join([value for value in ordered["statistical_review_reason"].astype(str).tolist() if value][:3])
        winner["contains_real_data"] = bool(ordered["contains_real_data"].any())
        winner["contains_synthetic_data"] = bool(ordered["contains_synthetic_data"].any())
        grouped_rows.append(winner)
    deduped = pd.DataFrame(grouped_rows)
    if deduped.empty:
        return deduped
    return deduped.sort_values(["priority_score", "risk_score", "confidence_score"], ascending=[False, False, False]).reset_index(drop=True)
