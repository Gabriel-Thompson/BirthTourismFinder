from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd

from src.connectors.source_metadata import is_real_source_type, merge_source_values

CONFIDENCE_SCORES = {
    "Very High": 1.0,
    "High": 0.85,
    "Medium": 0.65,
    "Low": 0.45,
    "Unknown": 0.25,
}

PRIORITY_ORDER = ["Critical", "High", "Medium", "Low"]


def _token_count(value: object) -> int:
    return len({token.strip() for token in str(value or "").split("|") if token.strip()})


def _priority(
    risk_score: float,
    confidence_score: float,
    source_count: int,
    relationship_count: int,
    cross_source: bool,
    resolution_confidence: float,
    marker_count: int,
) -> str:
    score = 0.0
    score += min(float(risk_score), 100.0) * 0.45
    score += confidence_score * 20.0
    score += min(source_count, 5) * 5.0
    score += min(relationship_count, 10) * 2.0
    score += min(marker_count, 8) * 2.0
    if cross_source:
        score += 10.0
    score += min(resolution_confidence, 1.0) * 10.0

    if score >= 80:
        return "Critical"
    if score >= 60:
        return "High"
    if score >= 35:
        return "Medium"
    return "Low"


def _lead_id(entity_id: str) -> str:
    digest = hashlib.sha1(entity_id.encode("utf-8")).hexdigest()[:12]
    return f"lead:{digest}"


def _risk_explanation(row: pd.Series, markers: pd.DataFrame, cross_source: bool) -> str:
    pieces: list[str] = []
    if row.get("contributing_rules"):
        pieces.append(f"Markers: {row['contributing_rules']}")
    if int(row.get("relationship_count", 0) or 0):
        pieces.append(f"Relationships: {int(row.get('relationship_count', 0) or 0)}")
    if int(row.get("source_count", 0) or 0):
        pieces.append(f"Sources: {int(row.get('source_count', 0) or 0)}")
    if cross_source:
        pieces.append("Cross-source correlation detected")
    if not markers.empty:
        top_marker = markers.sort_values(["risk_contribution", "confidence_score"], ascending=[False, False]).iloc[0]
        pieces.append(f"Top marker: {top_marker.get('marker_name', '')}")
    return " | ".join(pieces) if pieces else "Rule-based lead generated for manual review."


def build_investigation_leads(
    entity_risk_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    canonical_entities_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    aliases_df: pd.DataFrame,
) -> pd.DataFrame:
    if entity_risk_df.empty:
        return pd.DataFrame(
            columns=[
                "lead_id",
                "entity_id",
                "Primary Entity",
                "Lead Title",
                "Lead Summary",
                "Risk Score",
                "Confidence",
                "Priority",
                "Status",
                "Date Generated",
                "Fraud Marker Count",
                "Supporting Source Count",
                "Relationship Count",
                "source_name",
                "source_type",
                "data_scope",
                "Cross-Source Correlation",
                "Entity Resolution Confidence",
                "Risk Explanation",
                "Recommended Review",
                "Lead Notes",
                "Reviewer",
                "Review Date",
                "Disposition",
                "Review Status",
                "Follow-up Needed",
            ]
        )

    canonical_lookup = canonical_entities_df.set_index("entity_id", drop=False) if not canonical_entities_df.empty and "entity_id" in canonical_entities_df.columns else pd.DataFrame()
    alias_counts = aliases_df.groupby("canonical_entity_id").size().to_dict() if not aliases_df.empty else {}
    generated_at = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d")
    rows: list[dict[str, object]] = []

    for _, risk_row in entity_risk_df.fillna("").iterrows():
        entity_id = str(risk_row.get("entity_id", "")).strip()
        if not entity_id:
            continue
        marker_rows = fraud_markers_df[fraud_markers_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id].copy() if not fraud_markers_df.empty else pd.DataFrame()
        related_relationships = canonical_relationships_df[
            (canonical_relationships_df.get("source_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
            | (canonical_relationships_df.get("target_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
        ].copy() if not canonical_relationships_df.empty else pd.DataFrame()

        source_name = str(risk_row.get("source_name", "")).strip()
        source_type = str(risk_row.get("source_type", "")).strip()
        source_count = int(risk_row.get("source_count", 0) or _token_count(source_name))
        relationship_count = int(risk_row.get("relationship_count", 0) or len(related_relationships))
        marker_count = int(risk_row.get("marker_count", 0) or len(marker_rows))
        confidence_score = float(risk_row.get("average_marker_confidence", 0.0) or CONFIDENCE_SCORES.get(str(risk_row.get("confidence", "Unknown")), 0.25))
        resolution_value: object = 1.0
        if not canonical_lookup.empty and entity_id in canonical_lookup.index and "resolution_confidence" in canonical_lookup.columns:
            resolution_value = canonical_lookup.loc[entity_id, "resolution_confidence"]
            if isinstance(resolution_value, pd.Series):
                resolution_value = resolution_value.iloc[0]
        resolution_confidence = float(resolution_value or 1.0)
        cross_source = source_count > 1 or _token_count(source_type) > 1 or ("|" in source_name and is_real_source_type(source_type))
        priority = _priority(
            risk_score=float(risk_row.get("risk_score", 0) or 0),
            confidence_score=confidence_score,
            source_count=source_count,
            relationship_count=relationship_count,
            cross_source=cross_source,
            resolution_confidence=resolution_confidence,
            marker_count=marker_count + alias_counts.get(entity_id, 0),
        )
        display_name = str(risk_row.get("display_name", entity_id))
        marker_names = merge_source_values(*marker_rows.get("marker_name", pd.Series(dtype=str)).astype(str).tolist()) if not marker_rows.empty else ""
        lead_summary = (
            f"{display_name} scored {int(risk_row.get('risk_score', 0) or 0)} with {marker_count} fraud markers, "
            f"{relationship_count} relationships, and {source_count} supporting sources."
        )
        rows.append(
            {
                "lead_id": _lead_id(entity_id),
                "entity_id": entity_id,
                "Primary Entity": display_name,
                "Lead Title": f"{display_name} investigation lead",
                "Lead Summary": lead_summary,
                "Risk Score": int(risk_row.get("risk_score", 0) or 0),
                "Confidence": str(risk_row.get("confidence", "Unknown")),
                "Priority": priority,
                "Status": "Open",
                "Date Generated": generated_at,
                "Fraud Marker Count": marker_count,
                "Supporting Source Count": source_count,
                "Relationship Count": relationship_count,
                "source_name": source_name,
                "source_type": source_type,
                "data_scope": "real" if is_real_source_type(source_type) else "synthetic",
                "Cross-Source Correlation": "Yes" if cross_source else "No",
                "Entity Resolution Confidence": round(resolution_confidence, 4),
                "Risk Explanation": _risk_explanation(risk_row, marker_rows, cross_source),
                "Recommended Review": str(risk_row.get("recommended_review", "")),
                "Fraud Markers": marker_names,
                "Lead Notes": "",
                "Reviewer": "",
                "Review Date": "",
                "Disposition": "",
                "Review Status": "",
                "Follow-up Needed": "",
            }
        )

    leads = pd.DataFrame(rows)
    if leads.empty:
        return leads
    leads["Priority Rank"] = leads["Priority"].apply(lambda value: PRIORITY_ORDER.index(value) if value in PRIORITY_ORDER else len(PRIORITY_ORDER))
    leads = leads.sort_values(
        ["Priority Rank", "Risk Score", "Fraud Marker Count", "Supporting Source Count", "Relationship Count"],
        ascending=[True, False, False, False, False],
    ).drop(columns=["Priority Rank"])
    return leads.reset_index(drop=True)
