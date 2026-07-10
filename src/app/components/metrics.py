from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.utils.dashboard_filters import parse_bool_series, token_set


def build_dashboard_metrics(
    fraud_markers_df: pd.DataFrame,
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    entity_risk_df: pd.DataFrame,
    leads_df: pd.DataFrame | None = None,
) -> dict[str, int | float]:
    leads_df = leads_df if leads_df is not None else pd.DataFrame()
    critical_leads = int((leads_df.get("Priority") == "Critical").sum()) if not leads_df.empty else 0
    high_priority_leads = int(leads_df.get("Priority").isin(["Critical", "High"]).sum()) if not leads_df.empty else 0
    high_confidence_leads = int(leads_df.get("Confidence").isin(["Very High", "High"]).sum()) if not leads_df.empty else 0
    cross_source_leads = int((leads_df.get("Supporting Source Count", 0) > 1).sum()) if not leads_df.empty else 0
    newest_leads = int(len(leads_df.head(10))) if not leads_df.empty else 0
    return {
        "total_markers": int(len(fraud_markers_df)),
        "total_findings": int(len(fraud_markers_df)),
        "total_entities": int(len(entities_df)),
        "total_relationships": int(len(relationships_df)),
        "high_risk_entities": int((entity_risk_df.get("risk_level") == "High").sum()) if not entity_risk_df.empty else 0,
        "medium_risk_entities": int((entity_risk_df.get("risk_level") == "Medium").sum()) if not entity_risk_df.empty else 0,
        "critical_leads": critical_leads,
        "high_priority_leads": high_priority_leads,
        "high_confidence_leads": high_confidence_leads,
        "cross_source_leads": cross_source_leads,
        "avg_fraud_marker_score": round(pd.to_numeric(fraud_markers_df.get("risk_contribution", pd.Series(dtype=float)), errors="coerce").fillna(0).mean(), 2) if not fraud_markers_df.empty else 0.0,
        "avg_confidence": round(pd.to_numeric(entity_risk_df.get("average_marker_confidence", pd.Series(dtype=float)), errors="coerce").fillna(0).mean(), 2) if not entity_risk_df.empty else 0.0,
        "newest_leads": newest_leads,
    }


def build_resolution_metrics(raw_entities_df: pd.DataFrame, canonical_entities_df: pd.DataFrame, matches_df: pd.DataFrame) -> dict[str, int]:
    cross_source = 0
    if not canonical_entities_df.empty:
        cross_source = int(canonical_entities_df["source_name"].fillna("").astype(str).apply(lambda value: len(token_set(value)) > 1).sum())
    return {
        "raw_entities": int(len(raw_entities_df)),
        "canonical_entities": int(len(canonical_entities_df)),
        "entities_merged": max(int(len(raw_entities_df) - len(canonical_entities_df)), 0),
        "review_candidates": int((matches_df.get("decision") == "REVIEW").sum()) if not matches_df.empty else 0,
        "cross_source_canonical_entities": cross_source,
    }


def render_top_metrics(
    *,
    pipeline_summary: dict[str, object],
    filtered_leads_df: pd.DataFrame,
    statistical_rarity_df: pd.DataFrame,
    fallback_metrics: dict[str, int | float],
) -> None:
    metric_cols = st.columns(9)
    metric_cols[0].metric("Pipeline Total Leads", int(pipeline_summary.get("total_leads", len(filtered_leads_df))))
    metric_cols[1].metric("Critical", int((filtered_leads_df["priority"] == "CRITICAL").sum()) if not filtered_leads_df.empty and "priority" in filtered_leads_df.columns else int(pipeline_summary.get("critical_leads", 0)))
    metric_cols[2].metric("High", int((filtered_leads_df["priority"] == "HIGH").sum()) if not filtered_leads_df.empty and "priority" in filtered_leads_df.columns else int(pipeline_summary.get("high_leads", 0)))
    metric_cols[3].metric("Cross Source", int((pd.to_numeric(filtered_leads_df.get("cross_source_match_count", 0), errors="coerce").fillna(0) > 0).sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("cross_source_leads", 0)))
    metric_cols[4].metric("Networks", int((filtered_leads_df.get("lead_type", pd.Series(dtype=str)).astype(str) == "NETWORK").sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("network_leads", 0)))
    metric_cols[5].metric("Statistical Outliers", int((statistical_rarity_df.get("rarity_level", pd.Series(dtype=str)).astype(str).isin(["ELEVATED_REVIEW", "IMMEDIATE_REVIEW", "EXTREME_OUTLIER"])).sum()) if not statistical_rarity_df.empty else 0)
    metric_cols[6].metric("Average Confidence", round(float(pd.to_numeric(filtered_leads_df.get("confidence_score", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()), 2) if not filtered_leads_df.empty and "confidence_score" in filtered_leads_df.columns else round(float(pipeline_summary.get("average_confidence", fallback_metrics["avg_confidence"])), 2))
    metric_cols[7].metric("Average Risk", round(float(pd.to_numeric(filtered_leads_df.get("risk_score", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()), 2) if not filtered_leads_df.empty and "risk_score" in filtered_leads_df.columns else round(float(pipeline_summary.get("average_risk", fallback_metrics["avg_fraud_marker_score"])), 2))
    real_data_coverage = int(parse_bool_series(filtered_leads_df["contains_real_data"]).sum()) if not filtered_leads_df.empty and "contains_real_data" in filtered_leads_df.columns else int(pipeline_summary.get("real_data_leads", 0))
    metric_cols[8].metric("Real Data Coverage", real_data_coverage)
