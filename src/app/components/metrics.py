from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.utils.dashboard_filters import parse_bool_series, token_set


def _coalesced_series(df: pd.DataFrame, columns: list[str], *, default: object, dtype: str) -> pd.Series:
    series = pd.Series(default, index=df.index, dtype=dtype)
    for column in columns:
        if column in df.columns:
            series = series.where(series.notna(), df[column])
    return series


def _object_series(df: pd.DataFrame, column: str) -> pd.Series:
    return df.get(column, pd.Series(index=df.index, dtype="object")).astype(str)


def _numeric_series(df: pd.DataFrame, column: str, *, default: int | float = 0) -> pd.Series:
    return pd.to_numeric(df.get(column, pd.Series(default, index=df.index)), errors="coerce").fillna(default)


def _lead_priority_series(leads_df: pd.DataFrame) -> pd.Series:
    series = leads_df.get(
        "Priority",
        leads_df.get(
            "priority",
            pd.Series(index=leads_df.index, dtype="object"),
        ),
    )
    return series.astype(str).str.strip().str.upper()


def _lead_confidence_series(leads_df: pd.DataFrame) -> pd.Series:
    series = leads_df.get(
        "Confidence",
        leads_df.get(
            "confidence",
            pd.Series(index=leads_df.index, dtype="object"),
        ),
    )
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .str.replace("_", " ", regex=False)
    )


def _supporting_source_count_series(leads_df: pd.DataFrame) -> pd.Series:
    series = _coalesced_series(
        leads_df,
        ["Supporting Source Count", "independent_source_count", "cross_source_match_count"],
        default=pd.NA,
        dtype="object",
    )
    return pd.to_numeric(series, errors="coerce").fillna(0)


def build_dashboard_metrics(
    fraud_markers_df: pd.DataFrame,
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    entity_risk_df: pd.DataFrame,
    leads_df: pd.DataFrame | None = None,
) -> dict[str, int | float]:
    leads_df = leads_df if leads_df is not None else pd.DataFrame()
    priority_series = _lead_priority_series(leads_df)
    confidence_series = _lead_confidence_series(leads_df)
    supporting_source_count = _supporting_source_count_series(leads_df)
    risk_level_series = _object_series(entity_risk_df, "risk_level").str.strip()

    critical_leads = int((priority_series == "CRITICAL").sum())
    high_priority_leads = int(priority_series.isin(["CRITICAL", "HIGH"]).sum())
    high_confidence_leads = int(confidence_series.isin(["VERY HIGH", "HIGH"]).sum())
    cross_source_leads = int((supporting_source_count > 1).sum())
    newest_leads = int(len(leads_df.head(10))) if not leads_df.empty else 0
    return {
        "total_markers": int(len(fraud_markers_df)),
        "total_findings": int(len(fraud_markers_df)),
        "total_entities": int(len(entities_df)),
        "total_relationships": int(len(relationships_df)),
        "high_risk_entities": int((risk_level_series == "High").sum()),
        "medium_risk_entities": int((risk_level_series == "Medium").sum()),
        "critical_leads": critical_leads,
        "high_priority_leads": high_priority_leads,
        "high_confidence_leads": high_confidence_leads,
        "cross_source_leads": cross_source_leads,
        "avg_fraud_marker_score": round(_numeric_series(fraud_markers_df, "risk_contribution").mean(), 2) if not fraud_markers_df.empty else 0.0,
        "avg_confidence": round(_numeric_series(entity_risk_df, "average_marker_confidence").mean(), 2) if not entity_risk_df.empty else 0.0,
        "newest_leads": newest_leads,
    }


def build_resolution_metrics(raw_entities_df: pd.DataFrame, canonical_entities_df: pd.DataFrame, matches_df: pd.DataFrame) -> dict[str, int]:
    cross_source = 0
    if not canonical_entities_df.empty:
        cross_source = int(canonical_entities_df["source_name"].fillna("").astype(str).apply(lambda value: len(token_set(value)) > 1).sum())
    decision_series = _object_series(matches_df, "decision").str.strip().str.upper()
    return {
        "raw_entities": int(len(raw_entities_df)),
        "canonical_entities": int(len(canonical_entities_df)),
        "entities_merged": max(int(len(raw_entities_df) - len(canonical_entities_df)), 0),
        "review_candidates": int((decision_series == "REVIEW").sum()),
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
    priority_series = _object_series(filtered_leads_df, "priority").str.strip().str.upper()
    cross_source_match_count_series = _numeric_series(filtered_leads_df, "cross_source_match_count")
    lead_type_series = _object_series(filtered_leads_df, "lead_type").str.strip().str.upper()
    confidence_score_series = _numeric_series(filtered_leads_df, "confidence_score", default=0.0)
    risk_score_series = _numeric_series(filtered_leads_df, "risk_score", default=0.0)
    rarity_level_series = _object_series(statistical_rarity_df, "rarity_level").str.strip().str.upper()
    metric_cols[0].metric("Pipeline Total Leads", int(pipeline_summary.get("total_leads", len(filtered_leads_df))))
    metric_cols[1].metric("Critical", int((priority_series == "CRITICAL").sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("critical_leads", 0)))
    metric_cols[2].metric("High", int((priority_series == "HIGH").sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("high_leads", 0)))
    metric_cols[3].metric("Cross Source", int((cross_source_match_count_series > 0).sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("cross_source_leads", 0)))
    metric_cols[4].metric("Networks", int((lead_type_series == "NETWORK").sum()) if not filtered_leads_df.empty else int(pipeline_summary.get("network_leads", 0)))
    metric_cols[5].metric("Statistical Outliers", int(rarity_level_series.isin(["ELEVATED_REVIEW", "IMMEDIATE_REVIEW", "EXTREME_OUTLIER"]).sum()))
    metric_cols[6].metric("Average Confidence", round(float(confidence_score_series.mean()), 2) if not filtered_leads_df.empty else round(float(pipeline_summary.get("average_confidence", fallback_metrics["avg_confidence"])), 2))
    metric_cols[7].metric("Average Risk", round(float(risk_score_series.mean()), 2) if not filtered_leads_df.empty else round(float(pipeline_summary.get("average_risk", fallback_metrics["avg_fraud_marker_score"])), 2))
    real_data_coverage = int(parse_bool_series(filtered_leads_df["contains_real_data"]).sum()) if not filtered_leads_df.empty and "contains_real_data" in filtered_leads_df.columns else int(pipeline_summary.get("real_data_leads", 0))
    metric_cols[8].metric("Real Data Coverage", real_data_coverage)
