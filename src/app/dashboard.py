from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

def _bootstrap_repo_root() -> Path:
    current = Path(__file__).resolve()
    candidate_roots = [current.parents[2], *current.parents]
    for candidate in candidate_roots:
        if (candidate / "src").is_dir() and (candidate / "config").is_dir():
            root_value = str(candidate)
            src_value = str(candidate / "src")
            if root_value not in sys.path:
                sys.path.insert(0, root_value)
            if src_value not in sys.path:
                sys.path.insert(0, src_value)
            return candidate
    fallback = current.parents[2]
    root_value = str(fallback)
    if root_value not in sys.path:
        sys.path.insert(0, root_value)
    return fallback


REPO_ROOT = _bootstrap_repo_root()

from src.connectors.source_metadata import load_api_sources, load_manifest_sources
from src.investigation.analyst_workbench import (
    ANALYST_HISTORY_PATH as WORKBENCH_ANALYST_HISTORY_PATH,
    ANALYST_STATE_PATH as WORKBENCH_ANALYST_STATE_PATH,
    SAVED_SEARCHES_PATH as WORKBENCH_SAVED_SEARCHES_PATH,
    build_queue_view,
    build_source_health_report,
    compare_records,
    load_analyst_history,
    load_analyst_state,
    load_dashboard_config,
    load_saved_searches,
    persist_analyst_state,
    save_saved_searches,
    update_analyst_record,
)
from src.investigation.report_builder import lead_package_dir_name

def _repo_path(*parts: str) -> Path:
    return REPO_ROOT.joinpath(*parts)


FRAUD_MARKERS_PATH = _repo_path("data", "processed", "fraud_markers.csv")
FRAUD_MARKER_SUMMARY_PATH = _repo_path("data", "processed", "fraud_marker_summary.csv")
ANOMALY_REPORT_PATH = _repo_path("data", "processed", "anomaly_report.csv")
ENTITIES_PATH = _repo_path("data", "processed", "canonical_entities.csv")
RELATIONSHIPS_PATH = _repo_path("data", "processed", "canonical_relationships.csv")
ENTITY_RISK_PATH = _repo_path("data", "processed", "entity_risk.csv")
CANONICAL_ENTITIES_PATH = _repo_path("data", "processed", "canonical_entities.csv")
ENTITY_ALIASES_PATH = _repo_path("data", "processed", "entity_aliases.csv")
ENTITY_RESOLUTION_MATCHES_PATH = _repo_path("data", "processed", "entity_resolution_matches.csv")
INVESTIGATION_LEADS_PATH = _repo_path("data", "processed", "investigation_leads.csv")
ENTITY_TIMELINES_PATH = _repo_path("data", "processed", "entity_timelines.csv")
EVIDENCE_PACKETS_PATH = _repo_path("data", "processed", "evidence_packets.csv")
NETWORK_CLUSTERS_PATH = _repo_path("data", "processed", "network_clusters.csv")
NETWORK_SUMMARY_PATH = _repo_path("data", "processed", "network_summary.csv")
NETWORK_MEMBERS_PATH = _repo_path("data", "processed", "network_members.csv")
NETWORK_EDGES_PATH = _repo_path("data", "processed", "network_edges.csv")
PRIORITIZED_LEADS_PATH = _repo_path("data", "processed", "prioritized_leads.csv")
INVESTIGATION_SUMMARY_PATH = _repo_path("data", "processed", "investigation_summary.csv")
REVIEW_RECOMMENDATIONS_PATH = _repo_path("data", "processed", "review_recommendations.csv")
ANALYST_STATE_PATH = _repo_path("data", "processed", "analyst_lead_state.csv")
ANALYST_HISTORY_PATH = _repo_path("data", "processed", "analyst_history.csv")
CROSS_SOURCE_MATCHES_PATH = _repo_path("data", "processed", "cross_source_matches.csv")
CROSS_SOURCE_DIAGNOSTICS_PATH = _repo_path("data", "processed", "cross_source_diagnostics.csv")
CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH = _repo_path("data", "processed", "cross_source_diagnostic_summary.json")
STATISTICAL_BASELINES_PATH = _repo_path("data", "processed", "statistical_baselines.csv")
STATISTICAL_RARITY_PATH = _repo_path("data", "processed", "statistical_rarity.csv")
CONTEXTUAL_RISK_ADJUSTMENTS_PATH = _repo_path("data", "processed", "contextual_risk_adjustments.csv")
STATISTICAL_MARKER_SUMMARY_PATH = _repo_path("data", "processed", "statistical_marker_summary.json")
STATISTICAL_CALIBRATION_REPORT_PATH = _repo_path("data", "processed", "statistical_calibration_report.csv")
SAVED_SEARCHES_PATH = _repo_path("data", "processed", "dashboard_saved_searches.json")
REAL_SOURCE_TYPES = {"connector", "api", "arcgis", "manual"}


def _empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _token_set(value: object) -> set[str]:
    return {token.strip() for token in str(value or "").split("|") if token.strip()}


def load_csv(path: Path, columns: list[str], warning_message: str | None = None) -> pd.DataFrame:
    if not path.exists():
        if warning_message:
            st.warning(warning_message)
        return _empty_frame(columns)
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        if warning_message:
            st.warning(f"{warning_message} ({exc})")
        return _empty_frame(columns)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df


def load_fraud_markers(path: Path | str = FRAUD_MARKERS_PATH) -> pd.DataFrame:
    columns = [
        "entity_id",
        "marker_id",
        "marker_name",
        "marker_category",
        "risk_contribution",
        "confidence",
        "confidence_score",
        "support",
        "sources",
        "source_types",
        "supporting_entities",
        "supporting_relationships",
        "recommended_review",
        "explanation",
        "raw_risk_contribution",
        "contextual_adjustment",
        "adjusted_risk_contribution",
        "rarity_score",
        "rarity_level",
        "review_level",
        "observed_value",
        "expected_value",
        "comparison_group",
        "comparison_group_size",
        "probability_or_p_value",
        "model_used",
        "assumptions",
        "statistical_explanation",
        "source_scope",
    ]
    df = load_csv(Path(path), columns, "Fraud markers file not found. Run the full pipeline to generate fraud markers.")
    if not df.empty:
        for column in ["risk_contribution", "raw_risk_contribution", "contextual_adjustment", "adjusted_risk_contribution", "support"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
        for column in ["confidence_score", "rarity_score", "observed_value", "expected_value", "comparison_group_size"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        df["source_name"] = df["sources"]
        df["source_type"] = df["source_types"]
    return df


def load_fraud_marker_summary(path: Path | str = FRAUD_MARKER_SUMMARY_PATH) -> pd.DataFrame:
    columns = [
        "marker_name",
        "marker_category",
        "frequency",
        "average_risk_contribution",
        "average_support",
        "average_confidence_score",
        "average_rarity_score",
    ]
    df = load_csv(Path(path), columns, None)
    if not df.empty:
        for column in ["frequency", "average_risk_contribution", "average_support", "average_confidence_score"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_report(path: Path | str = ANOMALY_REPORT_PATH) -> pd.DataFrame:
    columns = ["Risk Score", "Risk Level", "Rule Triggered", "Supporting Evidence", "Entity IDs", "Addresses", "Phone Numbers", "Source Table", "source_name", "source_type", "data_scope"]
    df = load_csv(Path(path), columns, f"Anomaly report not found at {Path(path)}. Run the fraud marker engine first.")
    if not df.empty:
        df["Risk Score"] = pd.to_numeric(df["Risk Score"], errors="coerce").fillna(0).astype(int)
        if not df["Risk Level"].astype(str).str.len().any():
            df["Risk Level"] = pd.cut(
                df["Risk Score"],
                bins=[-1, 14, 24, float("inf")],
                labels=["Low", "Medium", "High"],
                right=True,
            ).astype(str)
    return df


def load_entities(path: Path | str = ENTITIES_PATH) -> pd.DataFrame:
    columns = ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type"]
    return load_csv(Path(path), columns, "Canonical entities file not found. Run the entity resolution step first.")


def load_relationships(path: Path | str = RELATIONSHIPS_PATH) -> pd.DataFrame:
    columns = ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type"]
    return load_csv(Path(path), columns, "Canonical relationships file not found. Run the entity resolution step first.")


def load_entity_risk(path: Path | str = ENTITY_RISK_PATH) -> pd.DataFrame:
    columns = [
        "entity_id",
        "entity_type",
        "display_name",
        "risk_score",
        "risk_level",
        "confidence",
        "relationship_count",
        "source_count",
        "source_name",
        "source_type",
        "data_scope",
        "contributing_rules",
        "marker_categories",
        "supporting_evidence",
        "recommended_review",
        "marker_count",
        "average_marker_confidence",
    ]
    df = load_csv(Path(path), columns, "Entity risk file not found. Run `python src/run_pipeline.py --include-connectors --health-check` first.")
    if not df.empty:
        df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0).astype(int)
    return df


def load_canonical_entities(path: Path | str = CANONICAL_ENTITIES_PATH) -> pd.DataFrame:
    columns = [
        "canonical_entity_id",
        "entity_id",
        "entity_type",
        "display_name",
        "normalized_value",
        "source_count",
        "record_count",
        "alias_count",
        "source_names",
        "source_name",
        "source_type",
        "resolution_confidence",
        "resolution_method",
    ]
    df = load_csv(Path(path), columns, None)
    if not df.empty and not df["source_name"].astype(str).str.len().any():
        df["source_name"] = df["source_names"]
    return df


def load_entity_aliases(path: Path | str = ENTITY_ALIASES_PATH) -> pd.DataFrame:
    columns = ["canonical_entity_id", "original_entity_id", "alias_value", "normalized_alias", "source_name", "source_type", "source_record_id", "resolution_method", "confidence_score"]
    return load_csv(Path(path), columns, None)


def load_resolution_matches(path: Path | str = ENTITY_RESOLUTION_MATCHES_PATH) -> pd.DataFrame:
    columns = ["match_id", "left_entity_id", "right_entity_id", "entity_type", "match_method", "confidence_score", "decision", "evidence", "source_names", "source_name", "source_type"]
    df = load_csv(Path(path), columns, None)
    if not df.empty and not df["source_name"].astype(str).str.len().any():
        df["source_name"] = df["source_names"]
    return df


def load_investigation_leads(path: Path | str = INVESTIGATION_LEADS_PATH) -> pd.DataFrame:
    columns = [
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
        "Fraud Markers",
        "Recommended Review",
        "Lead Notes",
        "Reviewer",
        "Review Date",
        "Disposition",
        "Review Status",
        "Follow-up Needed",
    ]
    df = load_csv(Path(path), columns, "Investigation leads file not found. Run the full pipeline to build the investigation workspace.")
    if not df.empty:
        for column in ["Risk Score", "Fraud Marker Count", "Supporting Source Count", "Relationship Count"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_entity_timelines(path: Path | str = ENTITY_TIMELINES_PATH) -> pd.DataFrame:
    columns = ["lead_id", "entity_id", "Date", "Event", "Entity", "Source", "source_name", "source_type", "Evidence", "Record ID", "Connector", "Import Date"]
    return load_csv(Path(path), columns, "Entity timeline output not found. Run the investigation workspace step first.")


def load_evidence_packets(path: Path | str = EVIDENCE_PACKETS_PATH) -> pd.DataFrame:
    columns = [
        "lead_id",
        "entity_id",
        "Primary Entity",
        "Aliases",
        "Fraud Markers",
        "Relationships",
        "Connected Entities",
        "Sources",
        "Timeline",
        "Supporting Evidence",
        "Risk Explanation",
        "Recommended Review",
        "Source",
        "Record ID",
        "Connector",
        "Import Date",
        "source_name",
        "source_type",
    ]
    return load_csv(Path(path), columns, "Evidence packets file not found. Run the investigation workspace step first.")


def load_network_clusters(path: Path | str = NETWORK_CLUSTERS_PATH) -> pd.DataFrame:
    columns = [
        "network_id",
        "network_size",
        "business_count",
        "address_count",
        "property_count",
        "owner_count",
        "registered_agent_count",
        "officer_count",
        "relationship_count",
        "average_relationships_per_entity",
        "fraud_marker_count",
        "independent_source_count",
        "cross_source_matches",
        "entity_resolution_confidence",
        "network_risk_score",
        "network_confidence_score",
        "network_confidence",
        "network_priority",
        "relationship_density",
        "entity_diversity",
        "bridge_entity_count",
        "community_count",
        "supporting_sources",
        "source_name",
        "source_type",
        "data_scope",
        "explanation",
        "top_markers",
        "bridge_entities",
        "latest_activity_date",
        "timeline_event_count",
        "fast_growth_score",
    ]
    df = load_csv(Path(path), columns, "Network clusters file not found. Run the network intelligence step first.")
    if not df.empty:
        for column in ["network_size", "network_risk_score", "fraud_marker_count", "relationship_count", "bridge_entity_count", "community_count"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_network_summary(path: Path | str = NETWORK_SUMMARY_PATH) -> pd.DataFrame:
    columns = [
        "network_count",
        "largest_network_id",
        "largest_network_size",
        "highest_risk_network_id",
        "highest_risk_score",
        "average_network_size",
        "bridge_entity_count",
        "community_count",
        "cross_source_network_count",
        "generated_at",
    ]
    return load_csv(Path(path), columns, "Network summary file not found. Run the network intelligence step first.")


def load_network_members(path: Path | str = NETWORK_MEMBERS_PATH) -> pd.DataFrame:
    columns = [
        "network_id",
        "community_id",
        "entity_id",
        "display_name",
        "entity_type",
        "degree",
        "degree_centrality",
        "bridge_flag",
        "bridge_score",
        "disconnected_groups",
        "marker_count",
        "marker_names",
        "source_name",
        "source_type",
        "resolution_confidence",
        "data_scope",
    ]
    return load_csv(Path(path), columns, "Network members file not found. Run the network intelligence step first.")


def load_network_edges(path: Path | str = NETWORK_EDGES_PATH) -> pd.DataFrame:
    columns = [
        "network_id",
        "relationship_id",
        "source_entity_id",
        "target_entity_id",
        "relationship_type",
        "confidence",
        "source_name",
        "source_type",
        "evidence",
        "source_community_id",
        "target_community_id",
    ]
    return load_csv(Path(path), columns, "Network edges file not found. Run the network intelligence step first.")


def load_prioritized_leads(path: Path | str = PRIORITIZED_LEADS_PATH) -> pd.DataFrame:
    columns = [
        "lead_id",
        "lead_type",
        "title",
        "primary_entity_id",
        "primary_entity_type",
        "network_id",
        "risk_score",
        "confidence",
        "confidence_score",
        "priority",
        "fraud_marker_count",
        "independent_source_count",
        "relationship_count",
        "cross_source_match_count",
        "network_member_count",
        "status",
        "recommended_review",
        "explanation",
        "source_names",
        "source_types",
        "contains_real_data",
        "contains_synthetic_data",
        "evidence_completeness_score",
        "missing_evidence_fields",
        "rarity_score",
        "highest_rarity_level",
        "rare_marker_count",
        "expected_value",
        "observed_value",
        "comparison_group",
        "contextual_adjustment_summary",
        "statistical_review_reason",
        "related_lead_ids",
        "generated_at",
        "analyst_notes",
        "reviewer",
        "review_date",
        "disposition",
        "follow_up_needed",
    ]
    df = load_csv(Path(path), columns, "Prioritized leads file not found. Run the investigation engine through the pipeline first.")
    if not df.empty:
        for column in ["risk_score", "confidence_score", "fraud_marker_count", "independent_source_count", "relationship_count", "cross_source_match_count", "network_member_count", "evidence_completeness_score", "rarity_score", "rare_marker_count", "expected_value", "observed_value"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_investigation_summary(path: Path | str = INVESTIGATION_SUMMARY_PATH) -> pd.DataFrame:
    columns = [
        "total_leads",
        "critical_leads",
        "high_leads",
        "medium_leads",
        "low_leads",
        "real_data_leads",
        "synthetic_data_leads",
        "cross_source_leads",
        "entity_leads",
        "network_leads",
        "average_risk",
        "average_confidence",
        "average_evidence_completeness",
        "most_common_markers",
        "most_common_source_combinations",
        "generated_at",
    ]
    return load_csv(Path(path), columns, "Investigation summary file not found. Run the investigation engine through the pipeline first.")


def load_review_recommendations(path: Path | str = REVIEW_RECOMMENDATIONS_PATH) -> pd.DataFrame:
    columns = ["lead_id", "lead_type", "priority", "confidence", "recommended_review", "evidence_completeness_score", "missing_evidence_fields", "status"]
    return load_csv(Path(path), columns, "Review recommendations file not found. Run the investigation engine through the pipeline first.")


def load_cross_source_matches(path: Path | str = CROSS_SOURCE_MATCHES_PATH) -> pd.DataFrame:
    columns = [
        "cross_source_match_id",
        "canonical_entity_id",
        "entity_type",
        "left_entity_id",
        "right_entity_id",
        "left_source_name",
        "right_source_name",
        "left_source_type",
        "right_source_type",
        "left_source_record_id",
        "right_source_record_id",
        "source_pair",
        "match_method",
        "confidence",
        "evidence",
        "decision",
        "independent_real_source_count",
        "contains_real_data",
        "contains_synthetic_data",
        "why_sources_independent",
    ]
    df = load_csv(Path(path), columns, "Cross-source matches file not found. Run the full pipeline to generate cross-source intelligence.")
    if not df.empty:
        for column in ["confidence", "independent_real_source_count"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
        df["source_name"] = df["left_source_name"].astype(str) + "|" + df["right_source_name"].astype(str)
        df["source_type"] = df["left_source_type"].astype(str) + "|" + df["right_source_type"].astype(str)
    return df


def load_cross_source_diagnostics(path: Path | str = CROSS_SOURCE_DIAGNOSTICS_PATH) -> pd.DataFrame:
    return load_csv(Path(path), ["metric", "value"], "Cross-source diagnostics file not found. Run the full pipeline to generate diagnostics.")


def load_cross_source_diagnostic_summary(path: Path | str = CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH) -> dict[str, object]:
    summary_path = Path(path)
    if not summary_path.exists() or summary_path.stat().st_size == 0:
        return {}
    try:
        with summary_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def load_statistical_rarity(path: Path | str = STATISTICAL_RARITY_PATH) -> pd.DataFrame:
    columns = [
        "marker_id",
        "marker_name",
        "entity_id",
        "entity_type",
        "source_name",
        "source_type",
        "jurisdiction",
        "source_scope",
        "address_context",
        "base_building_address",
        "unit_level_address",
        "classification_confidence",
        "observed_value",
        "expected_value",
        "comparison_group",
        "comparison_group_size",
        "percentile",
        "probability_or_p_value",
        "rarity_score",
        "rarity_level",
        "model_used",
        "assumptions",
        "explanation",
    ]
    df = load_csv(Path(path), columns, "Statistical rarity file not found. Run the statistical risk engine through the pipeline first.")
    if not df.empty:
        for column in ["observed_value", "expected_value", "comparison_group_size", "percentile", "rarity_score", "classification_confidence"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_contextual_adjustments(path: Path | str = CONTEXTUAL_RISK_ADJUSTMENTS_PATH) -> pd.DataFrame:
    columns = [
        "marker_id",
        "marker_name",
        "entity_id",
        "entity_type",
        "address_context",
        "adjustment_category",
        "original_marker_score",
        "contextual_adjustment",
        "adjusted_marker_score",
        "reason_for_adjustment",
        "source_scope",
    ]
    df = load_csv(Path(path), columns, "Contextual adjustment file not found. Run the statistical risk engine through the pipeline first.")
    if not df.empty:
        for column in ["original_marker_score", "contextual_adjustment", "adjusted_marker_score"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_statistical_baselines(path: Path | str = STATISTICAL_BASELINES_PATH) -> pd.DataFrame:
    columns = [
        "marker_id",
        "entity_type",
        "source_scope",
        "comparison_group",
        "address_context",
        "jurisdiction",
        "source_name",
        "comparison_group_size",
        "observed_mean",
        "observed_median",
        "observed_max",
        "observed_min",
        "observed_p90",
    ]
    df = load_csv(Path(path), columns, "Statistical baselines file not found. Run the statistical risk engine through the pipeline first.")
    if not df.empty:
        for column in ["comparison_group_size", "observed_mean", "observed_median", "observed_max", "observed_min", "observed_p90"]:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def load_statistical_summary(path: Path | str = STATISTICAL_MARKER_SUMMARY_PATH) -> dict[str, object]:
    summary_path = Path(path)
    if not summary_path.exists() or summary_path.stat().st_size == 0:
        return {}
    try:
        with summary_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def load_statistical_calibration_report(path: Path | str = STATISTICAL_CALIBRATION_REPORT_PATH) -> pd.DataFrame:
    return load_csv(Path(path), ["metric", "value"], "Statistical calibration report not found. Run the statistical risk engine through the pipeline first.")


def filter_dataframe_by_source_scope(df: pd.DataFrame, scope: str, source_names: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    source_type_column = "source_type" if "source_type" in filtered.columns else "source_types"
    source_name_column = "source_name" if "source_name" in filtered.columns else "sources"
    if source_type_column not in filtered.columns:
        return filtered
    if scope == "real_only":
        filtered = filtered[filtered[source_type_column].apply(lambda value: bool(_token_set(value) & REAL_SOURCE_TYPES))]
    elif scope == "synthetic_only":
        filtered = filtered[filtered[source_type_column].apply(lambda value: "synthetic" in _token_set(value) and not bool(_token_set(value) & REAL_SOURCE_TYPES))]
    if source_names and source_name_column in filtered.columns:
        selected = set(source_names)
        filtered = filtered[filtered[source_name_column].apply(lambda value: bool(_token_set(value) & selected))]
    return filtered


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
        cross_source = int(canonical_entities_df["source_name"].fillna("").astype(str).apply(lambda value: len(_token_set(value)) > 1).sum())
    return {
        "raw_entities": int(len(raw_entities_df)),
        "canonical_entities": int(len(canonical_entities_df)),
        "entities_merged": max(int(len(raw_entities_df) - len(canonical_entities_df)), 0),
        "review_candidates": int((matches_df.get("decision") == "REVIEW").sum()) if not matches_df.empty else 0,
        "cross_source_canonical_entities": cross_source,
    }


def build_relationship_explorer_data(entities_df: pd.DataFrame, relationships_df: pd.DataFrame, selected_entity_id: str) -> pd.DataFrame:
    if entities_df.empty or relationships_df.empty:
        return _empty_frame(["source_entity_id", "connected_entity_id", "relationship_type", "connected_entity_type", "connected_entity_name", "source_table", "source_name", "confidence", "direction"])
    entity_lookup = entities_df.set_index("entity_id")
    outgoing = relationships_df[relationships_df["source_entity_id"].astype(str) == str(selected_entity_id)].copy()
    incoming = relationships_df[relationships_df["target_entity_id"].astype(str) == str(selected_entity_id)].copy()
    outgoing["direction"] = "outgoing"
    incoming["direction"] = "incoming"
    filtered = pd.concat([outgoing, incoming], ignore_index=True)
    if filtered.empty:
        return _empty_frame(["source_entity_id", "connected_entity_id", "relationship_type", "connected_entity_type", "connected_entity_name", "source_table", "source_name", "confidence", "direction"])
    filtered["connected_entity_id"] = filtered.apply(
        lambda row: row["target_entity_id"] if str(row.get("source_entity_id")) == str(selected_entity_id) else row["source_entity_id"],
        axis=1,
    )
    filtered["connected_entity_type"] = filtered["connected_entity_id"].apply(lambda value: entity_lookup.loc[value, "entity_type"] if value in entity_lookup.index else "unknown")
    filtered["connected_entity_name"] = filtered["connected_entity_id"].apply(lambda value: entity_lookup.loc[value, "display_name"] if value in entity_lookup.index else value)
    filtered["source_table"] = filtered["connected_entity_id"].apply(lambda value: entity_lookup.loc[value, "source"] if value in entity_lookup.index and "source" in entity_lookup.columns else "canonical")
    if "source_name" not in filtered.columns:
        filtered["source_name"] = ""
    return filtered[["source_entity_id", "connected_entity_id", "relationship_type", "connected_entity_type", "connected_entity_name", "source_table", "source_name", "confidence", "direction"]].reset_index(drop=True)


def main() -> None:
    st.set_page_config(page_title="OpenFraud Dashboard", layout="wide")
    st.title("OpenFraud")
    st.caption("Local-first fraud-marker review dashboard. All results are leads only, not proof of fraud.")

    fraud_markers_df = load_fraud_markers()
    fraud_marker_summary_df = load_fraud_marker_summary()
    compatibility_report_df = load_report()
    entities_df = load_entities()
    relationships_df = load_relationships()
    entity_risk_df = load_entity_risk()
    canonical_entities_df = load_canonical_entities()
    entity_aliases_df = load_entity_aliases()
    resolution_matches_df = load_resolution_matches()
    investigation_leads_df = load_investigation_leads()
    entity_timelines_df = load_entity_timelines()
    evidence_packets_df = load_evidence_packets()
    network_clusters_df = load_network_clusters()
    network_summary_df = load_network_summary()
    network_members_df = load_network_members()
    network_edges_df = load_network_edges()
    prioritized_leads_df = load_prioritized_leads()
    investigation_summary_df = load_investigation_summary()
    review_recommendations_df = load_review_recommendations()
    cross_source_matches_df = load_cross_source_matches()
    cross_source_diagnostics_df = load_cross_source_diagnostics()
    cross_source_summary = load_cross_source_diagnostic_summary()
    statistical_baselines_df = load_statistical_baselines()
    statistical_rarity_df = load_statistical_rarity()
    contextual_adjustments_df = load_contextual_adjustments()
    statistical_summary = load_statistical_summary()
    statistical_calibration_df = load_statistical_calibration_report()

    with st.sidebar:
        st.header("Filters")
        real_data_only = st.toggle("Real Data Only", value=False)
        data_scope = st.selectbox("Data scope", ["All Data", "Real/API Connector Data Only", "Synthetic/Demo Data Only"], index=1 if real_data_only else 0)
        all_source_values = sorted(
            {
                token
                for frame in [fraud_markers_df, entity_risk_df, entities_df, canonical_entities_df]
                for column in ["source_name", "sources"]
                if column in frame.columns
                for value in frame[column].astype(str).tolist()
                for token in _token_set(value)
            }
        )
        selected_sources = st.multiselect("Source filter", options=all_source_values)

    scope_key = "real_only" if real_data_only or data_scope == "Real/API Connector Data Only" else "synthetic_only" if data_scope == "Synthetic/Demo Data Only" else "all"
    fraud_markers_df = filter_dataframe_by_source_scope(fraud_markers_df, scope_key, selected_sources)
    entity_risk_df = filter_dataframe_by_source_scope(entity_risk_df, scope_key, selected_sources)
    entities_df = filter_dataframe_by_source_scope(entities_df, scope_key, selected_sources)
    relationships_df = filter_dataframe_by_source_scope(relationships_df, scope_key, selected_sources)
    canonical_entities_df = filter_dataframe_by_source_scope(canonical_entities_df, scope_key, selected_sources)
    entity_aliases_df = filter_dataframe_by_source_scope(entity_aliases_df, scope_key, selected_sources)
    resolution_matches_df = filter_dataframe_by_source_scope(resolution_matches_df, scope_key, selected_sources)
    investigation_leads_df = filter_dataframe_by_source_scope(investigation_leads_df, scope_key, selected_sources)
    entity_timelines_df = filter_dataframe_by_source_scope(entity_timelines_df, scope_key, selected_sources)
    evidence_packets_df = filter_dataframe_by_source_scope(evidence_packets_df, scope_key, selected_sources)
    network_clusters_df = filter_dataframe_by_source_scope(network_clusters_df, scope_key, selected_sources)
    network_members_df = filter_dataframe_by_source_scope(network_members_df, scope_key, selected_sources)
    network_edges_df = filter_dataframe_by_source_scope(network_edges_df, scope_key, selected_sources)
    prioritized_leads_df = filter_dataframe_by_source_scope(prioritized_leads_df, scope_key, selected_sources)
    cross_source_matches_df = filter_dataframe_by_source_scope(cross_source_matches_df, scope_key, selected_sources)
    statistical_rarity_df = filter_dataframe_by_source_scope(statistical_rarity_df, scope_key, selected_sources)
    contextual_adjustments_df = filter_dataframe_by_source_scope(contextual_adjustments_df, scope_key, selected_sources)

    metrics = build_dashboard_metrics(fraud_markers_df, entities_df, relationships_df, entity_risk_df, investigation_leads_df)
    metric_cols = st.columns(7)
    metric_cols[0].metric("Critical Leads", metrics["critical_leads"])
    metric_cols[1].metric("High Priority Leads", metrics["high_priority_leads"])
    metric_cols[2].metric("High Confidence Leads", metrics["high_confidence_leads"])
    metric_cols[3].metric("Cross-Source Leads", metrics["cross_source_leads"])
    metric_cols[4].metric("Avg Fraud Marker Score", metrics["avg_fraud_marker_score"])
    metric_cols[5].metric("Avg Confidence", metrics["avg_confidence"])
    metric_cols[6].metric("Newest Leads", metrics["newest_leads"])

    st.subheader("Cross-Source Intelligence")
    if cross_source_matches_df.empty:
        st.info("Cross-source intelligence is not available yet. Run `python -m src.run_pipeline --reset --clear-lead-packages --include-connectors --health-check` first.")
    else:
        cross_metrics = st.columns(6)
        cross_metrics[0].metric("Total Matches", int(len(cross_source_matches_df)))
        cross_metrics[1].metric("Auto Matches", int((cross_source_matches_df["decision"] == "AUTO_MATCH").sum()))
        cross_metrics[2].metric("Review Candidates", int((cross_source_matches_df["decision"] == "REVIEW").sum()))
        cross_metrics[3].metric("Real Cross-Source Entities", int(cross_source_matches_df[(cross_source_matches_df["decision"] == "AUTO_MATCH") & (cross_source_matches_df["independent_real_source_count"] >= 2)]["canonical_entity_id"].nunique()))
        cross_metrics[4].metric("Cross-Source Markers", int((fraud_markers_df.get("marker_category", pd.Series(dtype=str)).astype(str) == "cross_source").sum()))
        cross_metrics[5].metric("Cluster Leads", int((prioritized_leads_df["lead_type"] == "CROSS_SOURCE_CLUSTER").sum()) if not prioritized_leads_df.empty else 0)

        cx_filters = st.columns(7)
        with cx_filters[0]:
            cx_real_only = st.toggle("Cross-Source Real Only", value=True)
        with cx_filters[1]:
            cx_source_pair = st.selectbox("Source Pair", ["All", *sorted(cross_source_matches_df["source_pair"].dropna().astype(str).unique())], key="cx_pair")
        with cx_filters[2]:
            cx_entity_type = st.selectbox("Entity Type", ["All", *sorted(cross_source_matches_df["entity_type"].dropna().astype(str).unique())], key="cx_entity_type")
        with cx_filters[3]:
            cx_decision = st.selectbox("Decision", ["All", *sorted(cross_source_matches_df["decision"].dropna().astype(str).unique())], key="cx_decision")
        with cx_filters[4]:
            cx_match_method = st.selectbox("Match Method", ["All", *sorted(cross_source_matches_df["match_method"].dropna().astype(str).unique())], key="cx_method")
        with cx_filters[5]:
            cx_confidence = st.selectbox("Confidence", ["All", "0.90+", "0.80+", "0.70+"], key="cx_confidence")
        with cx_filters[6]:
            cx_marker = st.selectbox("Fraud Marker", ["All", *sorted(fraud_markers_df[fraud_markers_df.get("marker_category", pd.Series(dtype=str)).astype(str) == "cross_source"]["marker_name"].dropna().astype(str).unique())], key="cx_marker")

        cross_view = cross_source_matches_df.copy()
        if cx_real_only:
            cross_view = cross_view[cross_view["independent_real_source_count"] >= 2]
        if cx_source_pair != "All":
            cross_view = cross_view[cross_view["source_pair"].astype(str) == cx_source_pair]
        if cx_entity_type != "All":
            cross_view = cross_view[cross_view["entity_type"].astype(str) == cx_entity_type]
        if cx_decision != "All":
            cross_view = cross_view[cross_view["decision"].astype(str) == cx_decision]
        if cx_match_method != "All":
            cross_view = cross_view[cross_view["match_method"].astype(str) == cx_match_method]
        if cx_confidence != "All":
            threshold = float(cx_confidence.replace("+", ""))
            cross_view = cross_view[cross_view["confidence"] >= threshold]
        if cx_marker != "All":
            marker_entity_ids = set(fraud_markers_df[fraud_markers_df["marker_name"].astype(str) == cx_marker]["entity_id"].astype(str))
            cross_view = cross_view[cross_view["canonical_entity_id"].astype(str).isin(marker_entity_ids)]

        st.dataframe(
            cross_view[["cross_source_match_id", "canonical_entity_id", "entity_type", "source_pair", "match_method", "confidence", "decision", "independent_real_source_count"]],
            use_container_width=True,
            hide_index=True,
        )

        selected_match_options = cross_view["cross_source_match_id"].astype(str).tolist() or cross_source_matches_df["cross_source_match_id"].astype(str).tolist()
        selected_match_id = st.selectbox("Select cross-source match", selected_match_options, key="selected_cross_source_match")
        selected_match = cross_source_matches_df[cross_source_matches_df["cross_source_match_id"].astype(str) == selected_match_id].iloc[0]
        match_cols = st.columns(2)
        with match_cols[0]:
            st.markdown("**Left Source Evidence**")
            st.write(f"Source: {selected_match.get('left_source_name', '')}")
            st.write(f"Source Record: {selected_match.get('left_source_record_id', '')}")
            st.write(f"Entity: {selected_match.get('left_entity_id', '')}")
        with match_cols[1]:
            st.markdown("**Right Source Evidence**")
            st.write(f"Source: {selected_match.get('right_source_name', '')}")
            st.write(f"Source Record: {selected_match.get('right_source_record_id', '')}")
            st.write(f"Entity: {selected_match.get('right_entity_id', '')}")
        st.markdown("**Cross-Source Evidence**")
        st.write(str(selected_match.get("evidence", "")))
        st.write(str(selected_match.get("why_sources_independent", "")))
        st.markdown("**Diagnostics Summary**")
        if cross_source_summary:
            st.json(cross_source_summary)
        elif not cross_source_diagnostics_df.empty:
            st.dataframe(cross_source_diagnostics_df, use_container_width=True, hide_index=True)

    st.subheader("Statistical Risk")
    if statistical_rarity_df.empty:
        st.info("Statistical risk is not available yet. Run `python -m src.run_pipeline --reset --clear-lead-packages --include-connectors --health-check` first.")
    else:
        stats_metrics = st.columns(7)
        stats_metrics[0].metric("Rarest Entities", int(statistical_rarity_df["entity_id"].nunique()))
        stats_metrics[1].metric("Routine Review", int((statistical_rarity_df["rarity_level"] == "ROUTINE_REVIEW").sum()))
        stats_metrics[2].metric("Elevated Review", int((statistical_rarity_df["rarity_level"] == "ELEVATED_REVIEW").sum()))
        stats_metrics[3].metric("Immediate Review", int((statistical_rarity_df["rarity_level"] == "IMMEDIATE_REVIEW").sum()))
        stats_metrics[4].metric("Extreme Outliers", int((statistical_rarity_df["rarity_level"] == "EXTREME_OUTLIER").sum()))
        stats_metrics[5].metric("Insufficient Baselines", int((statistical_rarity_df["rarity_level"] == "INSUFFICIENT_BASELINE").sum()))
        stats_metrics[6].metric("Max Rarity Score", float(pd.to_numeric(statistical_rarity_df["rarity_score"], errors="coerce").fillna(0).max()))

        stat_filters = st.columns(8)
        with stat_filters[0]:
            stat_review_level = st.selectbox("Review Level", ["All", *sorted(statistical_rarity_df["rarity_level"].dropna().astype(str).unique())], key="stat_level")
        with stat_filters[1]:
            stat_source = st.selectbox("Stat Source", ["All", *sorted({token for value in statistical_rarity_df["source_name"].astype(str) for token in _token_set(value)})], key="stat_source")
        with stat_filters[2]:
            stat_entity_type = st.selectbox("Stat Entity Type", ["All", *sorted(statistical_rarity_df["entity_type"].dropna().astype(str).unique())], key="stat_entity")
        with stat_filters[3]:
            stat_marker = st.selectbox("Stat Marker", ["All", *sorted(statistical_rarity_df["marker_name"].dropna().astype(str).unique())], key="stat_marker")
        with stat_filters[4]:
            stat_jurisdiction = st.selectbox("Jurisdiction", ["All", *sorted([value for value in statistical_rarity_df["jurisdiction"].dropna().astype(str).unique() if value])], key="stat_jurisdiction")
        with stat_filters[5]:
            stat_property_context = st.selectbox("Property Context", ["All", *sorted(statistical_rarity_df["address_context"].dropna().astype(str).unique())], key="stat_context")
        with stat_filters[6]:
            stat_model = st.selectbox("Stat Model", ["All", *sorted(statistical_rarity_df["model_used"].dropna().astype(str).unique())], key="stat_model")
        with stat_filters[7]:
            min_group_size = st.number_input("Min Group Size", min_value=0, value=5, step=1, key="stat_group_size")

        stat_view = statistical_rarity_df.copy()
        if stat_review_level != "All":
            stat_view = stat_view[stat_view["rarity_level"].astype(str) == stat_review_level]
        if stat_source != "All":
            stat_view = stat_view[stat_view["source_name"].astype(str).apply(lambda value: stat_source in _token_set(value))]
        if stat_entity_type != "All":
            stat_view = stat_view[stat_view["entity_type"].astype(str) == stat_entity_type]
        if stat_marker != "All":
            stat_view = stat_view[stat_view["marker_name"].astype(str) == stat_marker]
        if stat_jurisdiction != "All":
            stat_view = stat_view[stat_view["jurisdiction"].astype(str) == stat_jurisdiction]
        if stat_property_context != "All":
            stat_view = stat_view[stat_view["address_context"].astype(str) == stat_property_context]
        if stat_model != "All":
            stat_view = stat_view[stat_view["model_used"].astype(str) == stat_model]
        stat_view = stat_view[pd.to_numeric(stat_view["comparison_group_size"], errors="coerce").fillna(0) >= float(min_group_size)]

        st.dataframe(
            stat_view[["entity_id", "marker_name", "rarity_level", "rarity_score", "observed_value", "expected_value", "comparison_group_size", "address_context", "source_name"]],
            use_container_width=True,
            hide_index=True,
        )

        selected_stat_options = stat_view["entity_id"].astype(str) + " | " + stat_view["marker_name"].astype(str)
        selected_stat_option = st.selectbox("Select statistical result", selected_stat_options.tolist() or (statistical_rarity_df["entity_id"].astype(str) + " | " + statistical_rarity_df["marker_name"].astype(str)).tolist(), key="selected_statistical_result")
        selected_entity_id, selected_marker_name = [part.strip() for part in selected_stat_option.split(" | ", 1)]
        selected_stat = statistical_rarity_df[
            (statistical_rarity_df["entity_id"].astype(str) == selected_entity_id)
            & (statistical_rarity_df["marker_name"].astype(str) == selected_marker_name)
        ].iloc[0]
        selected_adjustment = contextual_adjustments_df[
            (contextual_adjustments_df["entity_id"].astype(str) == selected_entity_id)
            & (contextual_adjustments_df["marker_name"].astype(str) == selected_marker_name)
        ].head(1)

        stat_detail_cols = st.columns(2)
        with stat_detail_cols[0]:
            st.markdown("**Observed Versus Expected**")
            st.write(f"Observed: {selected_stat.get('observed_value', 0)}")
            st.write(f"Expected: {selected_stat.get('expected_value', 0)}")
            st.write(f"Comparison Group Size: {selected_stat.get('comparison_group_size', 0)}")
            st.write(f"Percentile: {selected_stat.get('percentile', 0)}")
            st.write(f"Probability/P-Value: {selected_stat.get('probability_or_p_value', '')}")
            st.write(f"Review Level: {selected_stat.get('rarity_level', '')}")
        with stat_detail_cols[1]:
            st.markdown("**Model And Assumptions**")
            st.write(f"Peer Group: {selected_stat.get('comparison_group', '')}")
            st.write(f"Model Used: {selected_stat.get('model_used', '')}")
            st.write(f"Address/Property Context: {selected_stat.get('address_context', '')}")
            st.write(f"Base Building Address: {selected_stat.get('base_building_address', '')}")
            st.write(f"Unit-Level Address: {selected_stat.get('unit_level_address', '')}")
            st.write(f"Assumptions: {selected_stat.get('assumptions', '')}")

        st.markdown("**Why Threshold Was Crossed**")
        st.write(str(selected_stat.get("explanation", "")))
        if not selected_adjustment.empty:
            adjustment_row = selected_adjustment.iloc[0]
            st.markdown("**Contextual Adjustment**")
            st.write(
                f"Original: {adjustment_row.get('original_marker_score', 0)} | "
                f"Adjustment: {adjustment_row.get('contextual_adjustment', 0)} | "
                f"Adjusted: {adjustment_row.get('adjusted_marker_score', 0)}"
            )
            st.write(str(adjustment_row.get("reason_for_adjustment", "")))
        st.markdown("**Calibration Summary**")
        if statistical_summary:
            st.json(statistical_summary)
        elif not statistical_calibration_df.empty:
            st.dataframe(statistical_calibration_df, use_container_width=True, hide_index=True)

    st.subheader("Investigation Queue")
    if prioritized_leads_df.empty:
        st.info("Prioritized investigation queue is not available yet. Run `python src/investigation/investigation_engine.py` or the full pipeline first.")
    else:
        summary_row = investigation_summary_df.iloc[0] if not investigation_summary_df.empty else pd.Series(dtype=object)
        iq_metrics = st.columns(7)
        iq_metrics[0].metric("Critical Leads", int(summary_row.get("critical_leads", (prioritized_leads_df["priority"] == "CRITICAL").sum())))
        iq_metrics[1].metric("High-Priority Leads", int((prioritized_leads_df["priority"].isin(["CRITICAL", "HIGH"])).sum()))
        iq_metrics[2].metric("High-Confidence Leads", int((prioritized_leads_df["confidence"].isin(["VERY_HIGH", "HIGH"])).sum()))
        iq_metrics[3].metric("Cross-Source Leads", int((prioritized_leads_df["cross_source_match_count"] > 0).sum()))
        iq_metrics[4].metric("Network Leads", int((prioritized_leads_df["lead_type"] == "NETWORK").sum()))
        iq_metrics[5].metric("Needs Validation", int((prioritized_leads_df["evidence_completeness_score"] < 60).sum()))
        iq_metrics[6].metric("Newest Leads", int(len(prioritized_leads_df.head(10))))

        iq_filters = st.columns(8)
        with iq_filters[0]:
            iq_real_only = st.toggle("Queue Real Data Only", value=True)
        with iq_filters[1]:
            iq_priority = st.selectbox("Queue Priority", ["All", *sorted(prioritized_leads_df["priority"].dropna().astype(str).unique())], key="iq_priority")
        with iq_filters[2]:
            iq_confidence = st.selectbox("Queue Confidence", ["All", *sorted(prioritized_leads_df["confidence"].dropna().astype(str).unique())], key="iq_confidence")
        with iq_filters[3]:
            iq_lead_type = st.selectbox("Queue Lead Type", ["All", *sorted(prioritized_leads_df["lead_type"].dropna().astype(str).unique())], key="iq_type")
        with iq_filters[4]:
            iq_entity_type = st.selectbox("Queue Entity Type", ["All", *sorted(prioritized_leads_df["primary_entity_type"].dropna().astype(str).unique())], key="iq_entity_type")
        with iq_filters[5]:
            iq_status = st.selectbox("Queue Status", ["All", *sorted(prioritized_leads_df["status"].dropna().astype(str).unique())], key="iq_status")
        with iq_filters[6]:
            iq_network_presence = st.selectbox("Queue Network", ["All", "With Network", "Without Network"], key="iq_network")
        with iq_filters[7]:
            iq_validation = st.selectbox("Evidence Completeness", ["All", "Needs Validation", "Sufficient"], key="iq_validation")

        queue_df = prioritized_leads_df.copy()
        if iq_real_only:
            queue_df = queue_df[queue_df["contains_real_data"].astype(str).str.lower().isin(["true", "1"])]
        if iq_priority != "All":
            queue_df = queue_df[queue_df["priority"].astype(str) == iq_priority]
        if iq_confidence != "All":
            queue_df = queue_df[queue_df["confidence"].astype(str) == iq_confidence]
        if iq_lead_type != "All":
            queue_df = queue_df[queue_df["lead_type"].astype(str) == iq_lead_type]
        if iq_entity_type != "All":
            queue_df = queue_df[queue_df["primary_entity_type"].astype(str) == iq_entity_type]
        if iq_status != "All":
            queue_df = queue_df[queue_df["status"].astype(str) == iq_status]
        if iq_network_presence == "With Network":
            queue_df = queue_df[queue_df["network_id"].astype(str).str.len() > 0]
        elif iq_network_presence == "Without Network":
            queue_df = queue_df[queue_df["network_id"].astype(str).str.len() == 0]
        if iq_validation == "Needs Validation":
            queue_df = queue_df[queue_df["evidence_completeness_score"] < 60]
        elif iq_validation == "Sufficient":
            queue_df = queue_df[queue_df["evidence_completeness_score"] >= 60]

        st.dataframe(
            queue_df[["lead_id", "lead_type", "title", "priority", "confidence", "risk_score", "fraud_marker_count", "independent_source_count", "network_id", "evidence_completeness_score", "status"]],
            use_container_width=True,
            hide_index=True,
        )

        selected_queue_options = queue_df["lead_id"].astype(str).tolist() or prioritized_leads_df["lead_id"].astype(str).tolist()
        selected_queue_lead_id = st.selectbox("Select prioritized lead", selected_queue_options, key="selected_queue_lead")
        selected_queue_lead = prioritized_leads_df[prioritized_leads_df["lead_id"].astype(str) == selected_queue_lead_id].iloc[0]
        queue_entity_id = str(selected_queue_lead.get("primary_entity_id", ""))
        queue_network_id = str(selected_queue_lead.get("network_id", ""))
        queue_markers = fraud_markers_df[fraud_markers_df["entity_id"].astype(str) == queue_entity_id].copy() if not fraud_markers_df.empty else pd.DataFrame()
        queue_aliases = entity_aliases_df[entity_aliases_df["canonical_entity_id"].astype(str) == queue_entity_id].copy() if not entity_aliases_df.empty else pd.DataFrame()
        queue_relationships = relationships_df[
            (relationships_df["source_entity_id"].astype(str) == queue_entity_id) | (relationships_df["target_entity_id"].astype(str) == queue_entity_id)
        ].copy() if not relationships_df.empty else pd.DataFrame()
        queue_timeline = entity_timelines_df[entity_timelines_df["lead_id"].astype(str) == selected_queue_lead_id].copy() if not entity_timelines_df.empty else pd.DataFrame()
        queue_recommendation = review_recommendations_df[review_recommendations_df["lead_id"].astype(str) == selected_queue_lead_id].copy() if not review_recommendations_df.empty else pd.DataFrame()
        queue_network = network_clusters_df[network_clusters_df["network_id"].astype(str) == queue_network_id].copy() if not network_clusters_df.empty and queue_network_id else pd.DataFrame()
        queue_network_members = network_members_df[network_members_df["network_id"].astype(str) == queue_network_id].copy() if not network_members_df.empty and queue_network_id else pd.DataFrame()
        queue_evidence = evidence_packets_df[evidence_packets_df["lead_id"].astype(str) == selected_queue_lead_id].copy() if not evidence_packets_df.empty else pd.DataFrame()

        queue_detail_cols = st.columns(2)
        with queue_detail_cols[0]:
            st.markdown("**Lead Summary**")
            st.write(selected_queue_lead[["title", "lead_type", "priority", "confidence", "risk_score", "confidence_score", "evidence_completeness_score", "status"]])
            st.markdown("**Why It Was Prioritized**")
            st.write(str(selected_queue_lead.get("explanation", "")))
            st.markdown("**Risk Versus Confidence**")
            st.write(f"Risk: {selected_queue_lead.get('risk_score', 0)} | Confidence: {selected_queue_lead.get('confidence', '')} ({selected_queue_lead.get('confidence_score', 0)})")
            st.markdown("**Recommended Review Actions**")
            if queue_recommendation.empty:
                st.write(str(selected_queue_lead.get("recommended_review", "")))
            else:
                st.dataframe(queue_recommendation, use_container_width=True, hide_index=True)
            st.markdown("**Related Leads**")
            st.write(str(selected_queue_lead.get("related_lead_ids", "")))
            st.markdown("**Export Location**")
            st.write(f"exports/leads/{lead_package_dir_name(selected_queue_lead_id)}")
        with queue_detail_cols[1]:
            st.markdown("**Primary Entity**")
            st.write(str(selected_queue_lead.get("primary_entity_id", "")))
            st.markdown("**Aliases**")
            if queue_aliases.empty:
                st.info("No aliases available.")
            else:
                st.dataframe(queue_aliases[["alias_value", "normalized_alias", "source_name", "source_type"]], use_container_width=True, hide_index=True)
            st.markdown("**Source Provenance**")
            st.write(str(selected_queue_lead.get("source_names", "")))
            st.markdown("**Analyst Notes**")
            st.write(str(selected_queue_lead.get("analyst_notes", "")))

        queue_lower_cols = st.columns(2)
        with queue_lower_cols[0]:
            st.markdown("**Fraud Markers**")
            if queue_markers.empty:
                st.info("No fraud markers linked to this lead.")
            else:
                st.dataframe(queue_markers[["marker_name", "risk_contribution", "confidence", "sources", "explanation"]], use_container_width=True, hide_index=True)
            st.markdown("**Timeline**")
            if queue_timeline.empty:
                st.info("No timeline available.")
            else:
                st.dataframe(queue_timeline[["Date", "Event", "Entity", "Source", "Evidence"]], use_container_width=True, hide_index=True)
            st.markdown("**Evidence Completeness**")
            st.write(f"Score: {selected_queue_lead.get('evidence_completeness_score', 0)}")
            st.write(f"Missing: {selected_queue_lead.get('missing_evidence_fields', '')}")
        with queue_lower_cols[1]:
            st.markdown("**Connected Entities / Relationships**")
            if queue_relationships.empty:
                st.info("No direct relationships available.")
            else:
                st.dataframe(queue_relationships, use_container_width=True, hide_index=True)
            st.markdown("**Network Context**")
            if queue_network.empty:
                st.info("No network context attached.")
            else:
                st.dataframe(queue_network, use_container_width=True, hide_index=True)
                if not queue_network_members.empty:
                    st.dataframe(queue_network_members[["display_name", "entity_type", "bridge_flag", "community_id"]], use_container_width=True, hide_index=True)
            st.markdown("**Evidence Packets**")
            if queue_evidence.empty:
                st.info("No evidence packet rows available.")
            else:
                st.dataframe(queue_evidence[["Primary Entity", "Fraud Markers", "Relationships", "Supporting Evidence", "Source"]], use_container_width=True, hide_index=True)

    st.subheader("Investigation Workspace")
    if investigation_leads_df.empty:
        st.info("Investigation leads are not available yet. Run `python src/run_pipeline.py --include-connectors --health-check` to generate them.")
    else:
        iw_cols = st.columns(6)
        with iw_cols[0]:
            lead_search = st.text_input("Lead Search")
        with iw_cols[1]:
            priority_filter = st.selectbox("Priority Filter", ["All", *sorted(investigation_leads_df["Priority"].dropna().astype(str).unique())])
        with iw_cols[2]:
            status_filter = st.selectbox("Status Filter", ["All", *sorted(investigation_leads_df["Status"].dropna().astype(str).unique())])
        with iw_cols[3]:
            confidence_filter = st.selectbox("Confidence Filter", ["All", *sorted(investigation_leads_df["Confidence"].dropna().astype(str).unique())])
        with iw_cols[4]:
            risk_filter = st.selectbox("Risk Filter", ["All", "80+", "60+", "35+"])
        with iw_cols[5]:
            fraud_marker_filter = st.selectbox("Fraud Marker Filter", ["All", *sorted({token for value in investigation_leads_df["Fraud Markers"].astype(str) for token in _token_set(value)})])

        lead_queue = investigation_leads_df.copy()
        if lead_search:
            mask = lead_queue["Primary Entity"].fillna("").str.contains(lead_search, case=False, na=False)
            mask |= lead_queue["Lead Summary"].fillna("").str.contains(lead_search, case=False, na=False)
            lead_queue = lead_queue[mask]
        if priority_filter != "All":
            lead_queue = lead_queue[lead_queue["Priority"].astype(str) == priority_filter]
        if status_filter != "All":
            lead_queue = lead_queue[lead_queue["Status"].astype(str) == status_filter]
        if confidence_filter != "All":
            lead_queue = lead_queue[lead_queue["Confidence"].astype(str) == confidence_filter]
        if risk_filter == "80+":
            lead_queue = lead_queue[lead_queue["Risk Score"] >= 80]
        elif risk_filter == "60+":
            lead_queue = lead_queue[lead_queue["Risk Score"] >= 60]
        elif risk_filter == "35+":
            lead_queue = lead_queue[lead_queue["Risk Score"] >= 35]
        if fraud_marker_filter != "All":
            lead_queue = lead_queue[lead_queue["Fraud Markers"].astype(str).apply(lambda value: fraud_marker_filter in _token_set(value))]

        st.markdown("**Lead Queue**")
        st.dataframe(
            lead_queue[["lead_id", "Primary Entity", "Priority", "Risk Score", "Confidence", "Fraud Marker Count", "Supporting Source Count", "Relationship Count", "Status"]],
            use_container_width=True,
            hide_index=True,
        )

        lead_options = lead_queue["lead_id"].astype(str).tolist() or investigation_leads_df["lead_id"].astype(str).tolist()
        selected_lead_id = st.selectbox("Select a lead", lead_options, key="investigation_lead")
        selected_lead = investigation_leads_df[investigation_leads_df["lead_id"].astype(str) == selected_lead_id].iloc[0]
        selected_timeline = entity_timelines_df[entity_timelines_df["lead_id"].astype(str) == selected_lead_id].copy()
        selected_evidence = evidence_packets_df[evidence_packets_df["lead_id"].astype(str) == selected_lead_id].copy()
        selected_aliases = entity_aliases_df[entity_aliases_df["canonical_entity_id"].astype(str) == str(selected_lead["entity_id"])].copy() if not entity_aliases_df.empty else pd.DataFrame()
        selected_markers = fraud_markers_df[fraud_markers_df["entity_id"].astype(str) == str(selected_lead["entity_id"])].copy() if not fraud_markers_df.empty else pd.DataFrame()
        selected_relationships = relationships_df[
            (relationships_df["source_entity_id"].astype(str) == str(selected_lead["entity_id"]))
            | (relationships_df["target_entity_id"].astype(str) == str(selected_lead["entity_id"]))
        ].copy() if not relationships_df.empty else pd.DataFrame()

        detail_cols = st.columns(2)
        with detail_cols[0]:
            st.markdown("**Entity Profile**")
            st.write(selected_lead[["Primary Entity", "Lead Title", "Lead Summary", "Priority", "Status", "Risk Score", "Confidence", "Recommended Review"]])
            st.markdown("**Aliases**")
            if selected_aliases.empty:
                st.info("No aliases available for this lead.")
            else:
                st.dataframe(selected_aliases[["alias_value", "normalized_alias", "source_name", "source_type", "source_record_id"]], use_container_width=True, hide_index=True)
            st.markdown("**Fraud Marker Breakdown**")
            if selected_markers.empty:
                st.info("No fraud markers available for this lead.")
            else:
                st.dataframe(selected_markers[["marker_name", "marker_category", "risk_contribution", "confidence", "sources", "explanation"]], use_container_width=True, hide_index=True)
        with detail_cols[1]:
            st.markdown("**Source Provenance**")
            if selected_evidence.empty:
                st.info("No evidence packet is available for this lead yet.")
            else:
                st.dataframe(selected_evidence[["Source", "Record ID", "Connector", "Import Date", "Sources", "Supporting Evidence", "Risk Explanation"]], use_container_width=True, hide_index=True)
            st.markdown("**Connected Sources**")
            st.write(str(selected_lead.get("source_name", "")))
            st.markdown("**Connected Entities**")
            if not selected_evidence.empty:
                st.write(selected_evidence.iloc[0].get("Connected Entities", ""))

        lower_cols = st.columns(2)
        with lower_cols[0]:
            st.markdown("**Timeline**")
            if selected_timeline.empty:
                st.info("No timeline events available for this lead.")
            else:
                st.dataframe(selected_timeline[["Date", "Event", "Source", "Evidence"]], use_container_width=True, hide_index=True)
            st.markdown("**Evidence**")
            if not selected_evidence.empty:
                st.dataframe(selected_evidence[["Fraud Markers", "Relationships", "Supporting Evidence", "Recommended Review"]], use_container_width=True, hide_index=True)
            else:
                st.info("No evidence packet rows available for this lead.")
        with lower_cols[1]:
            st.markdown("**Relationships**")
            if selected_relationships.empty:
                st.info("No relationships available for this lead.")
            else:
                st.dataframe(selected_relationships, use_container_width=True, hide_index=True)
            st.markdown("**Risk Explanation**")
            st.write(str(selected_lead.get("Risk Explanation", "")))

    st.subheader("Network Intelligence")
    if network_clusters_df.empty:
        st.info("Network intelligence outputs are not available yet. Run `python src/run_pipeline.py --include-connectors --health-check` to generate them.")
    else:
        summary_row = network_summary_df.iloc[0] if not network_summary_df.empty else pd.Series(dtype=object)
        network_metric_cols = st.columns(7)
        network_metric_cols[0].metric("Networks Identified", int(summary_row.get("network_count", len(network_clusters_df))))
        network_metric_cols[1].metric("Largest Network", int(summary_row.get("largest_network_size", network_clusters_df["network_size"].max())))
        network_metric_cols[2].metric("Highest Risk", float(summary_row.get("highest_risk_score", network_clusters_df["network_risk_score"].max())))
        network_metric_cols[3].metric("Avg Network Size", float(summary_row.get("average_network_size", pd.to_numeric(network_clusters_df["network_size"], errors="coerce").fillna(0).mean())))
        network_metric_cols[4].metric("Bridge Entities", int(summary_row.get("bridge_entity_count", (network_members_df.get("bridge_flag") == "Yes").sum())))
        network_metric_cols[5].metric("Communities", int(summary_row.get("community_count", network_members_df.get("community_id", pd.Series(dtype=str)).nunique())))
        network_metric_cols[6].metric("Cross-Source Networks", int(summary_row.get("cross_source_network_count", (network_clusters_df["independent_source_count"] > 1).sum())))

        network_cols = st.columns(3)
        with network_cols[0]:
            st.markdown("**Highest Risk Networks**")
            st.dataframe(
                network_clusters_df.sort_values(["network_risk_score", "network_size"], ascending=[False, False]).head(10)[["network_id", "network_priority", "network_risk_score", "network_size", "fraud_marker_count"]],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("**Fastest Growing Networks**")
            st.dataframe(
                network_clusters_df.sort_values(["fast_growth_score", "timeline_event_count"], ascending=[False, False]).head(10)[["network_id", "fast_growth_score", "timeline_event_count", "latest_activity_date"]],
                use_container_width=True,
                hide_index=True,
            )
        with network_cols[1]:
            st.markdown("**Largest Networks**")
            st.dataframe(
                network_clusters_df.sort_values(["network_size", "relationship_count"], ascending=[False, False]).head(10)[["network_id", "network_size", "relationship_count", "community_count"]],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("**Bridge Entities**")
            bridge_rows = network_members_df[network_members_df["bridge_flag"].astype(str) == "Yes"].sort_values(["bridge_score", "degree"], ascending=[False, False]).head(15)
            st.dataframe(
                bridge_rows[["network_id", "display_name", "entity_type", "bridge_score", "degree", "community_id"]],
                use_container_width=True,
                hide_index=True,
            )
        with network_cols[2]:
            st.markdown("**Most Connected Addresses**")
            st.dataframe(
                network_members_df[network_members_df["entity_type"].astype(str) == "address"].sort_values(["degree", "bridge_score"], ascending=[False, False]).head(10)[["network_id", "display_name", "degree", "bridge_score"]],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("**Most Connected Owners**")
            st.dataframe(
                network_members_df[network_members_df["entity_type"].astype(str) == "owner"].sort_values(["degree", "bridge_score"], ascending=[False, False]).head(10)[["network_id", "display_name", "degree", "bridge_score"]],
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("**Most Connected Registered Agents**")
            st.dataframe(
                network_members_df[network_members_df["entity_type"].astype(str) == "registered_agent"].sort_values(["degree", "bridge_score"], ascending=[False, False]).head(10)[["network_id", "display_name", "degree", "bridge_score"]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("**Community Summary**")
        community_summary = (
            network_members_df.groupby(["network_id", "community_id"], dropna=False)
            .agg(
                member_count=("entity_id", "count"),
                bridge_members=("bridge_flag", lambda values: int((pd.Series(values).astype(str) == "Yes").sum())),
                avg_degree=("degree", "mean"),
            )
            .reset_index()
            .sort_values(["member_count", "avg_degree"], ascending=[False, False])
        ) if not network_members_df.empty else pd.DataFrame(columns=["network_id", "community_id", "member_count", "bridge_members", "avg_degree"])
        st.dataframe(community_summary.head(15), use_container_width=True, hide_index=True)

        network_ids = network_clusters_df["network_id"].astype(str).tolist()
        selected_network_id = st.selectbox("Select a network", network_ids, key="selected_network")
        selected_network = network_clusters_df[network_clusters_df["network_id"].astype(str) == selected_network_id].iloc[0]
        selected_network_members = network_members_df[network_members_df["network_id"].astype(str) == selected_network_id].copy()
        selected_network_edges = network_edges_df[network_edges_df["network_id"].astype(str) == selected_network_id].copy()
        selected_member_ids = set(selected_network_members["entity_id"].astype(str).tolist())
        selected_network_markers = fraud_markers_df[fraud_markers_df["entity_id"].astype(str).isin(selected_member_ids)].copy() if not fraud_markers_df.empty else pd.DataFrame()
        selected_network_timeline = entity_timelines_df[entity_timelines_df["entity_id"].astype(str).isin(selected_member_ids)].copy() if not entity_timelines_df.empty else pd.DataFrame()
        selected_network_evidence = evidence_packets_df[evidence_packets_df["entity_id"].astype(str).isin(selected_member_ids)].copy() if not evidence_packets_df.empty else pd.DataFrame()

        network_detail_cols = st.columns(2)
        with network_detail_cols[0]:
            st.markdown("**Network Summary**")
            st.write(selected_network[["network_id", "network_priority", "network_risk_score", "network_confidence", "network_size", "relationship_count", "fraud_marker_count", "independent_source_count", "cross_source_matches", "community_count"]])
            st.markdown("**Cluster Statistics**")
            st.write(selected_network[["business_count", "address_count", "property_count", "owner_count", "registered_agent_count", "officer_count", "relationship_density", "entity_diversity", "bridge_entity_count"]])
            st.markdown("**Explainability**")
            st.write(str(selected_network.get("explanation", "")))
        with network_detail_cols[1]:
            st.markdown("**Source Provenance**")
            st.write(str(selected_network.get("supporting_sources", "")))
            st.markdown("**Bridge Entities**")
            bridge_members = selected_network_members[selected_network_members["bridge_flag"].astype(str) == "Yes"]
            if bridge_members.empty:
                st.info("No bridge entities identified for this network.")
            else:
                st.dataframe(bridge_members[["display_name", "entity_type", "bridge_score", "disconnected_groups", "source_name"]], use_container_width=True, hide_index=True)

        network_lower_cols = st.columns(2)
        with network_lower_cols[0]:
            st.markdown("**Members**")
            st.dataframe(selected_network_members[["community_id", "display_name", "entity_type", "degree", "degree_centrality", "bridge_flag", "marker_count"]], use_container_width=True, hide_index=True)
            st.markdown("**Fraud Markers**")
            if selected_network_markers.empty:
                st.info("No fraud markers attached to this network.")
            else:
                st.dataframe(selected_network_markers[["entity_id", "marker_name", "risk_contribution", "confidence", "sources", "explanation"]], use_container_width=True, hide_index=True)
        with network_lower_cols[1]:
            st.markdown("**Relationships**")
            st.dataframe(selected_network_edges[["relationship_type", "source_entity_id", "target_entity_id", "confidence", "source_name"]], use_container_width=True, hide_index=True)
            st.markdown("**Timeline**")
            if selected_network_timeline.empty:
                st.info("No timeline events available for this network.")
            else:
                st.dataframe(selected_network_timeline[["Date", "Event", "Entity", "Source", "Evidence"]], use_container_width=True, hide_index=True)
            st.markdown("**Evidence**")
            if selected_network_evidence.empty:
                st.info("No evidence packets available for this network.")
            else:
                st.dataframe(selected_network_evidence[["Primary Entity", "Fraud Markers", "Relationships", "Supporting Evidence", "Source"]], use_container_width=True, hide_index=True)

    st.subheader("Entity Risk")
    if entity_risk_df.empty:
        st.info("Entity risk data is not available yet. Run `python src/run_pipeline.py --include-connectors --health-check`.")
    else:
        er_type = st.selectbox("Entity type", ["All", *sorted(entity_risk_df["entity_type"].dropna().astype(str).unique())], key="er_type")
        er_level = st.selectbox("Risk level", ["All", *sorted(entity_risk_df["risk_level"].dropna().astype(str).unique())], key="er_level")
        er_table = entity_risk_df.copy()
        if er_type != "All":
            er_table = er_table[er_table["entity_type"].astype(str) == er_type]
        if er_level != "All":
            er_table = er_table[er_table["risk_level"].astype(str) == er_level]
        st.dataframe(
            er_table[["entity_id", "display_name", "entity_type", "risk_score", "risk_level", "confidence", "marker_count", "source_count"]],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Fraud Markers")
    if fraud_markers_df.empty:
        st.info("No fraud markers are available yet. Run the fraud marker engine through the pipeline.")
    else:
        fm_cols = st.columns(3)
        with fm_cols[0]:
            selected_marker = st.selectbox("Marker", ["All", *sorted(fraud_markers_df["marker_name"].dropna().astype(str).unique())])
        with fm_cols[1]:
            selected_category = st.selectbox("Category", ["All", *sorted(fraud_markers_df["marker_category"].dropna().astype(str).unique())])
        with fm_cols[2]:
            selected_confidence = st.selectbox("Confidence", ["All", "Very High", "High", "Medium", "Low", "Unknown"])

        marker_table = fraud_markers_df.copy()
        if selected_marker != "All":
            marker_table = marker_table[marker_table["marker_name"].astype(str) == selected_marker]
        if selected_category != "All":
            marker_table = marker_table[marker_table["marker_category"].astype(str) == selected_category]
        if selected_confidence != "All":
            marker_table = marker_table[marker_table["confidence"].astype(str) == selected_confidence]

        st.dataframe(
            marker_table[["entity_id", "marker_name", "marker_category", "risk_contribution", "confidence", "support", "sources", "recommended_review", "explanation"]],
            use_container_width=True,
            hide_index=True,
        )

        breakdown_cols = st.columns(3)
        with breakdown_cols[0]:
            st.markdown("**Top Fraud Markers**")
            st.dataframe(
                fraud_marker_summary_df.sort_values("frequency", ascending=False).head(10)[["marker_name", "marker_category", "frequency"]],
                use_container_width=True,
                hide_index=True,
            )
        with breakdown_cols[1]:
            st.markdown("**Marker Breakdown**")
            marker_breakdown = fraud_markers_df.groupby("marker_name", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
            st.dataframe(marker_breakdown, use_container_width=True, hide_index=True)
        with breakdown_cols[2]:
            st.markdown("**Category Breakdown**")
            category_breakdown = fraud_markers_df.groupby("marker_category", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
            st.dataframe(category_breakdown, use_container_width=True, hide_index=True)

        extra_cols = st.columns(2)
        with extra_cols[0]:
            st.markdown("**Marker Frequency**")
            st.dataframe(
                fraud_marker_summary_df[["marker_name", "frequency", "average_risk_contribution", "average_confidence_score"]].sort_values("frequency", ascending=False),
                use_container_width=True,
                hide_index=True,
            )
        with extra_cols[1]:
            st.markdown("**Source Breakdown**")
            source_rows = []
            for value in fraud_markers_df["sources"].astype(str).tolist():
                for token in _token_set(value):
                    source_rows.append(token)
            source_breakdown = pd.Series(source_rows, dtype=str).value_counts().reset_index()
            if not source_breakdown.empty:
                source_breakdown.columns = ["source_name", "count"]
            st.dataframe(source_breakdown, use_container_width=True, hide_index=True)

    st.subheader("Entity Resolution")
    if canonical_entities_df.empty:
        st.info("Canonical entity outputs are not available yet.")
    else:
        resolution_metrics = build_resolution_metrics(entities_df, canonical_entities_df, resolution_matches_df)
        resolution_cols = st.columns(5)
        resolution_cols[0].metric("Raw Entities", resolution_metrics["raw_entities"])
        resolution_cols[1].metric("Canonical Entities", resolution_metrics["canonical_entities"])
        resolution_cols[2].metric("Entities Merged", resolution_metrics["entities_merged"])
        resolution_cols[3].metric("Review Candidates", resolution_metrics["review_candidates"])
        resolution_cols[4].metric("Cross-Source Canonical", resolution_metrics["cross_source_canonical_entities"])

        resolution_search = st.text_input("Canonical entity search")
        resolution_entity_type = st.selectbox("Resolution entity type", ["All", *sorted(canonical_entities_df["entity_type"].dropna().astype(str).unique())], key="resolution_type")
        resolution_decision = st.selectbox("Resolution decision", ["All", "AUTO_MERGE", "REVIEW", "NO_MERGE"], key="resolution_decision")

        canonical_table = canonical_entities_df.copy()
        if resolution_entity_type != "All":
            canonical_table = canonical_table[canonical_table["entity_type"].astype(str) == resolution_entity_type]
        if resolution_search:
            mask = canonical_table["display_name"].fillna("").str.contains(resolution_search, case=False, na=False)
            mask |= canonical_table["normalized_value"].fillna("").str.contains(resolution_search, case=False, na=False)
            canonical_table = canonical_table[mask]
        st.dataframe(
            canonical_table[["canonical_entity_id", "display_name", "entity_type", "record_count", "source_count", "source_name", "resolution_method", "resolution_confidence"]],
            use_container_width=True,
            hide_index=True,
        )

        matches_table = resolution_matches_df.copy()
        if resolution_entity_type != "All" and not matches_table.empty:
            matches_table = matches_table[matches_table["entity_type"].astype(str) == resolution_entity_type]
        if resolution_decision != "All" and not matches_table.empty:
            matches_table = matches_table[matches_table["decision"].astype(str) == resolution_decision]
        st.dataframe(
            matches_table[["entity_type", "decision", "match_method", "confidence_score", "left_entity_id", "right_entity_id", "source_name", "evidence"]],
            use_container_width=True,
            hide_index=True,
        )

        aliases_table = entity_aliases_df.copy()
        if resolution_search and not aliases_table.empty:
            mask = aliases_table["alias_value"].fillna("").str.contains(resolution_search, case=False, na=False)
            mask |= aliases_table["normalized_alias"].fillna("").str.contains(resolution_search, case=False, na=False)
            aliases_table = aliases_table[mask]
        st.dataframe(
            aliases_table[["canonical_entity_id", "original_entity_id", "alias_value", "normalized_alias", "source_name", "source_type", "resolution_method", "confidence_score"]],
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("Relationship Explorer")
    if entities_df.empty or relationships_df.empty:
        st.info("Entity and relationship files are not available yet.")
        return

    selected_entity_type = st.selectbox("Entity type filter", ["All", *sorted(entities_df["entity_type"].dropna().astype(str).unique())], key="relationship_entity_type")
    selected_relationship_type = st.selectbox("Relationship type filter", ["All", *sorted(relationships_df["relationship_type"].dropna().astype(str).unique())], key="relationship_type")
    selected_relationship_source = st.selectbox("Relationship source filter", ["All", *sorted({token for value in relationships_df["source_name"].astype(str) for token in _token_set(value)})], key="relationship_source")
    selected_direction = st.selectbox("Direction", ["outgoing", "incoming", "both"], key="relationship_direction")
    explorer_entities = entities_df.copy()
    if selected_entity_type != "All":
        explorer_entities = explorer_entities[explorer_entities["entity_type"].astype(str) == selected_entity_type]
    entity_options = explorer_entities["entity_id"].astype(str).tolist()
    if not entity_options:
        st.info("No entities match the selected entity-type filter.")
        return
    selected_entity_id = st.selectbox("Select an entity", entity_options, format_func=lambda entity_id: explorer_entities.loc[explorer_entities["entity_id"].astype(str) == entity_id, "display_name"].iloc[0], key="relationship_entity")
    explorer_df = build_relationship_explorer_data(entities_df, relationships_df, selected_entity_id)
    if selected_relationship_type != "All":
        explorer_df = explorer_df[explorer_df["relationship_type"].astype(str) == selected_relationship_type]
    if selected_relationship_source != "All" and "source_name" in relationships_df.columns:
        source_filter = relationships_df[
            ((relationships_df["source_entity_id"].astype(str) == selected_entity_id) | (relationships_df["target_entity_id"].astype(str) == selected_entity_id))
            & relationships_df["source_name"].astype(str).apply(lambda value: selected_relationship_source in _token_set(value))
        ]
        allowed_pairs = set(source_filter["target_entity_id"].astype(str)).union(set(source_filter["source_entity_id"].astype(str)))
        explorer_df = explorer_df[explorer_df["connected_entity_id"].astype(str).isin(allowed_pairs)]
    if selected_direction != "both":
        direction_ids = relationships_df[relationships_df["source_entity_id"].astype(str) == selected_entity_id]["target_entity_id"].astype(str) if selected_direction == "outgoing" else relationships_df[relationships_df["target_entity_id"].astype(str) == selected_entity_id]["source_entity_id"].astype(str)
        explorer_df = explorer_df[explorer_df["connected_entity_id"].astype(str).isin(set(direction_ids))]
    st.caption(
        f"Relationship count: {len(explorer_df)} | Supporting sources: {explorer_df['source_table'].nunique() if not explorer_df.empty else 0} | Evidence count: {len(explorer_df)}"
    )
    st.dataframe(explorer_df[["relationship_type", "connected_entity_name", "connected_entity_type", "source_table"]], use_container_width=True, hide_index=True)

    if not compatibility_report_df.empty:
        with st.expander("Compatibility Anomaly Report"):
            st.dataframe(
                compatibility_report_df[["Risk Score", "Risk Level", "Rule Triggered", "Supporting Evidence", "Entity IDs", "source_name", "source_type"]],
                use_container_width=True,
                hide_index=True,
            )

def main() -> None:
    st.set_page_config(page_title="OpenFraud Analyst Workbench", layout="wide")
    config = load_dashboard_config()
    st.title("OpenFraud Analyst Workbench")
    st.caption("Local analyst workstation for explainable investigative leads. All results are leads only, not proof of fraud.")

    fraud_markers_df = load_fraud_markers()
    fraud_marker_summary_df = load_fraud_marker_summary()
    compatibility_report_df = load_report()
    entities_df = load_entities()
    relationships_df = load_relationships()
    entity_risk_df = load_entity_risk()
    canonical_entities_df = load_canonical_entities()
    entity_aliases_df = load_entity_aliases()
    investigation_leads_df = load_investigation_leads()
    entity_timelines_df = load_entity_timelines()
    evidence_packets_df = load_evidence_packets()
    network_clusters_df = load_network_clusters()
    network_summary_df = load_network_summary()
    network_members_df = load_network_members()
    network_edges_df = load_network_edges()
    prioritized_leads_df = load_prioritized_leads()
    investigation_summary_df = load_investigation_summary()
    review_recommendations_df = load_review_recommendations()
    cross_source_matches_df = load_cross_source_matches()
    cross_source_diagnostics_df = load_cross_source_diagnostics()
    cross_source_summary = load_cross_source_diagnostic_summary()
    statistical_baselines_df = load_statistical_baselines()
    statistical_rarity_df = load_statistical_rarity()
    contextual_adjustments_df = load_contextual_adjustments()
    statistical_summary = load_statistical_summary()
    statistical_calibration_df = load_statistical_calibration_report()
    analyst_state_df = load_analyst_state(WORKBENCH_ANALYST_STATE_PATH if WORKBENCH_ANALYST_STATE_PATH.exists() else ANALYST_STATE_PATH)
    analyst_history_df = load_analyst_history(WORKBENCH_ANALYST_HISTORY_PATH if WORKBENCH_ANALYST_HISTORY_PATH.exists() else ANALYST_HISTORY_PATH)
    saved_searches = load_saved_searches(WORKBENCH_SAVED_SEARCHES_PATH if WORKBENCH_SAVED_SEARCHES_PATH.exists() else SAVED_SEARCHES_PATH)
    source_health_df = build_source_health_report(load_manifest_sources(), load_api_sources(), REPO_ROOT / "data" / "processed")

    nav_options = [
        "Overview",
        "Investigation Queue",
        "Fraud Markers",
        "Statistical Risk",
        "Network Intelligence",
        "Cross Source Intelligence",
        "Entity Explorer",
        "Reports",
        "Source Health",
    ]
    with st.sidebar:
        navigation = st.radio(
            "Navigation",
            nav_options,
            index=nav_options.index(config.get("default_navigation", "Overview")) if config.get("default_navigation", "Overview") in nav_options else 0,
        )
        real_data_only = st.toggle("Real Data Only", value=bool(config.get("real_data_only", False)))
        scope_key = "real_only" if real_data_only else str(config.get("default_filters", {}).get("source_scope", "all"))
        all_source_values = sorted(
            {
                token
                for frame in [fraud_markers_df, entity_risk_df, entities_df, canonical_entities_df, prioritized_leads_df]
                for column in ["source_name", "source_names", "sources"]
                if column in frame.columns
                for value in frame[column].astype(str).tolist()
                for token in _token_set(value)
            }
        )
        selected_sources = st.multiselect("Source Filter", options=all_source_values)
        saved_search_labels = ["None", *[str(item.get("name", "")) for item in saved_searches]]
        selected_saved_search = st.selectbox("Saved Search", saved_search_labels)

    fraud_markers_df = filter_dataframe_by_source_scope(fraud_markers_df, scope_key, selected_sources)
    entities_df = filter_dataframe_by_source_scope(entities_df, scope_key, selected_sources)
    relationships_df = filter_dataframe_by_source_scope(relationships_df, scope_key, selected_sources)
    entity_risk_df = filter_dataframe_by_source_scope(entity_risk_df, scope_key, selected_sources)
    canonical_entities_df = filter_dataframe_by_source_scope(canonical_entities_df, scope_key, selected_sources)
    entity_aliases_df = filter_dataframe_by_source_scope(entity_aliases_df, scope_key, selected_sources)
    investigation_leads_df = filter_dataframe_by_source_scope(investigation_leads_df, scope_key, selected_sources)
    entity_timelines_df = filter_dataframe_by_source_scope(entity_timelines_df, scope_key, selected_sources)
    evidence_packets_df = filter_dataframe_by_source_scope(evidence_packets_df, scope_key, selected_sources)
    network_clusters_df = filter_dataframe_by_source_scope(network_clusters_df, scope_key, selected_sources)
    network_members_df = filter_dataframe_by_source_scope(network_members_df, scope_key, selected_sources)
    network_edges_df = filter_dataframe_by_source_scope(network_edges_df, scope_key, selected_sources)
    prioritized_leads_df = filter_dataframe_by_source_scope(prioritized_leads_df, scope_key, selected_sources)
    cross_source_matches_df = filter_dataframe_by_source_scope(cross_source_matches_df, scope_key, selected_sources)
    statistical_rarity_df = filter_dataframe_by_source_scope(statistical_rarity_df, scope_key, selected_sources)
    contextual_adjustments_df = filter_dataframe_by_source_scope(contextual_adjustments_df, scope_key, selected_sources)

    if selected_saved_search != "None":
        search_config = next((item for item in saved_searches if str(item.get("name", "")) == selected_saved_search), {})
        prioritized_leads_df = build_queue_view(
            prioritized_leads_df,
            priority=str(search_config.get("priority", "All")),
            confidence=str(search_config.get("confidence", "All")),
            source_name=str(search_config.get("source_name", "All")),
            marker=str(search_config.get("marker", "All")),
            network_mode=str(search_config.get("network_mode", "All")),
            entity_type=str(search_config.get("entity_type", "All")),
            status=str(search_config.get("status", "All")),
        )

    metrics = build_dashboard_metrics(fraud_markers_df, entities_df, relationships_df, entity_risk_df, investigation_leads_df)
    summary_row = investigation_summary_df.iloc[0].to_dict() if not investigation_summary_df.empty else {}
    metric_cols = st.columns(9)
    metric_cols[0].metric("Total Leads", int(summary_row.get("total_leads", len(prioritized_leads_df))))
    metric_cols[1].metric("Critical", int(summary_row.get("critical_leads", 0)))
    metric_cols[2].metric("High", int(summary_row.get("high_leads", 0)))
    metric_cols[3].metric("Cross Source", int(summary_row.get("cross_source_leads", 0)))
    metric_cols[4].metric("Networks", int(summary_row.get("network_leads", 0)))
    metric_cols[5].metric("Statistical Outliers", int((statistical_rarity_df.get("rarity_level", pd.Series(dtype=str)).astype(str).isin(["ELEVATED_REVIEW", "IMMEDIATE_REVIEW", "EXTREME_OUTLIER"])).sum()) if not statistical_rarity_df.empty else 0)
    metric_cols[6].metric("Average Confidence", round(float(summary_row.get("average_confidence", metrics["avg_confidence"])), 2))
    metric_cols[7].metric("Average Risk", round(float(summary_row.get("average_risk", metrics["avg_fraud_marker_score"])), 2))
    metric_cols[8].metric("Real Data Coverage", int(prioritized_leads_df["contains_real_data"].astype(bool).sum()) if not prioritized_leads_df.empty and "contains_real_data" in prioritized_leads_df.columns else 0)

    if navigation == "Overview":
        left, right = st.columns(2)
        with left:
            st.subheader("Queue Snapshot")
            st.dataframe(prioritized_leads_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not prioritized_leads_df.empty else st.info("Run the pipeline to populate the queue.")
        with right:
            st.subheader("Recent History")
            st.dataframe(analyst_history_df.tail(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not analyst_history_df.empty else st.info("No analyst history yet.")
        st.subheader("Saved Searches")
        st.dataframe(pd.DataFrame(saved_searches), use_container_width=True, hide_index=True) if saved_searches else st.info("No saved searches.")

    elif navigation == "Investigation Queue":
        if prioritized_leads_df.empty:
            st.info("Prioritized leads are not available yet.")
        else:
            filters = st.columns(8)
            priority = filters[0].selectbox("Risk", ["All", *sorted(prioritized_leads_df["priority"].astype(str).unique())])
            confidence = filters[1].selectbox("Confidence", ["All", *sorted(prioritized_leads_df["confidence"].astype(str).unique())])
            status = filters[2].selectbox("Status", ["All", *sorted(prioritized_leads_df["status"].astype(str).unique())])
            entity_type = filters[3].selectbox("Entity Type", ["All", *sorted(prioritized_leads_df["primary_entity_type"].astype(str).unique())])
            source_name = filters[4].selectbox("Source", ["All", *sorted({token for value in prioritized_leads_df["source_names"].astype(str) for token in _token_set(value)})])
            marker = filters[5].selectbox("Marker", ["All", *sorted({token for value in prioritized_leads_df["fraud_markers"].astype(str) for token in _token_set(value)})])
            network_mode = filters[6].selectbox("Network", ["All", "With Network", "Without Network"])
            reviewed_mode = filters[7].selectbox("Reviewed", ["All", "Reviewed", "Needs Review"])
            queue_view = build_queue_view(
                prioritized_leads_df,
                priority=priority,
                confidence=confidence,
                source_name=source_name,
                marker=marker,
                network_mode=network_mode,
                entity_type=entity_type,
                status=status,
                reviewed_mode=reviewed_mode,
            )
            st.dataframe(queue_view.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True)
            with st.expander("Save Current Search"):
                search_name = st.text_input("Search name")
                if st.button("Save Search") and search_name.strip():
                    new_searches = [item for item in saved_searches if str(item.get("name", "")) != search_name.strip()]
                    new_searches.append({"name": search_name.strip(), "priority": priority, "confidence": confidence, "status": status, "entity_type": entity_type, "source_name": source_name, "marker": marker, "network_mode": network_mode})
                    save_saved_searches(new_searches, SAVED_SEARCHES_PATH)
                    st.success("Saved search written locally.")

            selected_lead_id = st.selectbox("Lead", queue_view["lead_id"].astype(str).tolist() or prioritized_leads_df["lead_id"].astype(str).tolist())
            selected_lead = prioritized_leads_df[prioritized_leads_df["lead_id"].astype(str) == selected_lead_id].iloc[0]
            entity_id = str(selected_lead.get("primary_entity_id", ""))
            st.subheader("Why")
            st.write(str(selected_lead.get("explanation", "")))
            st.write(str(selected_lead.get("recommended_review", "")))
            state_col, notes_col = st.columns(2)
            with state_col:
                new_status = st.selectbox("Lead Status", ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"], index=0 if str(selected_lead.get("status", "NEW")) not in ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"] else ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"].index(str(selected_lead.get("status", "NEW"))))
                reviewer = st.text_input("Reviewer", value=str(selected_lead.get("reviewer", "")))
                follow_up = st.selectbox("Follow-up", ["", "Yes", "No"], index=0)
                bookmark = st.toggle("Bookmark", value=str(selected_lead.get("bookmark", "false")).lower() == "true")
            with notes_col:
                disposition = st.text_input("Disposition", value=str(selected_lead.get("disposition", "")))
                priority_override = st.text_input("Priority Override", value=str(selected_lead.get("priority_override", "")))
                notes = st.text_area("Notes", value=str(selected_lead.get("analyst_notes", "")))
                if st.button("Save Analyst State"):
                    updated_state, updated_history = update_analyst_record(
                        analyst_state_df,
                        analyst_history_df,
                        lead_id=selected_lead_id,
                        reviewer=reviewer,
                        updates={
                            "status": new_status,
                            "reviewer": reviewer,
                            "follow_up_needed": follow_up,
                            "bookmark": "true" if bookmark else "false",
                            "disposition": disposition,
                            "priority_override": priority_override,
                            "analyst_notes": notes,
                            "review_date": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
                        },
                    )
                    persist_analyst_state(updated_state, updated_history, state_path=ANALYST_STATE_PATH, history_path=ANALYST_HISTORY_PATH)
                    st.success("Analyst state saved.")
            marker_rows = fraud_markers_df[fraud_markers_df["entity_id"].astype(str) == entity_id]
            relationship_rows = relationships_df[(relationships_df["source_entity_id"].astype(str) == entity_id) | (relationships_df["target_entity_id"].astype(str) == entity_id)]
            evidence_rows = evidence_packets_df[evidence_packets_df["entity_id"].astype(str) == entity_id]
            timeline_rows = entity_timelines_df[entity_timelines_df["entity_id"].astype(str) == entity_id]
            lower_left, lower_right = st.columns(2)
            with lower_left:
                st.subheader("Fraud Markers")
                st.dataframe(marker_rows, use_container_width=True, hide_index=True) if not marker_rows.empty else st.info("No markers.")
                st.subheader("Timeline")
                st.dataframe(timeline_rows, use_container_width=True, hide_index=True) if not timeline_rows.empty else st.info("No timeline.")
            with lower_right:
                st.subheader("Relationships")
                st.dataframe(relationship_rows, use_container_width=True, hide_index=True) if not relationship_rows.empty else st.info("No relationships.")
                st.subheader("Evidence")
                st.dataframe(evidence_rows, use_container_width=True, hide_index=True) if not evidence_rows.empty else st.info("No evidence.")

    elif navigation == "Fraud Markers":
        st.dataframe(fraud_markers_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not fraud_markers_df.empty else st.info("No fraud markers available.")
        st.subheader("Summary")
        st.dataframe(fraud_marker_summary_df, use_container_width=True, hide_index=True) if not fraud_marker_summary_df.empty else st.info("No marker summary available.")

    elif navigation == "Statistical Risk":
        st.dataframe(statistical_rarity_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not statistical_rarity_df.empty else st.info("No statistical rarity output available.")
        if not contextual_adjustments_df.empty:
            st.subheader("Contextual Adjustments")
            st.dataframe(contextual_adjustments_df, use_container_width=True, hide_index=True)
        if not statistical_baselines_df.empty:
            st.subheader("Baselines")
            st.dataframe(statistical_baselines_df, use_container_width=True, hide_index=True)
        if statistical_summary:
            st.subheader("Marker Summary")
            st.json(statistical_summary)
        elif not statistical_calibration_df.empty:
            st.subheader("Calibration Report")
            st.dataframe(statistical_calibration_df, use_container_width=True, hide_index=True)

    elif navigation == "Network Intelligence":
        st.dataframe(network_clusters_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not network_clusters_df.empty else st.info("No network data available.")
        if len(network_clusters_df) >= 1:
            network_ids = network_clusters_df["network_id"].astype(str).tolist()
            left_id = st.selectbox("Left Network", network_ids)
            right_id = st.selectbox("Right Network", network_ids, index=1 if len(network_ids) > 1 else 0)
            left_row = network_clusters_df[network_clusters_df["network_id"].astype(str) == left_id].iloc[0].to_dict()
            right_row = network_clusters_df[network_clusters_df["network_id"].astype(str) == right_id].iloc[0].to_dict()
            st.subheader("Comparison")
            st.dataframe(compare_records(left_row, right_row, ["network_risk_score", "network_confidence", "network_size", "fraud_marker_count", "relationship_count", "cross_source_matches", "source_name"]), use_container_width=True, hide_index=True)
        if not network_summary_df.empty:
            st.subheader("Summary")
            st.dataframe(network_summary_df, use_container_width=True, hide_index=True)
        if not network_members_df.empty:
            st.subheader("Members")
            st.dataframe(network_members_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True)

    elif navigation == "Cross Source Intelligence":
        st.dataframe(cross_source_matches_df.head(int(config.get("page_size", 25))), use_container_width=True, hide_index=True) if not cross_source_matches_df.empty else st.info("No cross-source matches available.")
        if cross_source_summary:
            st.subheader("Diagnostic Summary")
            st.json(cross_source_summary)
        elif not cross_source_diagnostics_df.empty:
            st.subheader("Diagnostics")
            st.dataframe(cross_source_diagnostics_df, use_container_width=True, hide_index=True)

    elif navigation == "Entity Explorer":
        if canonical_entities_df.empty:
            st.info("No canonical entities available.")
        else:
            entity_options = canonical_entities_df["entity_id"].astype(str).tolist() if "entity_id" in canonical_entities_df.columns else canonical_entities_df["canonical_entity_id"].astype(str).tolist()
            selected_entity_id = st.selectbox("Entity", entity_options)
            compare_entity_id = st.selectbox("Compare With", entity_options, index=1 if len(entity_options) > 1 else 0)
            profile_row = canonical_entities_df[(canonical_entities_df.get("entity_id", canonical_entities_df.get("canonical_entity_id")).astype(str) == selected_entity_id)].iloc[0].to_dict()
            compare_row = canonical_entities_df[(canonical_entities_df.get("entity_id", canonical_entities_df.get("canonical_entity_id")).astype(str) == compare_entity_id)].iloc[0].to_dict()
            profile_aliases = entity_aliases_df[entity_aliases_df["canonical_entity_id"].astype(str) == selected_entity_id]
            profile_markers = fraud_markers_df[fraud_markers_df["entity_id"].astype(str) == selected_entity_id]
            profile_timeline = entity_timelines_df[entity_timelines_df["entity_id"].astype(str) == selected_entity_id]
            profile_relationships = build_relationship_explorer_data(entities_df, relationships_df, selected_entity_id)
            profile_evidence = evidence_packets_df[evidence_packets_df["entity_id"].astype(str) == selected_entity_id]
            left, right = st.columns(2)
            with left:
                st.subheader("Canonical Profile")
                st.json(profile_row)
                st.subheader("Aliases")
                st.dataframe(profile_aliases, use_container_width=True, hide_index=True) if not profile_aliases.empty else st.info("No aliases.")
                st.subheader("Fraud Markers")
                st.dataframe(profile_markers, use_container_width=True, hide_index=True) if not profile_markers.empty else st.info("No markers.")
            with right:
                st.subheader("Timeline")
                st.dataframe(profile_timeline, use_container_width=True, hide_index=True) if not profile_timeline.empty else st.info("No timeline.")
                st.subheader("Relationships")
                st.dataframe(profile_relationships, use_container_width=True, hide_index=True) if not profile_relationships.empty else st.info("No relationships.")
                st.subheader("Evidence")
                st.dataframe(profile_evidence, use_container_width=True, hide_index=True) if not profile_evidence.empty else st.info("No evidence.")
            st.subheader("Side-by-Side Comparison")
            st.dataframe(compare_records(profile_row, compare_row, ["display_name", "entity_type", "source_name", "source_type", "record_count", "source_count", "resolution_confidence"]), use_container_width=True, hide_index=True)

    elif navigation == "Reports":
        report_dir = REPO_ROOT / "exports"
        report_paths = [report_dir / "lead_summary.csv", report_dir / "lead_summary.json", report_dir / "lead_summary.md", report_dir / "lead_summary.html"]
        report_df = pd.DataFrame([{"path": str(path), "exists": path.exists()} for path in report_paths])
        st.dataframe(report_df, use_container_width=True, hide_index=True)
        if not compatibility_report_df.empty:
            st.subheader("Anomaly Report")
            st.dataframe(compatibility_report_df, use_container_width=True, hide_index=True)

    elif navigation == "Source Health":
        st.dataframe(source_health_df, use_container_width=True, hide_index=True)
        pending_review = source_health_df[source_health_df["pending_review"].astype(bool)] if not source_health_df.empty else pd.DataFrame()
        st.subheader("Pending Review")
        st.dataframe(pending_review, use_container_width=True, hide_index=True) if not pending_review.empty else st.info("No sources pending review.")


if __name__ == "__main__":
    main()
