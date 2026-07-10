from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

from src.app.utils.dashboard_filters import parse_bool_series
from src.connectors.source_manifest import REPO_ROOT


def repo_path(*parts: str) -> Path:
    return REPO_ROOT.joinpath(*parts)


FRAUD_MARKERS_PATH = repo_path("data", "processed", "fraud_markers.csv")
FRAUD_MARKER_SUMMARY_PATH = repo_path("data", "processed", "fraud_marker_summary.csv")
ANOMALY_REPORT_PATH = repo_path("data", "processed", "anomaly_report.csv")
ENTITIES_PATH = repo_path("data", "processed", "canonical_entities.csv")
RELATIONSHIPS_PATH = repo_path("data", "processed", "canonical_relationships.csv")
ENTITY_RISK_PATH = repo_path("data", "processed", "entity_risk.csv")
CANONICAL_ENTITIES_PATH = repo_path("data", "processed", "canonical_entities.csv")
ENTITY_ALIASES_PATH = repo_path("data", "processed", "entity_aliases.csv")
ENTITY_RESOLUTION_MATCHES_PATH = repo_path("data", "processed", "entity_resolution_matches.csv")
INVESTIGATION_LEADS_PATH = repo_path("data", "processed", "investigation_leads.csv")
ENTITY_TIMELINES_PATH = repo_path("data", "processed", "entity_timelines.csv")
EVIDENCE_PACKETS_PATH = repo_path("data", "processed", "evidence_packets.csv")
NETWORK_CLUSTERS_PATH = repo_path("data", "processed", "network_clusters.csv")
NETWORK_SUMMARY_PATH = repo_path("data", "processed", "network_summary.csv")
NETWORK_MEMBERS_PATH = repo_path("data", "processed", "network_members.csv")
NETWORK_EDGES_PATH = repo_path("data", "processed", "network_edges.csv")
PRIORITIZED_LEADS_PATH = repo_path("data", "processed", "prioritized_leads.csv")
INVESTIGATION_SUMMARY_PATH = repo_path("data", "processed", "investigation_summary.csv")
REVIEW_RECOMMENDATIONS_PATH = repo_path("data", "processed", "review_recommendations.csv")
CROSS_SOURCE_MATCHES_PATH = repo_path("data", "processed", "cross_source_matches.csv")
CROSS_SOURCE_DIAGNOSTICS_PATH = repo_path("data", "processed", "cross_source_diagnostics.csv")
CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH = repo_path("data", "processed", "cross_source_diagnostic_summary.json")
STATISTICAL_BASELINES_PATH = repo_path("data", "processed", "statistical_baselines.csv")
STATISTICAL_RARITY_PATH = repo_path("data", "processed", "statistical_rarity.csv")
CONTEXTUAL_RISK_ADJUSTMENTS_PATH = repo_path("data", "processed", "contextual_risk_adjustments.csv")
STATISTICAL_MARKER_SUMMARY_PATH = repo_path("data", "processed", "statistical_marker_summary.json")
STATISTICAL_CALIBRATION_REPORT_PATH = repo_path("data", "processed", "statistical_calibration_report.csv")


def empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def load_csv(path: Path, columns: list[str], warning_message: str | None = None) -> pd.DataFrame:
    if not path.exists():
        if warning_message:
            st.warning(warning_message)
        return empty_frame(columns)
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        if warning_message:
            st.warning(f"{warning_message} ({exc})")
        return empty_frame(columns)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df


def load_json(path: Path) -> dict[str, object]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def with_numeric_columns(df: pd.DataFrame, columns: list[str], *, integer: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
            if integer:
                df[column] = df[column].astype(int)
    return df


def with_boolean_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    for column in columns:
        if column in df.columns:
            df[column] = parse_bool_series(df[column])
    return df


def load_table_with_postprocess(
    path: Path,
    columns: list[str],
    *,
    warning_message: str | None = None,
    transform: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    df = load_csv(path, columns, warning_message)
    if transform is not None and not df.empty:
        df = transform(df)
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

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        with_numeric_columns(
            df,
            ["risk_contribution", "raw_risk_contribution", "contextual_adjustment", "adjusted_risk_contribution", "support"],
            integer=True,
        )
        with_numeric_columns(df, ["confidence_score", "rarity_score", "observed_value", "expected_value", "comparison_group_size"])
        df["source_name"] = df["sources"]
        df["source_type"] = df["source_types"]
        return df

    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Fraud markers file not found. Run the full pipeline to generate fraud markers.",
        transform=transform,
    )


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        transform=lambda df: with_numeric_columns(
            df,
            ["frequency", "average_risk_contribution", "average_support", "average_confidence_score"],
        ),
    )


def load_report(path: Path | str = ANOMALY_REPORT_PATH) -> pd.DataFrame:
    columns = [
        "Risk Score",
        "Risk Level",
        "Rule Triggered",
        "Supporting Evidence",
        "Entity IDs",
        "Addresses",
        "Phone Numbers",
        "Source Table",
        "source_name",
        "source_type",
        "data_scope",
    ]

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        with_numeric_columns(df, ["Risk Score"], integer=True)
        if not df["Risk Level"].astype(str).str.len().any():
            df["Risk Level"] = pd.cut(
                df["Risk Score"],
                bins=[-1, 14, 24, float("inf")],
                labels=["Low", "Medium", "High"],
                right=True,
            ).astype(str)
        return df

    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message=f"Anomaly report not found at {Path(path)}. Run the fraud marker engine first.",
        transform=transform,
    )


def load_entities(path: Path | str = ENTITIES_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["entity_id", "display_name", "entity_type", "source", "source_name", "source_type"],
        "Canonical entities file not found. Run the entity resolution step first.",
    )


def load_relationships(path: Path | str = RELATIONSHIPS_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["source_entity_id", "target_entity_id", "relationship_type", "confidence", "source_name", "source_type"],
        "Canonical relationships file not found. Run the entity resolution step first.",
    )


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Entity risk file not found. Run `python src/run_pipeline.py --include-connectors --health-check` first.",
        transform=lambda df: with_numeric_columns(df, ["risk_score"], integer=True),
    )


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

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if not df["source_name"].astype(str).str.len().any():
            df["source_name"] = df["source_names"]
        return df

    return load_table_with_postprocess(Path(path), columns, transform=transform)


def load_entity_aliases(path: Path | str = ENTITY_ALIASES_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["canonical_entity_id", "original_entity_id", "alias_value", "normalized_alias", "source_name", "source_type", "source_record_id", "resolution_method", "confidence_score"],
    )


def load_resolution_matches(path: Path | str = ENTITY_RESOLUTION_MATCHES_PATH) -> pd.DataFrame:
    columns = ["match_id", "left_entity_id", "right_entity_id", "entity_type", "match_method", "confidence_score", "decision", "evidence", "source_names", "source_name", "source_type"]

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if not df["source_name"].astype(str).str.len().any():
            df["source_name"] = df["source_names"]
        return df

    return load_table_with_postprocess(Path(path), columns, transform=transform)


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Investigation leads file not found. Run the full pipeline to build the investigation workspace.",
        transform=lambda df: with_numeric_columns(
            df,
            ["Risk Score", "Fraud Marker Count", "Supporting Source Count", "Relationship Count"],
        ),
    )


def load_entity_timelines(path: Path | str = ENTITY_TIMELINES_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["lead_id", "entity_id", "Date", "Event", "Entity", "Source", "source_name", "source_type", "Evidence", "Record ID", "Connector", "Import Date"],
        "Entity timeline output not found. Run the investigation workspace step first.",
    )


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
    return load_csv(
        Path(path),
        columns,
        "Evidence packets file not found. Run the investigation workspace step first.",
    )


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Network clusters file not found. Run the network intelligence step first.",
        transform=lambda df: with_numeric_columns(
            df,
            ["network_size", "network_risk_score", "fraud_marker_count", "relationship_count", "bridge_entity_count", "community_count"],
        ),
    )


def load_network_summary(path: Path | str = NETWORK_SUMMARY_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        [
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
        ],
        "Network summary file not found. Run the network intelligence step first.",
    )


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
        "bookmark",
        "needs_review",
    ]

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        with_numeric_columns(
            df,
            [
                "risk_score",
                "confidence_score",
                "fraud_marker_count",
                "independent_source_count",
                "relationship_count",
                "cross_source_match_count",
                "network_member_count",
                "evidence_completeness_score",
                "rarity_score",
                "rare_marker_count",
                "expected_value",
                "observed_value",
            ],
        )
        with_boolean_columns(df, ["contains_real_data", "contains_synthetic_data", "bookmark", "needs_review"])
        return df

    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Prioritized leads file not found. Run the investigation engine through the pipeline first.",
        transform=transform,
    )


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
    return load_csv(
        Path(path),
        columns,
        "Investigation summary file not found. Run the investigation engine through the pipeline first.",
    )


def load_review_recommendations(path: Path | str = REVIEW_RECOMMENDATIONS_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["lead_id", "lead_type", "priority", "confidence", "recommended_review", "evidence_completeness_score", "missing_evidence_fields", "status"],
        "Review recommendations file not found. Run the investigation engine through the pipeline first.",
    )


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

    def transform(df: pd.DataFrame) -> pd.DataFrame:
        with_numeric_columns(df, ["confidence", "independent_real_source_count"])
        with_boolean_columns(df, ["contains_real_data", "contains_synthetic_data"])
        df["source_name"] = df["left_source_name"].astype(str) + "|" + df["right_source_name"].astype(str)
        df["source_type"] = df["left_source_type"].astype(str) + "|" + df["right_source_type"].astype(str)
        return df

    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Cross-source matches file not found. Run the full pipeline to generate cross-source intelligence.",
        transform=transform,
    )


def load_cross_source_diagnostics(path: Path | str = CROSS_SOURCE_DIAGNOSTICS_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["metric", "value"],
        "Cross-source diagnostics file not found. Run the full pipeline to generate diagnostics.",
    )


def load_cross_source_diagnostic_summary(path: Path | str = CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH) -> dict[str, object]:
    return load_json(Path(path))


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Statistical rarity file not found. Run the statistical risk engine through the pipeline first.",
        transform=lambda df: with_numeric_columns(
            df,
            ["observed_value", "expected_value", "comparison_group_size", "percentile", "rarity_score", "classification_confidence"],
        ),
    )


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Contextual adjustment file not found. Run the statistical risk engine through the pipeline first.",
        transform=lambda df: with_numeric_columns(
            df,
            ["original_marker_score", "contextual_adjustment", "adjusted_marker_score"],
        ),
    )


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
    return load_table_with_postprocess(
        Path(path),
        columns,
        warning_message="Statistical baselines file not found. Run the statistical risk engine through the pipeline first.",
        transform=lambda df: with_numeric_columns(
            df,
            ["comparison_group_size", "observed_mean", "observed_median", "observed_max", "observed_min", "observed_p90"],
        ),
    )


def load_statistical_summary(path: Path | str = STATISTICAL_MARKER_SUMMARY_PATH) -> dict[str, object]:
    return load_json(Path(path))


def load_statistical_calibration_report(path: Path | str = STATISTICAL_CALIBRATION_REPORT_PATH) -> pd.DataFrame:
    return load_csv(
        Path(path),
        ["metric", "value"],
        "Statistical calibration report not found. Run the statistical risk engine through the pipeline first.",
    )


def build_relationship_explorer_data(
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    selected_entity_id: str,
) -> pd.DataFrame:
    columns = [
        "source_entity_id",
        "connected_entity_id",
        "relationship_type",
        "connected_entity_type",
        "connected_entity_name",
        "source_table",
        "source_name",
        "confidence",
        "direction",
    ]
    if entities_df.empty or relationships_df.empty:
        return empty_frame(columns)
    entity_lookup = entities_df.set_index("entity_id")
    outgoing = relationships_df[relationships_df["source_entity_id"].astype(str) == str(selected_entity_id)].copy()
    incoming = relationships_df[relationships_df["target_entity_id"].astype(str) == str(selected_entity_id)].copy()
    outgoing["direction"] = "outgoing"
    incoming["direction"] = "incoming"
    filtered = pd.concat([outgoing, incoming], ignore_index=True)
    if filtered.empty:
        return empty_frame(columns)
    filtered["connected_entity_id"] = filtered.apply(
        lambda row: row["target_entity_id"] if str(row.get("source_entity_id")) == str(selected_entity_id) else row["source_entity_id"],
        axis=1,
    )
    filtered["connected_entity_type"] = filtered["connected_entity_id"].apply(
        lambda value: entity_lookup.loc[value, "entity_type"] if value in entity_lookup.index else "unknown"
    )
    filtered["connected_entity_name"] = filtered["connected_entity_id"].apply(
        lambda value: entity_lookup.loc[value, "display_name"] if value in entity_lookup.index else value
    )
    filtered["source_table"] = filtered["connected_entity_id"].apply(
        lambda value: entity_lookup.loc[value, "source"] if value in entity_lookup.index and "source" in entity_lookup.columns else "canonical"
    )
    if "source_name" not in filtered.columns:
        filtered["source_name"] = ""
    return filtered[columns].reset_index(drop=True)
