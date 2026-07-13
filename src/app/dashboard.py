from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

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

from src.app.components.filters import collect_source_options, render_sidebar_filters
from src.app.components.metrics import build_dashboard_metrics, build_resolution_metrics, render_top_metrics
from src.app.pages import cross_source, entity_explorer, fraud_markers, investigation_queue, network_intelligence, overview, reports, source_health, statistical_risk
from src.app.utils.dashboard_filters import filter_dataframe_by_source_scope
from src.app.utils.dashboard_io import (
    ANOMALY_REPORT_PATH,
    FRAUD_MARKERS_PATH,
    FRAUD_MARKER_SUMMARY_PATH,
    build_relationship_explorer_data,
    load_contextual_adjustments,
    load_cross_source_diagnostic_summary,
    load_cross_source_diagnostics,
    load_cross_source_matches,
    load_entity_aliases,
    load_entity_risk,
    load_entity_timelines,
    load_entities,
    load_evidence_packets,
    load_fraud_marker_summary,
    load_fraud_markers,
    load_investigation_summary,
    load_network_clusters,
    load_network_members,
    load_network_summary,
    load_prioritized_leads,
    load_relationships,
    load_report,
    load_statistical_baselines,
    load_statistical_calibration_report,
    load_statistical_rarity,
    load_statistical_summary,
    load_canonical_entities,
)
from src.connectors.source_metadata import load_api_sources, load_manifest_sources
from src.connectors.source_manifest import REPO_ROOT as MANIFEST_REPO_ROOT
from src.investigation.analyst_workbench import (
    ANALYST_HISTORY_PATH,
    ANALYST_STATE_PATH,
    SAVED_SEARCHES_PATH,
    build_queue_view,
    build_source_health_report,
    load_analyst_history,
    load_analyst_state,
    load_dashboard_config,
    load_saved_searches,
)


@dataclass
class DatasetManager:
    loaders: dict[str, Callable[[], object]]

    def __post_init__(self) -> None:
        self._cache: dict[str, object] = {}

    def get(self, name: str):
        if name not in self._cache:
            self._cache[name] = self.loaders[name]()
        return self._cache[name]


def _apply_source_filters(df: pd.DataFrame, scope_key: str, selected_sources: list[str]) -> pd.DataFrame:
    return filter_dataframe_by_source_scope(df, scope_key, selected_sources)


def _apply_saved_search(prioritized_leads_df: pd.DataFrame, selected_saved_search: str, saved_searches: list[dict[str, object]]) -> pd.DataFrame:
    if selected_saved_search == "None":
        return prioritized_leads_df
    search_config = next((item for item in saved_searches if str(item.get("name", "")) == selected_saved_search), {})
    if not search_config:
        return prioritized_leads_df
    return build_queue_view(
        prioritized_leads_df,
        priority=str(search_config.get("priority", "All")),
        confidence=str(search_config.get("confidence", "All")),
        source_name=str(search_config.get("source_name", "All")),
        marker=str(search_config.get("marker", "All")),
        network_mode=str(search_config.get("network_mode", "All")),
        entity_type=str(search_config.get("entity_type", "All")),
        status=str(search_config.get("status", "All")),
        reviewed_mode=str(search_config.get("reviewed_mode", "All")),
    )


def _build_dataset_manager() -> DatasetManager:
    processed_dir = MANIFEST_REPO_ROOT / "data" / "processed"
    return DatasetManager(
        {
            "fraud_markers": load_fraud_markers,
            "fraud_marker_summary": load_fraud_marker_summary,
            "report": load_report,
            "entities": load_entities,
            "relationships": load_relationships,
            "entity_risk": load_entity_risk,
            "canonical_entities": load_canonical_entities,
            "entity_aliases": load_entity_aliases,
            "entity_timelines": load_entity_timelines,
            "evidence_packets": load_evidence_packets,
            "network_clusters": load_network_clusters,
            "network_summary": load_network_summary,
            "network_members": load_network_members,
            "prioritized_leads": load_prioritized_leads,
            "investigation_summary": load_investigation_summary,
            "cross_source_matches": load_cross_source_matches,
            "cross_source_diagnostics": load_cross_source_diagnostics,
            "cross_source_summary": load_cross_source_diagnostic_summary,
            "statistical_baselines": load_statistical_baselines,
            "statistical_rarity": load_statistical_rarity,
            "contextual_adjustments": load_contextual_adjustments,
            "statistical_summary": load_statistical_summary,
            "statistical_calibration": load_statistical_calibration_report,
            "analyst_state": load_analyst_state,
            "analyst_history": load_analyst_history,
            "saved_searches": load_saved_searches,
            "dashboard_config": load_dashboard_config,
            "source_health": lambda: build_source_health_report(
                load_manifest_sources(),
                load_api_sources(),
                processed_dir,
            ),
        }
    )


def main() -> None:
    st.set_page_config(page_title="OpenFraud Analyst Workbench", layout="wide")
    manager = _build_dataset_manager()
    config = manager.get("dashboard_config")

    st.title("OpenFraud Analyst Workbench")
    st.caption("Local analyst workstation for explainable investigative leads. All results are leads only, not proof of fraud.")

    fraud_markers_df = manager.get("fraud_markers")
    entity_risk_df = manager.get("entity_risk")
    entities_df = manager.get("entities")
    canonical_entities_df = manager.get("canonical_entities")
    prioritized_leads_df = manager.get("prioritized_leads")
    saved_searches = manager.get("saved_searches")

    sidebar_filters = render_sidebar_filters(
        config=config,
        saved_searches=saved_searches,
        available_sources=collect_source_options(
            fraud_markers_df,
            entity_risk_df,
            entities_df,
            canonical_entities_df,
            prioritized_leads_df,
        ),
    )
    navigation = str(sidebar_filters["navigation"])
    scope_key = str(sidebar_filters["scope_key"])
    selected_sources = list(sidebar_filters["selected_sources"])
    selected_saved_search = str(sidebar_filters["selected_saved_search"])
    page_size = int(config.get("page_size", 25))

    filtered_fraud_markers_df = _apply_source_filters(fraud_markers_df, scope_key, selected_sources)
    filtered_entities_df = _apply_source_filters(entities_df, scope_key, selected_sources)
    filtered_relationships_df = _apply_source_filters(manager.get("relationships"), scope_key, selected_sources)
    filtered_entity_risk_df = _apply_source_filters(entity_risk_df, scope_key, selected_sources)
    filtered_prioritized_leads_df = _apply_saved_search(
        _apply_source_filters(prioritized_leads_df, scope_key, selected_sources),
        selected_saved_search,
        saved_searches,
    )
    filtered_statistical_rarity_df = _apply_source_filters(manager.get("statistical_rarity"), scope_key, selected_sources)
    pipeline_summary = manager.get("investigation_summary").iloc[0].to_dict() if not manager.get("investigation_summary").empty else {}
    fallback_metrics = build_dashboard_metrics(
        filtered_fraud_markers_df,
        filtered_entities_df,
        filtered_relationships_df,
        filtered_entity_risk_df,
        filtered_prioritized_leads_df,
    )
    render_top_metrics(
        pipeline_summary=pipeline_summary,
        filtered_leads_df=filtered_prioritized_leads_df,
        statistical_rarity_df=filtered_statistical_rarity_df,
        fallback_metrics=fallback_metrics,
    )

    if navigation == "Overview":
        overview.render_page(
            prioritized_leads_df=filtered_prioritized_leads_df,
            analyst_history_df=manager.get("analyst_history"),
            saved_searches=saved_searches,
            page_size=page_size,
        )
    elif navigation == "Investigation Queue":
        investigation_queue.render_page(
            prioritized_leads_df=filtered_prioritized_leads_df,
            fraud_markers_df=filtered_fraud_markers_df,
            relationships_df=filtered_relationships_df,
            evidence_packets_df=_apply_source_filters(manager.get("evidence_packets"), scope_key, selected_sources),
            entity_timelines_df=_apply_source_filters(manager.get("entity_timelines"), scope_key, selected_sources),
            analyst_state_df=manager.get("analyst_state"),
            analyst_history_df=manager.get("analyst_history"),
            saved_searches=saved_searches,
            saved_searches_path=SAVED_SEARCHES_PATH,
            analyst_state_path=ANALYST_STATE_PATH,
            analyst_history_path=ANALYST_HISTORY_PATH,
            page_size=page_size,
        )
    elif navigation == "Fraud Markers":
        fraud_markers.render_page(
            fraud_markers_df=filtered_fraud_markers_df,
            fraud_marker_summary_df=manager.get("fraud_marker_summary"),
            page_size=page_size,
        )
    elif navigation == "Statistical Risk":
        statistical_risk.render_page(
            statistical_rarity_df=filtered_statistical_rarity_df,
            contextual_adjustments_df=_apply_source_filters(manager.get("contextual_adjustments"), scope_key, selected_sources),
            statistical_baselines_df=manager.get("statistical_baselines"),
            statistical_summary=manager.get("statistical_summary"),
            statistical_calibration_df=manager.get("statistical_calibration"),
            page_size=page_size,
        )
    elif navigation == "Network Intelligence":
        network_intelligence.render_page(
            network_clusters_df=_apply_source_filters(manager.get("network_clusters"), scope_key, selected_sources),
            network_summary_df=manager.get("network_summary"),
            network_members_df=_apply_source_filters(manager.get("network_members"), scope_key, selected_sources),
            page_size=page_size,
        )
    elif navigation == "Cross Source Intelligence":
        cross_source.render_page(
            cross_source_matches_df=_apply_source_filters(manager.get("cross_source_matches"), scope_key, selected_sources),
            cross_source_summary=manager.get("cross_source_summary"),
            cross_source_diagnostics_df=manager.get("cross_source_diagnostics"),
            page_size=page_size,
        )
    elif navigation == "Entity Explorer":
        entity_explorer.render_page(
            canonical_entities_df=_apply_source_filters(manager.get("canonical_entities"), scope_key, selected_sources),
            entity_aliases_df=_apply_source_filters(manager.get("entity_aliases"), scope_key, selected_sources),
            fraud_markers_df=filtered_fraud_markers_df,
            entity_timelines_df=_apply_source_filters(manager.get("entity_timelines"), scope_key, selected_sources),
            evidence_packets_df=_apply_source_filters(manager.get("evidence_packets"), scope_key, selected_sources),
            entities_df=filtered_entities_df,
            relationships_df=filtered_relationships_df,
        )
    elif navigation == "Reports":
        reports.render_page(
            export_dir=REPO_ROOT / "exports",
            compatibility_report_df=manager.get("report"),
        )
    elif navigation == "Source Health":
        source_health.render_page(source_health_df=manager.get("source_health"))


if __name__ == "__main__":
    main()
