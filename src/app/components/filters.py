from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.utils.dashboard_filters import token_set

SOURCE_SCOPE_LABELS = {
    "all": "All Data",
    "real_only": "Real/API Connector Data Only",
    "synthetic_only": "Synthetic/Demo Data Only",
}


def collect_source_options(*frames: pd.DataFrame) -> list[str]:
    values: set[str] = set()
    for frame in frames:
        if frame.empty:
            continue
        for column in ["source_name", "source_names", "sources"]:
            if column not in frame.columns:
                continue
            for value in frame[column].astype(str).tolist():
                values.update(token_set(value))
    return sorted(values)


def render_sidebar_filters(
    *,
    config: dict[str, object],
    saved_searches: list[dict[str, object]],
    available_sources: list[str],
) -> dict[str, object]:
    default_filters = config.get("default_filters", {}) if isinstance(config.get("default_filters"), dict) else {}
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
    default_navigation = str(config.get("default_navigation", "Overview"))
    navigation_index = nav_options.index(default_navigation) if default_navigation in nav_options else 0
    default_scope = "real_only" if bool(config.get("real_data_only", False)) else str(default_filters.get("source_scope", "all"))
    default_scope_index = list(SOURCE_SCOPE_LABELS).index(default_scope) if default_scope in SOURCE_SCOPE_LABELS else 0
    saved_search_labels = ["None", *[str(item.get("name", "")) for item in saved_searches]]

    with st.sidebar:
        navigation = st.radio("Navigation", nav_options, index=navigation_index)
        with st.expander("Primary Filters", expanded=True):
            real_data_only = st.toggle("Real Data Only", value=bool(config.get("real_data_only", False)))
            if real_data_only:
                scope_key = "real_only"
                st.caption(SOURCE_SCOPE_LABELS[scope_key])
            else:
                scope_key = st.selectbox(
                    "Source Scope",
                    list(SOURCE_SCOPE_LABELS),
                    index=default_scope_index,
                    format_func=lambda key: SOURCE_SCOPE_LABELS[key],
                )
        with st.expander("Source Filters", expanded=True):
            selected_sources = st.multiselect("Sources", options=available_sources)
            selected_saved_search = st.selectbox("Saved Search", saved_search_labels)

    return {
        "navigation": navigation,
        "scope_key": "real_only" if real_data_only else scope_key,
        "selected_sources": selected_sources,
        "selected_saved_search": selected_saved_search,
    }


def render_queue_filters(prioritized_leads_df: pd.DataFrame) -> dict[str, str]:
    if prioritized_leads_df.empty:
        return {
            "priority": "All",
            "confidence": "All",
            "status": "All",
            "entity_type": "All",
            "source_name": "All",
            "marker": "All",
            "network_mode": "All",
            "reviewed_mode": "All",
        }
    with st.expander("Queue Filters", expanded=True):
        row_one = st.columns(4)
        priority = row_one[0].selectbox("Priority", ["All", *sorted(prioritized_leads_df["priority"].astype(str).unique())])
        confidence = row_one[1].selectbox("Confidence", ["All", *sorted(prioritized_leads_df["confidence"].astype(str).unique())])
        status = row_one[2].selectbox("Status", ["All", *sorted(prioritized_leads_df["status"].astype(str).unique())])
        entity_type = row_one[3].selectbox("Entity Type", ["All", *sorted(prioritized_leads_df["primary_entity_type"].astype(str).unique())])

        row_two = st.columns(4)
        source_name = row_two[0].selectbox(
            "Source",
            ["All", *sorted({token for value in prioritized_leads_df["source_names"].astype(str) for token in token_set(value)})],
        )
        marker = row_two[1].selectbox(
            "Marker",
            ["All", *sorted({token for value in prioritized_leads_df["fraud_markers"].astype(str) for token in token_set(value)})],
        )
        network_mode = row_two[2].selectbox("Network", ["All", "With Network", "Without Network"])
        reviewed_mode = row_two[3].selectbox("Reviewed", ["All", "Reviewed", "Needs Review"])

    return {
        "priority": priority,
        "confidence": confidence,
        "status": status,
        "entity_type": entity_type,
        "source_name": source_name,
        "marker": marker,
        "network_mode": network_mode,
        "reviewed_mode": reviewed_mode,
    }
