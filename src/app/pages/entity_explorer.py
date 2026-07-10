from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe
from src.app.utils.dashboard_io import build_relationship_explorer_data
from src.investigation.analyst_workbench import compare_records


def render_page(
    *,
    canonical_entities_df: pd.DataFrame,
    entity_aliases_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    entity_timelines_df: pd.DataFrame,
    evidence_packets_df: pd.DataFrame,
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
) -> None:
    if canonical_entities_df.empty:
        st.info("No canonical entities available.")
        return
    entity_id_series = (
        canonical_entities_df["entity_id"]
        if "entity_id" in canonical_entities_df.columns
        else canonical_entities_df["canonical_entity_id"]
    )
    entity_options = entity_id_series.astype(str).tolist()
    selected_entity_id = st.selectbox("Entity", entity_options)
    compare_entity_id = st.selectbox("Compare With", entity_options, index=1 if len(entity_options) > 1 else 0)
    selector_series = canonical_entities_df["entity_id"] if "entity_id" in canonical_entities_df.columns else canonical_entities_df["canonical_entity_id"]
    profile_row = canonical_entities_df[selector_series.astype(str) == selected_entity_id].iloc[0].to_dict()
    compare_row = canonical_entities_df[selector_series.astype(str) == compare_entity_id].iloc[0].to_dict()
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
        show_dataframe(profile_aliases, empty_message="No aliases.")
        st.subheader("Fraud Markers")
        show_dataframe(profile_markers, empty_message="No markers.")
    with right:
        st.subheader("Timeline")
        show_dataframe(profile_timeline, empty_message="No timeline.")
        st.subheader("Relationships")
        show_dataframe(profile_relationships, empty_message="No relationships.")
        st.subheader("Evidence")
        show_dataframe(profile_evidence, empty_message="No evidence.")

    st.subheader("Side-by-Side Comparison")
    st.dataframe(
        compare_records(
            profile_row,
            compare_row,
            ["display_name", "entity_type", "source_name", "source_type", "record_count", "source_count", "resolution_confidence"],
        ),
        use_container_width=True,
        hide_index=True,
    )
