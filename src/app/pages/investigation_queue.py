from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.components.analyst_state import render_analyst_state_editor
from src.app.components.filters import render_queue_filters
from src.app.components.tables import show_dataframe
from src.investigation.analyst_workbench import build_queue_view, save_saved_searches


def render_page(
    *,
    prioritized_leads_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    evidence_packets_df: pd.DataFrame,
    entity_timelines_df: pd.DataFrame,
    analyst_state_df: pd.DataFrame,
    analyst_history_df: pd.DataFrame,
    saved_searches: list[dict[str, object]],
    saved_searches_path: Path,
    analyst_state_path: Path,
    analyst_history_path: Path,
    page_size: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if prioritized_leads_df.empty:
        st.info("Prioritized leads are not available yet.")
        return analyst_state_df, analyst_history_df

    filters = render_queue_filters(prioritized_leads_df)
    queue_view = build_queue_view(prioritized_leads_df, **filters)
    show_dataframe(queue_view.head(page_size), empty_message="No leads match the current filters.")

    with st.expander("Save Current Search"):
        search_name = st.text_input("Search name")
        if st.button("Save Search") and search_name.strip():
            new_searches = [item for item in saved_searches if str(item.get("name", "")) != search_name.strip()]
            new_searches.append({"name": search_name.strip(), **filters})
            save_saved_searches(new_searches, saved_searches_path)
            st.success("Saved search written locally.")

    lead_options = queue_view["lead_id"].astype(str).tolist() or prioritized_leads_df["lead_id"].astype(str).tolist()
    selected_lead_id = st.selectbox("Lead", lead_options)
    selected_lead = prioritized_leads_df[prioritized_leads_df["lead_id"].astype(str) == selected_lead_id].iloc[0]
    entity_id = str(selected_lead.get("primary_entity_id", ""))

    st.subheader("Why")
    st.write(str(selected_lead.get("explanation", "")))
    st.write(str(selected_lead.get("recommended_review", "")))

    state_col, notes_col = st.columns(2)
    with state_col:
        analyst_state_df, analyst_history_df = render_analyst_state_editor(
            selected_lead=selected_lead,
            analyst_state_df=analyst_state_df,
            analyst_history_df=analyst_history_df,
            analyst_state_path=analyst_state_path,
            analyst_history_path=analyst_history_path,
        )
    with notes_col:
        st.subheader("Lead Snapshot")
        st.json(selected_lead.to_dict())

    marker_rows = fraud_markers_df[fraud_markers_df["entity_id"].astype(str) == entity_id]
    relationship_rows = relationships_df[
        (relationships_df["source_entity_id"].astype(str) == entity_id)
        | (relationships_df["target_entity_id"].astype(str) == entity_id)
    ]
    evidence_rows = evidence_packets_df[evidence_packets_df["entity_id"].astype(str) == entity_id]
    timeline_rows = entity_timelines_df[entity_timelines_df["entity_id"].astype(str) == entity_id]

    lower_left, lower_right = st.columns(2)
    with lower_left:
        st.subheader("Fraud Markers")
        show_dataframe(marker_rows, empty_message="No markers.")
        st.subheader("Timeline")
        show_dataframe(timeline_rows, empty_message="No timeline.")
    with lower_right:
        st.subheader("Relationships")
        show_dataframe(relationship_rows, empty_message="No relationships.")
        st.subheader("Evidence")
        show_dataframe(evidence_rows, empty_message="No evidence.")

    return analyst_state_df, analyst_history_df
