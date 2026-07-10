from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe
from src.investigation.analyst_workbench import compare_records


def render_page(
    *,
    network_clusters_df: pd.DataFrame,
    network_summary_df: pd.DataFrame,
    network_members_df: pd.DataFrame,
    page_size: int,
) -> None:
    show_dataframe(
        network_clusters_df.head(page_size),
        empty_message="No network data available.",
    )
    if len(network_clusters_df) >= 1:
        network_ids = network_clusters_df["network_id"].astype(str).tolist()
        left_id = st.selectbox("Left Network", network_ids)
        right_id = st.selectbox("Right Network", network_ids, index=1 if len(network_ids) > 1 else 0)
        left_row = network_clusters_df[network_clusters_df["network_id"].astype(str) == left_id].iloc[0].to_dict()
        right_row = network_clusters_df[network_clusters_df["network_id"].astype(str) == right_id].iloc[0].to_dict()
        st.subheader("Comparison")
        st.dataframe(
            compare_records(
                left_row,
                right_row,
                [
                    "network_risk_score",
                    "network_confidence",
                    "network_size",
                    "fraud_marker_count",
                    "relationship_count",
                    "cross_source_matches",
                    "source_name",
                ],
            ),
            use_container_width=True,
            hide_index=True,
        )
    if not network_summary_df.empty:
        st.subheader("Summary")
        st.dataframe(network_summary_df, use_container_width=True, hide_index=True)
    if not network_members_df.empty:
        st.subheader("Members")
        st.dataframe(network_members_df.head(page_size), use_container_width=True, hide_index=True)
