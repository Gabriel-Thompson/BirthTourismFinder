from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe


def render_page(
    *,
    statistical_rarity_df: pd.DataFrame,
    contextual_adjustments_df: pd.DataFrame,
    statistical_baselines_df: pd.DataFrame,
    statistical_summary: dict[str, object],
    statistical_calibration_df: pd.DataFrame,
    page_size: int,
) -> None:
    show_dataframe(
        statistical_rarity_df.head(page_size),
        empty_message="No statistical rarity output available.",
    )
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
