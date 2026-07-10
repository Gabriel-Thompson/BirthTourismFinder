from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe


def render_page(*, fraud_markers_df: pd.DataFrame, fraud_marker_summary_df: pd.DataFrame, page_size: int) -> None:
    show_dataframe(
        fraud_markers_df.head(page_size),
        empty_message="No fraud markers available.",
    )
    st.subheader("Summary")
    show_dataframe(
        fraud_marker_summary_df,
        empty_message="No marker summary available.",
    )
