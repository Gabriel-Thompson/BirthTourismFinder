from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe


def render_page(
    *,
    cross_source_matches_df: pd.DataFrame,
    cross_source_summary: dict[str, object],
    cross_source_diagnostics_df: pd.DataFrame,
    page_size: int,
) -> None:
    show_dataframe(
        cross_source_matches_df.head(page_size),
        empty_message="No cross-source matches available.",
    )
    if cross_source_summary:
        st.subheader("Diagnostic Summary")
        st.json(cross_source_summary)
    elif not cross_source_diagnostics_df.empty:
        st.subheader("Diagnostics")
        st.dataframe(cross_source_diagnostics_df, use_container_width=True, hide_index=True)
