from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe


def render_page(
    *,
    prioritized_leads_df: pd.DataFrame,
    analyst_history_df: pd.DataFrame,
    saved_searches: list[dict[str, object]],
    page_size: int,
) -> None:
    left, right = st.columns(2)
    with left:
        st.subheader("Queue Snapshot")
        show_dataframe(
            prioritized_leads_df.head(page_size),
            empty_message="Run the pipeline to populate the queue.",
        )
    with right:
        st.subheader("Recent History")
        show_dataframe(
            analyst_history_df.tail(page_size),
            empty_message="No analyst history yet.",
        )
    st.subheader("Saved Searches")
    if saved_searches:
        st.dataframe(pd.DataFrame(saved_searches), use_container_width=True, hide_index=True)
    else:
        st.info("No saved searches.")
