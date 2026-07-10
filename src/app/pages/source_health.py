from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe
from src.app.utils.dashboard_filters import parse_bool_series


def render_page(*, source_health_df: pd.DataFrame) -> None:
    show_dataframe(source_health_df, empty_message="No source health report available.")
    pending_review = (
        source_health_df[parse_bool_series(source_health_df["pending_review"])]
        if not source_health_df.empty and "pending_review" in source_health_df.columns
        else pd.DataFrame()
    )
    st.subheader("Pending Review")
    show_dataframe(pending_review, empty_message="No sources pending review.")
