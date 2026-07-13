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
    filtered = cross_source_matches_df.copy()
    if not filtered.empty:
        with st.expander("Cross-Source Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            source_filter = col1.selectbox(
                "Sunbiz Source",
                ["All", *sorted(set(filtered["source_name"].astype(str)))],
            )
            match_type_filter = col2.selectbox(
                "Match Type",
                ["All", *sorted(set(filtered["match_method"].astype(str)))],
            )
            decision_filter = col3.selectbox(
                "Decision",
                ["All", *sorted(set(filtered["decision"].astype(str)))],
            )
            col4, col5, col6 = st.columns(3)
            confidence_filter = col4.selectbox("Confidence", ["All", ">= 0.9", ">= 0.75", "< 0.75"])
            corporation_number_filter = col5.text_input("Corporation Number")
            business_name_filter = col6.text_input("Business Name")
            parcel_id_filter = st.text_input("Parcel ID")

        if source_filter != "All":
            filtered = filtered[filtered["source_name"].astype(str) == source_filter]
        if match_type_filter != "All":
            filtered = filtered[filtered["match_method"].astype(str) == match_type_filter]
        if decision_filter != "All":
            filtered = filtered[filtered["decision"].astype(str) == decision_filter]
        if confidence_filter == ">= 0.9":
            filtered = filtered[pd.to_numeric(filtered["confidence"], errors="coerce").fillna(0) >= 0.9]
        elif confidence_filter == ">= 0.75":
            filtered = filtered[pd.to_numeric(filtered["confidence"], errors="coerce").fillna(0) >= 0.75]
        elif confidence_filter == "< 0.75":
            filtered = filtered[pd.to_numeric(filtered["confidence"], errors="coerce").fillna(0) < 0.75]
        if corporation_number_filter and "sunbiz_corporation_number" in filtered.columns:
            filtered = filtered[filtered["sunbiz_corporation_number"].astype(str).str.contains(corporation_number_filter, case=False, na=False)]
        if business_name_filter and "sunbiz_business_name" in filtered.columns:
            filtered = filtered[filtered["sunbiz_business_name"].astype(str).str.contains(business_name_filter, case=False, na=False)]
        if parcel_id_filter and "parcel_id" in filtered.columns:
            filtered = filtered[filtered["parcel_id"].astype(str).str.contains(parcel_id_filter, case=False, na=False)]

    show_dataframe(
        filtered.head(page_size),
        empty_message="No cross-source matches available.",
    )
    if cross_source_summary:
        st.subheader("Diagnostic Summary")
        st.json(cross_source_summary)
    elif not cross_source_diagnostics_df.empty:
        st.subheader("Diagnostics")
        st.dataframe(cross_source_diagnostics_df, use_container_width=True, hide_index=True)
