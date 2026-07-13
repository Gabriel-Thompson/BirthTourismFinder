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
            col7, col8, col9 = st.columns(3)
            address_scope_filter = col7.selectbox(
                "Address Match Scope",
                ["All", *sorted(set(filtered.get("address_match_scope", pd.Series(dtype=str)).astype(str)))],
            )
            person_role_filter = col8.selectbox(
                "Person Role",
                ["All", *sorted(set(filtered.get("person_role", pd.Series(dtype=str)).astype(str)))],
            )
            privacy_filter = col9.selectbox("Privacy Redaction", ["All", "Redacted", "Not Redacted"])
            col10, col11, col12 = st.columns(3)
            common_name_filter = col10.selectbox("Common-Name Downgrade", ["All", "Downgraded", "Not Downgraded"])
            insufficient_reason_filter = col11.text_input("Insufficient-Data Reason")
            parcel_id_filter = col12.text_input("Parcel ID")

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
        if address_scope_filter != "All" and "address_match_scope" in filtered.columns:
            filtered = filtered[filtered["address_match_scope"].astype(str) == address_scope_filter]
        if person_role_filter != "All" and "person_role" in filtered.columns:
            filtered = filtered[filtered["person_role"].astype(str) == person_role_filter]
        if privacy_filter == "Redacted" and "privacy_redacted" in filtered.columns:
            filtered = filtered[filtered["privacy_redacted"].astype(str).str.lower().isin(["true", "1"])]
        elif privacy_filter == "Not Redacted" and "privacy_redacted" in filtered.columns:
            filtered = filtered[~filtered["privacy_redacted"].astype(str).str.lower().isin(["true", "1"])]
        if common_name_filter == "Downgraded" and "common_name_downgraded" in filtered.columns:
            filtered = filtered[filtered["common_name_downgraded"].astype(str).str.lower().isin(["true", "1"])]
        elif common_name_filter == "Not Downgraded" and "common_name_downgraded" in filtered.columns:
            filtered = filtered[~filtered["common_name_downgraded"].astype(str).str.lower().isin(["true", "1"])]
        if insufficient_reason_filter and "insufficient_data_reason" in filtered.columns:
            filtered = filtered[filtered["insufficient_data_reason"].astype(str).str.contains(insufficient_reason_filter, case=False, na=False)]
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
