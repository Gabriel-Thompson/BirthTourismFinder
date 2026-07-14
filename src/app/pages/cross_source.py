from __future__ import annotations

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe
from src.app.utils.dashboard_filters import parse_bool_series


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
                "Source Pair",
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
            npi_filter = col7.text_input("NPI")
            provider_name_filter = col8.text_input("Provider Name")
            parcel_id_filter = col9.text_input("Parcel ID")
            col10, col11, col12 = st.columns(3)
            address_scope_filter = col10.selectbox(
                "Address Match Scope",
                ["All", *sorted(set(filtered.get("address_match_scope", pd.Series(dtype=str)).astype(str)))],
            )
            taxonomy_filter = col11.text_input("Taxonomy")
            enumeration_type_filter = col12.selectbox(
                "Enumeration Type",
                ["All", *sorted(set(filtered.get("enumeration_type", pd.Series(dtype=str)).astype(str)))],
            )
            col13, col14, col15 = st.columns(3)
            person_role_filter = col13.selectbox(
                "Person Role",
                ["All", *sorted(set(filtered.get("person_role", pd.Series(dtype=str)).astype(str)))],
            )
            privacy_filter = col14.selectbox("Privacy Redaction", ["All", "Redacted", "Not Redacted"])
            deactivated_filter = col15.selectbox("Deactivated Status", ["All", "Active", "Deactivated"])
            col16, col17, col18 = st.columns(3)
            common_name_filter = col16.selectbox("Common-Name Downgrade", ["All", "Downgraded", "Not Downgraded"])
            insufficient_reason_filter = col17.text_input("Insufficient-Data Reason")
            three_source_only_filter = col18.selectbox("Three-Source Only", ["All", "Yes", "No"])

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
        if npi_filter and "npi" in filtered.columns:
            filtered = filtered[filtered["npi"].astype(str).str.contains(npi_filter, case=False, na=False)]
        if provider_name_filter and "provider_name" in filtered.columns:
            filtered = filtered[filtered["provider_name"].astype(str).str.contains(provider_name_filter, case=False, na=False)]
        if parcel_id_filter and "parcel_id" in filtered.columns:
            filtered = filtered[filtered["parcel_id"].astype(str).str.contains(parcel_id_filter, case=False, na=False)]
        if address_scope_filter != "All" and "address_match_scope" in filtered.columns:
            filtered = filtered[filtered["address_match_scope"].astype(str) == address_scope_filter]
        if taxonomy_filter and "taxonomy" in filtered.columns:
            filtered = filtered[filtered["taxonomy"].astype(str).str.contains(taxonomy_filter, case=False, na=False)]
        if enumeration_type_filter != "All" and "enumeration_type" in filtered.columns:
            filtered = filtered[filtered["enumeration_type"].astype(str) == enumeration_type_filter]
        if person_role_filter != "All" and "person_role" in filtered.columns:
            filtered = filtered[filtered["person_role"].astype(str) == person_role_filter]
        if privacy_filter == "Redacted" and "privacy_redacted" in filtered.columns:
            filtered = filtered[parse_bool_series(filtered["privacy_redacted"])]
        elif privacy_filter == "Not Redacted" and "privacy_redacted" in filtered.columns:
            filtered = filtered[~parse_bool_series(filtered["privacy_redacted"])]
        if deactivated_filter == "Active" and "deactivated_status" in filtered.columns:
            filtered = filtered[~parse_bool_series(filtered["deactivated_status"])]
        elif deactivated_filter == "Deactivated" and "deactivated_status" in filtered.columns:
            filtered = filtered[parse_bool_series(filtered["deactivated_status"])]
        if common_name_filter == "Downgraded" and "common_name_downgraded" in filtered.columns:
            filtered = filtered[parse_bool_series(filtered["common_name_downgraded"])]
        elif common_name_filter == "Not Downgraded" and "common_name_downgraded" in filtered.columns:
            filtered = filtered[~parse_bool_series(filtered["common_name_downgraded"])]
        if insufficient_reason_filter and "insufficient_data_reason" in filtered.columns:
            filtered = filtered[filtered["insufficient_data_reason"].astype(str).str.contains(insufficient_reason_filter, case=False, na=False)]
        if three_source_only_filter == "Yes" and "three_source_only" in filtered.columns:
            filtered = filtered[parse_bool_series(filtered["three_source_only"])]
        elif three_source_only_filter == "No" and "three_source_only" in filtered.columns:
            filtered = filtered[~parse_bool_series(filtered["three_source_only"])]

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
