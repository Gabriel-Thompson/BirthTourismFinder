from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.app.components.tables import show_dataframe
from src.app.utils.dashboard_filters import parse_bool_series
from src.connectors.source_manifest import REPO_ROOT


def render_page(*, source_health_df: pd.DataFrame) -> None:
    show_dataframe(source_health_df, empty_message="No source health report available.")
    load_dotenv(REPO_ROOT / ".env")
    status_path = REPO_ROOT / "data" / "processed" / "sunbiz_daily_import_summary.json"
    compatibility_status_path = REPO_ROOT / "data" / "processed" / "sunbiz_daily_status.json"
    st.subheader("Sunbiz Daily")
    if status_path.exists() and status_path.stat().st_size > 0:
        try:
            with status_path.open("r", encoding="utf-8") as handle:
                status = json.load(handle)
        except Exception:
            status = {}
    elif compatibility_status_path.exists() and compatibility_status_path.stat().st_size > 0:
        try:
            with compatibility_status_path.open("r", encoding="utf-8") as handle:
                status = json.load(handle)
        except Exception:
            status = {}
    else:
        status = {}

    if status:
        metric_cols = st.columns(6)
        metric_cols[0].metric("Businesses Imported", int(status.get("businesses_imported", 0)))
        metric_cols[1].metric("Officers Imported", int(status.get("officers_imported", 0)))
        metric_cols[2].metric("Registered Agents", int(status.get("registered_agents_imported", 0)))
        metric_cols[3].metric("Addresses", int(status.get("addresses_imported", 0)))
        metric_cols[4].metric("Cross Source Matches", int(status.get("cross_source_matches", 0)))
        metric_cols[5].metric("API Status", str(status.get("api_status", "UNKNOWN")))
        detail_cols = st.columns(3)
        detail_cols[0].caption(f"Key Present: {'Yes' if bool(os.getenv('SUNBIZ_DAILY_API_KEY', '').strip()) else 'No'}")
        detail_cols[1].caption(f"Last Successful Import: {status.get('last_successful_import', '') or 'Not imported yet'}")
        detail_cols[2].caption(f"County Coverage: {status.get('county_coverage', '') or 'Not set'}")
        filter_cols = st.columns(3)
        import_filters = status.get("import_filters", {}) if isinstance(status.get("import_filters"), dict) else {}
        filter_cols[0].caption(f"Import Filters: county={import_filters.get('county', '') or 'n/a'} status={import_filters.get('status', '') or 'n/a'}")
        filter_cols[1].caption(f"Records Fetched: {status.get('records_fetched', 0)} | Redacted/Incomplete: {status.get('redacted_or_incomplete_records', 0)}")
        filter_cols[2].caption(
            f"Async Jobs: {status.get('asynchronous_jobs', 0)} | Truncated: {'Yes' if bool(status.get('truncated_results', False)) else 'No'}"
        )
        st.caption(
            f"Rate Limit Remaining: {status.get('rate_limit_remaining', 'unknown')} | "
            f"Config Enabled: {'Yes' if bool(status.get('config_enabled', False)) else 'No'} | "
            f"Live Mode: {'Yes' if bool(status.get('live_mode', False)) else 'No'}"
        )
        if status.get("errors"):
            st.info(str(status["errors"]))
    else:
        st.info(
            "Sunbiz Daily metrics are not available yet. Run `python -m src.connectors.sunbiz_daily_connector --mock --county Hillsborough --max-records 100` "
            "or `python src/run_pipeline.py --include-sunbiz --include-connectors --health-check`."
        )

    nppes_summary_path = REPO_ROOT / "data" / "processed" / "nppes_import_summary.json"
    st.subheader("CMS NPPES / NPI")
    if nppes_summary_path.exists() and nppes_summary_path.stat().st_size > 0:
        try:
            with nppes_summary_path.open("r", encoding="utf-8") as handle:
                nppes_status = json.load(handle)
        except Exception:
            nppes_status = {}
    else:
        nppes_status = {}

    if nppes_status:
        metric_cols = st.columns(6)
        metric_cols[0].metric("Providers", int(nppes_status.get("providers_normalized", 0)))
        metric_cols[1].metric("Individuals", int(nppes_status.get("individual_providers", 0)))
        metric_cols[2].metric("Organizations", int(nppes_status.get("organization_providers", 0)))
        metric_cols[3].metric("Practice Addresses", int(nppes_status.get("practice_addresses", 0)))
        metric_cols[4].metric("Mailing Addresses", int(nppes_status.get("mailing_addresses", 0)))
        metric_cols[5].metric("Taxonomies", int(nppes_status.get("taxonomy_records", 0)))
        detail_cols = st.columns(4)
        detail_cols[0].caption(f"Mode: {nppes_status.get('mode', 'unknown')}")
        detail_cols[1].caption(f"Deactivated NPIs: {nppes_status.get('deactivated_npis', 0)}")
        detail_cols[2].caption(f"Incomplete Records: {nppes_status.get('incomplete_records', 0)}")
        detail_cols[3].caption(f"Last Import: {nppes_status.get('last_successful_import', '') or 'Not imported yet'}")
        filters = nppes_status.get("filters", {}) if isinstance(nppes_status.get("filters"), dict) else {}
        st.caption(
            "Filters: "
            f"state={filters.get('state', '') or 'n/a'} "
            f"city={filters.get('city', '') or 'n/a'} "
            f"postal={filters.get('postal_code', '') or filters.get('postal_prefix', '') or 'n/a'} "
            f"taxonomy={filters.get('taxonomy_description', '') or filters.get('taxonomy_code', '') or 'n/a'}"
        )
    else:
        st.info(
            "NPPES metrics are not available yet. Run `python -m src.connectors.nppes.api_connector --mock --state FL --city Tampa --max-records 100` "
            "or `python src/run_pipeline.py --include-nppes --nppes-mock --include-sunbiz --sunbiz-mock --include-connectors --health-check`."
        )

    pending_review = (
        source_health_df[parse_bool_series(source_health_df["pending_review"])]
        if not source_health_df.empty and "pending_review" in source_health_df.columns
        else pd.DataFrame()
    )
    st.subheader("Pending Review")
    show_dataframe(pending_review, empty_message="No sources pending review.")
