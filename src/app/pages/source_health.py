from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe
from src.app.utils.dashboard_filters import parse_bool_series
from src.connectors.source_manifest import REPO_ROOT


def render_page(*, source_health_df: pd.DataFrame) -> None:
    show_dataframe(source_health_df, empty_message="No source health report available.")
    status_path = REPO_ROOT / "data" / "processed" / "sunbiz_daily_status.json"
    st.subheader("Sunbiz Daily")
    if status_path.exists() and status_path.stat().st_size > 0:
        try:
            with status_path.open("r", encoding="utf-8") as handle:
                status = json.load(handle)
        except Exception:
            status = {}
        metric_cols = st.columns(6)
        metric_cols[0].metric("Businesses Imported", int(status.get("businesses_imported", 0)))
        metric_cols[1].metric("Officers Imported", int(status.get("officers_imported", 0)))
        metric_cols[2].metric("Registered Agents", int(status.get("registered_agents_imported", 0)))
        metric_cols[3].metric("Addresses", int(status.get("addresses_imported", 0)))
        metric_cols[4].metric("Cross Source Matches", int(status.get("cross_source_matches", 0)))
        metric_cols[5].metric("API Status", str(status.get("api_status", "UNKNOWN")))
        detail_cols = st.columns(2)
        detail_cols[0].caption(f"Last Import: {status.get('last_import', '') or 'Not imported yet'}")
        detail_cols[1].caption(f"County Coverage: {status.get('county_coverage', '') or 'Not set'}")
        if status.get("error"):
            st.info(str(status["error"]))
    else:
        st.info(
            "Sunbiz Daily metrics are not available yet. Run `python src/connectors/sunbiz_daily_connector.py --county Hillsborough --limit 100` "
            "or `python src/run_pipeline.py --include-sunbiz --include-connectors --health-check`."
        )
    pending_review = (
        source_health_df[parse_bool_series(source_health_df["pending_review"])]
        if not source_health_df.empty and "pending_review" in source_health_df.columns
        else pd.DataFrame()
    )
    st.subheader("Pending Review")
    show_dataframe(pending_review, empty_message="No sources pending review.")
