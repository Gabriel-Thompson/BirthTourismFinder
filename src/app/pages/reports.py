from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.app.components.tables import show_dataframe


def render_page(*, export_dir: Path, compatibility_report_df: pd.DataFrame) -> None:
    report_paths = [
        export_dir / "lead_summary.csv",
        export_dir / "lead_summary.json",
        export_dir / "lead_summary.md",
        export_dir / "lead_summary.html",
    ]
    report_df = pd.DataFrame([{"path": str(path), "exists": path.exists()} for path in report_paths])
    st.dataframe(report_df, use_container_width=True, hide_index=True)
    if not compatibility_report_df.empty:
        st.subheader("Anomaly Report")
        show_dataframe(compatibility_report_df, empty_message="No anomaly report available.")
