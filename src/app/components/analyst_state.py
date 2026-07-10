from __future__ import annotations

import pandas as pd
import streamlit as st

from src.investigation.analyst_workbench import persist_analyst_state, update_analyst_record
from src.app.utils.dashboard_filters import parse_bool_value


def render_analyst_state_editor(
    *,
    selected_lead: pd.Series,
    analyst_state_df: pd.DataFrame,
    analyst_history_df: pd.DataFrame,
    analyst_state_path,
    analyst_history_path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    new_status = st.selectbox(
        "Lead Status",
        ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"],
        index=0 if str(selected_lead.get("status", "NEW")) not in ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"] else ["NEW", "IN_REVIEW", "REVIEWED", "CLOSED"].index(str(selected_lead.get("status", "NEW"))),
    )
    reviewer = st.text_input("Reviewer", value=str(selected_lead.get("reviewer", "")))
    follow_up = st.selectbox("Follow-up", ["", "Yes", "No"], index=["", "Yes", "No"].index(str(selected_lead.get("follow_up_needed", ""))) if str(selected_lead.get("follow_up_needed", "")) in {"", "Yes", "No"} else 0)
    bookmark = st.toggle("Bookmark", value=parse_bool_value(selected_lead.get("bookmark", False)))
    disposition = st.text_input("Disposition", value=str(selected_lead.get("disposition", "")))
    priority_override = st.text_input("Priority Override", value=str(selected_lead.get("priority_override", "")))
    notes = st.text_area("Notes", value=str(selected_lead.get("analyst_notes", "")))

    if st.button("Save Analyst State"):
        analyst_state_df, analyst_history_df = update_analyst_record(
            analyst_state_df,
            analyst_history_df,
            lead_id=str(selected_lead.get("lead_id", "")),
            reviewer=reviewer,
            updates={
                "status": new_status,
                "reviewer": reviewer,
                "follow_up_needed": follow_up,
                "bookmark": "true" if bookmark else "false",
                "disposition": disposition,
                "priority_override": priority_override,
                "analyst_notes": notes,
                "review_date": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
            },
        )
        persist_analyst_state(analyst_state_df, analyst_history_df, state_path=analyst_state_path, history_path=analyst_history_path)
        st.success("Analyst state saved.")
    return analyst_state_df, analyst_history_df
