from __future__ import annotations

import pandas as pd
import streamlit as st


def show_dataframe(df: pd.DataFrame, *, empty_message: str, columns: list[str] | None = None) -> None:
    if df.empty:
        st.info(empty_message)
        return
    display_df = df[columns] if columns is not None else df
    st.dataframe(display_df, use_container_width=True, hide_index=True)
