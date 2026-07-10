from __future__ import annotations

import pandas as pd

REAL_SOURCE_TYPES = {"connector", "api", "arcgis", "manual"}
TRUE_VALUES = {"true", "1", "yes", "y", "t"}
FALSE_VALUES = {"false", "0", "no", "n", "f", ""}


def token_set(value: object) -> set[str]:
    return {token.strip() for token in str(value or "").split("|") if token.strip()}


def parse_bool_value(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(int(value))
    normalized = str(value).strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    return default


def parse_bool_series(series: pd.Series, default: bool = False) -> pd.Series:
    return series.apply(lambda value: parse_bool_value(value, default=default))


def filter_dataframe_by_source_scope(df: pd.DataFrame, scope: str, source_names: list[str] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    filtered = df.copy()
    source_type_column = "source_type" if "source_type" in filtered.columns else "source_types"
    source_name_column = "source_name" if "source_name" in filtered.columns else "sources" if "sources" in filtered.columns else "source_names"
    if source_type_column not in filtered.columns:
        return filtered
    if scope == "real_only":
        filtered = filtered[filtered[source_type_column].apply(lambda value: bool(token_set(value) & REAL_SOURCE_TYPES))]
    elif scope == "synthetic_only":
        filtered = filtered[filtered[source_type_column].apply(lambda value: "synthetic" in token_set(value) and not bool(token_set(value) & REAL_SOURCE_TYPES))]
    if source_names and source_name_column in filtered.columns:
        selected = set(source_names)
        filtered = filtered[filtered[source_name_column].apply(lambda value: bool(token_set(value) & selected))]
    return filtered
