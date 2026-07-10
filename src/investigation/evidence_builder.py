from __future__ import annotations

import pandas as pd

from src.connectors.source_metadata import merge_source_values


def _join(values: list[str]) -> str:
    return merge_source_values(*[value for value in values if value])


def build_evidence_packets(
    leads_df: pd.DataFrame,
    aliases_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    canonical_entities_df: pd.DataFrame,
    timeline_df: pd.DataFrame,
) -> pd.DataFrame:
    if leads_df.empty:
        return pd.DataFrame(
            columns=[
                "lead_id",
                "entity_id",
                "Primary Entity",
                "Aliases",
                "Fraud Markers",
                "Relationships",
                "Connected Entities",
                "Sources",
                "Timeline",
                "Supporting Evidence",
                "Risk Explanation",
                "Recommended Review",
                "Source",
                "Record ID",
                "Connector",
                "Import Date",
                "source_name",
                "source_type",
            ]
        )

    entity_lookup = canonical_entities_df.set_index("entity_id", drop=False) if not canonical_entities_df.empty and "entity_id" in canonical_entities_df.columns else pd.DataFrame()
    rows: list[dict[str, object]] = []
    for _, lead in leads_df.iterrows():
        entity_id = str(lead.get("entity_id", ""))
        lead_aliases = aliases_df[aliases_df.get("canonical_entity_id", pd.Series(dtype=str)).astype(str) == entity_id] if not aliases_df.empty else pd.DataFrame()
        lead_markers = fraud_markers_df[fraud_markers_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id] if not fraud_markers_df.empty else pd.DataFrame()
        lead_relationships = canonical_relationships_df[
            (canonical_relationships_df.get("source_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
            | (canonical_relationships_df.get("target_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
        ] if not canonical_relationships_df.empty else pd.DataFrame()
        lead_timeline = timeline_df[timeline_df.get("lead_id", pd.Series(dtype=str)).astype(str) == str(lead.get("lead_id", ""))] if not timeline_df.empty else pd.DataFrame()

        connected_entities: list[str] = []
        for _, relationship in lead_relationships.fillna("").iterrows():
            other_id = str(relationship.get("target_entity_id")) if str(relationship.get("source_entity_id")) == entity_id else str(relationship.get("source_entity_id"))
            if not entity_lookup.empty and other_id in entity_lookup.index and "display_name" in entity_lookup.columns:
                connected_entities.append(str(entity_lookup.loc[other_id, "display_name"]))
            elif other_id:
                connected_entities.append(other_id)

        rows.append(
            {
                "lead_id": lead["lead_id"],
                "entity_id": entity_id,
                "Primary Entity": lead.get("Primary Entity", ""),
                "Aliases": _join(lead_aliases.get("alias_value", pd.Series(dtype=str)).astype(str).tolist()),
                "Fraud Markers": _join(lead_markers.get("marker_name", pd.Series(dtype=str)).astype(str).tolist()),
                "Relationships": _join(lead_relationships.get("relationship_type", pd.Series(dtype=str)).astype(str).tolist()),
                "Connected Entities": _join(connected_entities),
                "Sources": _join(
                    lead_markers.get("sources", pd.Series(dtype=str)).astype(str).tolist()
                    + lead_relationships.get("source_name", pd.Series(dtype=str)).astype(str).tolist()
                    + [str(lead.get("source_name", ""))]
                ),
                "Timeline": " | ".join(
                    f"{row.get('Date', '')}: {row.get('Event', '')}"
                    for _, row in lead_timeline.fillna("").iterrows()
                ),
                "Supporting Evidence": _join(
                    lead_markers.get("explanation", pd.Series(dtype=str)).astype(str).tolist()
                    + lead_relationships.get("evidence", pd.Series(dtype=str)).astype(str).tolist()
                ),
                "Risk Explanation": str(lead.get("Risk Explanation", "")),
                "Recommended Review": str(lead.get("Recommended Review", "")),
                "Source": str(lead.get("source_name", "")),
                "Record ID": _join(
                    lead_markers.get("marker_id", pd.Series(dtype=str)).astype(str).tolist()
                    + lead_relationships.get("relationship_id", pd.Series(dtype=str)).astype(str).tolist()
                ),
                "Connector": str(lead.get("source_type", "")),
                "Import Date": str(lead.get("Date Generated", "")),
                "source_name": str(lead.get("source_name", "")),
                "source_type": str(lead.get("source_type", "")),
            }
        )

    return pd.DataFrame(rows)
