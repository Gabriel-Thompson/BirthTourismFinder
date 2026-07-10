from __future__ import annotations

import pandas as pd


EVENT_LABELS = {
    "PROPERTY_OWNED_BY": "Property ownership link",
    "PROPERTY_HAS_SITUS_ADDRESS": "Property situs address",
    "PROPERTY_HAS_MAILING_ADDRESS": "Property mailing address",
    "CASE_HAS_PARTY": "Court party linked",
    "CASE_HAS_DOCUMENT": "Court document linked",
    "BUSINESS_LINKED_TO_CASE": "Business linked to case",
    "LOCATED_AT": "Business location",
    "USES_PHONE": "Phone usage",
    "USES_EMAIL": "Email usage",
    "HAS_WEBSITE": "Website usage",
}

DATE_COLUMNS = ["sale_date", "filing_date", "record_date", "date", "first_seen", "last_seen", "import_date", "Date Generated"]


def _first_value(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        value = str(row.get(column, "") or "").strip()
        if value:
            return value
    return ""


def build_entity_timelines(
    leads_df: pd.DataFrame,
    canonical_entities_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
) -> pd.DataFrame:
    if leads_df.empty:
        return pd.DataFrame(
            columns=[
                "lead_id",
                "entity_id",
                "Date",
                "Event",
                "Entity",
                "Source",
                "source_name",
                "source_type",
                "Evidence",
                "Record ID",
                "Connector",
                "Import Date",
            ]
        )

    entity_lookup = canonical_entities_df.set_index("entity_id", drop=False) if not canonical_entities_df.empty and "entity_id" in canonical_entities_df.columns else pd.DataFrame()
    rows: list[dict[str, object]] = []

    for _, lead in leads_df.iterrows():
        entity_id = str(lead.get("entity_id", ""))
        display_name = str(lead.get("Primary Entity", entity_id))
        lead_relationships = canonical_relationships_df[
            (canonical_relationships_df.get("source_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
            | (canonical_relationships_df.get("target_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
        ].copy() if not canonical_relationships_df.empty else pd.DataFrame()

        for _, relationship in lead_relationships.fillna("").iterrows():
            relationship_type = str(relationship.get("relationship_type", ""))
            other_id = str(relationship.get("target_entity_id")) if str(relationship.get("source_entity_id")) == entity_id else str(relationship.get("source_entity_id"))
            other_name = (
                str(entity_lookup.loc[other_id, "display_name"])
                if not entity_lookup.empty and other_id in entity_lookup.index and "display_name" in entity_lookup.columns
                else other_id
            )
            rows.append(
                {
                    "lead_id": lead["lead_id"],
                    "entity_id": entity_id,
                    "Date": _first_value(relationship, DATE_COLUMNS) or str(lead.get("Date Generated", "")),
                    "Event": EVENT_LABELS.get(relationship_type, relationship_type or "Relationship event"),
                    "Entity": display_name,
                    "Source": str(relationship.get("source_name", "")),
                    "source_name": str(relationship.get("source_name", "")),
                    "source_type": str(relationship.get("source_type", "")),
                    "Evidence": str(relationship.get("evidence", "")) or f"{display_name} linked to {other_name} via {relationship_type}",
                    "Record ID": str(relationship.get("relationship_id", "")),
                    "Connector": str(relationship.get("source_type", "")),
                    "Import Date": _first_value(relationship, DATE_COLUMNS),
                }
            )

        lead_markers = fraud_markers_df[fraud_markers_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id].copy() if not fraud_markers_df.empty else pd.DataFrame()
        for _, marker in lead_markers.fillna("").iterrows():
            rows.append(
                {
                    "lead_id": lead["lead_id"],
                    "entity_id": entity_id,
                    "Date": str(lead.get("Date Generated", "")),
                    "Event": f"Fraud marker: {marker.get('marker_name', '')}",
                    "Entity": display_name,
                    "Source": str(marker.get("sources", "")),
                    "source_name": str(marker.get("sources", "")),
                    "source_type": str(marker.get("source_types", "")),
                    "Evidence": str(marker.get("explanation", "")),
                    "Record ID": str(marker.get("marker_id", "")),
                    "Connector": str(marker.get("source_types", "")),
                    "Import Date": "",
                }
            )

    timeline_df = pd.DataFrame(rows)
    if timeline_df.empty:
        return timeline_df
    return timeline_df.sort_values(["lead_id", "Date", "Event"], ascending=[True, True, True]).reset_index(drop=True)
