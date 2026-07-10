from __future__ import annotations

from collections import defaultdict
from typing import Any

import pandas as pd

from src.analytics.statistical_risk.context import classify_address_context
from src.analytics.statistical_risk.models import StatisticalBaselineRow
from src.analytics.statistical_risk.rarity import most_common, rolling_window_peak
from src.connectors.source_metadata import is_real_source_type, merge_source_values

MARKER_LABELS = {
    "shared_address_businesses": "Shared Address",
    "mailbox_address_cluster": "Mailbox Style Address Cluster",
    "mailing_address_reuse": "Mailing Address Reuse",
    "shared_phone": "Shared Phone",
    "shared_email": "Shared Email",
    "shared_website": "Shared Website",
    "arcgis_owner_in_business_records": "ArcGIS Owner Appears in Business Records",
    "county_clerk_party_in_business_records": "County Clerk Party Appears in Business Records",
    "dense_entity_cluster": "Dense Entity Cluster",
    "bridge_entity": "Bridge Entity",
    "cross_source_multi_source_cluster": "Cross-Source Multi-Source Cluster",
}


def _in_scope(source_type: object, mode: str) -> bool:
    normalized = str(mode or "REAL_ONLY").upper()
    value = str(source_type or "")
    if normalized == "REAL_ONLY":
        return is_real_source_type(value)
    if normalized == "SYNTHETIC_ONLY":
        return "synthetic" in {token.strip().lower() for token in value.split("|") if token.strip()}
    return True


def _entity_lookup(entities_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {
        str(row["entity_id"]): row
        for row in entities_df.fillna("").to_dict("records")
        if str(row.get("entity_id", "")).strip()
    }


def _relationship_rows(relationships_df: pd.DataFrame, mode: str) -> list[dict[str, Any]]:
    if relationships_df.empty:
        return []
    return [
        row
        for row in relationships_df.fillna("").to_dict("records")
        if _in_scope(row.get("source_type", ""), mode)
    ]


def build_statistical_observations(
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    cross_source_matches_df: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    mode = str(config.get("baseline_mode", "REAL_ONLY"))
    lookup = _entity_lookup(entities_df)
    rows: list[dict[str, Any]] = []
    relationship_rows = _relationship_rows(relationships_df, mode)
    temporal_windows = [int(value) for value in config.get("temporal_windows", [3, 7, 30, 90, 365])]

    address_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    identifier_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    incident_counts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in relationship_rows:
        source_id = str(row.get("source_entity_id", ""))
        target_id = str(row.get("target_entity_id", ""))
        source_row = lookup.get(source_id, {})
        target_row = lookup.get(target_id, {})
        rel_type = str(row.get("relationship_type", ""))
        incident_counts[source_id].append(row)
        incident_counts[target_id].append(row)
        if target_row.get("entity_type") == "address" and rel_type in {"LOCATED_AT", "PROPERTY_HAS_MAILING_ADDRESS", "PROPERTY_HAS_SITUS_ADDRESS"}:
            address_groups[target_id].append(row)
        if rel_type in {"USES_PHONE", "USES_EMAIL", "HAS_WEBSITE"}:
            identifier_groups[(rel_type, target_id)].append(row)

    for address_id, group_rows in address_groups.items():
        address_row = lookup.get(address_id, {})
        address_display = str(address_row.get("display_name", ""))
        entity_types = [str(lookup.get(str(row.get("source_entity_id", "")), {}).get("entity_type", "")) for row in group_rows]
        address_context = classify_address_context(address_display, connected_entity_types=entity_types)
        dates = [row.get("imported_at", "") for row in group_rows]
        peak_window = max((rolling_window_peak(dates, window_days) for window_days in temporal_windows), default=0)
        businesses = sorted({str(row.get("source_entity_id", "")) for row in group_rows if str(lookup.get(str(row.get("source_entity_id", "")), {}).get("entity_type", "")) == "business"})
        if businesses:
            for business_id in businesses:
                rows.append(
                    {
                        "marker_id": "shared_address_businesses",
                        "marker_name": MARKER_LABELS["shared_address_businesses"],
                        "entity_id": business_id,
                        "entity_type": "business",
                        "source_name": str(lookup.get(business_id, {}).get("source_name", "")),
                        "source_type": str(lookup.get(business_id, {}).get("source_type", "")),
                        "jurisdiction": merge_source_values(*[str(row.get("jurisdiction", "")) for row in group_rows]),
                        "source_scope": mode,
                        "address_context": address_context["address_context"],
                        "base_building_address": address_context["base_building_address"],
                        "unit_level_address": address_context["unit_level_address"],
                        "classification_confidence": address_context["classification_confidence"],
                        "observed_value": float(len(businesses)),
                        "observation_date": most_common(dates),
                        "temporal_peak": peak_window,
                        "comparison_group_hint": address_context["address_context"],
                    }
                )
        mailing_properties = sorted({str(row.get("source_entity_id", "")) for row in group_rows if str(row.get("relationship_type", "")) == "PROPERTY_HAS_MAILING_ADDRESS"})
        if mailing_properties:
            rows.append(
                {
                    "marker_id": "mailing_address_reuse",
                    "marker_name": MARKER_LABELS["mailing_address_reuse"],
                    "entity_id": address_id,
                    "entity_type": "address",
                    "source_name": str(address_row.get("source_name", "")),
                    "source_type": str(address_row.get("source_type", "")),
                    "jurisdiction": merge_source_values(*[str(row.get("jurisdiction", "")) for row in group_rows]),
                    "source_scope": mode,
                    "address_context": address_context["address_context"],
                    "base_building_address": address_context["base_building_address"],
                    "unit_level_address": address_context["unit_level_address"],
                    "classification_confidence": address_context["classification_confidence"],
                    "observed_value": float(len(mailing_properties)),
                    "observation_date": most_common(dates),
                    "temporal_peak": peak_window,
                    "comparison_group_hint": address_context["address_context"],
                }
            )
        if address_context["address_context"] == "VIRTUAL_OFFICE_OR_MAILBOX":
            linked_entities = sorted({str(row.get("source_entity_id", "")) for row in group_rows})
            rows.append(
                {
                    "marker_id": "mailbox_address_cluster",
                    "marker_name": MARKER_LABELS["mailbox_address_cluster"],
                    "entity_id": address_id,
                    "entity_type": "address",
                    "source_name": str(address_row.get("source_name", "")),
                    "source_type": str(address_row.get("source_type", "")),
                    "jurisdiction": merge_source_values(*[str(row.get("jurisdiction", "")) for row in group_rows]),
                    "source_scope": mode,
                    "address_context": address_context["address_context"],
                    "base_building_address": address_context["base_building_address"],
                    "unit_level_address": address_context["unit_level_address"],
                    "classification_confidence": address_context["classification_confidence"],
                    "observed_value": float(len(linked_entities)),
                    "observation_date": most_common(dates),
                    "temporal_peak": peak_window,
                    "comparison_group_hint": address_context["address_context"],
                }
            )

    identifier_map = {
        "USES_PHONE": "shared_phone",
        "USES_EMAIL": "shared_email",
        "HAS_WEBSITE": "shared_website",
    }
    for (rel_type, target_id), group_rows in identifier_groups.items():
        entity_ids = sorted({str(row.get("source_entity_id", "")) for row in group_rows})
        if not entity_ids:
            continue
        dates = [row.get("imported_at", "") for row in group_rows]
        peak_window = max((rolling_window_peak(dates, window_days) for window_days in temporal_windows), default=0)
        marker_id = identifier_map[rel_type]
        for entity_id in entity_ids:
            rows.append(
                {
                    "marker_id": marker_id,
                    "marker_name": MARKER_LABELS[marker_id],
                    "entity_id": entity_id,
                    "entity_type": str(lookup.get(entity_id, {}).get("entity_type", "")),
                    "source_name": str(lookup.get(entity_id, {}).get("source_name", "")),
                    "source_type": str(lookup.get(entity_id, {}).get("source_type", "")),
                    "jurisdiction": merge_source_values(*[str(row.get("jurisdiction", "")) for row in group_rows]),
                    "source_scope": mode,
                    "address_context": "UNKNOWN",
                    "base_building_address": "",
                    "unit_level_address": "",
                    "classification_confidence": 0.0,
                    "observed_value": float(len(entity_ids)),
                    "observation_date": most_common(dates),
                    "temporal_peak": peak_window,
                    "comparison_group_hint": str(lookup.get(entity_id, {}).get("entity_type", "")),
                }
            )

    for entity_id, rels in incident_counts.items():
        entity_row = lookup.get(entity_id, {})
        if not entity_row:
            continue
        dates = [row.get("imported_at", "") for row in rels]
        peak_window = max((rolling_window_peak(dates, window_days) for window_days in temporal_windows), default=0)
        source_names = {token for row in rels for token in str(row.get("source_name", "")).split("|") if token.strip()}
        other_types = set()
        for rel in rels:
            other_id = str(rel.get("target_entity_id", "")) if str(rel.get("source_entity_id", "")) == entity_id else str(rel.get("source_entity_id", ""))
            other_types.add(str(lookup.get(other_id, {}).get("entity_type", "")))
        rows.append(
            {
                "marker_id": "dense_entity_cluster",
                "marker_name": MARKER_LABELS["dense_entity_cluster"],
                "entity_id": entity_id,
                "entity_type": str(entity_row.get("entity_type", "")),
                "source_name": str(entity_row.get("source_name", "")),
                "source_type": str(entity_row.get("source_type", "")),
                "jurisdiction": merge_source_values(*[str(rel.get("jurisdiction", "")) for rel in rels]),
                "source_scope": mode,
                "address_context": "UNKNOWN",
                "base_building_address": "",
                "unit_level_address": "",
                "classification_confidence": 0.0,
                "observed_value": float(len(rels)),
                "observation_date": most_common(dates),
                "temporal_peak": peak_window,
                "comparison_group_hint": str(entity_row.get("entity_type", "")),
            }
        )
        rows.append(
            {
                "marker_id": "bridge_entity",
                "marker_name": MARKER_LABELS["bridge_entity"],
                "entity_id": entity_id,
                "entity_type": str(entity_row.get("entity_type", "")),
                "source_name": str(entity_row.get("source_name", "")),
                "source_type": str(entity_row.get("source_type", "")),
                "jurisdiction": merge_source_values(*[str(rel.get("jurisdiction", "")) for rel in rels]),
                "source_scope": mode,
                "address_context": "UNKNOWN",
                "base_building_address": "",
                "unit_level_address": "",
                "classification_confidence": 0.0,
                "observed_value": float(len(source_names) + len([value for value in other_types if value])),
                "observation_date": most_common(dates),
                "temporal_peak": peak_window,
                "comparison_group_hint": str(entity_row.get("entity_type", "")),
            }
        )

    scoped_entities = entities_df[
        entities_df.get("source_type", pd.Series(dtype=str)).apply(lambda value: _in_scope(value, mode))
    ].copy() if not entities_df.empty else pd.DataFrame()
    if not scoped_entities.empty and "normalized_value" in scoped_entities.columns:
        business_like = scoped_entities[
            scoped_entities["entity_type"].astype(str).isin(["owner", "business", "person", "registered_agent", "officer"])
            & scoped_entities["normalized_value"].fillna("").astype(str).ne("")
        ].copy()
        for normalized_value, group in business_like.groupby("normalized_value"):
            arcgis_owners = group[(group["entity_type"].astype(str) == "owner") & group["source_type"].astype(str).str.contains("arcgis", case=False, na=False)]
            non_arcgis = group[~group["source_type"].astype(str).str.contains("arcgis", case=False, na=False)]
            if not arcgis_owners.empty and not non_arcgis.empty:
                for _, owner_row in arcgis_owners.iterrows():
                    rows.append(
                        {
                            "marker_id": "arcgis_owner_in_business_records",
                            "marker_name": MARKER_LABELS["arcgis_owner_in_business_records"],
                            "entity_id": str(owner_row["entity_id"]),
                            "entity_type": str(owner_row["entity_type"]),
                            "source_name": str(owner_row.get("source_name", "")),
                            "source_type": str(owner_row.get("source_type", "")),
                            "jurisdiction": str(owner_row.get("jurisdiction", "")),
                            "source_scope": mode,
                            "address_context": "UNKNOWN",
                            "base_building_address": "",
                            "unit_level_address": "",
                            "classification_confidence": 0.0,
                            "observed_value": float(len(non_arcgis)),
                            "observation_date": str(owner_row.get("imported_at", "")),
                            "temporal_peak": 0,
                            "comparison_group_hint": "owner_cross_source",
                        }
                    )
            clerk_rows = group[group["source_name"].astype(str).str.contains("county_clerk", case=False, na=False)]
            business_rows = group[group["entity_type"].astype(str).isin(["business", "officer", "registered_agent", "person"]) & ~group["source_name"].astype(str).str.contains("county_clerk", case=False, na=False)]
            if not clerk_rows.empty and not business_rows.empty:
                for _, clerk_row in clerk_rows.iterrows():
                    rows.append(
                        {
                            "marker_id": "county_clerk_party_in_business_records",
                            "marker_name": MARKER_LABELS["county_clerk_party_in_business_records"],
                            "entity_id": str(clerk_row["entity_id"]),
                            "entity_type": str(clerk_row["entity_type"]),
                            "source_name": str(clerk_row.get("source_name", "")),
                            "source_type": str(clerk_row.get("source_type", "")),
                            "jurisdiction": str(clerk_row.get("jurisdiction", "")),
                            "source_scope": mode,
                            "address_context": "UNKNOWN",
                            "base_building_address": "",
                            "unit_level_address": "",
                            "classification_confidence": 0.0,
                            "observed_value": float(len(business_rows)),
                            "observation_date": str(clerk_row.get("imported_at", "")),
                            "temporal_peak": 0,
                            "comparison_group_hint": "clerk_cross_source",
                        }
                    )

    if not cross_source_matches_df.empty:
        scoped_matches = cross_source_matches_df[
            cross_source_matches_df.get("contains_real_data", pd.Series(dtype=bool)).astype(str).str.lower().isin(["true", "1"])
        ].copy() if mode == "REAL_ONLY" else cross_source_matches_df.copy()
        scoped_matches = scoped_matches[scoped_matches["decision"].astype(str) == "AUTO_MATCH"] if not scoped_matches.empty else scoped_matches
        for canonical_entity_id, group in scoped_matches.groupby("canonical_entity_id"):
            rows.append(
                {
                    "marker_id": "cross_source_multi_source_cluster",
                    "marker_name": MARKER_LABELS["cross_source_multi_source_cluster"],
                    "entity_id": str(canonical_entity_id),
                    "entity_type": str(group["entity_type"].iloc[0]),
                    "source_name": merge_source_values(*group["left_source_name"].astype(str).tolist(), *group["right_source_name"].astype(str).tolist()),
                    "source_type": merge_source_values(*group["left_source_type"].astype(str).tolist(), *group["right_source_type"].astype(str).tolist()),
                    "jurisdiction": "",
                    "source_scope": mode,
                    "address_context": "UNKNOWN",
                    "base_building_address": "",
                    "unit_level_address": "",
                    "classification_confidence": 0.0,
                    "observed_value": float(len(group)),
                    "observation_date": "",
                    "temporal_peak": 0,
                    "comparison_group_hint": str(group["entity_type"].iloc[0]),
                }
            )

    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(
            columns=[
                "marker_id", "marker_name", "entity_id", "entity_type", "source_name", "source_type",
                "jurisdiction", "source_scope", "address_context", "base_building_address", "unit_level_address",
                "classification_confidence", "observed_value", "observation_date", "temporal_peak", "comparison_group_hint",
            ]
        )
    return output


def summarize_baselines(observations_df: pd.DataFrame) -> pd.DataFrame:
    if observations_df.empty:
        return pd.DataFrame(columns=[field for field in StatisticalBaselineRow.__dataclass_fields__])
    grouped = observations_df.copy()
    grouped["comparison_group"] = grouped.apply(
        lambda row: "|".join(
            [
                str(row.get("marker_id", "")),
                str(row.get("entity_type", "")),
                str(row.get("source_scope", "")),
                str(row.get("address_context", "UNKNOWN") or "UNKNOWN"),
                str(row.get("jurisdiction", "") or "ALL"),
                str(row.get("comparison_group_hint", "") or "ALL"),
            ]
        ),
        axis=1,
    )
    rows: list[dict[str, object]] = []
    for _, group in grouped.groupby("comparison_group"):
        values = pd.to_numeric(group["observed_value"], errors="coerce").fillna(0)
        rows.append(
            StatisticalBaselineRow(
                marker_id=str(group["marker_id"].iloc[0]),
                entity_type=str(group["entity_type"].iloc[0]),
                source_scope=str(group["source_scope"].iloc[0]),
                comparison_group=str(group["comparison_group"].iloc[0]),
                address_context=str(group["address_context"].iloc[0]),
                jurisdiction=str(group["jurisdiction"].iloc[0]),
                source_name=str(group["source_name"].iloc[0]),
                comparison_group_size=int(len(group)),
                observed_mean=round(float(values.mean()), 4),
                observed_median=round(float(values.median()), 4),
                observed_max=round(float(values.max()), 4),
                observed_min=round(float(values.min()), 4),
                observed_p90=round(float(values.quantile(0.9)), 4),
            ).to_dict()
        )
    return pd.DataFrame(rows)
