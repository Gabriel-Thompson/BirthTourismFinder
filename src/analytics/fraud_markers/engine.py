from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.fraud_markers.marker_base import BaseMarker, FraudMarkerRecord, MarkerContext
from src.analytics.fraud_markers.marker_registry import get_registered_markers, register_marker
from src.connectors.source_metadata import is_real_source_type, merge_source_values

DB_PATH = Path("local_osint.duckdb")
CANONICAL_ENTITIES_PATH = Path("data/processed/canonical_entities.csv")
CANONICAL_RELATIONSHIPS_PATH = Path("data/processed/canonical_relationships.csv")
ALIASES_PATH = Path("data/processed/entity_aliases.csv")
FRAUD_MARKERS_PATH = Path("data/processed/fraud_markers.csv")
FRAUD_MARKER_SUMMARY_PATH = Path("data/processed/fraud_marker_summary.csv")
ANOMALY_COMPAT_PATH = Path("data/processed/anomaly_report.csv")
CONFIG_PATH = Path("config/fraud_markers.json")
CROSS_SOURCE_MATCHES_PATH = Path("data/processed/cross_source_matches.csv")
STATISTICAL_RARITY_PATH = Path("data/processed/statistical_rarity.csv")
STATISTICAL_ADJUSTMENTS_PATH = Path("data/processed/contextual_risk_adjustments.csv")


def load_fraud_marker_config(path: Path | str = CONFIG_PATH) -> dict[str, object]:
    defaults = {
        "risk_bands": {"high": 70, "medium": 35, "low": 1},
        "confidence_bands": {"very_high": 0.9, "high": 0.75, "medium": 0.55, "low": 0.35},
        "markers": {},
    }
    p = Path(path)
    if not p.exists():
        return defaults
    try:
        with p.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return defaults
    merged = defaults.copy()
    merged.update(data)
    return merged


def confidence_label(score: float, bands: dict[str, float]) -> str:
    if score >= float(bands.get("very_high", 0.9)):
        return "Very High"
    if score >= float(bands.get("high", 0.75)):
        return "High"
    if score >= float(bands.get("medium", 0.55)):
        return "Medium"
    if score >= float(bands.get("low", 0.35)):
        return "Low"
    return "Unknown"


def risk_level(score: int, config: dict[str, object]) -> str:
    bands = config.get("risk_bands", {})
    if score >= int(bands.get("high", 70)):
        return "High"
    if score >= int(bands.get("medium", 35)):
        return "Medium"
    if score >= int(bands.get("low", 1)):
        return "Low"
    return "None"


def merge_values(values: Iterable[str]) -> str:
    merged = ""
    for value in values:
        merged = merge_source_values(merged, str(value or ""))
    return merged


def related_entities(context: MarkerContext, entity_id: str, relationship_type: str | None = None, direction: str = "out") -> list[dict[str, object]]:
    rows = context.outgoing.get(entity_id, []) if direction == "out" else context.incoming.get(entity_id, [])
    if relationship_type is None:
        return list(rows)
    return [row for row in rows if str(row.get("relationship_type", "")) == relationship_type]


def entity_row(context: MarkerContext, entity_id: str) -> dict[str, object]:
    return context.entity_lookup.get(entity_id, {})


def marker_record(
    marker: BaseMarker,
    context: MarkerContext,
    entity_id: str,
    support: int,
    confidence_score: float,
    sources: list[str],
    source_types: list[str],
    supporting_entities: list[str],
    supporting_relationships: list[str],
    recommended_review: str,
    explanation: str,
) -> FraudMarkerRecord | None:
    if support < marker.minimum_support:
        return None
    distinct_sources = {token for token in merge_values(sources).split("|") if token}
    if len(distinct_sources) < marker.minimum_sources:
        return None
    if confidence_score < marker.minimum_confidence:
        return None
    statistical_row = context.statistical_lookup.get((entity_id, marker.marker_id), {})
    raw_risk_contribution = int(marker.weight)
    contextual_adjustment = int(pd.to_numeric(statistical_row.get("contextual_adjustment", 0), errors="coerce")) if statistical_row else 0
    adjusted_risk_contribution = int(
        pd.to_numeric(
            statistical_row.get("adjusted_marker_score", raw_risk_contribution) if statistical_row else raw_risk_contribution,
            errors="coerce",
        )
    )
    statistical_explanation = str(statistical_row.get("statistical_explanation", statistical_row.get("explanation", ""))) if statistical_row else ""
    review_level = str(statistical_row.get("rarity_level", "")) if statistical_row else ""
    explanation_text = explanation if not statistical_explanation else f"{explanation} Statistical context: {statistical_explanation}"
    recommended_review_text = recommended_review
    if review_level and review_level not in {"COMMON", "INSUFFICIENT_BASELINE"}:
        recommended_review_text = f"{recommended_review} Statistical review level: {review_level}."
    return FraudMarkerRecord(
        entity_id=entity_id,
        marker_id=marker.marker_id,
        marker_name=marker.marker_name,
        marker_category=marker.category,
        risk_contribution=adjusted_risk_contribution,
        raw_risk_contribution=raw_risk_contribution,
        contextual_adjustment=contextual_adjustment,
        adjusted_risk_contribution=adjusted_risk_contribution,
        confidence=confidence_label(confidence_score, context.config.get("confidence_bands", {})),
        confidence_score=round(confidence_score, 4),
        support=support,
        sources=merge_values(sources),
        source_types=merge_values(source_types),
        supporting_entities=merge_values(supporting_entities),
        supporting_relationships=merge_values(supporting_relationships),
        recommended_review=recommended_review_text,
        explanation=explanation_text,
        rarity_score=float(pd.to_numeric(statistical_row.get("rarity_score", 0), errors="coerce")) if statistical_row else 0.0,
        rarity_level=review_level,
        review_level=review_level,
        observed_value=float(pd.to_numeric(statistical_row.get("observed_value", 0), errors="coerce")) if statistical_row else 0.0,
        expected_value=float(pd.to_numeric(statistical_row.get("expected_value", 0), errors="coerce")) if statistical_row else 0.0,
        comparison_group=str(statistical_row.get("comparison_group", "")) if statistical_row else "",
        comparison_group_size=int(pd.to_numeric(statistical_row.get("comparison_group_size", 0), errors="coerce")) if statistical_row else 0,
        probability_or_p_value=str(statistical_row.get("probability_or_p_value", "")) if statistical_row else "",
        model_used=str(statistical_row.get("model_used", "")) if statistical_row else "",
        assumptions=str(statistical_row.get("assumptions", "")) if statistical_row else "",
        statistical_explanation=statistical_explanation,
        source_scope=str(statistical_row.get("source_scope", "")) if statistical_row else "",
    )


@register_marker("shared_address_businesses")
class SharedAddressBusinessesMarker(BaseMarker):
    marker_id = "shared_address_businesses"
    marker_name = "Shared Address"
    category = "address"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        address_groups: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in context.relationships_df.to_dict("records"):
            if str(row.get("relationship_type", "")) != "LOCATED_AT":
                continue
            source = entity_row(context, str(row.get("source_entity_id", "")))
            target = entity_row(context, str(row.get("target_entity_id", "")))
            if source.get("entity_type") == "business" and target.get("entity_type") == "address":
                address_groups[str(row["target_entity_id"])].append(row)

        for address_id, rows in address_groups.items():
            business_ids = sorted({str(row["source_entity_id"]) for row in rows})
            if len(business_ids) < self.minimum_support:
                continue
            score = min(0.55 + 0.08 * len(business_ids), 0.95)
            address_name = str(entity_row(context, address_id).get("display_name", address_id))
            rel_ids = [str(row.get("relationship_id", "")) for row in rows]
            for business_id in business_ids:
                business_name = str(entity_row(context, business_id).get("display_name", business_id))
                rec = marker_record(
                    self,
                    context,
                    entity_id=business_id,
                    support=len(business_ids),
                    confidence_score=score,
                    sources=[entity_row(context, bid).get("source_name", "") for bid in business_ids],
                    source_types=[entity_row(context, bid).get("source_type", "") for bid in business_ids],
                    supporting_entities=[address_id, *business_ids],
                    supporting_relationships=rel_ids,
                    recommended_review="Review the shared address cluster and validate whether the co-located businesses are operationally distinct.",
                    explanation=f"{len(business_ids)} businesses share address {address_name}, including {business_name}.",
                )
                if rec is not None:
                    records.append(rec)
        return records


@register_marker("mailbox_address_cluster")
class MailboxAddressClusterMarker(BaseMarker):
    marker_id = "mailbox_address_cluster"
    marker_name = "Mailbox Style Address Cluster"
    category = "address"
    MAILBOX_TOKENS = ("PO BOX", "P O BOX", "PMB", "MAIL DROP", "UPS STORE", "BOX ")

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        inbound_counts: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in context.relationships_df.to_dict("records"):
            if "ADDRESS" not in str(row.get("relationship_type", "")) and str(row.get("relationship_type", "")) != "LOCATED_AT":
                continue
            target = entity_row(context, str(row.get("target_entity_id", "")))
            if target.get("entity_type") == "address":
                inbound_counts[str(row["target_entity_id"])].append(row)

        for address_id, rows in inbound_counts.items():
            display_name = str(entity_row(context, address_id).get("display_name", "")).upper()
            if not any(token in display_name for token in self.MAILBOX_TOKENS):
                continue
            support_entities = sorted({str(row["source_entity_id"]) for row in rows})
            if len(support_entities) < self.minimum_support:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=address_id,
                support=len(support_entities),
                confidence_score=min(0.6 + 0.05 * len(support_entities), 0.92),
                sources=[entity_row(context, eid).get("source_name", "") for eid in support_entities],
                source_types=[entity_row(context, eid).get("source_type", "") for eid in support_entities],
                supporting_entities=[address_id, *support_entities],
                supporting_relationships=[str(row.get("relationship_id", "")) for row in rows],
                recommended_review="Review whether the address is a mailbox, forwarding service, or high-volume registration point.",
                explanation=f"Address {display_name} is mailbox-style and is reused by {len(support_entities)} connected entities.",
            )
            if rec is not None:
                records.append(rec)
        return records


@register_marker("mailing_address_reuse")
class MailingAddressReuseMarker(BaseMarker):
    marker_id = "mailing_address_reuse"
    marker_name = "Mailing Address Reuse"
    category = "address"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        groups: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in context.relationships_df.to_dict("records"):
            if str(row.get("relationship_type", "")) != "PROPERTY_HAS_MAILING_ADDRESS":
                continue
            groups[str(row["target_entity_id"])].append(row)

        for address_id, rows in groups.items():
            sources = sorted({str(row["source_entity_id"]) for row in rows})
            if len(sources) < self.minimum_support:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=address_id,
                support=len(sources),
                confidence_score=min(0.58 + 0.06 * len(sources), 0.88),
                sources=[entity_row(context, eid).get("source_name", "") for eid in sources],
                source_types=[entity_row(context, eid).get("source_type", "") for eid in sources],
                supporting_entities=[address_id, *sources],
                supporting_relationships=[str(row.get("relationship_id", "")) for row in rows],
                recommended_review="Review whether the same mailing address is acting as a hub for multiple property records or owners.",
                explanation=f"Mailing address {entity_row(context, address_id).get('display_name', address_id)} is reused across {len(sources)} property records.",
            )
            if rec is not None:
                records.append(rec)
        return records


class _SharedIdentifierMarker(BaseMarker):
    relationship_type = ""
    target_type = ""
    marker_label = ""

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        groups: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in context.relationships_df.to_dict("records"):
            if str(row.get("relationship_type", "")) != self.relationship_type:
                continue
            target = entity_row(context, str(row.get("target_entity_id", "")))
            if target.get("entity_type") != self.target_type:
                continue
            groups[str(row["target_entity_id"])].append(row)

        for target_id, rows in groups.items():
            source_ids = sorted({str(row["source_entity_id"]) for row in rows})
            if len(source_ids) < self.minimum_support:
                continue
            display = str(entity_row(context, target_id).get("display_name", target_id))
            rel_ids = [str(row.get("relationship_id", "")) for row in rows]
            for entity_id in source_ids:
                rec = marker_record(
                    self,
                    context,
                    entity_id=entity_id,
                    support=len(source_ids),
                    confidence_score=min(0.6 + 0.07 * len(source_ids), 0.93),
                    sources=[entity_row(context, eid).get("source_name", "") for eid in source_ids],
                    source_types=[entity_row(context, eid).get("source_type", "") for eid in source_ids],
                    supporting_entities=[target_id, *source_ids],
                    supporting_relationships=rel_ids,
                    recommended_review=f"Review whether the shared {self.marker_label.lower()} reflects a common operator, service provider, or shell-company pattern.",
                    explanation=f"{len(source_ids)} entities share {self.marker_label.lower()} {display}.",
                )
                if rec is not None:
                    records.append(rec)
        return records


@register_marker("shared_phone")
class SharedPhoneMarker(_SharedIdentifierMarker):
    marker_id = "shared_phone"
    marker_name = "Shared Phone"
    category = "communication"
    relationship_type = "USES_PHONE"
    target_type = "phone"
    marker_label = "Phone"


@register_marker("shared_email")
class SharedEmailMarker(_SharedIdentifierMarker):
    marker_id = "shared_email"
    marker_name = "Shared Email"
    category = "communication"
    relationship_type = "USES_EMAIL"
    target_type = "email"
    marker_label = "Email"


@register_marker("shared_website")
class SharedWebsiteMarker(_SharedIdentifierMarker):
    marker_id = "shared_website"
    marker_name = "Shared Website"
    category = "communication"
    relationship_type = "HAS_WEBSITE"
    target_type = "website"
    marker_label = "Website"


@register_marker("similar_business_names")
class SimilarBusinessNamesMarker(BaseMarker):
    marker_id = "similar_business_names"
    marker_name = "Similar Business Names"
    category = "business"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        from difflib import SequenceMatcher

        records: List[FraudMarkerRecord] = []
        address_to_businesses: dict[str, list[str]] = defaultdict(list)
        for row in context.relationships_df.to_dict("records"):
            if str(row.get("relationship_type", "")) != "LOCATED_AT":
                continue
            source = entity_row(context, str(row.get("source_entity_id", "")))
            target = entity_row(context, str(row.get("target_entity_id", "")))
            if source.get("entity_type") == "business" and target.get("entity_type") == "address":
                address_to_businesses[str(row["target_entity_id"])].append(str(row["source_entity_id"]))

        for address_id, business_ids in address_to_businesses.items():
            unique_ids = sorted(set(business_ids))
            if len(unique_ids) < 2:
                continue
            for idx, left_id in enumerate(unique_ids):
                left_name = str(entity_row(context, left_id).get("normalized_value") or entity_row(context, left_id).get("display_name", ""))
                cluster = [left_id]
                for right_id in unique_ids[idx + 1 :]:
                    right_name = str(entity_row(context, right_id).get("normalized_value") or entity_row(context, right_id).get("display_name", ""))
                    if SequenceMatcher(None, left_name, right_name).ratio() >= 0.86:
                        cluster.append(right_id)
                if len(cluster) < 2:
                    continue
                for business_id in cluster:
                    rec = marker_record(
                        self,
                        context,
                        entity_id=business_id,
                        support=len(cluster),
                        confidence_score=0.62,
                        sources=[entity_row(context, bid).get("source_name", "") for bid in cluster],
                        source_types=[entity_row(context, bid).get("source_type", "") for bid in cluster],
                        supporting_entities=[address_id, *cluster],
                        supporting_relationships=[],
                        recommended_review="Review whether the similar business names reflect the same operator or a cluster of closely related companies.",
                        explanation=f"Business name variants cluster at the same address {entity_row(context, address_id).get('display_name', address_id)}.",
                    )
                    if rec is not None:
                        records.append(rec)
        return records


@register_marker("arcgis_owner_in_business_records")
class ArcgisOwnerAppearsInBusinessRecordsMarker(BaseMarker):
    marker_id = "arcgis_owner_in_business_records"
    marker_name = "ArcGIS Owner Appears in Business Records"
    category = "cross_source"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        entities = context.entities_df.fillna("").to_dict("records")
        owner_rows = [
            row for row in entities
            if str(row.get("entity_type", "")) == "owner" and "arcgis" in str(row.get("source_type", "")).split("|")
        ]
        business_like_rows = [
            row for row in entities
            if str(row.get("entity_type", "")) in {"business", "owner", "officer", "person", "registered_agent"}
            and "arcgis" not in str(row.get("source_type", "")).split("|")
        ]
        by_normalized: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in business_like_rows:
            normalized = str(row.get("normalized_value", "")).strip()
            if normalized:
                by_normalized[normalized].append(row)

        for owner in owner_rows:
            matches = by_normalized.get(str(owner.get("normalized_value", "")).strip(), [])
            if not matches:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=str(owner["entity_id"]),
                support=len(matches),
                confidence_score=min(0.72 + 0.05 * len(matches), 0.96),
                sources=[str(owner.get("source_name", "")), *[str(match.get("source_name", "")) for match in matches]],
                source_types=[str(owner.get("source_type", "")), *[str(match.get("source_type", "")) for match in matches]],
                supporting_entities=[str(owner["entity_id"]), *[str(match["entity_id"]) for match in matches]],
                supporting_relationships=[],
                recommended_review="Review whether the parcel owner also appears in business-registration records across sources.",
                explanation=f"ArcGIS owner {owner.get('display_name', owner['entity_id'])} appears in {len(matches)} business-related records across other sources.",
            )
            if rec is not None:
                records.append(rec)
        return records


@register_marker("county_clerk_party_in_business_records")
class CountyClerkPartyAppearsInBusinessRecordsMarker(BaseMarker):
    marker_id = "county_clerk_party_in_business_records"
    marker_name = "County Clerk Party Appears in Business Records"
    category = "cross_source"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        entities = context.entities_df.fillna("").to_dict("records")
        clerk_rows = [
            row for row in entities
            if "county_clerk_local_file" in str(row.get("source_name", "")).split("|")
            and str(row.get("entity_type", "")) in {"person", "business", "owner"}
        ]
        other_rows = [
            row for row in entities
            if "county_clerk_local_file" not in str(row.get("source_name", "")).split("|")
            and str(row.get("entity_type", "")) in {"business", "owner", "officer", "person", "registered_agent"}
        ]
        by_normalized: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in other_rows:
            normalized = str(row.get("normalized_value", "")).strip()
            if normalized:
                by_normalized[normalized].append(row)

        for clerk in clerk_rows:
            matches = by_normalized.get(str(clerk.get("normalized_value", "")).strip(), [])
            if not matches:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=str(clerk["entity_id"]),
                support=len(matches),
                confidence_score=min(0.7 + 0.05 * len(matches), 0.94),
                sources=[str(clerk.get("source_name", "")), *[str(match.get("source_name", "")) for match in matches]],
                source_types=[str(clerk.get("source_type", "")), *[str(match.get("source_type", "")) for match in matches]],
                supporting_entities=[str(clerk["entity_id"]), *[str(match["entity_id"]) for match in matches]],
                supporting_relationships=[],
                recommended_review="Review whether the public court party also appears in business-registration or ownership records.",
                explanation=f"County clerk entity {clerk.get('display_name', clerk['entity_id'])} matches {len(matches)} business-related records across other sources.",
            )
            if rec is not None:
                records.append(rec)
        return records


@register_marker("dense_entity_cluster")
class DenseEntityClusterMarker(BaseMarker):
    marker_id = "dense_entity_cluster"
    marker_name = "Dense Entity Cluster"
    category = "network"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        for entity_id, rels in context.outgoing.items():
            total = len(rels) + len(context.incoming.get(entity_id, []))
            if total < self.minimum_support:
                continue
            distinct_types = {
                str(entity_row(context, str(rel["target_entity_id"])).get("entity_type", ""))
                for rel in rels
            } | {
                str(entity_row(context, str(rel["source_entity_id"])).get("entity_type", ""))
                for rel in context.incoming.get(entity_id, [])
            }
            if len(distinct_types) < 2:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=entity_id,
                support=total,
                confidence_score=min(0.5 + 0.03 * total, 0.9),
                sources=[str(entity_row(context, entity_id).get("source_name", ""))],
                source_types=[str(entity_row(context, entity_id).get("source_type", ""))],
                supporting_entities=[entity_id, *[str(rel["target_entity_id"]) for rel in rels]],
                supporting_relationships=[str(rel.get("relationship_id", "")) for rel in rels[:25]],
                recommended_review="Review the dense relationship cluster for potential intermediary or hub behavior.",
                explanation=f"Entity {entity_row(context, entity_id).get('display_name', entity_id)} participates in a dense cluster with {total} relationships.",
            )
            if rec is not None:
                records.append(rec)
        return records


@register_marker("bridge_entity")
class BridgeEntityMarker(BaseMarker):
    marker_id = "bridge_entity"
    marker_name = "Bridge Entity"
    category = "network"

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        for entity_id in context.entity_lookup:
            rels = context.outgoing.get(entity_id, []) + context.incoming.get(entity_id, [])
            if len(rels) < self.minimum_support:
                continue
            source_names = set()
            relationship_types = set()
            other_types = set()
            for rel in rels:
                relationship_types.add(str(rel.get("relationship_type", "")))
                source_names.update(str(rel.get("source_name", "")).split("|"))
                other_id = str(rel.get("target_entity_id") if str(rel.get("source_entity_id")) == entity_id else rel.get("source_entity_id"))
                other_types.add(str(entity_row(context, other_id).get("entity_type", "")))
            source_names = {token for token in source_names if token}
            if len(source_names) < self.minimum_sources or len(other_types) < 2 or len(relationship_types) < 2:
                continue
            rec = marker_record(
                self,
                context,
                entity_id=entity_id,
                support=len(rels),
                confidence_score=min(0.58 + 0.04 * len(other_types) + 0.03 * len(source_names), 0.91),
                sources=list(source_names),
                source_types=[str(entity_row(context, entity_id).get("source_type", ""))],
                supporting_entities=[entity_id],
                supporting_relationships=[str(rel.get("relationship_id", "")) for rel in rels[:25]],
                recommended_review="Review whether the entity is bridging otherwise separate clusters or source systems.",
                explanation=f"Entity {entity_row(context, entity_id).get('display_name', entity_id)} bridges {len(other_types)} entity types across {len(source_names)} sources.",
            )
            if rec is not None:
                records.append(rec)
        return records


@register_marker("business_cluster_compound")
class BusinessClusterCompoundMarker(BaseMarker):
    marker_id = "business_cluster_compound"
    marker_name = "Business Cluster Compound Marker"
    category = "compound"
    component_markers = {"Shared Address", "Shared Phone", "Shared Email", "Shared Website", "Similar Business Names"}

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        records: List[FraudMarkerRecord] = []
        by_entity: dict[str, list[FraudMarkerRecord]] = defaultdict(list)
        for record in context.prior_marker_records:
            if record.marker_name in self.component_markers:
                by_entity[record.entity_id].append(record)

        for entity_id, component_records in by_entity.items():
            marker_names = {record.marker_name for record in component_records}
            if len(marker_names) < self.minimum_support:
                continue
            score = min(0.72 + 0.06 * len(marker_names), 0.97)
            rec = marker_record(
                self,
                context,
                entity_id=entity_id,
                support=len(marker_names),
                confidence_score=score,
                sources=[record.sources for record in component_records],
                source_types=[record.source_types for record in component_records],
                supporting_entities=[entity_id, *[record.supporting_entities for record in component_records]],
                supporting_relationships=[record.supporting_relationships for record in component_records],
                recommended_review="Investigate whether the clustered identifiers indicate a coordinated business network or shell-company pattern.",
                explanation=f"Entity {entity_row(context, entity_id).get('display_name', entity_id)} triggered compound markers: {', '.join(sorted(marker_names))}.",
            )
            if rec is not None:
                records.append(rec)
        return records


class FraudMarkerEngine:
    def __init__(
        self,
        db_path: Path | str = DB_PATH,
        entities_path: Path | str = CANONICAL_ENTITIES_PATH,
        relationships_path: Path | str = CANONICAL_RELATIONSHIPS_PATH,
        aliases_path: Path | str = ALIASES_PATH,
        output_path: Path | str = FRAUD_MARKERS_PATH,
        summary_path: Path | str = FRAUD_MARKER_SUMMARY_PATH,
        compatibility_output_path: Path | str = ANOMALY_COMPAT_PATH,
        cross_source_matches_path: Path | str = CROSS_SOURCE_MATCHES_PATH,
        statistical_rarity_path: Path | str = STATISTICAL_RARITY_PATH,
        statistical_adjustments_path: Path | str = STATISTICAL_ADJUSTMENTS_PATH,
        config_path: Path | str = CONFIG_PATH,
    ) -> None:
        self.db_path = Path(db_path)
        self.entities_path = Path(entities_path)
        self.relationships_path = Path(relationships_path)
        self.aliases_path = Path(aliases_path)
        self.output_path = Path(output_path)
        self.summary_path = Path(summary_path)
        self.compatibility_output_path = Path(compatibility_output_path)
        self.cross_source_matches_path = Path(cross_source_matches_path)
        self.statistical_rarity_path = Path(statistical_rarity_path)
        self.statistical_adjustments_path = Path(statistical_adjustments_path)
        self.config_path = Path(config_path)
        self.config = load_fraud_marker_config(self.config_path)
        self.markers = self.load_markers()

    def load_markers(self) -> List[BaseMarker]:
        markers: List[BaseMarker] = []
        marker_configs = self.config.get("markers", {})
        for marker_id, marker_cls in get_registered_markers().items():
            markers.append(marker_cls(marker_configs.get(marker_id, {})))
        return markers

    def load_context(self) -> MarkerContext:
        entities_df = pd.read_csv(self.entities_path) if self.entities_path.exists() else pd.DataFrame()
        relationships_df = pd.read_csv(self.relationships_path) if self.relationships_path.exists() else pd.DataFrame()
        aliases_df = pd.read_csv(self.aliases_path) if self.aliases_path.exists() else pd.DataFrame()
        statistical_lookup: Dict[tuple[str, str], dict[str, object]] = {}
        if self.statistical_rarity_path.exists() and self.statistical_rarity_path.stat().st_size > 0:
            rarity_df = pd.read_csv(self.statistical_rarity_path).fillna("")
            if self.statistical_adjustments_path.exists() and self.statistical_adjustments_path.stat().st_size > 0:
                adjustments_df = pd.read_csv(self.statistical_adjustments_path).fillna("")
                rarity_df = rarity_df.merge(
                    adjustments_df[["entity_id", "marker_id", "contextual_adjustment", "adjusted_marker_score", "reason_for_adjustment"]],
                    on=["entity_id", "marker_id"],
                    how="left",
                )
            for row in rarity_df.to_dict("records"):
                statistical_lookup[(str(row.get("entity_id", "")), str(row.get("marker_id", "")))] = row
        entity_lookup = {
            str(row["entity_id"]): row
            for row in entities_df.fillna("").to_dict("records")
            if str(row.get("entity_id", "")).strip()
        }
        outgoing: Dict[str, List[dict[str, object]]] = defaultdict(list)
        incoming: Dict[str, List[dict[str, object]]] = defaultdict(list)
        for row in relationships_df.fillna("").to_dict("records"):
            source_id = str(row.get("source_entity_id", "")).strip()
            target_id = str(row.get("target_entity_id", "")).strip()
            if source_id:
                outgoing[source_id].append(row)
            if target_id:
                incoming[target_id].append(row)
        return MarkerContext(
            entities_df=entities_df,
            relationships_df=relationships_df,
            aliases_df=aliases_df,
            config=self.config,
            entity_lookup=entity_lookup,
            outgoing=outgoing,
            incoming=incoming,
            statistical_lookup=statistical_lookup,
        )

    def run(self) -> List[Dict[str, object]]:
        start_time = time.time()
        print("Fraud Marker Engine: started")
        print(f"Fraud Marker Engine: canonical entities input {self.entities_path}")
        print(f"Fraud Marker Engine: canonical relationships input {self.relationships_path}")
        print(f"Fraud Marker Engine: config {self.config_path}")
        context = self.load_context()
        print(f"Fraud Marker Engine: entities loaded {len(context.entities_df)}")
        print(f"Fraud Marker Engine: relationships loaded {len(context.relationships_df)}")
        all_records: List[FraudMarkerRecord] = []

        for marker in self.markers:
            if not marker.enabled:
                print(f"Fraud Marker Engine: skipped disabled marker {marker.marker_id}")
                continue
            print(f"Fraud Marker Engine: evaluating marker {marker.marker_id}")
            if marker.marker_id == "business_cluster_compound":
                context.prior_marker_records = list(all_records)
            marker_records = marker.evaluate(context)
            all_records.extend(marker_records)
            print(f"Fraud Marker Engine: marker {marker.marker_id} produced {len(marker_records)} records")

        cross_source_records = self.build_cross_source_records()
        all_records.extend(cross_source_records)
        print(f"Fraud Marker Engine: cross_source produced {len(cross_source_records)} records")

        rows = [record.to_dict() for record in all_records]
        fraud_markers_df = pd.DataFrame(rows)
        self.write_outputs(fraud_markers_df)
        compatibility_rows = self.write_compatibility_output(fraud_markers_df)
        duration = time.time() - start_time
        print(f"Fraud Marker Engine: wrote {len(fraud_markers_df)} fraud markers to {self.output_path}")
        print(f"Fraud Marker Engine: wrote compatibility anomaly report to {self.compatibility_output_path}")
        print(f"Fraud Marker Engine: completed in {duration:.2f}s")
        return compatibility_rows

    def build_cross_source_records(self) -> List[FraudMarkerRecord]:
        if not self.cross_source_matches_path.exists() or self.cross_source_matches_path.stat().st_size == 0:
            return []
        matches_df = pd.read_csv(self.cross_source_matches_path)
        if matches_df.empty:
            return []
        matches_df = matches_df[
            (matches_df["decision"].astype(str) == "AUTO_MATCH")
            & (pd.to_numeric(matches_df.get("independent_real_source_count", 0), errors="coerce").fillna(0) >= 2)
        ].copy()
        if matches_df.empty:
            return []

        marker_map = {
            "exact_canonical_address": ("cross_source_multi_source_address", "Cross-Source Address Support", "cross_source"),
            "exact_canonical_phone": ("cross_source_shared_identifier", "Cross-Source Shared Phone", "cross_source"),
            "exact_canonical_email": ("cross_source_shared_identifier", "Cross-Source Shared Email", "cross_source"),
            "exact_canonical_website": ("cross_source_shared_identifier", "Cross-Source Shared Website", "cross_source"),
            "property_situs_matches_business_address": ("cross_source_property_business_address", "Business Principal Address Matches Parcel Situs Address", "cross_source"),
            "property_mailing_matches_business_address": ("cross_source_property_business_address", "Business Address Matches Parcel Mailing Address", "cross_source"),
            "parcel_owner_matches_business_name": ("cross_source_owner_business", "Parcel Owner Appears as a Business Entity", "cross_source"),
            "parcel_owner_matches_person_with_secondary": ("cross_source_owner_person", "Parcel Owner Matches Business Officer or Registered Agent", "cross_source"),
            "clerk_party_matches_business_person_with_secondary": ("cross_source_clerk_business", "Clerk Party Matches Business Officer or Owner", "cross_source"),
        }
        marker_config = self.config.get("markers", {})
        records: List[FraudMarkerRecord] = []
        rarity_df = pd.read_csv(self.statistical_rarity_path).fillna("") if self.statistical_rarity_path.exists() and self.statistical_rarity_path.stat().st_size > 0 else pd.DataFrame()
        adjustment_df = pd.read_csv(self.statistical_adjustments_path).fillna("") if self.statistical_adjustments_path.exists() and self.statistical_adjustments_path.stat().st_size > 0 else pd.DataFrame()
        for _, row in matches_df.iterrows():
            match_method = str(row.get("match_method", ""))
            marker_id, marker_name, category = marker_map.get(
                match_method,
                ("cross_source_multi_source_cluster", "Property, Business, and Person Form a Multi-Source Cluster", "cross_source"),
            )
            config_row = marker_config.get(marker_id, {})
            weight = int(config_row.get("weight", 18))
            statistical_row = {}
            if not rarity_df.empty:
                # Cross-source statistics are keyed on the canonical entity and derived cross-source marker id.
                matched = rarity_df[
                    (rarity_df["entity_id"].astype(str) == str(row.get("canonical_entity_id", "")))
                    & (rarity_df["marker_id"].astype(str) == marker_id)
                ].head(1)
                if not matched.empty:
                    statistical_row = matched.iloc[0].to_dict()
                    if not adjustment_df.empty:
                        adj_match = adjustment_df[
                            (adjustment_df["entity_id"].astype(str) == str(row.get("canonical_entity_id", "")))
                            & (adjustment_df["marker_id"].astype(str) == marker_id)
                        ].head(1)
                        if not adj_match.empty:
                            statistical_row.update(adj_match.iloc[0].to_dict())
            confidence_score = float(pd.to_numeric(row.get("confidence", 0), errors="coerce"))
            confidence_label_value = confidence_label(confidence_score, self.config.get("confidence_bands", {}))
            sources = merge_values([str(row.get("left_source_name", "")), str(row.get("right_source_name", ""))])
            source_types = merge_values([str(row.get("left_source_type", "")), str(row.get("right_source_type", ""))])
            support_entities = merge_values([str(row.get("left_entity_id", "")), str(row.get("right_entity_id", "")), str(row.get("canonical_entity_id", ""))])
            adjusted_weight = int(pd.to_numeric(statistical_row.get("adjusted_marker_score", weight), errors="coerce")) if statistical_row else weight
            contextual_adjustment = int(pd.to_numeric(statistical_row.get("contextual_adjustment", 0), errors="coerce")) if statistical_row else 0
            statistical_explanation = str(statistical_row.get("explanation", "")) if statistical_row else ""
            explanation_text = str(row.get("evidence", ""))
            if statistical_explanation:
                explanation_text = f"{explanation_text} Statistical context: {statistical_explanation}"
            records.append(
                FraudMarkerRecord(
                    entity_id=str(row.get("canonical_entity_id", "")),
                    marker_id=marker_id,
                    marker_name=marker_name,
                    marker_category=category,
                    risk_contribution=adjusted_weight,
                    raw_risk_contribution=weight,
                    contextual_adjustment=contextual_adjustment,
                    adjusted_risk_contribution=adjusted_weight,
                    confidence=confidence_label_value,
                    confidence_score=round(confidence_score, 4),
                    support=int(pd.to_numeric(row.get("independent_real_source_count", 0), errors="coerce")),
                    sources=sources,
                    source_types=source_types,
                    supporting_entities=support_entities,
                    supporting_relationships="",
                    recommended_review=f"Review side-by-side evidence from {row.get('left_source_name', '')} and {row.get('right_source_name', '')} and confirm the cross-source relationship manually.",
                    explanation=explanation_text,
                    rarity_score=float(pd.to_numeric(statistical_row.get("rarity_score", 0), errors="coerce")) if statistical_row else 0.0,
                    rarity_level=str(statistical_row.get("rarity_level", "")) if statistical_row else "",
                    review_level=str(statistical_row.get("rarity_level", "")) if statistical_row else "",
                    observed_value=float(pd.to_numeric(statistical_row.get("observed_value", 0), errors="coerce")) if statistical_row else 0.0,
                    expected_value=float(pd.to_numeric(statistical_row.get("expected_value", 0), errors="coerce")) if statistical_row else 0.0,
                    comparison_group=str(statistical_row.get("comparison_group", "")) if statistical_row else "",
                    comparison_group_size=int(pd.to_numeric(statistical_row.get("comparison_group_size", 0), errors="coerce")) if statistical_row else 0,
                    probability_or_p_value=str(statistical_row.get("probability_or_p_value", "")) if statistical_row else "",
                    model_used=str(statistical_row.get("model_used", "")) if statistical_row else "",
                    assumptions=str(statistical_row.get("assumptions", "")) if statistical_row else "",
                    statistical_explanation=statistical_explanation,
                    source_scope=str(statistical_row.get("source_scope", "")) if statistical_row else "",
                )
            )
        return records

    def summarize(self, findings: List[Dict[str, object]]) -> Dict[str, int]:
        summary = {"High": 0, "Medium": 0, "Low": 0}
        for finding in findings:
            level = str(finding.get("Risk Level", "Low")).capitalize()
            if level in summary:
                summary[level] += 1
            else:
                summary["Low"] += 1
        return summary

    def write_outputs(self, fraud_markers_df: pd.DataFrame) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        if fraud_markers_df.empty:
            pd.DataFrame(
                columns=[
                    "entity_id",
                    "marker_id",
                    "marker_name",
                    "marker_category",
                    "risk_contribution",
                    "raw_risk_contribution",
                    "contextual_adjustment",
                    "adjusted_risk_contribution",
                    "confidence",
                    "confidence_score",
                    "support",
                    "sources",
                    "source_types",
                    "supporting_entities",
                    "supporting_relationships",
                    "recommended_review",
                    "explanation",
                    "rarity_score",
                    "rarity_level",
                    "review_level",
                    "observed_value",
                    "expected_value",
                    "comparison_group",
                    "comparison_group_size",
                    "probability_or_p_value",
                    "model_used",
                    "assumptions",
                    "statistical_explanation",
                    "source_scope",
                ]
            ).to_csv(self.output_path, index=False)
            pd.DataFrame(columns=["marker_name", "marker_category", "frequency", "average_risk_contribution", "average_support", "average_confidence_score"]).to_csv(
                self.summary_path, index=False
            )
            return
        fraud_markers_df.to_csv(self.output_path, index=False)
        summary = (
            fraud_markers_df.groupby(["marker_name", "marker_category"], dropna=False)
            .agg(
                frequency=("entity_id", "count"),
                average_risk_contribution=("risk_contribution", "mean"),
                average_support=("support", "mean"),
                average_confidence_score=("confidence_score", "mean"),
                average_rarity_score=("rarity_score", "mean"),
            )
            .reset_index()
        )
        summary.to_csv(self.summary_path, index=False)

    def write_compatibility_output(self, fraud_markers_df: pd.DataFrame) -> List[Dict[str, object]]:
        fieldnames = [
            "Risk Score",
            "Risk Level",
            "Rule Triggered",
            "Supporting Evidence",
            "Entity IDs",
            "Addresses",
            "Phone Numbers",
            "Source Table",
            "source_name",
            "source_type",
            "data_scope",
        ]
        rows: List[Dict[str, object]] = []
        if not fraud_markers_df.empty:
            for _, row in fraud_markers_df.iterrows():
                entity_ids = merge_values([str(row.get("entity_id", "")), str(row.get("supporting_entities", ""))])
                supporting_entities = str(row.get("supporting_entities", "") or "")
                addresses = "|".join(token for token in supporting_entities.split("|") if token.startswith("canonical:address:"))
                phones = "|".join(token for token in supporting_entities.split("|") if token.startswith("canonical:phone:"))
                source_types = str(row.get("source_types", "") or "")
                rows.append(
                    {
                        "Risk Score": int(row.get("risk_contribution", 0)),
                        "Risk Level": risk_level(int(row.get("risk_contribution", 0)), self.config),
                        "Rule Triggered": str(row.get("marker_name", "")),
                        "Supporting Evidence": str(row.get("explanation", "")),
                        "Entity IDs": entity_ids,
                        "Addresses": addresses,
                        "Phone Numbers": phones,
                        "Source Table": str(row.get("marker_category", "")),
                        "source_name": str(row.get("sources", "")),
                        "source_type": source_types,
                        "data_scope": "real" if is_real_source_type(source_types) else "synthetic",
                    }
                )
        self.compatibility_output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.compatibility_output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the configurable Fraud Marker Engine against canonical OpenFraud outputs.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to the DuckDB database")
    parser.add_argument("--entities-path", default=str(CANONICAL_ENTITIES_PATH), help="Path to canonical entities CSV")
    parser.add_argument("--relationships-path", default=str(CANONICAL_RELATIONSHIPS_PATH), help="Path to canonical relationships CSV")
    parser.add_argument("--aliases-path", default=str(ALIASES_PATH), help="Path to entity aliases CSV")
    parser.add_argument("--output-path", default=str(FRAUD_MARKERS_PATH), help="Path to fraud markers CSV")
    parser.add_argument("--summary-path", default=str(FRAUD_MARKER_SUMMARY_PATH), help="Path to fraud marker summary CSV")
    parser.add_argument("--compatibility-output-path", default=str(ANOMALY_COMPAT_PATH), help="Path to compatibility anomaly CSV")
    parser.add_argument("--config-path", default=str(CONFIG_PATH), help="Path to fraud marker config JSON")
    args = parser.parse_args()

    engine = FraudMarkerEngine(
        db_path=args.db_path,
        entities_path=args.entities_path,
        relationships_path=args.relationships_path,
        aliases_path=args.aliases_path,
        output_path=args.output_path,
        summary_path=args.summary_path,
        compatibility_output_path=args.compatibility_output_path,
        config_path=args.config_path,
    )
    findings = engine.run()
    summary = engine.summarize(findings)
    print("Fraud Marker Engine: PASS")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
