from __future__ import annotations

from typing import Any


def build_recommendation(lead_row: dict[str, Any]) -> str:
    lead_type = str(lead_row.get("lead_type", "")).upper()
    primary_entity_type = str(lead_row.get("primary_entity_type", "")).lower()
    markers = str(lead_row.get("fraud_markers", "") or "").lower()
    if lead_type == "ADDRESS_CLUSTER" or primary_entity_type == "address":
        return (
            "Verify whether the address is residential, commercial, mailbox, virtual office, shelter, or multifamily; "
            "review businesses, owners, officers, phones, websites, and filing dates connected to the address."
        )
    if lead_type == "PROPERTY_CLUSTER" or primary_entity_type == "property":
        return (
            "Compare parcel owner, mailing address, business officers, registered agents, and related filings; "
            "confirm whether shared infrastructure explains the property-business correlation."
        )
    if lead_type == "COMMUNICATION_CLUSTER" or any(token in markers for token in ["phone", "email", "website"]):
        return (
            "Review all entities sharing the same phone, email, domain, or website and determine whether the "
            "identifier appears to belong to a service provider or facilitator."
        )
    if lead_type == "TEMPORAL_CLUSTER":
        return (
            "Review formation, filing, transaction, and source dates for coordinated activity and confirm whether "
            "the timing reflects a legitimate batch process or unusual concentration."
        )
    if lead_type == "NETWORK":
        return (
            "Identify central and bridge entities, review the shortest paths connecting high-risk members, and "
            "determine whether shared infrastructure explains the network legitimately."
        )
    if lead_type == "CROSS_SOURCE_CLUSTER":
        return (
            "Compare records across independent sources, validate whether the matched entity attributes resolve to "
            "the same party, and review any conflicting source values before escalation."
        )
    return "Review the primary entity, linked fraud markers, connected relationships, and supporting public sources before taking further action."


def review_steps(lead_row: dict[str, Any]) -> str:
    recommendation = build_recommendation(lead_row)
    if float(lead_row.get("evidence_completeness_score", 0) or 0) < 60:
        recommendation += " Lead requires evidence validation before any referral or enforcement action."
    return recommendation
