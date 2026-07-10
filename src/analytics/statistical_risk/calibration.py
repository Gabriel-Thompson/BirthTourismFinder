from __future__ import annotations

from typing import Any


def determine_review_level(
    probability: float | None,
    percentile: float,
    rarity_score: float,
    thresholds: dict[str, float],
    *,
    insufficient_baseline: bool = False,
) -> str:
    if insufficient_baseline:
        return "INSUFFICIENT_BASELINE"
    if probability is not None:
        if probability <= float(thresholds.get("extreme_outlier", 0.001)):
            return "EXTREME_OUTLIER"
        if probability <= float(thresholds.get("immediate_review", 0.01)):
            return "IMMEDIATE_REVIEW"
        if probability <= float(thresholds.get("elevated_review", 0.05)):
            return "ELEVATED_REVIEW"
        if probability <= float(thresholds.get("routine_review", 0.15)):
            return "ROUTINE_REVIEW"
        return "COMMON"
    if percentile >= float(thresholds.get("percentile_extreme", 0.999)):
        return "EXTREME_OUTLIER"
    if percentile >= float(thresholds.get("percentile_immediate", 0.99)):
        return "IMMEDIATE_REVIEW"
    if percentile >= float(thresholds.get("percentile_elevated", 0.95)):
        return "ELEVATED_REVIEW"
    if rarity_score >= 20:
        return "ROUTINE_REVIEW"
    return "COMMON"


def adjustment_value_for_context(
    marker_id: str,
    address_context: str,
    config: dict[str, Any],
    *,
    source_count: int = 0,
    support: int = 0,
) -> tuple[int, str, str]:
    address_adjustments = config.get("address_context_adjustments", {})
    communication_adjustments = config.get("communication_context_adjustments", {})
    property_adjustments = config.get("property_context_adjustments", {})
    max_positive = int(config.get("maximum_positive_adjustment", 8))
    max_negative = abs(int(config.get("maximum_negative_adjustment", -8)))
    context_key = str(address_context or "UNKNOWN").upper()

    adjustment = 0
    reason = "No contextual adjustment applied."
    category = "none"

    if marker_id in {"shared_address_businesses", "mailbox_address_cluster", "mailing_address_reuse"}:
        adjustment = int(address_adjustments.get(context_key, 0))
        reason = f"Address context {context_key} adjusted the marker score."
        category = "address_context"
    elif marker_id in {"shared_phone", "shared_email", "shared_website"}:
        adjustment = int(communication_adjustments.get(context_key, 0))
        if support >= 5 and source_count <= 1:
            adjustment = min(adjustment, -2)
            reason = "High-volume communication reuse within one source is more likely operationally routine."
        else:
            reason = f"Communication context {context_key} adjusted the marker score."
        category = "communication_context"
    elif marker_id in {"arcgis_owner_in_business_records", "county_clerk_party_in_business_records"}:
        adjustment = int(property_adjustments.get(context_key, 0))
        reason = f"Property/owner context {context_key} adjusted the marker score."
        category = "property_context"

    adjustment = max(-max_negative, min(max_positive, adjustment))
    return adjustment, category, reason
