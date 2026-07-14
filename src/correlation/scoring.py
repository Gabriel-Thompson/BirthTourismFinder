from __future__ import annotations

from pathlib import Path
import json
from typing import Any


DEFAULT_CONFIG = {
    "weights": {
        "exact_address": 0.34,
        "exact_unit": 0.12,
        "exact_business_name": 0.18,
        "exact_person_name": 0.16,
        "exact_provider_identifier": 0.28,
        "multiple_independent_sources": 0.12,
        "address_building_only": 0.08,
        "address_mailing_only": 0.04,
        "address_association_bonus": 0.22,
        "residential_parcel_bonus": 0.06,
        "commercial_parcel_adjustment": -0.03,
        "recent_formation_adjustment": 0.03,
        "common_name_penalty": -0.16,
        "po_box_penalty": -0.28,
        "conflicting_address_penalty": -0.18,
        "missing_evidence_penalty": -0.12,
    },
    "thresholds": {
        "accepted_exact": 0.9,
        "review_strong": 0.72,
        "review_weak": 0.45,
    },
}

CONFIG_PATH = Path("config/correlation_scoring.json")


def load_scoring_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in loaded.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def weighted_score(evidence: dict[str, bool | int | float], config: dict[str, Any] | None = None) -> float:
    scoring = config or DEFAULT_CONFIG
    weights = scoring.get("weights", {})
    score = 0.0
    for key, value in evidence.items():
        if not value:
            continue
        contribution = float(weights.get(key, 0.0))
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            contribution *= float(value)
        score += contribution
    return max(0.0, min(1.0, score))


def classify_score(score: float, *, config: dict[str, Any] | None = None) -> str:
    thresholds = (config or DEFAULT_CONFIG).get("thresholds", {})
    if score >= float(thresholds.get("accepted_exact", 0.9)):
        return "ACCEPTED_EXACT"
    if score >= float(thresholds.get("review_strong", 0.72)):
        return "REVIEW_STRONG"
    if score >= float(thresholds.get("review_weak", 0.45)):
        return "REVIEW_WEAK"
    return "REJECTED"


def explain_evidence(
    *,
    matched: list[str],
    reduced_confidence: list[str],
    missing: list[str],
    next_step: str,
) -> str:
    parts = []
    if matched:
        parts.append(f"Matched on {', '.join(matched)}.")
    if reduced_confidence:
        parts.append(f"Confidence reduced by {', '.join(reduced_confidence)}.")
    if missing:
        parts.append(f"Missing evidence: {', '.join(missing)}.")
    parts.append(next_step)
    return " ".join(parts).strip()
