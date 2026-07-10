from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class StatisticalBaselineRow:
    marker_id: str
    entity_type: str
    source_scope: str
    comparison_group: str
    address_context: str
    jurisdiction: str
    source_name: str
    comparison_group_size: int
    observed_mean: float
    observed_median: float
    observed_max: float
    observed_min: float
    observed_p90: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class StatisticalRarityRow:
    marker_id: str
    marker_name: str
    entity_id: str
    entity_type: str
    source_name: str
    source_type: str
    jurisdiction: str
    source_scope: str
    address_context: str
    base_building_address: str
    unit_level_address: str
    classification_confidence: float
    observed_value: float
    expected_value: float
    comparison_group: str
    comparison_group_size: int
    percentile: float
    probability_or_p_value: float | str
    rarity_score: float
    rarity_level: str
    model_used: str
    assumptions: str
    explanation: str
    observation_date: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class ContextualAdjustmentRow:
    marker_id: str
    marker_name: str
    entity_id: str
    entity_type: str
    address_context: str
    adjustment_category: str
    original_marker_score: int
    contextual_adjustment: int
    adjusted_marker_score: int
    reason_for_adjustment: str
    source_scope: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
