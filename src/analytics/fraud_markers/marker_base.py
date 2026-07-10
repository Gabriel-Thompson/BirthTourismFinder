from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List

import pandas as pd


@dataclass
class FraudMarkerRecord:
    entity_id: str
    marker_id: str
    marker_name: str
    marker_category: str
    risk_contribution: int
    confidence: str
    confidence_score: float
    support: int
    sources: str
    source_types: str
    supporting_entities: str
    supporting_relationships: str
    recommended_review: str
    explanation: str
    raw_risk_contribution: int = 0
    contextual_adjustment: int = 0
    adjusted_risk_contribution: int = 0
    rarity_score: float = 0.0
    rarity_level: str = ""
    review_level: str = ""
    observed_value: float = 0.0
    expected_value: float = 0.0
    comparison_group: str = ""
    comparison_group_size: int = 0
    probability_or_p_value: str = ""
    model_used: str = ""
    assumptions: str = ""
    statistical_explanation: str = ""
    source_scope: str = ""

    def to_dict(self) -> dict[str, object]:
        output = asdict(self)
        if not output["raw_risk_contribution"]:
            output["raw_risk_contribution"] = int(output["risk_contribution"])
        if not output["adjusted_risk_contribution"]:
            output["adjusted_risk_contribution"] = int(output["risk_contribution"])
        return output


@dataclass
class MarkerContext:
    entities_df: pd.DataFrame
    relationships_df: pd.DataFrame
    aliases_df: pd.DataFrame
    config: dict[str, object]
    entity_lookup: Dict[str, dict[str, object]]
    outgoing: Dict[str, List[dict[str, object]]]
    incoming: Dict[str, List[dict[str, object]]]
    statistical_lookup: Dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    prior_marker_records: List[FraudMarkerRecord] = field(default_factory=list)


class BaseMarker:
    marker_id = "base_marker"
    marker_name = "Base Marker"
    category = "generic"

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    @property
    def weight(self) -> int:
        return int(self.config.get("weight", 10))

    @property
    def minimum_confidence(self) -> float:
        return float(self.config.get("minimum_confidence", 0.5))

    @property
    def minimum_support(self) -> int:
        return int(self.config.get("minimum_support", 1))

    @property
    def minimum_sources(self) -> int:
        return int(self.config.get("minimum_sources", 1))

    def evaluate(self, context: MarkerContext) -> List[FraudMarkerRecord]:
        raise NotImplementedError
