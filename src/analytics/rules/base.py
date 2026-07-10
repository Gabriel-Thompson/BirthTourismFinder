from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

import duckdb


class BaseRule(ABC):
    name: str = ""
    description: str = ""
    base_score: int = 0

    def __init__(self, config: Optional[dict[str, object]] = None) -> None:
        self.config = config or {}

    @property
    def enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    @property
    def threshold(self) -> int:
        return int(self.config.get("threshold", 1))

    @property
    def score(self) -> int:
        return int(self.config.get("base_score", self.base_score))

    @property
    def description_text(self) -> str:
        return str(self.config.get("description", self.description))

    @abstractmethod
    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        """Return a list of anomaly findings."""
