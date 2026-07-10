from __future__ import annotations

import abc
from typing import Any, Dict, List


class APIConnectorBase(abc.ABC):
    """Base interface for official API and open-data connectors."""

    source_name: str
    base_url: str
    endpoint: str
    query_params: Dict[str, Any]

    @abc.abstractmethod
    def fetch(self) -> str:
        """Fetch raw API response text."""

    @abc.abstractmethod
    def parse(self, payload: str) -> List[Dict[str, Any]]:
        """Parse raw API response text into records."""

    @abc.abstractmethod
    def normalize(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize parsed records into connector-standard rows."""

    @abc.abstractmethod
    def to_entities(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert normalized records to entity rows."""

    @abc.abstractmethod
    def to_relationships(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert normalized records to relationship rows."""
