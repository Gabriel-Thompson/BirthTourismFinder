from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, List


class ConnectorBase(ABC):
    """Base connector interface for local ingestion sources."""

    @abstractmethod
    def discover_inputs(self) -> Iterable[Path]:
        """List available source files for ingestion."""

    @abstractmethod
    def ingest(self, source_path: Path) -> List[Dict[str, Any]]:
        """Read source data into normalized row dictionaries."""

    @abstractmethod
    def export_entities(self, rows: List[Dict[str, Any]], output_path: Path) -> None:
        """Export normalized entities to CSV."""

    @abstractmethod
    def export_relationships(self, rows: List[Dict[str, Any]], output_path: Path) -> None:
        """Export normalized relationships to CSV."""
