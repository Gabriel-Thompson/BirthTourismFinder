from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics.rules.base import BaseRule


class KnownPatternAddressRule(BaseRule):
    name = "known_pattern_addresses"
    description = "Addresses listed in known_patterns"
    base_score = 15

    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        rows = connection.execute(
            """
            SELECT DISTINCT kp.entity_name, bp.address, kp.keyword, kp.record_id AS pattern_id
            FROM known_patterns kp
            LEFT JOIN business_entities bp ON bp.business_name = kp.entity_name
            WHERE bp.address IS NOT NULL
            ORDER BY bp.address, kp.entity_name
            """
        ).fetchall()

        findings: List[Dict[str, object]] = []
        for entity_name, address, keyword, pattern_id in rows:
            findings.append(
                {
                    "Risk Score": self.score,
                    "Risk Level": "Medium" if self.score >= 15 else "Low",
                    "Rule Triggered": self.description_text,
                    "Supporting Evidence": f"Known pattern keyword '{keyword}' linked to address {address}",
                    "Entity IDs": str(pattern_id),
                    "Addresses": address,
                    "Phone Numbers": "",
                    "Source Table": "known_patterns",
                }
            )
        return findings
