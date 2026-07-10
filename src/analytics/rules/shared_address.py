from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics.rules.base import BaseRule


class SharedAddressRule(BaseRule):
    name = "shared_address_businesses"
    description = "Multiple businesses sharing one address"
    base_score = 25

    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        rows = connection.execute(
            """
            SELECT address, COUNT(*) AS business_count, STRING_AGG(record_id, ',') AS entity_ids
            FROM business_entities
            GROUP BY address
            HAVING COUNT(*) > 1
            ORDER BY business_count DESC, address
            """
        ).fetchall()

        findings: List[Dict[str, object]] = []
        for address, business_count, entity_ids in rows:
            if int(business_count) < self.threshold + 1:
                continue
            risk_score = self.score + min(int(business_count) - 2, 5) * 5
            findings.append(
                {
                    "Risk Score": risk_score,
                    "Risk Level": "High" if risk_score >= 25 else "Medium",
                    "Rule Triggered": self.description_text,
                    "Supporting Evidence": f"{business_count} businesses share address {address}",
                    "Entity IDs": entity_ids,
                    "Addresses": address,
                    "Phone Numbers": "",
                    "Source Table": "business_entities",
                }
            )
        return findings
