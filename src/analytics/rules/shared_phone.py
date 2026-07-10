from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics.rules.base import BaseRule


class SharedPhoneRule(BaseRule):
    name = "shared_address_phones"
    description = "Multiple phone numbers linked to one address"
    base_score = 20

    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        rows = connection.execute(
            """
            SELECT address, COUNT(DISTINCT phone) AS phone_count, STRING_AGG(DISTINCT phone, ',') AS phone_numbers, STRING_AGG(record_id, ',') AS entity_ids
            FROM business_entities
            WHERE phone IS NOT NULL AND phone <> ''
            GROUP BY address
            HAVING COUNT(DISTINCT phone) > 1
            ORDER BY phone_count DESC, address
            """
        ).fetchall()

        findings: List[Dict[str, object]] = []
        for address, phone_count, phone_numbers, entity_ids in rows:
            if int(phone_count) < self.threshold + 1:
                continue
            risk_score = self.score + min(int(phone_count) - 2, 5) * 3
            findings.append(
                {
                    "Risk Score": risk_score,
                    "Risk Level": "High" if risk_score >= 25 else "Medium",
                    "Rule Triggered": self.description_text,
                    "Supporting Evidence": f"{phone_count} phone numbers appear at address {address}",
                    "Entity IDs": entity_ids,
                    "Addresses": address,
                    "Phone Numbers": phone_numbers,
                    "Source Table": "business_entities",
                }
            )
        return findings
