from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics.rules.base import BaseRule


class SharedWebsiteRule(BaseRule):
    name = "shared_address_websites"
    description = "Multiple websites linked to one address"
    base_score = 10

    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        rows = connection.execute(
            """
            SELECT address, COUNT(DISTINCT website) AS website_count, STRING_AGG(DISTINCT website, '|') AS websites, STRING_AGG(record_id, ',') AS entity_ids
            FROM business_entities
            WHERE website IS NOT NULL AND website <> ''
            GROUP BY address
            HAVING COUNT(DISTINCT website) > 1
            ORDER BY website_count DESC, address
            """
        ).fetchall()

        findings: List[Dict[str, object]] = []
        for address, website_count, websites, entity_ids in rows:
            if int(website_count) < self.threshold + 1:
                continue
            risk_score = self.score + min(int(website_count) - 2, 5) * 2
            findings.append(
                {
                    "Risk Score": risk_score,
                    "Risk Level": "Medium" if risk_score >= 15 else "Low",
                    "Rule Triggered": self.description_text,
                    "Supporting Evidence": f"{website_count} websites appear at address {address}",
                    "Entity IDs": entity_ids,
                    "Addresses": address,
                    "Phone Numbers": "",
                    "Source Table": "business_entities",
                }
            )
        return findings
