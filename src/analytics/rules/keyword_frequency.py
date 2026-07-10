from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from analytics.rules.base import BaseRule


class KeywordFrequencyRule(BaseRule):
    name = "keyword_frequency"
    description = "Repeated maternity/postpartum/travel keywords"
    base_score = 25

    def execute(self, connection: duckdb.DuckDBPyConnection) -> List[Dict[str, object]]:
        rows = connection.execute(
            """
            SELECT keyword, COUNT(*) AS keyword_count, STRING_AGG(entity_name, '|') AS entity_names, STRING_AGG(record_id, ',') AS entity_ids
            FROM known_patterns
            GROUP BY keyword
            HAVING COUNT(*) > 1
            ORDER BY keyword_count DESC, keyword
            """
        ).fetchall()

        findings: List[Dict[str, object]] = []
        for keyword, keyword_count, entity_names, entity_ids in rows:
            if int(keyword_count) < self.threshold + 1:
                continue
            risk_score = self.score + min(int(keyword_count) - 2, 5) * 4
            findings.append(
                {
                    "Risk Score": risk_score,
                    "Risk Level": "High" if risk_score >= 25 else "Medium",
                    "Rule Triggered": self.description_text,
                    "Supporting Evidence": f"Keyword '{keyword}' appears {keyword_count} times",
                    "Entity IDs": entity_ids,
                    "Addresses": "",
                    "Phone Numbers": "",
                    "Source Table": "known_patterns",
                }
            )
        return findings
