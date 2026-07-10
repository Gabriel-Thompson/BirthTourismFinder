from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb

DB_PATH = Path("local_osint.duckdb")
OUTPUT_PATH = Path("data/processed/anomaly_report.csv")

RULES = {
    "shared_address_businesses": {
        "label": "Multiple businesses sharing one address",
        "weight": 25,
        "severity": "High",
    },
    "shared_address_phones": {
        "label": "Multiple phone numbers linked to one address",
        "weight": 20,
        "severity": "Medium",
    },
    "shared_address_websites": {
        "label": "Multiple websites linked to one address",
        "weight": 10,
        "severity": "Low",
    },
    "shared_phone_llcs": {
        "label": "Multiple LLCs sharing one phone number",
        "weight": 20,
        "severity": "Medium",
    },
    "keyword_repeats": {
        "label": "Repeated maternity/postpartum/travel keywords",
        "weight": 25,
        "severity": "High",
    },
    "known_pattern_addresses": {
        "label": "Addresses listed in known_patterns",
        "weight": 15,
        "severity": "Medium",
    },
}


def get_connection(db_path: Path | str = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))


def run_query(conn: duckdb.DuckDBPyConnection, query: str) -> List[Dict[str, object]]:
    cursor = conn.execute(query)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def build_anomaly_report(db_path: Path | str = DB_PATH, output_path: Path | str = OUTPUT_PATH) -> List[Dict[str, object]]:
    conn = get_connection(db_path)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    findings: List[Dict[str, object]] = []

    shared_address_businesses = run_query(
        conn,
        """
        SELECT address, COUNT(*) AS business_count, STRING_AGG(record_id, ',') AS entity_ids
        FROM business_entities
        GROUP BY address
        HAVING COUNT(*) > 1
        ORDER BY business_count DESC, address
        """,
    )
    for row in shared_address_businesses:
        findings.append(
            {
                "Risk Score": RULES["shared_address_businesses"]["weight"] + min(int(row["business_count"]) - 2, 5) * 5,
                "Rule Triggered": RULES["shared_address_businesses"]["label"],
                "Supporting Evidence": f"{row['business_count']} businesses share address {row['address']}",
                "Entity IDs": row["entity_ids"],
                "Addresses": row["address"],
                "Phone Numbers": "",
                "Source Table": "business_entities",
            }
        )

    shared_address_phones = run_query(
        conn,
        """
        SELECT address, COUNT(DISTINCT phone) AS phone_count, STRING_AGG(DISTINCT phone, ',') AS phone_numbers, STRING_AGG(record_id, ',') AS entity_ids
        FROM business_entities
        WHERE phone IS NOT NULL AND phone <> ''
        GROUP BY address
        HAVING COUNT(DISTINCT phone) > 1
        ORDER BY phone_count DESC, address
        """,
    )
    for row in shared_address_phones:
        findings.append(
            {
                "Risk Score": RULES["shared_address_phones"]["weight"] + min(int(row["phone_count"]) - 2, 5) * 3,
                "Rule Triggered": RULES["shared_address_phones"]["label"],
                "Supporting Evidence": f"{row['phone_count']} phone numbers appear at address {row['address']}",
                "Entity IDs": row["entity_ids"],
                "Addresses": row["address"],
                "Phone Numbers": row["phone_numbers"],
                "Source Table": "business_entities",
            }
        )

    shared_address_websites = run_query(
        conn,
        """
        SELECT address, COUNT(DISTINCT website) AS website_count, STRING_AGG(DISTINCT website, '|') AS websites, STRING_AGG(record_id, ',') AS entity_ids
        FROM business_entities
        WHERE website IS NOT NULL AND website <> ''
        GROUP BY address
        HAVING COUNT(DISTINCT website) > 1
        ORDER BY website_count DESC, address
        """,
    )
    for row in shared_address_websites:
        findings.append(
            {
                "Risk Score": RULES["shared_address_websites"]["weight"] + min(int(row["website_count"]) - 2, 5) * 2,
                "Rule Triggered": RULES["shared_address_websites"]["label"],
                "Supporting Evidence": f"{row['website_count']} websites appear at address {row['address']}",
                "Entity IDs": row["entity_ids"],
                "Addresses": row["address"],
                "Phone Numbers": "",
                "Source Table": "business_entities",
            }
        )

    shared_phone_llcs = run_query(
        conn,
        """
        SELECT phone, COUNT(DISTINCT llc_name) AS llc_count, STRING_AGG(DISTINCT llc_name, '|') AS llc_names, STRING_AGG(record_id, ',') AS entity_ids
        FROM business_entities
        WHERE phone IS NOT NULL AND phone <> '' AND llc_name IS NOT NULL AND llc_name <> ''
        GROUP BY phone
        HAVING COUNT(DISTINCT llc_name) > 1
        ORDER BY llc_count DESC, phone
        """,
    )
    for row in shared_phone_llcs:
        findings.append(
            {
                "Risk Score": RULES["shared_phone_llcs"]["weight"] + min(int(row["llc_count"]) - 2, 5) * 3,
                "Rule Triggered": RULES["shared_phone_llcs"]["label"],
                "Supporting Evidence": f"{row['llc_count']} LLCs share phone {row['phone']}",
                "Entity IDs": row["entity_ids"],
                "Addresses": "",
                "Phone Numbers": row["phone"],
                "Source Table": "business_entities",
            }
        )

    keyword_repeats = run_query(
        conn,
        """
        SELECT keyword, COUNT(*) AS keyword_count, STRING_AGG(entity_name, '|') AS entity_names, STRING_AGG(record_id, ',') AS entity_ids
        FROM known_patterns
        GROUP BY keyword
        HAVING COUNT(*) > 1
        ORDER BY keyword_count DESC, keyword
        """,
    )
    for row in keyword_repeats:
        findings.append(
            {
                "Risk Score": RULES["keyword_repeats"]["weight"] + min(int(row["keyword_count"]) - 2, 5) * 4,
                "Rule Triggered": RULES["keyword_repeats"]["label"],
                "Supporting Evidence": f"Keyword '{row['keyword']}' appears {row['keyword_count']} times",
                "Entity IDs": row["entity_ids"],
                "Addresses": "",
                "Phone Numbers": "",
                "Source Table": "known_patterns",
            }
        )

    known_pattern_addresses = run_query(
        conn,
        """
        SELECT DISTINCT kp.entity_name, bp.address, kp.keyword, kp.record_id AS pattern_id
        FROM known_patterns kp
        LEFT JOIN business_entities bp ON bp.business_name = kp.entity_name
        WHERE bp.address IS NOT NULL
        ORDER BY bp.address, kp.entity_name
        """,
    )
    for row in known_pattern_addresses:
        findings.append(
            {
                "Risk Score": RULES["known_pattern_addresses"]["weight"],
                "Rule Triggered": RULES["known_pattern_addresses"]["label"],
                "Supporting Evidence": f"Known pattern keyword '{row['keyword']}' linked to address {row['address']}",
                "Entity IDs": str(row["pattern_id"]),
                "Addresses": row["address"],
                "Phone Numbers": "",
                "Source Table": "known_patterns",
            }
        )

    fieldnames = [
        "Risk Score",
        "Rule Triggered",
        "Supporting Evidence",
        "Entity IDs",
        "Addresses",
        "Phone Numbers",
        "Source Table",
    ]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(findings)

    return findings


def summarize_findings(findings: List[Dict[str, object]]) -> Dict[str, int]:
    summary = {"High": 0, "Medium": 0, "Low": 0}
    for finding in findings:
        risk_score = int(finding["Risk Score"])
        if risk_score >= 25:
            summary["High"] += 1
        elif risk_score >= 15:
            summary["Medium"] += 1
        else:
            summary["Low"] += 1
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local anomaly detection over DuckDB datasets and write a CSV report.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to the local DuckDB database")
    parser.add_argument("--output-path", default=str(OUTPUT_PATH), help="Path to the anomaly CSV report")
    args = parser.parse_args()

    findings = build_anomaly_report(db_path=args.db_path, output_path=args.output_path)
    summary = summarize_findings(findings)
    print(f"Found {summary['High']} High Risk Findings")
    print(f"Found {summary['Medium']} Medium Risk Findings")
    print(f"Found {summary['Low']} Low Risk Findings")


if __name__ == "__main__":
    main()
