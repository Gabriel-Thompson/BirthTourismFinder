from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import pandas as pd

ENTITY_RISK_PATH = Path("data/processed/entity_risk.csv")
RELATIONSHIPS_PATH = Path("data/processed/relationships.csv")
ANOMALY_PATH = Path("data/processed/anomaly_report.csv")
INVESTIGATION_LEADS_PATH = Path("data/processed/investigation_leads.csv")
ENTITY_TIMELINES_PATH = Path("data/processed/entity_timelines.csv")
EVIDENCE_PACKETS_PATH = Path("data/processed/evidence_packets.csv")
PRIORITIZED_LEADS_PATH = Path("data/processed/prioritized_leads.csv")
EXPORT_DIR = Path("exports")
HIGH_RISK_EXPORT = EXPORT_DIR / "high_risk_entities.csv"
SUMMARY_EXPORT = EXPORT_DIR / "lead_summary.csv"
SUMMARY_JSON_EXPORT = EXPORT_DIR / "lead_summary.json"
SUMMARY_MARKDOWN_EXPORT = EXPORT_DIR / "lead_summary.md"
SUMMARY_HTML_EXPORT = EXPORT_DIR / "lead_summary.html"


def load_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required input file not found: {path}")
    return pd.read_csv(path)


def count_anomaly_matches(anomaly_df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    if anomaly_df.empty:
        return counts

    for _, row in anomaly_df.iterrows():
        entity_ids = str(row.get("Entity IDs", "") or "")
        for eid in [x.strip() for x in entity_ids.split(",") if x.strip()]:
            counts[eid] = counts.get(eid, 0) + 1
    return counts


def format_why_flagged(row: pd.Series, anomaly_count: int) -> str:
    reasons = []
    if row.get("contributing_rules"):
        reasons.append(f"Rules: {row['contributing_rules']}")
    if row.get("supporting_evidence"):
        reasons.append(f"Evidence: {row['supporting_evidence']}")
    if anomaly_count:
        reasons.append(f"Matched {anomaly_count} anomaly findings")
    if not reasons:
        return "High-risk entity identified by rule-based scoring"
    return " | ".join(reasons)


def recommend_review_action(row: pd.Series) -> str:
    rules = str(row.get("contributing_rules") or "").lower()
    if "shared address" in rules:
        return "Review shared address connections and validate ownership."
    if "shared phone" in rules:
        return "Verify phone ownership and call history for connected entities."
    if "shared website" in rules:
        return "Inspect website registration and linked entity relationships."
    if "keyword" in rules:
        return "Review suspicious keywords and associated entity filings."
    return "Conduct a manual review of this high-risk entity and its relationships."


def build_lead_exports(
    entity_risk_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    anomaly_df: pd.DataFrame,
    investigation_leads_df: Optional[pd.DataFrame] = None,
    entity_timelines_df: Optional[pd.DataFrame] = None,
    evidence_packets_df: Optional[pd.DataFrame] = None,
    prioritized_leads_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    investigation_leads_df = investigation_leads_df if investigation_leads_df is not None else pd.DataFrame()
    entity_timelines_df = entity_timelines_df if entity_timelines_df is not None else pd.DataFrame()
    evidence_packets_df = evidence_packets_df if evidence_packets_df is not None else pd.DataFrame()
    prioritized_leads_df = prioritized_leads_df if prioritized_leads_df is not None else pd.DataFrame()

    if not prioritized_leads_df.empty:
        exported = prioritized_leads_df[prioritized_leads_df["priority"].isin(["CRITICAL", "HIGH"])].copy()
        if exported.empty:
            exported = prioritized_leads_df.copy()
        high_risk_export = exported.loc[:, [
            "lead_id",
            "lead_type",
            "title",
            "primary_entity_id",
            "primary_entity_type",
            "network_id",
            "risk_score",
            "confidence",
            "priority",
            "fraud_marker_count",
            "independent_source_count",
            "relationship_count",
            "source_names",
            "contains_real_data",
            "contains_synthetic_data",
        ]].copy()
        summary_rows = []
        for _, row in exported.iterrows():
            summary_rows.append(
                {
                    "display_name": row["title"],
                    "risk_score": float(row["risk_score"]),
                    "why_flagged": str(row.get("explanation", "")),
                    "connected_entity_count": int(row.get("relationship_count", 0) or 0),
                    "recommended_review_action": str(row.get("recommended_review", "")),
                    "lead_id": row["lead_id"],
                    "priority": row["priority"],
                    "timeline_event_count": int(row.get("timeline_event_count", 0) or 0),
                    "supporting_sources": str(row.get("source_names", "")),
                    "evidence": str(row.get("missing_evidence_fields", "")),
                }
            )
        return high_risk_export, pd.DataFrame(summary_rows)

    if not investigation_leads_df.empty:
        high_priority = investigation_leads_df[investigation_leads_df["Priority"].isin(["Critical", "High"])].copy()
        if high_priority.empty:
            high_priority = investigation_leads_df.copy()
        evidence_lookup = evidence_packets_df.set_index("lead_id", drop=False) if not evidence_packets_df.empty and "lead_id" in evidence_packets_df.columns else pd.DataFrame()
        timeline_counts = entity_timelines_df.groupby("lead_id").size().to_dict() if not entity_timelines_df.empty and "lead_id" in entity_timelines_df.columns else {}
        high_risk_export = high_priority.loc[:, [
            "lead_id",
            "entity_id",
            "Primary Entity",
            "Risk Score",
            "Confidence",
            "Priority",
            "Status",
            "Fraud Marker Count",
            "Supporting Source Count",
            "Relationship Count",
            "source_name",
            "source_type",
        ]].copy()
        summary_rows = []
        for _, row in high_priority.iterrows():
            evidence = evidence_lookup.loc[row["lead_id"], "Supporting Evidence"] if not evidence_lookup.empty and row["lead_id"] in evidence_lookup.index else ""
            recommended = evidence_lookup.loc[row["lead_id"], "Recommended Review"] if not evidence_lookup.empty and row["lead_id"] in evidence_lookup.index else row.get("Recommended Review", "")
            summary_rows.append(
                {
                    "display_name": row["Primary Entity"],
                    "risk_score": int(row["Risk Score"]),
                    "why_flagged": row["Lead Summary"],
                    "connected_entity_count": int(row["Relationship Count"]),
                    "recommended_review_action": recommended_review_action(pd.Series({"contributing_rules": row.get("Fraud Markers", "")})) if not recommended else recommended,
                    "lead_id": row["lead_id"],
                    "priority": row["Priority"],
                    "timeline_event_count": int(timeline_counts.get(row["lead_id"], 0)),
                    "supporting_sources": str(row.get("source_name", "")),
                    "evidence": evidence,
                }
            )
        return high_risk_export, pd.DataFrame(summary_rows)

    high_risk = entity_risk_df[entity_risk_df["risk_level"] == "High"].copy()
    if high_risk.empty:
        return pd.DataFrame(columns=[
            "entity_id",
            "entity_type",
            "display_name",
            "risk_score",
            "risk_level",
            "relationship_count",
            "contributing_rules",
            "supporting_evidence",
        ]), pd.DataFrame(columns=[
            "display_name",
            "risk_score",
            "why_flagged",
            "connected_entity_count",
            "recommended_review_action",
        ])

    anomaly_match_counts = count_anomaly_matches(anomaly_df)

    # ensure required columns exist before write
    high_risk_export = high_risk.loc[:, [
        "entity_id",
        "entity_type",
        "display_name",
        "risk_score",
        "risk_level",
        "relationship_count",
        "contributing_rules",
        "supporting_evidence",
    ]].copy()

    summary_rows = []
    for _, row in high_risk.iterrows():
        summary_rows.append({
            "display_name": row["display_name"],
            "risk_score": int(row["risk_score"]),
            "why_flagged": format_why_flagged(row, anomaly_match_counts.get(str(row["entity_id"]), 0)),
            "connected_entity_count": int(row["relationship_count"]),
            "recommended_review_action": recommend_review_action(row),
        })

    lead_summary = pd.DataFrame(summary_rows)
    return high_risk_export, lead_summary


def write_export(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def build_report_payload(
    prioritized_leads_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    evidence_packets_df: pd.DataFrame,
    entity_timelines_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    network_clusters_df: pd.DataFrame,
) -> dict[str, object]:
    lead_rows = prioritized_leads_df.fillna("").to_dict(orient="records")
    return {
        "lead_summary": lead_rows,
        "evidence": evidence_packets_df.fillna("").to_dict(orient="records"),
        "timeline": entity_timelines_df.fillna("").to_dict(orient="records"),
        "fraud_markers": fraud_markers_df.fillna("").to_dict(orient="records"),
        "relationships": relationships_df.fillna("").to_dict(orient="records"),
        "networks": network_clusters_df.fillna("").to_dict(orient="records"),
        "source_provenance": [
            {
                "lead_id": str(row.get("lead_id", "")),
                "source_names": str(row.get("source_names", row.get("source_name", ""))),
                "source_types": str(row.get("source_types", row.get("source_type", ""))),
                "contains_real_data": bool(row.get("contains_real_data", False)),
            }
            for row in lead_rows
        ],
        "recommendations": [
            {
                "lead_id": str(row.get("lead_id", "")),
                "recommended_review": str(row.get("recommended_review", "")),
                "priority": str(row.get("priority", row.get("Priority", ""))),
            }
            for row in lead_rows
        ],
    }


def write_report_formats(
    payload: dict[str, object],
    *,
    json_path: Path,
    markdown_path: Path,
    html_path: Path,
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

    lead_summary = payload.get("lead_summary", [])
    recommendations = payload.get("recommendations", [])
    markdown_lines = [
        "# OpenFraud Lead Report",
        "",
        "All results are investigative leads only, not proof of fraud.",
        "",
        "## Lead Summary",
    ]
    for lead in lead_summary[:25]:
        if isinstance(lead, dict):
            markdown_lines.extend(
                [
                    f"- `{lead.get('lead_id', '')}` {lead.get('title', lead.get('Primary Entity', ''))}",
                    f"  Risk: {lead.get('risk_score', lead.get('Risk Score', ''))} | Priority: {lead.get('priority', lead.get('Priority', ''))} | Confidence: {lead.get('confidence', lead.get('Confidence', ''))}",
                    f"  Why: {lead.get('explanation', lead.get('Lead Summary', ''))}",
                ]
            )
    markdown_lines.extend(["", "## Recommendations"])
    for item in recommendations[:25]:
        if isinstance(item, dict):
            markdown_lines.append(f"- `{item.get('lead_id', '')}` {item.get('recommended_review', '')}")
    markdown_path.write_text("\n".join(markdown_lines), encoding="utf-8")

    html_lines = [
        "<html><head><title>OpenFraud Lead Report</title></head><body>",
        "<h1>OpenFraud Lead Report</h1>",
        "<p>All results are investigative leads only, not proof of fraud.</p>",
        "<h2>Lead Summary</h2>",
        "<table border='1' cellspacing='0' cellpadding='4'>",
        "<tr><th>Lead ID</th><th>Title</th><th>Risk</th><th>Priority</th><th>Confidence</th><th>Why</th></tr>",
    ]
    for lead in lead_summary[:50]:
        if isinstance(lead, dict):
            html_lines.append(
                "<tr>"
                f"<td>{lead.get('lead_id', '')}</td>"
                f"<td>{lead.get('title', lead.get('Primary Entity', ''))}</td>"
                f"<td>{lead.get('risk_score', lead.get('Risk Score', ''))}</td>"
                f"<td>{lead.get('priority', lead.get('Priority', ''))}</td>"
                f"<td>{lead.get('confidence', lead.get('Confidence', ''))}</td>"
                f"<td>{lead.get('explanation', lead.get('Lead Summary', ''))}</td>"
                "</tr>"
            )
    html_lines.extend(["</table>", "</body></html>"])
    html_path.write_text("\n".join(html_lines), encoding="utf-8")


def main(
    entity_risk_path: Optional[Path] = None,
    relationships_path: Optional[Path] = None,
    anomaly_path: Optional[Path] = None,
    high_risk_export_path: Optional[Path] = None,
    summary_export_path: Optional[Path] = None,
) -> None:
    entity_risk_path = Path(entity_risk_path) if entity_risk_path is not None else ENTITY_RISK_PATH
    relationships_path = Path(relationships_path) if relationships_path is not None else RELATIONSHIPS_PATH
    anomaly_path = Path(anomaly_path) if anomaly_path is not None else ANOMALY_PATH
    high_risk_export_path = Path(high_risk_export_path) if high_risk_export_path is not None else HIGH_RISK_EXPORT
    summary_export_path = Path(summary_export_path) if summary_export_path is not None else SUMMARY_EXPORT

    entity_risk_df = load_dataframe(entity_risk_path)
    relationships_df = load_dataframe(relationships_path)
    anomaly_df = load_dataframe(anomaly_path)
    processed_dir = entity_risk_path.parent
    investigation_leads_path = processed_dir / INVESTIGATION_LEADS_PATH.name
    entity_timelines_path = processed_dir / ENTITY_TIMELINES_PATH.name
    evidence_packets_path = processed_dir / EVIDENCE_PACKETS_PATH.name
    prioritized_leads_path = processed_dir / PRIORITIZED_LEADS_PATH.name
    fraud_markers_path = processed_dir / "fraud_markers.csv"
    network_clusters_path = processed_dir / "network_clusters.csv"
    investigation_leads_df = pd.read_csv(investigation_leads_path) if investigation_leads_path.exists() else pd.DataFrame()
    entity_timelines_df = pd.read_csv(entity_timelines_path) if entity_timelines_path.exists() else pd.DataFrame()
    evidence_packets_df = pd.read_csv(evidence_packets_path) if evidence_packets_path.exists() else pd.DataFrame()
    prioritized_leads_df = pd.read_csv(prioritized_leads_path) if prioritized_leads_path.exists() else pd.DataFrame()
    fraud_markers_df = pd.read_csv(fraud_markers_path) if fraud_markers_path.exists() else pd.DataFrame()
    network_clusters_df = pd.read_csv(network_clusters_path) if network_clusters_path.exists() else pd.DataFrame()

    high_risk_export, lead_summary = build_lead_exports(
        entity_risk_df,
        relationships_df,
        anomaly_df,
        investigation_leads_df=investigation_leads_df,
        entity_timelines_df=entity_timelines_df,
        evidence_packets_df=evidence_packets_df,
        prioritized_leads_df=prioritized_leads_df,
    )
    write_export(high_risk_export_path, high_risk_export)
    write_export(summary_export_path, lead_summary)
    payload = build_report_payload(
        prioritized_leads_df if not prioritized_leads_df.empty else investigation_leads_df,
        fraud_markers_df,
        evidence_packets_df,
        entity_timelines_df,
        relationships_df,
        network_clusters_df,
    )
    summary_root = summary_export_path.parent
    summary_json_path = summary_root / SUMMARY_JSON_EXPORT.name
    summary_markdown_path = summary_root / SUMMARY_MARKDOWN_EXPORT.name
    summary_html_path = summary_root / SUMMARY_HTML_EXPORT.name
    write_report_formats(
        payload,
        json_path=summary_json_path,
        markdown_path=summary_markdown_path,
        html_path=summary_html_path,
    )

    print(f"Wrote high-risk leads to {high_risk_export_path}")
    print(f"Wrote lead summary to {summary_export_path}")
    print(f"Wrote lead summary JSON to {summary_json_path}")
    print(f"Wrote lead summary Markdown to {summary_markdown_path}")
    print(f"Wrote lead summary HTML to {summary_html_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export high-risk leads from processed OpenFraud outputs.")
    parser.add_argument("--entity-risk-path", default=str(ENTITY_RISK_PATH), help="Path to the entity risk CSV file")
    parser.add_argument("--relationships-path", default=str(RELATIONSHIPS_PATH), help="Path to the relationships CSV file")
    parser.add_argument("--anomaly-path", default=str(ANOMALY_PATH), help="Path to the anomaly report CSV file")
    parser.add_argument("--high-risk-export", default=str(HIGH_RISK_EXPORT), help="Path for the high-risk entities export")
    parser.add_argument("--lead-summary-export", default=str(SUMMARY_EXPORT), help="Path for the lead summary export")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        entity_risk_path=Path(args.entity_risk_path),
        relationships_path=Path(args.relationships_path),
        anomaly_path=Path(args.anomaly_path),
        high_risk_export_path=Path(args.high_risk_export),
        summary_export_path=Path(args.lead_summary_export),
    )
