from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from src.connectors.source_metadata import merge_source_values
from src.investigation.analyst_workbench import (
    ANALYST_HISTORY_COLUMNS,
    ANALYST_STATE_COLUMNS,
    build_history_entry,
    load_analyst_history,
    merge_analyst_state_with_leads,
    record_export_history,
)


def lead_package_dir_name(lead_id: str) -> str:
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(lead_id).strip())
    return safe_name or "lead"


def build_investigation_summary(prioritized_leads_df: pd.DataFrame) -> pd.DataFrame:
    if prioritized_leads_df.empty:
        return pd.DataFrame(
            [
                {
                    "total_leads": 0,
                    "critical_leads": 0,
                    "high_leads": 0,
                    "medium_leads": 0,
                    "low_leads": 0,
                    "real_data_leads": 0,
                    "synthetic_data_leads": 0,
                    "cross_source_leads": 0,
                    "entity_leads": 0,
                    "network_leads": 0,
                    "average_risk": 0,
                    "average_confidence": 0,
                    "average_evidence_completeness": 0,
                    "most_common_markers": "",
                    "most_common_source_combinations": "",
                    "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
                }
            ]
        )
    marker_counts = (
        prioritized_leads_df["fraud_markers"].fillna("").astype(str).str.split("|").explode().str.strip()
    )
    source_combos = prioritized_leads_df["source_names"].fillna("").astype(str)
    return pd.DataFrame(
        [
            {
                "total_leads": int(len(prioritized_leads_df)),
                "critical_leads": int((prioritized_leads_df["priority"] == "CRITICAL").sum()),
                "high_leads": int((prioritized_leads_df["priority"] == "HIGH").sum()),
                "medium_leads": int((prioritized_leads_df["priority"] == "MEDIUM").sum()),
                "low_leads": int((prioritized_leads_df["priority"] == "LOW").sum()),
                "real_data_leads": int(prioritized_leads_df["contains_real_data"].astype(bool).sum()),
                "synthetic_data_leads": int((~prioritized_leads_df["contains_real_data"].astype(bool)).sum()),
                "cross_source_leads": int((pd.to_numeric(prioritized_leads_df["cross_source_match_count"], errors="coerce").fillna(0) > 0).sum()),
                "entity_leads": int((prioritized_leads_df["lead_type"] != "NETWORK").sum()),
                "network_leads": int((prioritized_leads_df["lead_type"] == "NETWORK").sum()),
                "average_risk": round(pd.to_numeric(prioritized_leads_df["risk_score"], errors="coerce").fillna(0).mean(), 2),
                "average_confidence": round(pd.to_numeric(prioritized_leads_df["confidence_score"], errors="coerce").fillna(0).mean(), 4),
                "average_evidence_completeness": round(pd.to_numeric(prioritized_leads_df["evidence_completeness_score"], errors="coerce").fillna(0).mean(), 2),
                "most_common_markers": merge_source_values(*marker_counts.value_counts().head(5).index.astype(str).tolist()),
                "most_common_source_combinations": merge_source_values(*source_combos.value_counts().head(5).index.astype(str).tolist()),
                "generated_at": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d"),
            }
        ]
    )


def build_lead_evidence_index(
    prioritized_leads_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    evidence_packets_df: pd.DataFrame,
    entity_timelines_df: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    evidence_id = 1
    if prioritized_leads_df.empty:
        return pd.DataFrame(columns=[
            "lead_id", "evidence_id", "evidence_type", "source_name", "source_type", "source_record_id", "entity_id",
            "relationship_id", "fraud_marker_id", "network_id", "event_date", "evidence_summary", "confidence"
        ])
    for _, lead in prioritized_leads_df.iterrows():
        lead_id = str(lead["lead_id"])
        entity_id = str(lead.get("primary_entity_id", ""))
        network_id = str(lead.get("network_id", ""))
        markers = fraud_markers_df[fraud_markers_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id].copy() if not fraud_markers_df.empty else pd.DataFrame()
        relationships = canonical_relationships_df[
            (canonical_relationships_df.get("source_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
            | (canonical_relationships_df.get("target_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
        ].copy() if not canonical_relationships_df.empty else pd.DataFrame()
        evidence_rows = evidence_packets_df[evidence_packets_df.get("lead_id", pd.Series(dtype=str)).astype(str) == lead_id].copy() if not evidence_packets_df.empty else pd.DataFrame()
        timeline_rows = entity_timelines_df[entity_timelines_df.get("lead_id", pd.Series(dtype=str)).astype(str) == lead_id].copy() if not entity_timelines_df.empty else pd.DataFrame()

        for _, marker in markers.fillna("").iterrows():
            rows.append(
                {
                    "lead_id": lead_id,
                    "evidence_id": f"evidence:{evidence_id:08d}",
                    "evidence_type": "FRAUD_MARKER",
                    "source_name": str(marker.get("sources", "")),
                    "source_type": str(marker.get("source_types", "")),
                    "source_record_id": "",
                    "entity_id": entity_id,
                    "relationship_id": "",
                    "fraud_marker_id": str(marker.get("marker_id", "")),
                    "network_id": network_id,
                    "event_date": "",
                    "evidence_summary": str(marker.get("explanation", "")),
                    "confidence": str(marker.get("confidence", "")),
                }
            )
            evidence_id += 1
        for _, relationship in relationships.fillna("").iterrows():
            rows.append(
                {
                    "lead_id": lead_id,
                    "evidence_id": f"evidence:{evidence_id:08d}",
                    "evidence_type": "RELATIONSHIP",
                    "source_name": str(relationship.get("source_name", "")),
                    "source_type": str(relationship.get("source_type", "")),
                    "source_record_id": "",
                    "entity_id": entity_id,
                    "relationship_id": str(relationship.get("relationship_id", "")),
                    "fraud_marker_id": "",
                    "network_id": network_id,
                    "event_date": "",
                    "evidence_summary": str(relationship.get("evidence", "")),
                    "confidence": str(relationship.get("confidence", relationship.get("confidence_score", ""))),
                }
            )
            evidence_id += 1
        for _, evidence in evidence_rows.fillna("").iterrows():
            rows.append(
                {
                    "lead_id": lead_id,
                    "evidence_id": f"evidence:{evidence_id:08d}",
                    "evidence_type": "EVIDENCE_PACKET",
                    "source_name": str(evidence.get("source_name", evidence.get("Source", ""))),
                    "source_type": str(evidence.get("source_type", evidence.get("Connector", ""))),
                    "source_record_id": str(evidence.get("Record ID", "")),
                    "entity_id": entity_id,
                    "relationship_id": "",
                    "fraud_marker_id": "",
                    "network_id": network_id,
                    "event_date": str(evidence.get("Import Date", "")),
                    "evidence_summary": str(evidence.get("Supporting Evidence", "")),
                    "confidence": str(lead.get("confidence", "")),
                }
            )
            evidence_id += 1
        for _, event in timeline_rows.fillna("").iterrows():
            rows.append(
                {
                    "lead_id": lead_id,
                    "evidence_id": f"evidence:{evidence_id:08d}",
                    "evidence_type": "TIMELINE_EVENT",
                    "source_name": str(event.get("source_name", event.get("Source", ""))),
                    "source_type": str(event.get("source_type", event.get("Connector", ""))),
                    "source_record_id": str(event.get("Record ID", "")),
                    "entity_id": entity_id,
                    "relationship_id": "",
                    "fraud_marker_id": "",
                    "network_id": network_id,
                    "event_date": str(event.get("Date", "")),
                    "evidence_summary": str(event.get("Evidence", event.get("Event", ""))),
                    "confidence": str(lead.get("confidence", "")),
                }
            )
            evidence_id += 1
    return pd.DataFrame(rows)


def preserve_analyst_state(
    prioritized_leads_df: pd.DataFrame,
    analyst_state_path: Path,
    analyst_history_path: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if analyst_state_path.exists() and analyst_state_path.stat().st_size > 0:
        analyst_state_df = pd.read_csv(analyst_state_path)
    else:
        analyst_state_df = pd.DataFrame(columns=ANALYST_STATE_COLUMNS)
    history_path = analyst_history_path or analyst_state_path.with_name("analyst_history.csv")
    analyst_history_df = load_analyst_history(history_path)
    leads_with_state_df, generated_state_df, merged_history_df = merge_analyst_state_with_leads(
        prioritized_leads_df,
        analyst_state_df,
        analyst_history_df=analyst_history_df,
    )
    return leads_with_state_df, generated_state_df, merged_history_df


def export_lead_packages(
    prioritized_leads_df: pd.DataFrame,
    lead_evidence_index_df: pd.DataFrame,
    canonical_entities_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    entity_timelines_df: pd.DataFrame,
    package_root: Path,
    package_priorities: list[str],
    analyst_state_df: pd.DataFrame | None = None,
    analyst_history_df: pd.DataFrame | None = None,
) -> tuple[int, pd.DataFrame, pd.DataFrame]:
    package_root.mkdir(parents=True, exist_ok=True)
    created = 0
    exported_lead_ids: list[str] = []
    for _, lead in prioritized_leads_df.iterrows():
        if str(lead.get("priority", "")).upper() not in {priority.upper() for priority in package_priorities}:
            continue
        lead_id = str(lead["lead_id"])
        entity_id = str(lead.get("primary_entity_id", ""))
        package_dir = package_root / lead_package_dir_name(lead_id)
        package_dir.mkdir(parents=True, exist_ok=True)
        related_entities = canonical_entities_df[canonical_entities_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id].copy()
        related_relationships = canonical_relationships_df[
            (canonical_relationships_df.get("source_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
            | (canonical_relationships_df.get("target_entity_id", pd.Series(dtype=str)).astype(str) == entity_id)
        ].copy()
        related_markers = fraud_markers_df[fraud_markers_df.get("entity_id", pd.Series(dtype=str)).astype(str) == entity_id].copy()
        related_evidence = lead_evidence_index_df[lead_evidence_index_df.get("lead_id", pd.Series(dtype=str)).astype(str) == lead_id].copy()
        related_timeline = entity_timelines_df[entity_timelines_df.get("lead_id", pd.Series(dtype=str)).astype(str) == lead_id].copy()
        sources_df = related_evidence[["source_name", "source_type", "source_record_id", "confidence"]].drop_duplicates() if not related_evidence.empty else pd.DataFrame(columns=["source_name", "source_type", "source_record_id", "confidence"])

        pd.DataFrame([lead]).to_csv(package_dir / "lead_summary.csv", index=False)
        related_entities.to_csv(package_dir / "entities.csv", index=False)
        related_relationships.to_csv(package_dir / "relationships.csv", index=False)
        related_markers.to_csv(package_dir / "fraud_markers.csv", index=False)
        related_evidence.to_csv(package_dir / "evidence.csv", index=False)
        related_timeline.to_csv(package_dir / "timeline.csv", index=False)
        sources_df.to_csv(package_dir / "sources.csv", index=False)
        (package_dir / "recommendations.txt").write_text(str(lead.get("recommended_review", "")), encoding="utf-8")
        created += 1
        exported_lead_ids.append(lead_id)
    state_df = analyst_state_df if analyst_state_df is not None else pd.DataFrame(columns=ANALYST_STATE_COLUMNS)
    history_df = analyst_history_df if analyst_history_df is not None else pd.DataFrame(columns=ANALYST_HISTORY_COLUMNS)
    state_df, history_df = record_export_history(exported_lead_ids, analyst_state_df=state_df, analyst_history_df=history_df)
    return created, state_df, history_df
