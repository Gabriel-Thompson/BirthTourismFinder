from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from src.investigation.evidence_builder import build_evidence_packets
from src.investigation.lead_builder import build_investigation_leads
from src.investigation.timeline import build_entity_timelines

DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_ENTITY_RISK_PATH = DEFAULT_PROCESSED_DIR / "entity_risk.csv"
DEFAULT_FRAUD_MARKERS_PATH = DEFAULT_PROCESSED_DIR / "fraud_markers.csv"
DEFAULT_CANONICAL_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_CANONICAL_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_ENTITY_ALIASES_PATH = DEFAULT_PROCESSED_DIR / "entity_aliases.csv"
DEFAULT_INVESTIGATION_LEADS_PATH = DEFAULT_PROCESSED_DIR / "investigation_leads.csv"
DEFAULT_ENTITY_TIMELINES_PATH = DEFAULT_PROCESSED_DIR / "entity_timelines.csv"
DEFAULT_EVIDENCE_PACKETS_PATH = DEFAULT_PROCESSED_DIR / "evidence_packets.csv"


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def build_investigation_workspace(
    entity_risk_path: Path | str = DEFAULT_ENTITY_RISK_PATH,
    fraud_markers_path: Path | str = DEFAULT_FRAUD_MARKERS_PATH,
    canonical_entities_path: Path | str = DEFAULT_CANONICAL_ENTITIES_PATH,
    canonical_relationships_path: Path | str = DEFAULT_CANONICAL_RELATIONSHIPS_PATH,
    aliases_path: Path | str = DEFAULT_ENTITY_ALIASES_PATH,
    leads_output_path: Path | str = DEFAULT_INVESTIGATION_LEADS_PATH,
    timelines_output_path: Path | str = DEFAULT_ENTITY_TIMELINES_PATH,
    evidence_output_path: Path | str = DEFAULT_EVIDENCE_PACKETS_PATH,
) -> dict[str, int | float]:
    start_time = time.time()
    entity_risk_path = Path(entity_risk_path)
    fraud_markers_path = Path(fraud_markers_path)
    canonical_entities_path = Path(canonical_entities_path)
    canonical_relationships_path = Path(canonical_relationships_path)
    aliases_path = Path(aliases_path)
    leads_output_path = Path(leads_output_path)
    timelines_output_path = Path(timelines_output_path)
    evidence_output_path = Path(evidence_output_path)

    print("Investigation Workspace: started")
    print(f"Investigation Workspace: entity risk input {entity_risk_path}")
    print(f"Investigation Workspace: fraud markers input {fraud_markers_path}")
    print(f"Investigation Workspace: canonical entities input {canonical_entities_path}")
    print(f"Investigation Workspace: canonical relationships input {canonical_relationships_path}")
    print(f"Investigation Workspace: aliases input {aliases_path}")

    entity_risk_df = _load_frame(entity_risk_path)
    fraud_markers_df = _load_frame(fraud_markers_path)
    canonical_entities_df = _load_frame(canonical_entities_path)
    canonical_relationships_df = _load_frame(canonical_relationships_path)
    aliases_df = _load_frame(aliases_path)

    print(f"Investigation Workspace: entity risk rows {len(entity_risk_df)}")
    print(f"Investigation Workspace: fraud marker rows {len(fraud_markers_df)}")
    print(f"Investigation Workspace: canonical entity rows {len(canonical_entities_df)}")
    print(f"Investigation Workspace: canonical relationship rows {len(canonical_relationships_df)}")

    leads_df = build_investigation_leads(
        entity_risk_df=entity_risk_df,
        fraud_markers_df=fraud_markers_df,
        canonical_entities_df=canonical_entities_df,
        canonical_relationships_df=canonical_relationships_df,
        aliases_df=aliases_df,
    )
    timeline_df = build_entity_timelines(
        leads_df=leads_df,
        canonical_entities_df=canonical_entities_df,
        canonical_relationships_df=canonical_relationships_df,
        fraud_markers_df=fraud_markers_df,
    )
    evidence_df = build_evidence_packets(
        leads_df=leads_df,
        aliases_df=aliases_df,
        fraud_markers_df=fraud_markers_df,
        canonical_relationships_df=canonical_relationships_df,
        canonical_entities_df=canonical_entities_df,
        timeline_df=timeline_df,
    )

    for path, frame in [
        (leads_output_path, leads_df),
        (timelines_output_path, timeline_df),
        (evidence_output_path, evidence_df),
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)
        print(f"Investigation Workspace: wrote {len(frame)} rows to {path}")

    duration = time.time() - start_time
    print(f"Investigation Workspace: completed in {duration:.2f}s")
    print("Investigation Workspace: PASS")
    return {
        "lead_count": int(len(leads_df)),
        "timeline_event_count": int(len(timeline_df)),
        "evidence_packet_count": int(len(evidence_df)),
        "average_confidence": round(pd.to_numeric(leads_df.get("Entity Resolution Confidence", pd.Series(dtype=float)), errors="coerce").fillna(0).mean(), 4)
        if not leads_df.empty
        else 0.0,
        "runtime_seconds": round(duration, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build investigation leads, timelines, and evidence packets from processed OpenFraud outputs.")
    parser.add_argument("--entity-risk-path", default=str(DEFAULT_ENTITY_RISK_PATH))
    parser.add_argument("--fraud-markers-path", default=str(DEFAULT_FRAUD_MARKERS_PATH))
    parser.add_argument("--canonical-entities-path", default=str(DEFAULT_CANONICAL_ENTITIES_PATH))
    parser.add_argument("--canonical-relationships-path", default=str(DEFAULT_CANONICAL_RELATIONSHIPS_PATH))
    parser.add_argument("--aliases-path", default=str(DEFAULT_ENTITY_ALIASES_PATH))
    parser.add_argument("--leads-output-path", default=str(DEFAULT_INVESTIGATION_LEADS_PATH))
    parser.add_argument("--timelines-output-path", default=str(DEFAULT_ENTITY_TIMELINES_PATH))
    parser.add_argument("--evidence-output-path", default=str(DEFAULT_EVIDENCE_PACKETS_PATH))
    args = parser.parse_args()
    build_investigation_workspace(
        entity_risk_path=args.entity_risk_path,
        fraud_markers_path=args.fraud_markers_path,
        canonical_entities_path=args.canonical_entities_path,
        canonical_relationships_path=args.canonical_relationships_path,
        aliases_path=args.aliases_path,
        leads_output_path=args.leads_output_path,
        timelines_output_path=args.timelines_output_path,
        evidence_output_path=args.evidence_output_path,
    )


if __name__ == "__main__":
    main()
