from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.connectors.source_metadata import is_real_source_type, merge_source_values

ENTITIES_PATH = Path("data/processed/canonical_entities.csv")
RELATIONSHIPS_PATH = Path("data/processed/canonical_relationships.csv")
FRAUD_MARKERS_PATH = Path("data/processed/fraud_markers.csv")
OUTPUT_PATH = Path("data/processed/entity_risk.csv")
CONFIG_PATH = Path("config/entity_scoring.json")

CONFIDENCE_SCORES = {
    "Very High": 1.0,
    "High": 0.85,
    "Medium": 0.65,
    "Low": 0.45,
    "Unknown": 0.25,
}


def load_scoring_config(path: Optional[Path] = None) -> Dict[str, float]:
    p = Path(path) if path is not None else CONFIG_PATH
    defaults = {
        "direct_entity_id_match": 30,
        "address_text_match": 8,
        "phone_text_match": 6,
        "supporting_evidence_match": 10,
        "relationship_count_multiplier": 2,
        "max_relationship_bonus": 20,
        "max_score": 100,
        "high_threshold": 70,
        "medium_threshold": 35,
        "low_threshold": 1,
        "source_diversity_bonus": 5,
        "very_high_confidence_bonus": 10,
        "high_confidence_bonus": 6,
        "medium_confidence_bonus": 3,
    }
    if not p.exists():
        return defaults
    try:
        with p.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return defaults
    merged = defaults.copy()
    for key, value in data.items():
        if key in merged:
            merged[key] = value
    return merged


def _normalize_markers(markers_df: pd.DataFrame) -> pd.DataFrame:
    df = markers_df.copy()
    if "marker_name" not in df.columns and "Rule Triggered" in df.columns:
        df["marker_name"] = df["Rule Triggered"]
        df["risk_contribution"] = pd.to_numeric(df.get("Risk Score", 0), errors="coerce").fillna(0)
        df["confidence"] = "Medium"
        df["support"] = 1
        df["sources"] = df.get("source_name", "")
        df["source_types"] = df.get("source_type", "")
        df["supporting_entities"] = df.get("Entity IDs", "")
        df["supporting_relationships"] = ""
        df["recommended_review"] = "Conduct a manual review of the linked public-record pattern."
        df["explanation"] = df.get("Supporting Evidence", "")
        if "entity_id" not in df.columns:
            df["entity_id"] = df.get("Entity IDs", "").astype(str).str.split(",").str[0].str.strip()
    return df


def build_entity_risk(
    entities_df: pd.DataFrame,
    relationships_df: pd.DataFrame,
    markers_df: Optional[pd.DataFrame] = None,
    scoring: Optional[Dict[str, float]] = None,
) -> pd.DataFrame:
    start_time = time.time()
    scoring = scoring or load_scoring_config()
    entities_df = entities_df.copy()
    entities_df["entity_id"] = entities_df["entity_id"].astype(str)
    markers_df = _normalize_markers(markers_df if markers_df is not None else pd.DataFrame())

    accumulators: Dict[str, dict[str, object]] = {}
    for _, entity in entities_df.iterrows():
        entity_id = str(entity["entity_id"])
        accumulators[entity_id] = {
            "entity_id": entity_id,
            "entity_type": str(entity.get("entity_type", "")),
            "display_name": str(entity.get("display_name", "")),
            "risk_score": 0.0,
            "relationship_count": 0,
            "marker_names": [],
            "marker_categories": [],
            "supporting_evidence": [],
            "recommended_reviews": [],
            "sources": set(token for token in str(entity.get("source_name", "")).split("|") if token),
            "source_types": set(token for token in str(entity.get("source_type", "")).split("|") if token),
            "confidence_scores": [],
        }

    if not relationships_df.empty:
        for _, relationship in relationships_df.iterrows():
            source_id = str(relationship.get("source_entity_id", ""))
            target_id = str(relationship.get("target_entity_id", ""))
            if source_id in accumulators:
                accumulators[source_id]["relationship_count"] += 1
            if target_id in accumulators:
                accumulators[target_id]["relationship_count"] += 1

    marker_rows_processed = 0
    if not markers_df.empty:
        for _, marker in markers_df.iterrows():
            entity_id = str(marker.get("entity_id", "")).strip()
            if not entity_id or entity_id not in accumulators:
                continue
            marker_rows_processed += 1
            acc = accumulators[entity_id]
            risk_contribution = float(pd.to_numeric(marker.get("risk_contribution", 0), errors="coerce"))
            acc["risk_score"] += risk_contribution
            acc["marker_names"].append(str(marker.get("marker_name", "")))
            acc["marker_categories"].append(str(marker.get("marker_category", "")))
            acc["supporting_evidence"].append(str(marker.get("explanation", "")))
            acc["recommended_reviews"].append(str(marker.get("recommended_review", "")))
            acc["sources"].update(token for token in str(marker.get("sources", "")).split("|") if token)
            acc["source_types"].update(token for token in str(marker.get("source_types", "")).split("|") if token)
            acc["confidence_scores"].append(CONFIDENCE_SCORES.get(str(marker.get("confidence", "Unknown")), 0.25))

    rows: list[dict[str, object]] = []
    rel_mult = float(scoring.get("relationship_count_multiplier", 2))
    max_rel_bonus = int(scoring.get("max_relationship_bonus", 20))
    max_score = int(scoring.get("max_score", 100))
    high_t = int(scoring.get("high_threshold", 70))
    med_t = int(scoring.get("medium_threshold", 35))
    low_t = int(scoring.get("low_threshold", 1))
    source_diversity_bonus = int(scoring.get("source_diversity_bonus", 5))

    for _, acc in accumulators.items():
        rel_bonus = min(int(acc["relationship_count"] * rel_mult), max_rel_bonus)
        acc["risk_score"] += rel_bonus
        if len(acc["sources"]) >= 3:
            acc["risk_score"] += source_diversity_bonus
        average_confidence_score = sum(acc["confidence_scores"]) / len(acc["confidence_scores"]) if acc["confidence_scores"] else 0.0
        if average_confidence_score >= 0.9:
            acc["risk_score"] += int(scoring.get("very_high_confidence_bonus", 10))
        elif average_confidence_score >= 0.75:
            acc["risk_score"] += int(scoring.get("high_confidence_bonus", 6))
        elif average_confidence_score >= 0.55:
            acc["risk_score"] += int(scoring.get("medium_confidence_bonus", 3))
        acc["risk_score"] = min(int(round(acc["risk_score"])), max_score)

        if acc["risk_score"] >= high_t:
            risk_level = "High"
        elif acc["risk_score"] >= med_t:
            risk_level = "Medium"
        elif acc["risk_score"] >= low_t:
            risk_level = "Low"
        else:
            risk_level = "None"

        confidence_label = "Unknown"
        if average_confidence_score >= 0.9:
            confidence_label = "Very High"
        elif average_confidence_score >= 0.75:
            confidence_label = "High"
        elif average_confidence_score >= 0.55:
            confidence_label = "Medium"
        elif average_confidence_score >= 0.35:
            confidence_label = "Low"

        source_type_value = merge_source_values(*sorted(acc["source_types"]))
        rows.append(
            {
                "entity_id": acc["entity_id"],
                "entity_type": acc["entity_type"],
                "display_name": acc["display_name"],
                "risk_score": acc["risk_score"],
                "risk_level": risk_level,
                "confidence": confidence_label,
                "relationship_count": int(acc["relationship_count"]),
                "source_count": len(acc["sources"]),
                "source_name": merge_source_values(*sorted(acc["sources"])),
                "source_type": source_type_value,
                "data_scope": "real" if is_real_source_type(source_type_value) else "synthetic",
                "contributing_rules": "|".join(sorted(set(acc["marker_names"]))),
                "marker_categories": "|".join(sorted(set(acc["marker_categories"]))),
                "supporting_evidence": "|".join(sorted(set(filter(None, acc["supporting_evidence"])))),
                "recommended_review": "|".join(sorted(set(filter(None, acc["recommended_reviews"])))),
                "marker_count": len(acc["marker_names"]),
                "average_marker_confidence": round(average_confidence_score, 4),
            }
        )

    output = pd.DataFrame(rows)
    output.attrs["marker_rows_processed"] = marker_rows_processed
    output.attrs["anomaly_rows_processed"] = marker_rows_processed
    output.attrs["entities_loaded"] = len(entities_df)
    output.attrs["relationships_loaded"] = len(relationships_df)
    output.attrs["duration_seconds"] = time.time() - start_time
    return output


def main(
    entities_path: Optional[Path] = None,
    relationships_path: Optional[Path] = None,
    anomaly_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
    config_path: Optional[Path] = None,
) -> None:
    start_time = time.time()
    entities_path = Path(entities_path) if entities_path is not None else ENTITIES_PATH
    relationships_path = Path(relationships_path) if relationships_path is not None else RELATIONSHIPS_PATH
    markers_path = Path(anomaly_path) if anomaly_path is not None else FRAUD_MARKERS_PATH
    output_path = Path(output_path) if output_path is not None else OUTPUT_PATH
    print("Entity Intelligence: started")
    print(f"Entity Intelligence: entities input {entities_path}")
    print(f"Entity Intelligence: relationships input {relationships_path}")
    print(f"Entity Intelligence: fraud markers input {markers_path}")
    print(f"Entity Intelligence: scoring config {Path(config_path) if config_path is not None else CONFIG_PATH}")

    entities_df = pd.read_csv(entities_path) if entities_path.exists() else pd.DataFrame()
    relationships_df = pd.read_csv(relationships_path) if relationships_path.exists() else pd.DataFrame()
    markers_df = pd.read_csv(markers_path) if markers_path.exists() and markers_path.stat().st_size > 0 else pd.DataFrame()
    scoring = load_scoring_config(Path(config_path) if config_path is not None else None)

    print(f"Entities loaded: {len(entities_df)}")
    print(f"Relationships loaded: {len(relationships_df)}")
    print(f"Fraud markers loaded: {len(markers_df)}")

    out = build_entity_risk(entities_df, relationships_df, markers_df, scoring=scoring)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    print(f"Entity risk rows written: {len(out)}")
    print(f"High risk entities: {(out['risk_level'] == 'High').sum() if not out.empty else 0}")
    print(f"Medium risk entities: {(out['risk_level'] == 'Medium').sum() if not out.empty else 0}")
    print(f"Low risk entities: {(out['risk_level'] == 'Low').sum() if not out.empty else 0}")
    print(f"Marker rows processed: {out.attrs.get('marker_rows_processed', 0)}")
    print(f"Output: {output_path}")
    print(f"Entity Intelligence: completed in {time.time() - start_time:.2f}s")
    print("Entity Intelligence: PASS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Aggregate fraud markers into per-entity risk outputs.")
    parser.add_argument("--entities-path", default=str(ENTITIES_PATH))
    parser.add_argument("--relationships-path", default=str(RELATIONSHIPS_PATH))
    parser.add_argument("--fraud-markers-path", default=str(FRAUD_MARKERS_PATH))
    parser.add_argument("--output-path", default=str(OUTPUT_PATH))
    parser.add_argument("--config-path", default=str(CONFIG_PATH))
    args = parser.parse_args()
    main(
        entities_path=Path(args.entities_path),
        relationships_path=Path(args.relationships_path),
        anomaly_path=Path(args.fraud_markers_path),
        output_path=Path(args.output_path),
        config_path=Path(args.config_path),
    )
