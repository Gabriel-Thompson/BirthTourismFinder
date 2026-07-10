from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.connectors.source_metadata import is_real_source_type, standardize_source_type

DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_CANONICAL_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_ENTITY_ALIASES_PATH = DEFAULT_PROCESSED_DIR / "entity_aliases.csv"
DEFAULT_ENTITY_RESOLUTION_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "entity_resolution_matches.csv"
DEFAULT_CANONICAL_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_FRAUD_MARKERS_PATH = DEFAULT_PROCESSED_DIR / "fraud_markers.csv"
DEFAULT_PRIORITIZED_LEADS_PATH = DEFAULT_PROCESSED_DIR / "prioritized_leads.csv"
DEFAULT_CROSS_SOURCE_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "cross_source_matches.csv"
DEFAULT_CROSS_SOURCE_DIAGNOSTICS_PATH = DEFAULT_PROCESSED_DIR / "cross_source_diagnostics.csv"
DEFAULT_CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "cross_source_diagnostic_summary.json"
CONFIG_PATH = Path("config/cross_source.json")

DEFAULT_CONFIG: dict[str, Any] = {
    "minimum_independent_sources": 2,
    "minimum_real_sources_for_marker": 2,
    "exact_match_confidence": 0.97,
    "compound_match_confidence": 0.88,
    "review_match_confidence": 0.76,
    "minimum_evidence_completeness": 60,
    "minimum_entity_resolution_confidence": 0.75,
    "enabled_source_pairs": [],
    "enabled_entity_types": [
        "address",
        "business",
        "owner",
        "person",
        "registered_agent",
        "officer",
        "phone",
        "email",
        "website",
        "property",
    ],
    "real_synthetic_separation": True,
    "cross_source_lead_thresholds": {
        "minimum_matches": 1,
        "minimum_real_matches": 1,
    },
}


def load_cross_source_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in loaded.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def _token_set(value: object) -> set[str]:
    return {token.strip() for token in str(value or "").split("|") if token.strip()}


def _source_pair(left_source_name: str, right_source_name: str) -> str:
    return "|".join(sorted({left_source_name, right_source_name}))


def _match_id(canonical_entity_id: str, left_source_record_id: str, right_source_record_id: str, match_method: str) -> str:
    basis = "|".join([canonical_entity_id, left_source_record_id, right_source_record_id, match_method])
    return f"cross:{hashlib.sha1(basis.encode('utf-8')).hexdigest()[:16]}"


def _normalize_aliases(aliases_df: pd.DataFrame, canonical_entities_df: pd.DataFrame) -> pd.DataFrame:
    if aliases_df.empty:
        return pd.DataFrame()
    aliases = aliases_df.copy()
    aliases["source_name"] = aliases["source_name"].fillna("").astype(str).str.strip()
    aliases["source_type"] = aliases["source_type"].fillna("").astype(str).map(standardize_source_type)
    aliases["source_record_id"] = aliases["source_record_id"].fillna("").astype(str) if "source_record_id" in aliases.columns else aliases["original_entity_id"].fillna("").astype(str)
    aliases["connector_name"] = aliases["connector_name"].fillna("").astype(str) if "connector_name" in aliases.columns else aliases["source_name"].fillna("").astype(str)
    aliases["import_batch_id"] = aliases["import_batch_id"].fillna("").astype(str) if "import_batch_id" in aliases.columns else ""
    aliases["imported_at"] = aliases["imported_at"].fillna("").astype(str) if "imported_at" in aliases.columns else ""
    aliases["jurisdiction"] = aliases["jurisdiction"].fillna("").astype(str) if "jurisdiction" in aliases.columns else ""
    aliases["is_synthetic"] = aliases["is_synthetic"].fillna("").astype(str).str.lower().replace({"": "false"}) if "is_synthetic" in aliases.columns else "false"
    if not canonical_entities_df.empty:
        entity_meta = canonical_entities_df[["canonical_entity_id", "entity_type", "resolution_confidence", "resolution_method", "display_name", "normalized_value"]].copy()
        aliases = aliases.merge(entity_meta, on="canonical_entity_id", how="left")
        if "resolution_method_x" in aliases.columns and "resolution_method_y" in aliases.columns:
            aliases["resolution_method"] = aliases["resolution_method_x"].fillna("").astype(str)
            aliases.loc[aliases["resolution_method"] == "", "resolution_method"] = aliases.loc[aliases["resolution_method"] == "", "resolution_method_y"].astype(str)
            aliases = aliases.drop(columns=["resolution_method_x", "resolution_method_y"])
        elif "resolution_method_y" in aliases.columns:
            aliases = aliases.rename(columns={"resolution_method_y": "resolution_method"})
        if "resolution_confidence_x" in aliases.columns and "resolution_confidence_y" in aliases.columns:
            aliases["resolution_confidence"] = pd.to_numeric(aliases["resolution_confidence_x"], errors="coerce").fillna(
                pd.to_numeric(aliases["resolution_confidence_y"], errors="coerce")
            )
            aliases = aliases.drop(columns=["resolution_confidence_x", "resolution_confidence_y"])
    return aliases


def _normalize_relationships(relationships_df: pd.DataFrame, canonical_entities_df: pd.DataFrame) -> pd.DataFrame:
    if relationships_df.empty:
        return pd.DataFrame()
    relationships = relationships_df.copy()
    relationships["source_name"] = relationships["source_name"].fillna("").astype(str).str.strip()
    relationships["source_type"] = relationships["source_type"].fillna("").astype(str).map(standardize_source_type)
    relationships["source_record_id"] = relationships["source_record_id"].fillna("").astype(str) if "source_record_id" in relationships.columns else ""
    relationships["jurisdiction"] = relationships["jurisdiction"].fillna("").astype(str) if "jurisdiction" in relationships.columns else ""
    relationships["is_synthetic"] = (
        relationships["is_synthetic"].fillna("").astype(str).str.lower().replace({"": "false"})
        if "is_synthetic" in relationships.columns
        else "false"
    )
    if not canonical_entities_df.empty:
        entity_types = canonical_entities_df[["canonical_entity_id", "entity_type", "display_name", "normalized_value", "source_name", "source_type"]].copy()
        entity_types = entity_types.rename(
            columns={
                "canonical_entity_id": "source_entity_id",
                "entity_type": "source_entity_type",
                "display_name": "source_display_name",
                "normalized_value": "source_normalized_value",
                "source_name": "source_entity_sources",
                "source_type": "source_entity_source_types",
            }
        )
        relationships = relationships.merge(entity_types, on="source_entity_id", how="left")
        target_types = canonical_entities_df[["canonical_entity_id", "entity_type", "display_name", "normalized_value", "source_name", "source_type"]].copy()
        target_types = target_types.rename(
            columns={
                "canonical_entity_id": "target_entity_id",
                "entity_type": "target_entity_type",
                "display_name": "target_display_name",
                "normalized_value": "target_normalized_value",
                "source_name": "target_entity_sources",
                "source_type": "target_entity_source_types",
            }
        )
        relationships = relationships.merge(target_types, on="target_entity_id", how="left")
    return relationships


def _secondary_context(relationships_df: pd.DataFrame) -> dict[str, set[str]]:
    context: dict[str, set[str]] = {}
    if relationships_df.empty:
        return context
    for _, row in relationships_df.iterrows():
        source_entity_id = str(row.get("source_entity_id", "")).strip()
        target_entity_id = str(row.get("target_entity_id", "")).strip()
        target_type = str(row.get("target_entity_type", "")).strip()
        if not source_entity_id or not target_entity_id or not target_type:
            continue
        context.setdefault(source_entity_id, set()).add(f"{target_type}:{target_entity_id}")
    return context


def _independent_real_source_count(left_source_type: str, right_source_type: str, left_source_name: str, right_source_name: str) -> int:
    distinct = set()
    if is_real_source_type(left_source_type):
        distinct.add(left_source_name)
    if is_real_source_type(right_source_type):
        distinct.add(right_source_name)
    return len({token for token in distinct if token})


def _row_from_pair(
    *,
    canonical_entity_id: str,
    entity_type: str,
    left_source_name: str,
    right_source_name: str,
    left_source_type: str,
    right_source_type: str,
    left_source_record_id: str,
    right_source_record_id: str,
    left_entity_id: str,
    right_entity_id: str,
    match_method: str,
    confidence: float,
    evidence: str,
    decision: str,
) -> dict[str, Any]:
    contains_real_data = bool(is_real_source_type(left_source_type) or is_real_source_type(right_source_type))
    contains_synthetic_data = "synthetic" in {left_source_type, right_source_type}
    independent_real_sources = _independent_real_source_count(left_source_type, right_source_type, left_source_name, right_source_name)
    return {
        "cross_source_match_id": _match_id(canonical_entity_id, left_source_record_id, right_source_record_id, match_method),
        "canonical_entity_id": canonical_entity_id,
        "entity_type": entity_type,
        "left_entity_id": left_entity_id,
        "right_entity_id": right_entity_id,
        "left_source_name": left_source_name,
        "right_source_name": right_source_name,
        "left_source_type": left_source_type,
        "right_source_type": right_source_type,
        "left_source_record_id": left_source_record_id,
        "right_source_record_id": right_source_record_id,
        "source_pair": _source_pair(left_source_name, right_source_name),
        "match_method": match_method,
        "confidence": round(float(confidence), 4),
        "evidence": evidence,
        "decision": decision,
        "independent_real_source_count": independent_real_sources,
        "contains_real_data": contains_real_data,
        "contains_synthetic_data": contains_synthetic_data,
        "why_sources_independent": "Different source_name values" if left_source_name != right_source_name else "Same source_name; not independent",
    }


def build_cross_source_matches(
    canonical_entities_df: pd.DataFrame,
    aliases_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    enabled_entity_types = {str(value).strip().lower() for value in config.get("enabled_entity_types", [])}
    aliases = _normalize_aliases(aliases_df, canonical_entities_df)
    relationships = _normalize_relationships(canonical_relationships_df, canonical_entities_df)
    secondary = _secondary_context(relationships)
    rows: list[dict[str, Any]] = []

    if not aliases.empty and "canonical_entity_id" in aliases.columns:
        for canonical_entity_id, group in aliases.groupby("canonical_entity_id"):
            entity_type = str(group["entity_type"].iloc[0]).strip().lower()
            if entity_type not in enabled_entity_types:
                continue
            required_columns = ["source_name", "source_type", "source_record_id", "original_entity_id", "normalized_alias", "resolution_confidence", "resolution_method"]
            for column in required_columns:
                if column not in group.columns:
                    group[column] = ""
            pair_rows = group[required_columns].drop_duplicates()
            records = pair_rows.to_dict("records")
            for left_index, left in enumerate(records):
                for right in records[left_index + 1 :]:
                    left_source_name = str(left.get("source_name", ""))
                    right_source_name = str(right.get("source_name", ""))
                    left_source_type = standardize_source_type(str(left.get("source_type", "")))
                    right_source_type = standardize_source_type(str(right.get("source_type", "")))
                    left_record_id = str(left.get("source_record_id", "") or left.get("original_entity_id", ""))
                    right_record_id = str(right.get("source_record_id", "") or right.get("original_entity_id", ""))
                    if left_source_name == right_source_name:
                        rows.append(
                            _row_from_pair(
                                canonical_entity_id=str(canonical_entity_id),
                                entity_type=entity_type,
                                left_source_name=left_source_name,
                                right_source_name=right_source_name,
                                left_source_type=left_source_type,
                                right_source_type=right_source_type,
                                left_source_record_id=left_record_id,
                                right_source_record_id=right_record_id,
                                left_entity_id=str(left.get("original_entity_id", "")),
                                right_entity_id=str(right.get("original_entity_id", "")),
                                match_method="same_canonical_same_source",
                                confidence=config.get("review_match_confidence", 0.76),
                                evidence="Records resolve to the same canonical entity but come from the same source_name, so they do not count as independent corroboration.",
                                decision="REJECTED_SAME_SOURCE",
                            )
                        )
                        continue
                    real_count = _independent_real_source_count(left_source_type, right_source_type, left_source_name, right_source_name)
                    decision = "AUTO_MATCH"
                    confidence = float(config.get("exact_match_confidence", 0.97))
                    evidence = f"Exact canonical support for normalized value '{str(left.get('normalized_alias', ''))}' across {left_source_name} and {right_source_name}."
                    if config.get("real_synthetic_separation", True) and real_count < int(config.get("minimum_independent_sources", 2)):
                        decision = "REJECTED_SYNTHETIC_OR_SINGLE_REAL"
                    if entity_type in {"person", "registered_agent", "officer"} and "secondary" not in str(group.get("resolution_method", pd.Series([""])).iloc[0]).lower():
                        decision = "REVIEW"
                        confidence = float(config.get("review_match_confidence", 0.76))
                        evidence += " Exact person-name support lacks explicit secondary evidence in the canonical resolution method."
                    rows.append(
                        _row_from_pair(
                            canonical_entity_id=str(canonical_entity_id),
                            entity_type=entity_type,
                            left_source_name=left_source_name,
                            right_source_name=right_source_name,
                            left_source_type=left_source_type,
                            right_source_type=right_source_type,
                            left_source_record_id=left_record_id,
                            right_source_record_id=right_record_id,
                            left_entity_id=str(left.get("original_entity_id", "")),
                            right_entity_id=str(right.get("original_entity_id", "")),
                            match_method=f"exact_canonical_{entity_type}",
                            confidence=confidence,
                            evidence=evidence,
                            decision=decision,
                        )
                    )

    address_relationships = relationships[relationships["target_entity_type"].fillna("").astype(str).eq("address")].copy() if not relationships.empty else pd.DataFrame()
    if not address_relationships.empty:
        for address_id, group in address_relationships.groupby("target_entity_id"):
            records = group.to_dict("records")
            for left_index, left in enumerate(records):
                for right in records[left_index + 1 :]:
                    left_source_name = str(left.get("source_name", ""))
                    right_source_name = str(right.get("source_name", ""))
                    if left_source_name == right_source_name:
                        continue
                    left_type = str(left.get("source_entity_type", "")).lower()
                    right_type = str(right.get("source_entity_type", "")).lower()
                    rel_pair = {str(left.get("relationship_type", "")), str(right.get("relationship_type", ""))}
                    match_method = ""
                    if {left_type, right_type} == {"property", "business"} and "PROPERTY_HAS_SITUS_ADDRESS" in rel_pair:
                        match_method = "property_situs_matches_business_address"
                    elif {left_type, right_type} == {"property", "business"} and "PROPERTY_HAS_MAILING_ADDRESS" in rel_pair:
                        match_method = "property_mailing_matches_business_address"
                    elif {left_type, right_type} == {"property", "person"}:
                        match_method = "property_address_linked_to_person_record"
                    elif {left_type, right_type} == {"property", "case"}:
                        match_method = "property_address_linked_to_case_record"
                    if not match_method:
                        continue
                    left_source_type = standardize_source_type(str(left.get("source_type", "")))
                    right_source_type = standardize_source_type(str(right.get("source_type", "")))
                    real_count = _independent_real_source_count(left_source_type, right_source_type, left_source_name, right_source_name)
                    decision = "AUTO_MATCH" if real_count >= int(config.get("minimum_independent_sources", 2)) else "REJECTED_SYNTHETIC_OR_SINGLE_REAL"
                    rows.append(
                        _row_from_pair(
                            canonical_entity_id=str(address_id),
                            entity_type="address",
                            left_source_name=left_source_name,
                            right_source_name=right_source_name,
                            left_source_type=left_source_type,
                            right_source_type=right_source_type,
                            left_source_record_id=str(left.get("source_record_id", "") or left.get("source_entity_id", "")),
                            right_source_record_id=str(right.get("source_record_id", "") or right.get("source_entity_id", "")),
                            left_entity_id=str(left.get("source_entity_id", "")),
                            right_entity_id=str(right.get("source_entity_id", "")),
                            match_method=match_method,
                            confidence=float(config.get("compound_match_confidence", 0.88)),
                            evidence=f"Canonical address {address_id} is linked to {left_type} and {right_type} records across {left_source_name} and {right_source_name}.",
                            decision=decision,
                        )
                    )

    if not canonical_entities_df.empty:
        entities = canonical_entities_df.copy()
        entities["entity_type"] = entities["entity_type"].fillna("").astype(str).str.lower()
        entities["normalized_value"] = entities["normalized_value"].fillna("").astype(str)
        entities["source_name"] = entities["source_name"].fillna("").astype(str)
        entities["source_type"] = entities["source_type"].fillna("").astype(str).map(standardize_source_type)
        entities["resolution_confidence"] = pd.to_numeric(entities.get("resolution_confidence", 0), errors="coerce").fillna(0)
        name_entities = entities[entities["entity_type"].isin(["business", "owner", "person", "registered_agent", "officer"]) & entities["normalized_value"].ne("")].copy()
        for normalized_value, group in name_entities.groupby("normalized_value"):
            records = group.to_dict("records")
            for left_index, left in enumerate(records):
                for right in records[left_index + 1 :]:
                    left_source_name = str(left.get("source_name", ""))
                    right_source_name = str(right.get("source_name", ""))
                    if left_source_name == right_source_name:
                        continue
                    left_type = str(left.get("entity_type", "")).lower()
                    right_type = str(right.get("entity_type", "")).lower()
                    method = ""
                    shared_secondary = sorted(secondary.get(str(left.get("canonical_entity_id", "")), set()) & secondary.get(str(right.get("canonical_entity_id", "")), set()))
                    if {left_type, right_type} == {"owner", "business"}:
                        method = "parcel_owner_matches_business_name"
                    elif {left_type, right_type} & {"person", "registered_agent", "officer"} and "owner" in {left_type, right_type}:
                        method = "parcel_owner_matches_person_with_secondary"
                    elif "person" in {left_type, right_type} and "business" in {left_type, right_type}:
                        method = "clerk_party_matches_business_person_with_secondary"
                    if not method:
                        continue
                    left_source_type = standardize_source_type(str(left.get("source_type", "")))
                    right_source_type = standardize_source_type(str(right.get("source_type", "")))
                    real_count = _independent_real_source_count(left_source_type, right_source_type, left_source_name, right_source_name)
                    decision = "AUTO_MATCH"
                    confidence = float(config.get("compound_match_confidence", 0.88))
                    evidence = f"Exact normalized value '{normalized_value}' appears across {left_source_name} ({left_type}) and {right_source_name} ({right_type})."
                    if "secondary" in method:
                        if shared_secondary:
                            evidence += f" Secondary evidence: {shared_secondary[0]}."
                        else:
                            decision = "REJECTED_NO_SECONDARY_EVIDENCE"
                            confidence = float(config.get("review_match_confidence", 0.76))
                            evidence += " No secondary address, phone, email, website, or property evidence was found."
                    if decision == "AUTO_MATCH" and real_count < int(config.get("minimum_independent_sources", 2)):
                        decision = "REJECTED_SYNTHETIC_OR_SINGLE_REAL"
                    rows.append(
                        _row_from_pair(
                            canonical_entity_id=str(left.get("canonical_entity_id", "")),
                            entity_type=left_type,
                            left_source_name=left_source_name,
                            right_source_name=right_source_name,
                            left_source_type=left_source_type,
                            right_source_type=right_source_type,
                            left_source_record_id=str(left.get("source_record_ids", "") or left.get("canonical_entity_id", "")),
                            right_source_record_id=str(right.get("source_record_ids", "") or right.get("canonical_entity_id", "")),
                            left_entity_id=str(left.get("canonical_entity_id", "")),
                            right_entity_id=str(right.get("canonical_entity_id", "")),
                            match_method=method,
                            confidence=confidence,
                            evidence=evidence,
                            decision=decision,
                        )
                    )

    output = pd.DataFrame(rows)
    if output.empty:
        return pd.DataFrame(
            columns=[
                "cross_source_match_id",
                "canonical_entity_id",
                "entity_type",
                "left_source_name",
                "right_source_name",
                "left_source_record_id",
                "right_source_record_id",
                "match_method",
                "confidence",
                "evidence",
                "decision",
                "contains_real_data",
                "contains_synthetic_data",
            ]
        )
    output = output.sort_values(["decision", "confidence", "canonical_entity_id"], ascending=[True, False, True]).drop_duplicates("cross_source_match_id").reset_index(drop=True)
    return output


def build_cross_source_diagnostics(
    canonical_entities_df: pd.DataFrame,
    aliases_df: pd.DataFrame,
    entity_resolution_matches_df: pd.DataFrame,
    canonical_relationships_df: pd.DataFrame,
    fraud_markers_df: pd.DataFrame,
    prioritized_leads_df: pd.DataFrame,
    cross_source_matches_df: pd.DataFrame,
    connector_frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    diagnostics_rows: list[dict[str, Any]] = []
    aliases = _normalize_aliases(aliases_df, canonical_entities_df)
    relationships = _normalize_relationships(canonical_relationships_df, canonical_entities_df)

    source_names_present = sorted(
        {
            *set(aliases.get("source_name", pd.Series(dtype=str)).fillna("").astype(str)),
            *set(relationships.get("source_name", pd.Series(dtype=str)).fillna("").astype(str)),
        }
        - {""}
    )
    source_types_present = sorted(
        {
            *set(aliases.get("source_type", pd.Series(dtype=str)).fillna("").astype(str)),
            *set(relationships.get("source_type", pd.Series(dtype=str)).fillna("").astype(str)),
        }
        - {""}
    )

    canonical_source_counts = pd.DataFrame()
    if not aliases.empty:
        canonical_source_counts = aliases.groupby("canonical_entity_id").agg(
            entity_type=("entity_type", "first"),
            independent_sources=("source_name", lambda series: len({value for value in series if value})),
            real_sources=("source_name", lambda _: 0),
        )
        real_counts: list[int] = []
        for canonical_entity_id, group in aliases.groupby("canonical_entity_id"):
            real_counts.append(len({str(row["source_name"]) for _, row in group.iterrows() if is_real_source_type(str(row["source_type"]))}))
        canonical_source_counts["real_sources"] = real_counts

    correlated_entity_types = sorted(canonical_source_counts[canonical_source_counts["independent_sources"] > 1]["entity_type"].astype(str).unique().tolist()) if not canonical_source_counts.empty else []
    all_entity_types = sorted(canonical_entities_df.get("entity_type", pd.Series(dtype=str)).fillna("").astype(str).unique().tolist()) if not canonical_entities_df.empty else []
    failed_entity_types = sorted(set(all_entity_types) - set(correlated_entity_types))

    relationship_multi_source_count = 0
    if not relationships.empty:
        relationship_multi_source_count = int(relationships["source_name"].fillna("").astype(str).apply(lambda value: len(_token_set(value)) > 1).sum())

    exact_cross_source_candidate_count = int(cross_source_matches_df["match_method"].astype(str).str.startswith("exact_").sum()) if not cross_source_matches_df.empty else 0
    review_match_count = int((cross_source_matches_df["decision"] == "REVIEW").sum()) if not cross_source_matches_df.empty else 0
    rejected_match_count = int(cross_source_matches_df["decision"].astype(str).str.startswith("REJECTED").sum()) if not cross_source_matches_df.empty else 0
    auto_match_count = int((cross_source_matches_df["decision"] == "AUTO_MATCH").sum()) if not cross_source_matches_df.empty else 0

    rejection_reasons: dict[str, int] = {}
    if not cross_source_matches_df.empty:
        rejection_reasons = (
            cross_source_matches_df[cross_source_matches_df["decision"].astype(str).str.startswith("REJECTED")]
            .groupby("decision")
            .size()
            .sort_values(ascending=False)
            .to_dict()
        )

    inconsistent_source_labels = []
    if not aliases.empty:
        for source_name, group in aliases.groupby("source_name"):
            source_types = {standardize_source_type(value) for value in group["source_type"].astype(str) if value}
            if len(source_types) > 1:
                inconsistent_source_labels.append({"source_name": source_name, "source_types": sorted(source_types)})

    missing_connectors = sorted([name for name, frame in connector_frames.items() if frame.empty])
    real_cross_source_canonical_entities = 0
    if not cross_source_matches_df.empty:
        real_cross_source_canonical_entities = int(
            cross_source_matches_df[(cross_source_matches_df["decision"] == "AUTO_MATCH") & (cross_source_matches_df["independent_real_source_count"] >= 2)]["canonical_entity_id"].nunique()
        )

    root_cause_notes: list[str] = []
    if not correlated_entity_types:
        root_cause_notes.append("No canonical entities currently resolve across more than one independent source.")
    if missing_connectors:
        root_cause_notes.append(f"Connector outputs missing or empty: {', '.join(missing_connectors)}.")
    if "florida_county_arcgis_parcels" in source_names_present and "sample_api" in source_names_present and real_cross_source_canonical_entities == 0:
        root_cause_notes.append("The current live real-data set is limited to ArcGIS parcels and sample_api records, and those two sources do not currently overlap on exact canonical entities or approved secondary correlation keys.")

    summary = {
        "source_names_present": source_names_present,
        "source_types_present": source_types_present,
        "entity_counts_by_source": aliases.groupby("source_name").size().sort_values(ascending=False).to_dict() if not aliases.empty else {},
        "canonical_entities_supported_by_more_than_one_source": int((canonical_source_counts["independent_sources"] > 1).sum()) if not canonical_source_counts.empty else 0,
        "relationships_supported_by_more_than_one_source": relationship_multi_source_count,
        "entity_types_currently_correlating": correlated_entity_types,
        "entity_types_failing_to_correlate": failed_entity_types,
        "exact_cross_source_candidate_count": exact_cross_source_candidate_count,
        "auto_match_count": auto_match_count,
        "review_match_count": review_match_count,
        "rejected_match_count": rejected_match_count,
        "rejection_reasons": rejection_reasons,
        "inconsistent_source_labels": inconsistent_source_labels,
        "real_cross_source_canonical_entity_count": real_cross_source_canonical_entities,
        "root_cause_notes": root_cause_notes,
    }

    for key, value in summary.items():
        diagnostics_rows.append({"metric": key, "value": json.dumps(value) if isinstance(value, (dict, list)) else value})

    return pd.DataFrame(diagnostics_rows), summary


def run_cross_source_correlation(
    canonical_entities_path: Path | str = DEFAULT_CANONICAL_ENTITIES_PATH,
    aliases_path: Path | str = DEFAULT_ENTITY_ALIASES_PATH,
    entity_resolution_matches_path: Path | str = DEFAULT_ENTITY_RESOLUTION_MATCHES_PATH,
    canonical_relationships_path: Path | str = DEFAULT_CANONICAL_RELATIONSHIPS_PATH,
    fraud_markers_path: Path | str = DEFAULT_FRAUD_MARKERS_PATH,
    prioritized_leads_path: Path | str = DEFAULT_PRIORITIZED_LEADS_PATH,
    cross_source_matches_path: Path | str = DEFAULT_CROSS_SOURCE_MATCHES_PATH,
    diagnostics_path: Path | str = DEFAULT_CROSS_SOURCE_DIAGNOSTICS_PATH,
    diagnostic_summary_path: Path | str = DEFAULT_CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH,
    config_path: Path | str = CONFIG_PATH,
) -> dict[str, Any]:
    start_time = time.time()
    canonical_entities_df = _load_frame(Path(canonical_entities_path))
    aliases_df = _load_frame(Path(aliases_path))
    entity_resolution_matches_df = _load_frame(Path(entity_resolution_matches_path))
    canonical_relationships_df = _load_frame(Path(canonical_relationships_path))
    fraud_markers_df = _load_frame(Path(fraud_markers_path))
    prioritized_leads_df = _load_frame(Path(prioritized_leads_path))
    config = load_cross_source_config(config_path)

    connector_frames = {
        "arcgis_entities": _load_frame(DEFAULT_PROCESSED_DIR / "arcgis_entities.csv"),
        "arcgis_relationships": _load_frame(DEFAULT_PROCESSED_DIR / "arcgis_relationships.csv"),
        "api_entities": _load_frame(DEFAULT_PROCESSED_DIR / "api_entities.csv"),
        "api_relationships": _load_frame(DEFAULT_PROCESSED_DIR / "api_relationships.csv"),
        "sunbiz_entities": _load_frame(DEFAULT_PROCESSED_DIR / "sunbiz_entities.csv"),
        "sunbiz_relationships": _load_frame(DEFAULT_PROCESSED_DIR / "sunbiz_relationships.csv"),
        "county_property_entities": _load_frame(DEFAULT_PROCESSED_DIR / "county_property_entities.csv"),
        "county_property_relationships": _load_frame(DEFAULT_PROCESSED_DIR / "county_property_relationships.csv"),
        "county_clerk_entities": _load_frame(DEFAULT_PROCESSED_DIR / "county_clerk_entities.csv"),
        "county_clerk_relationships": _load_frame(DEFAULT_PROCESSED_DIR / "county_clerk_relationships.csv"),
    }

    print("Cross-Source Correlation: started")
    print(f"Cross-Source Correlation: canonical entities loaded {len(canonical_entities_df)}")
    print(f"Cross-Source Correlation: canonical relationships loaded {len(canonical_relationships_df)}")
    print(f"Cross-Source Correlation: aliases loaded {len(aliases_df)}")

    cross_source_matches_df = build_cross_source_matches(canonical_entities_df, aliases_df, canonical_relationships_df, config)
    diagnostics_df, diagnostics_summary = build_cross_source_diagnostics(
        canonical_entities_df,
        aliases_df,
        entity_resolution_matches_df,
        canonical_relationships_df,
        fraud_markers_df,
        prioritized_leads_df,
        cross_source_matches_df,
        connector_frames,
    )

    Path(cross_source_matches_path).parent.mkdir(parents=True, exist_ok=True)
    cross_source_matches_df.to_csv(cross_source_matches_path, index=False)
    diagnostics_df.to_csv(diagnostics_path, index=False)
    with Path(diagnostic_summary_path).open("w", encoding="utf-8") as handle:
        json.dump(diagnostics_summary, handle, indent=2, sort_keys=True)

    duration = time.time() - start_time
    print(f"Cross-Source Correlation: candidate rows {len(cross_source_matches_df)}")
    print(f"Cross-Source Correlation: auto matches {(cross_source_matches_df['decision'] == 'AUTO_MATCH').sum() if not cross_source_matches_df.empty else 0}")
    print(f"Cross-Source Correlation: completed in {duration:.2f}s")
    print("Cross-Source Correlation: PASS")
    return {
        "cross_source_match_count": int(len(cross_source_matches_df)),
        "auto_match_count": int((cross_source_matches_df["decision"] == "AUTO_MATCH").sum()) if not cross_source_matches_df.empty else 0,
        "review_match_count": int((cross_source_matches_df["decision"] == "REVIEW").sum()) if not cross_source_matches_df.empty else 0,
        "rejected_match_count": int(cross_source_matches_df["decision"].astype(str).str.startswith("REJECTED").sum()) if not cross_source_matches_df.empty else 0,
        "real_cross_source_canonical_entity_count": int(
            cross_source_matches_df[(cross_source_matches_df["decision"] == "AUTO_MATCH") & (cross_source_matches_df["independent_real_source_count"] >= 2)]["canonical_entity_id"].nunique()
        )
        if not cross_source_matches_df.empty
        else 0,
        "runtime_seconds": round(duration, 2),
    }
