from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from src.analytics.entity_resolution.normalizers import normalize_address_value, normalize_business_name, normalize_person_name
from src.normalize.address_normalizer import normalize_address

OUTPUT_COLUMNS = {
    "nppes_sunbiz_matches": [
        "match_id", "match_type", "decision", "confidence", "npi", "provider_name", "provider_type", "taxonomy",
        "enumeration_type",
        "sunbiz_corporation_number", "sunbiz_business_name", "sunbiz_person_name", "sunbiz_person_role",
        "nppes_entity_id", "sunbiz_entity_id", "address_match_scope", "corroborating_fields", "conflicting_fields",
        "common_name_downgraded", "deactivated_status", "explanation", "recommended_review", "source_record_ids", "imported_at"
    ],
    "nppes_parcel_matches": [
        "match_id", "match_type", "decision", "confidence", "npi", "provider_name", "provider_type", "taxonomy",
        "enumeration_type",
        "parcel_id", "parcel_owner", "parcel_address", "nppes_entity_id", "parcel_entity_id", "address_match_scope",
        "corroborating_fields", "conflicting_fields", "common_name_downgraded", "deactivated_status",
        "explanation", "recommended_review", "source_record_ids", "imported_at"
    ],
    "nppes_sunbiz_parcel_paths": [
        "path_id", "npi", "provider_name", "provider_type", "taxonomy", "sunbiz_corporation_number", "sunbiz_business_name",
        "sunbiz_person_or_role", "parcel_id", "parcel_owner", "nppes_entity_id", "sunbiz_entity_id", "parcel_entity_id", "enumeration_type",
        "edge_sequence", "sources", "source_record_ids", "corroborating_fields", "conflicting_fields",
        "decision", "confidence", "explanation", "recommended_review", "imported_at"
    ],
}


def _load(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _normalized_name(value: str, *, organization: bool) -> str:
    return normalize_business_name(value).get("normalized_value", "") if organization else normalize_person_name(value).get("normalized_value", "")


def _address_scope(left: str, right: str, *, mailing: bool = False) -> str:
    left_norm = normalize_address(left)
    right_norm = normalize_address(right)
    if not left_norm or not right_norm:
        return "INCOMPLETE"
    if "PO BOX" in left_norm or "PO BOX" in right_norm:
        return "PO_BOX"
    if "GENERAL DELIVERY" in left_norm or "GENERAL DELIVERY" in right_norm:
        return "GENERALIZED"
    left_parts = normalize_address_value(left)
    right_parts = normalize_address_value(right)
    if left_norm == right_norm:
        if left_parts.get("unit_key") and right_parts.get("unit_key"):
            return "EXACT_UNIT"
        return "MAILING_ONLY" if mailing else "EXACT_BUILDING"
    if left_parts.get("building_key") and left_parts.get("building_key") == right_parts.get("building_key"):
        if left_parts.get("unit_key") and right_parts.get("unit_key") and left_parts.get("unit_key") != right_parts.get("unit_key"):
            return "GENERALIZED"
        return "MAILING_ONLY" if mailing else "EXACT_BUILDING"
    return "INCOMPLETE"


def _decision(*, exact_name: bool, exact_address: bool, building_only: bool, common_name: bool, po_box: bool, deactivated: bool, has_core_fields: bool) -> tuple[str, float, str]:
    if not has_core_fields:
        return "INSUFFICIENT_DATA", 0.2, "Missing critical fields prevented a reliable correlation decision."
    if po_box:
        return "REJECTED", 0.05, "PO Box values were suppressed from physical-location matching."
    if exact_name and exact_address and not common_name:
        return "ACCEPTED_EXACT", 0.97, "Exact normalized name and exact supported address matched."
    if exact_name and exact_address and common_name:
        return "REVIEW_STRONG", 0.82, "Exact name and address matched, but the name was common in the imported set."
    if exact_address and not exact_name:
        return "REVIEW_STRONG", 0.76, "Exact supported address matched and was retained as association evidence for analyst review."
    if exact_name and building_only:
        return "REVIEW_STRONG", 0.79, "Exact name matched at the same building, but unit-level specificity was incomplete."
    if exact_name:
        return "REVIEW_WEAK", 0.48, "Name-only match was retained for analyst review but not accepted."
    if deactivated:
        return "INSUFFICIENT_DATA", 0.25, "Deactivated NPI without stronger corroboration was kept as historical context only."
    return "REJECTED", 0.1, "Candidate lacked enough corroborating evidence for a reliable match."


def _provider_taxonomy(provider_row: dict[str, Any], taxonomy_df: pd.DataFrame) -> str:
    if taxonomy_df.empty:
        return ""
    matches = taxonomy_df[taxonomy_df["npi"].astype(str) == str(provider_row.get("npi", ""))]
    if matches.empty:
        return ""
    return "|".join(matches["taxonomy_description"].astype(str).head(3).tolist())


def generate_nppes_correlations(
    *,
    processed_dir: Path | str,
    append_to_cross_source: bool = True,
) -> dict[str, Any]:
    processed_path = Path(processed_dir)
    nppes_entities = _load(processed_path / "nppes_entities.csv")
    nppes_relationships = _load(processed_path / "nppes_relationships.csv")
    nppes_providers = _load(processed_path / "nppes_providers.csv")
    nppes_taxonomies = _load(processed_path / "nppes_taxonomies.csv")
    sunbiz_entities = _load(processed_path / "sunbiz_entities.csv")
    county_property_entities = _load(processed_path / "county_property_entities.csv")
    county_property_relationships = _load(processed_path / "county_property_relationships.csv")
    if nppes_entities.empty:
        for name, columns in OUTPUT_COLUMNS.items():
            _write_csv(processed_path / f"{name}.csv", [], columns)
        _write_json(processed_path / "nppes_match_quality_report.json", {"providers_processed": 0})
        _write_csv(processed_path / "nppes_match_quality_samples.csv", [], ["decision"])
        return {"providers_processed": 0}

    imported_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    provider_lookup = {str(row.get("entity_id", "")): row for _, row in nppes_entities.iterrows() if str(row.get("entity_type", "")).endswith("_provider")}
    address_lookup = {str(row.get("entity_id", "")): row for _, row in nppes_entities.iterrows() if str(row.get("entity_type", "")) == "address"}
    sunbiz_lookup = {str(row.get("entity_id", "")): row for _, row in sunbiz_entities.iterrows()}
    property_lookup = {str(row.get("entity_id", "")): row for _, row in county_property_entities.iterrows()}

    provider_address_rows = []
    for _, rel in nppes_relationships.iterrows():
        rel_type = str(rel.get("relationship_type", ""))
        if rel_type not in {"PROVIDER_PRACTICES_AT", "PROVIDER_MAILS_TO"}:
            continue
        provider = provider_lookup.get(str(rel.get("source_entity_id", "")), {})
        address = address_lookup.get(str(rel.get("target_entity_id", "")), {})
        if len(provider) > 0 and len(address) > 0:
            provider_address_rows.append((provider, address, rel_type))

    sunbiz_matches: list[dict[str, Any]] = []
    parcel_matches: list[dict[str, Any]] = []
    common_provider_names = nppes_providers["provider_name"].astype(str).value_counts().to_dict() if not nppes_providers.empty and "provider_name" in nppes_providers.columns else {}
    org_name_frequency = nppes_providers["organization_name"].astype(str).value_counts().to_dict() if not nppes_providers.empty and "organization_name" in nppes_providers.columns else {}

    for provider, address, rel_type in provider_address_rows:
        provider_name = str(provider.get("display_name", ""))
        provider_entity_id = str(provider.get("entity_id", ""))
        provider_row = nppes_providers[nppes_providers["npi"].astype(str) == str(provider.get("npi", ""))].head(1)
        provider_data = provider_row.iloc[0].to_dict() if not provider_row.empty else {}
        provider_type = str(provider.get("entity_type", ""))
        enumeration_type = str(provider_data.get("enumeration_type", provider.get("enumeration_type", "")))
        taxonomy = _provider_taxonomy(provider_data, nppes_taxonomies)
        npi = str(provider.get("npi", provider_data.get("npi", "")))
        is_org = provider_type == "organization_provider"
        provider_norm = _normalized_name(provider_name, organization=is_org)
        common_name = (org_name_frequency.get(provider_name, 0) if is_org else common_provider_names.get(provider_name, 0)) > 1
        address_text = str(address.get("display_name", ""))
        for sunbiz_entity_id, sunbiz_row in sunbiz_lookup.items():
            sunbiz_type = str(sunbiz_row.get("entity_type", ""))
            if sunbiz_type not in {"business", "officer", "registered_agent", "address"}:
                continue
            sunbiz_name = str(sunbiz_row.get("display_name", ""))
            sunbiz_norm = _normalized_name(sunbiz_name, organization=sunbiz_type == "business")
            if is_org and sunbiz_type not in {"business", "address"}:
                continue
            if not is_org and sunbiz_type not in {"officer", "registered_agent", "address"}:
                continue
            scope = _address_scope(address_text, str(sunbiz_row.get("display_name", "")), mailing=rel_type == "PROVIDER_MAILS_TO")
            exact_name = provider_norm and provider_norm == sunbiz_norm
            exact_address = scope in {"EXACT_UNIT", "EXACT_BUILDING"} and not rel_type == "PROVIDER_MAILS_TO"
            building_only = scope in {"EXACT_BUILDING", "MAILING_ONLY"}
            decision, confidence, explanation = _decision(
                exact_name=exact_name,
                exact_address=exact_address,
                building_only=building_only,
                common_name=common_name,
                po_box=scope == "PO_BOX",
                deactivated=bool(str(provider_data.get("deactivation_date", "")).strip()),
                has_core_fields=bool(provider_name and address_text),
            )
            if decision == "REJECTED" and not exact_name and scope == "INCOMPLETE":
                continue
            match_type = {
                ("organization_provider", "business"): "NPI_ORGANIZATION_TO_SUNBIZ_BUSINESS",
                ("individual_provider", "officer"): "NPI_PROVIDER_NAME_TO_SUNBIZ_OFFICER",
                ("individual_provider", "registered_agent"): "NPI_AUTHORIZED_OFFICIAL_TO_SUNBIZ_OFFICER",
            }.get((provider_type, sunbiz_type), "NPI_ORGANIZATION_ADDRESS_TO_REGISTERED_AGENT_ADDRESS")
            if rel_type == "PROVIDER_PRACTICES_AT" and sunbiz_type == "address":
                match_type = "NPI_PRACTICE_ADDRESS_TO_SUNBIZ_PRINCIPAL_ADDRESS"
            elif rel_type == "PROVIDER_MAILS_TO" and sunbiz_type == "address":
                match_type = "NPI_MAILING_ADDRESS_TO_SUNBIZ_MAILING_ADDRESS"
            sunbiz_matches.append(
                {
                    "match_id": f"nppes-sunbiz:{npi}:{sunbiz_entity_id}",
                    "match_type": match_type,
                    "decision": decision,
                    "confidence": round(confidence, 4),
                    "npi": npi,
                    "provider_name": provider_name,
                    "provider_type": provider_type,
                    "taxonomy": taxonomy,
                    "enumeration_type": enumeration_type,
                    "sunbiz_corporation_number": str(sunbiz_row.get("corporation_number", "")),
                    "sunbiz_business_name": sunbiz_name if sunbiz_type == "business" else str(sunbiz_row.get("corporation_number", "")),
                    "sunbiz_person_name": sunbiz_name if sunbiz_type in {"officer", "registered_agent"} else "",
                    "sunbiz_person_role": sunbiz_type,
                    "nppes_entity_id": provider_entity_id,
                    "sunbiz_entity_id": sunbiz_entity_id,
                    "address_match_scope": scope,
                    "corroborating_fields": "name|address" if exact_name and scope in {"EXACT_UNIT", "EXACT_BUILDING", "MAILING_ONLY"} else "name" if exact_name else "address",
                    "conflicting_fields": "" if decision != "REJECTED" else "address_or_name",
                    "common_name_downgraded": str(common_name).lower(),
                    "deactivated_status": str(bool(str(provider_data.get("deactivation_date", "")).strip())).lower(),
                    "explanation": explanation,
                    "recommended_review": "Verify provider, Sunbiz, and address evidence side by side before treating this as an investigative lead.",
                    "source_record_ids": f"{npi}|{sunbiz_row.get('source_record_id', '')}",
                    "imported_at": imported_at,
                }
            )
        for _, property_rel in county_property_relationships.iterrows():
            property_type = str(property_rel.get("relationship_type", ""))
            if property_type not in {"PROPERTY_HAS_SITUS_ADDRESS", "PROPERTY_HAS_MAILING_ADDRESS", "PROPERTY_OWNED_BY"}:
                continue
            property_entity = property_lookup.get(str(property_rel.get("source_entity_id", "")), {})
            parcel_id = str(property_entity.get("entity_id", property_rel.get("source_record_id", ""))).replace("property:", "")
            if property_type == "PROPERTY_OWNED_BY":
                owner_name = str(property_lookup.get(str(property_rel.get("target_entity_id", "")), {}).get("display_name", property_rel.get("target_entity_id", "")))
                exact_name = provider_norm and provider_norm == _normalized_name(owner_name, organization=False)
                scope = "INCOMPLETE"
                decision, confidence, explanation = _decision(
                    exact_name=exact_name,
                    exact_address=False,
                    building_only=False,
                    common_name=common_name,
                    po_box=False,
                    deactivated=bool(str(provider_data.get("deactivation_date", "")).strip()),
                    has_core_fields=bool(provider_name),
                )
                if not exact_name:
                    continue
                match_type = "PROVIDER_NAME_TO_PARCEL_OWNER" if provider_type == "individual_provider" else "ORGANIZATION_NAME_TO_PARCEL_OWNER"
                parcel_matches.append(
                    {
                        "match_id": f"nppes-parcel:{npi}:{parcel_id}:{match_type}",
                        "match_type": match_type,
                        "decision": decision,
                        "confidence": round(confidence, 4),
                        "npi": npi,
                        "provider_name": provider_name,
                        "provider_type": provider_type,
                        "taxonomy": taxonomy,
                        "enumeration_type": enumeration_type,
                        "parcel_id": parcel_id,
                        "parcel_owner": owner_name,
                        "parcel_address": "",
                        "nppes_entity_id": provider_entity_id,
                        "parcel_entity_id": str(property_entity.get("entity_id", "")),
                        "address_match_scope": scope,
                        "corroborating_fields": "name",
                        "conflicting_fields": "" if decision != "REJECTED" else "secondary_evidence",
                        "common_name_downgraded": str(common_name).lower(),
                        "deactivated_status": str(bool(str(provider_data.get("deactivation_date", "")).strip())).lower(),
                        "explanation": explanation,
                        "recommended_review": "Confirm that the parcel-owner relationship is supported by additional address evidence before treating it as meaningful.",
                        "source_record_ids": f"{npi}|{property_rel.get('source_record_id', '')}",
                        "imported_at": imported_at,
                    }
                )
                continue
            address_entity = property_lookup.get(str(property_rel.get("target_entity_id", "")), {})
            parcel_address = str(address_entity.get("display_name", ""))
            scope = _address_scope(address_text, parcel_address, mailing=property_type == "PROPERTY_HAS_MAILING_ADDRESS")
            decision, confidence, explanation = _decision(
                exact_name=False,
                exact_address=scope in {"EXACT_UNIT", "EXACT_BUILDING"} and property_type == "PROPERTY_HAS_SITUS_ADDRESS",
                building_only=scope in {"EXACT_BUILDING", "MAILING_ONLY"},
                common_name=common_name,
                po_box=scope == "PO_BOX",
                deactivated=bool(str(provider_data.get("deactivation_date", "")).strip()),
                has_core_fields=bool(address_text and parcel_address),
            )
            if decision == "REJECTED" and scope == "INCOMPLETE":
                continue
            match_type = "PROVIDER_PRACTICE_ADDRESS_TO_PARCEL_SITUS" if property_type == "PROPERTY_HAS_SITUS_ADDRESS" else "PROVIDER_MAILING_ADDRESS_TO_PARCEL_MAILING"
            parcel_matches.append(
                {
                    "match_id": f"nppes-parcel:{npi}:{parcel_id}:{match_type}",
                    "match_type": match_type,
                    "decision": decision,
                    "confidence": round(confidence, 4),
                    "npi": npi,
                    "provider_name": provider_name,
                    "provider_type": provider_type,
                    "taxonomy": taxonomy,
                    "enumeration_type": enumeration_type,
                    "parcel_id": parcel_id,
                    "parcel_owner": "",
                    "parcel_address": parcel_address,
                    "nppes_entity_id": provider_entity_id,
                    "parcel_entity_id": str(property_entity.get("entity_id", "")),
                    "address_match_scope": scope,
                    "corroborating_fields": "address" if scope in {"EXACT_UNIT", "EXACT_BUILDING", "MAILING_ONLY"} else "",
                    "conflicting_fields": "" if decision != "REJECTED" else "address",
                    "common_name_downgraded": str(common_name).lower(),
                    "deactivated_status": str(bool(str(provider_data.get("deactivation_date", "")).strip())).lower(),
                    "explanation": explanation,
                    "recommended_review": "Treat exact practice-to-situs matches as address association evidence, not ownership evidence.",
                    "source_record_ids": f"{npi}|{property_rel.get('source_record_id', '')}",
                    "imported_at": imported_at,
                }
            )

    three_source_paths: list[dict[str, Any]] = []
    accepted_sunbiz = [row for row in sunbiz_matches if row["decision"] in {"ACCEPTED_EXACT", "REVIEW_STRONG"}]
    accepted_parcel = [row for row in parcel_matches if row["decision"] in {"ACCEPTED_EXACT", "REVIEW_STRONG"}]
    for sunbiz_row in accepted_sunbiz:
        for parcel_row in accepted_parcel:
            if sunbiz_row["npi"] != parcel_row["npi"]:
                continue
            if not (sunbiz_row["address_match_scope"] in {"EXACT_UNIT", "EXACT_BUILDING", "MAILING_ONLY"} and parcel_row["address_match_scope"] in {"EXACT_UNIT", "EXACT_BUILDING", "MAILING_ONLY"}):
                continue
            confidence = round(min(float(sunbiz_row["confidence"]), float(parcel_row["confidence"])), 4)
            decision = "ACCEPTED_EXACT" if confidence >= 0.9 else "REVIEW_STRONG"
            three_source_paths.append(
                {
                    "path_id": f"nppes-path:{sunbiz_row['npi']}:{sunbiz_row['sunbiz_entity_id']}:{parcel_row['parcel_entity_id']}",
                    "npi": sunbiz_row["npi"],
                    "provider_name": sunbiz_row["provider_name"],
                    "provider_type": sunbiz_row["provider_type"],
                    "taxonomy": sunbiz_row["taxonomy"],
                    "sunbiz_corporation_number": sunbiz_row["sunbiz_corporation_number"],
                    "sunbiz_business_name": sunbiz_row["sunbiz_business_name"],
                    "sunbiz_person_or_role": sunbiz_row["sunbiz_person_name"] or sunbiz_row["sunbiz_person_role"],
                    "parcel_id": parcel_row["parcel_id"],
                    "parcel_owner": parcel_row["parcel_owner"],
                    "nppes_entity_id": sunbiz_row["nppes_entity_id"],
                    "sunbiz_entity_id": sunbiz_row["sunbiz_entity_id"],
                    "parcel_entity_id": parcel_row["parcel_entity_id"],
                    "enumeration_type": sunbiz_row.get("enumeration_type", ""),
                    "edge_sequence": "NPPES->SUNBIZ|NPPES->PARCEL",
                    "sources": "nppes_npi|sunbiz_daily|county_property_local_file",
                    "source_record_ids": f"{sunbiz_row['source_record_ids']}|{parcel_row['source_record_ids']}",
                    "corroborating_fields": "address|source_triage",
                    "conflicting_fields": "",
                    "decision": decision,
                    "confidence": confidence,
                    "explanation": "Three-source path built from NPPES-to-Sunbiz and NPPES-to-parcel evidence with supported address context.",
                    "recommended_review": "Verify the provider, business, and parcel records together. Treat the path as a lead only.",
                    "imported_at": imported_at,
                }
            )

    _write_csv(processed_path / "nppes_sunbiz_matches.csv", sunbiz_matches, OUTPUT_COLUMNS["nppes_sunbiz_matches"])
    _write_csv(processed_path / "nppes_parcel_matches.csv", parcel_matches, OUTPUT_COLUMNS["nppes_parcel_matches"])
    _write_csv(processed_path / "nppes_sunbiz_parcel_paths.csv", three_source_paths, OUTPUT_COLUMNS["nppes_sunbiz_parcel_paths"])

    if append_to_cross_source:
        cross_source_path = processed_path / "cross_source_matches.csv"
        cross_source = _load(cross_source_path)
        appended_rows = []
        for row in [*sunbiz_matches, *parcel_matches]:
            appended_rows.append(
                {
                    "cross_source_match_id": row["match_id"],
                    "canonical_entity_id": row["nppes_entity_id"],
                    "entity_type": row["provider_type"],
                    "left_entity_id": row["nppes_entity_id"],
                    "right_entity_id": row.get("sunbiz_entity_id") or row.get("parcel_entity_id"),
                    "left_source_name": "nppes_npi",
                    "right_source_name": "sunbiz_daily" if row.get("sunbiz_entity_id") else "county_property_local_file",
                    "left_source_type": "api",
                    "right_source_type": "api" if row.get("sunbiz_entity_id") else "connector",
                    "left_source_record_id": row["npi"],
                    "right_source_record_id": row.get("sunbiz_corporation_number") or row.get("parcel_id"),
                    "source_pair": "nppes_npi|sunbiz_daily" if row.get("sunbiz_entity_id") else "county_property_local_file|nppes_npi",
                    "match_method": row["match_type"],
                    "confidence": row["confidence"],
                    "evidence": row["explanation"],
                    "decision": row["decision"],
                    "independent_real_source_count": 2,
                    "contains_real_data": True,
                    "contains_synthetic_data": False,
                    "why_sources_independent": "Different source_name values",
                    "sunbiz_corporation_number": row.get("sunbiz_corporation_number", ""),
                    "sunbiz_business_name": row.get("sunbiz_business_name", ""),
                    "parcel_id": row.get("parcel_id", ""),
                    "npi": row["npi"],
                    "provider_name": row["provider_name"],
                    "provider_type": row["provider_type"],
                    "taxonomy": row["taxonomy"],
                    "enumeration_type": row.get("enumeration_type", ""),
                    "address_match_scope": row["address_match_scope"],
                    "deactivated_status": row["deactivated_status"],
                    "three_source_only": False,
                }
            )
        for row in three_source_paths:
            appended_rows.append(
                {
                    "cross_source_match_id": row["path_id"],
                    "canonical_entity_id": row["nppes_entity_id"],
                    "entity_type": row["provider_type"],
                    "left_entity_id": row["nppes_entity_id"],
                    "right_entity_id": row["parcel_entity_id"],
                    "left_source_name": "nppes_npi",
                    "right_source_name": "sunbiz_daily|county_property_local_file",
                    "left_source_type": "api",
                    "right_source_type": "api|connector",
                    "left_source_record_id": row["npi"],
                    "right_source_record_id": row["parcel_id"],
                    "source_pair": "county_property_local_file|nppes_npi|sunbiz_daily",
                    "match_method": "NPPES_SUNBIZ_PARCEL_PATH",
                    "confidence": row["confidence"],
                    "evidence": row["explanation"],
                    "decision": row["decision"],
                    "independent_real_source_count": 3,
                    "contains_real_data": True,
                    "contains_synthetic_data": False,
                    "why_sources_independent": "Three distinct public sources",
                    "sunbiz_corporation_number": row["sunbiz_corporation_number"],
                    "sunbiz_business_name": row["sunbiz_business_name"],
                    "parcel_id": row["parcel_id"],
                    "npi": row["npi"],
                    "provider_name": row["provider_name"],
                    "provider_type": row["provider_type"],
                    "taxonomy": row["taxonomy"],
                    "enumeration_type": row.get("enumeration_type", ""),
                    "address_match_scope": "EXACT_BUILDING",
                    "deactivated_status": "false",
                    "three_source_only": True,
                }
            )
        combined = pd.concat([cross_source, pd.DataFrame(appended_rows)], ignore_index=True, sort=False) if not cross_source.empty else pd.DataFrame(appended_rows)
        combined = combined.drop_duplicates(subset=["cross_source_match_id"]).reset_index(drop=True)
        combined.to_csv(cross_source_path, index=False)

    quality_counts = {}
    all_match_rows = [*sunbiz_matches, *parcel_matches, *three_source_paths]
    for key in ["ACCEPTED_EXACT", "REVIEW_STRONG", "REVIEW_WEAK", "REJECTED", "INSUFFICIENT_DATA"]:
        quality_counts[key] = sum(1 for row in all_match_rows if row.get("decision") == key)
    report = {
        "providers_processed": int(len(nppes_providers)),
        "individual_providers": int((nppes_providers.get("entity_type_code", pd.Series(dtype=str)).astype(str) == "1").sum()) if not nppes_providers.empty else 0,
        "organization_providers": int((nppes_providers.get("entity_type_code", pd.Series(dtype=str)).astype(str) == "2").sum()) if not nppes_providers.empty else 0,
        "exact_npi_identities": int(nppes_providers.get("npi", pd.Series(dtype=str)).astype(str).nunique()) if not nppes_providers.empty else 0,
        "npi_to_sunbiz_candidates": len(sunbiz_matches),
        "npi_to_parcel_candidates": len(parcel_matches),
        "three_source_paths": len(three_source_paths),
        "accepted_exact": quality_counts["ACCEPTED_EXACT"],
        "strong_review": quality_counts["REVIEW_STRONG"],
        "weak_review": quality_counts["REVIEW_WEAK"],
        "rejected": quality_counts["REJECTED"],
        "insufficient_data": quality_counts["INSUFFICIENT_DATA"],
        "name_only_matches_suppressed": sum(1 for row in all_match_rows if row.get("decision") == "REVIEW_WEAK"),
        "organization_name_only_matches_suppressed": sum(1 for row in sunbiz_matches if row.get("provider_type") == "organization_provider" and row.get("decision") == "REVIEW_WEAK"),
        "po_box_physical_matches_suppressed": sum(1 for row in all_match_rows if row.get("address_match_scope") == "PO_BOX"),
        "unit_conflicts": sum(1 for row in all_match_rows if row.get("address_match_scope") == "GENERALIZED"),
        "common_name_downgrades": sum(1 for row in all_match_rows if str(row.get("common_name_downgraded", "")).lower() == "true"),
        "shared_medical_building_suppressions": 0,
        "deactivated_records": sum(1 for row in nppes_providers.to_dict("records") if str(row.get("deactivation_date", "")).strip()),
        "duplicate_records_removed": int(len(nppes_providers) - nppes_providers.get("npi", pd.Series(dtype=str)).astype(str).nunique()) if not nppes_providers.empty else 0,
        "source_coverage": ["nppes_npi", "sunbiz_daily", "county_property_local_file"],
        "filters_used": {},
    }
    _write_json(processed_path / "nppes_match_quality_report.json", report)
    samples = []
    for decision in ["ACCEPTED_EXACT", "REVIEW_STRONG", "REVIEW_WEAK", "REJECTED", "INSUFFICIENT_DATA"]:
        samples.extend([row for row in all_match_rows if row.get("decision") == decision][:5])
    sample_columns = sorted({key for row in samples for key in row.keys()} or {"decision"})
    _write_csv(processed_path / "nppes_match_quality_samples.csv", samples, sample_columns)
    return report
