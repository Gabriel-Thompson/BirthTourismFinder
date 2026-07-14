from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from src.analytics.entity_resolution.normalizers import normalize_address_value, normalize_business_name, normalize_person_name
from src.connectors.source_metadata import apply_provenance
from src.normalize.address_normalizer import normalize_address

from .models import NPPESAddress, NPPESProvider, NPPESTaxonomy

SOURCE_NAME = "nppes_npi"


def load_nppes_config(config_path: Path | str | None = None) -> dict[str, Any]:
    path = Path(config_path or "config/nppes_npi.json")
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[3] / path
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def build_import_batch_id(mode: str, filters: dict[str, Any], *, imported_at: str | None = None) -> str:
    timestamp = imported_at or utc_now()
    material = json.dumps({"mode": mode, "filters": filters, "timestamp": timestamp}, sort_keys=True)
    return f"{SOURCE_NAME}:{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def hash_payload(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _zip5(value: str) -> str:
    digits = "".join(char for char in str(value or "") if char.isdigit())
    return digits[:5]


def classify_address_scope(address_text: str) -> tuple[str, str, str, str, bool, str]:
    normalized_text = normalize_address(address_text)
    parsed = normalize_address_value(address_text)
    unit_key = str(parsed.get("unit_key", "")).strip()
    building_key = str(parsed.get("building_key", "")).strip()
    po_box_flag = "PO BOX" in normalized_text or "P O BOX" in normalized_text
    generalized = any(token in normalized_text for token in {"GENERAL DELIVERY", "UNKNOWN", "REDACTED", "PRIVATE"})
    incomplete = not parsed.get("address_number") or not building_key
    if po_box_flag:
        scope = "PO_BOX"
    elif generalized:
        scope = "GENERALIZED"
    elif incomplete:
        scope = "INCOMPLETE"
    elif unit_key:
        scope = "EXACT_UNIT"
    else:
        scope = "EXACT_BUILDING"
    completeness = "complete"
    if scope in {"PO_BOX", "GENERALIZED"}:
        completeness = "limited"
    elif scope == "INCOMPLETE":
        completeness = "incomplete"
    return normalized_text, building_key, unit_key, _zip5(parsed.get("zip_code", "")), po_box_flag, completeness


def normalize_address_record(raw: dict[str, Any], *, purpose: str) -> NPPESAddress:
    address_1 = str(raw.get("address_1") or raw.get("line_1") or "").strip()
    address_2 = str(raw.get("address_2") or raw.get("line_2") or "").strip()
    city = str(raw.get("city") or "").strip()
    state = str(raw.get("state") or "").strip()
    postal_code = str(raw.get("postal_code") or raw.get("zip") or "").strip()
    country_code = str(raw.get("country_code") or raw.get("country") or "US").strip()
    full = ", ".join(part for part in [address_1, address_2, city, state, postal_code] if part)
    normalized_full, building_key, unit_key, zip5, po_box_flag, completeness = classify_address_scope(full)
    address_type = "practice" if purpose.upper() == "LOCATION" else "mailing"
    if po_box_flag:
        scope = "PO_BOX"
    elif completeness == "incomplete":
        scope = "INCOMPLETE"
    elif completeness == "limited":
        scope = "GENERALIZED"
    elif unit_key:
        scope = "EXACT_UNIT"
    elif normalized_full:
        scope = "EXACT_BUILDING"
    else:
        scope = "INCOMPLETE"
    if address_type == "mailing" and scope not in {"PO_BOX", "GENERALIZED", "INCOMPLETE"}:
        scope = "MAILING_ONLY"
    return NPPESAddress(
        address_purpose=purpose.upper(),
        address_1=address_1,
        address_2=address_2,
        city=city,
        state=state,
        postal_code=postal_code,
        country_code=country_code,
        telephone_number=str(raw.get("telephone_number") or raw.get("telephone") or "").strip(),
        fax_number=str(raw.get("fax_number") or raw.get("fax") or "").strip(),
        normalized_full_address=normalized_full,
        building_key=building_key,
        unit_key=unit_key,
        zip5=zip5,
        po_box_flag=po_box_flag,
        completeness=completeness,
        address_match_scope=scope,
        address_type=address_type,
    )


def normalize_taxonomy_record(raw: dict[str, Any]) -> NPPESTaxonomy:
    return NPPESTaxonomy(
        code=str(raw.get("code") or raw.get("taxonomy_code") or "").strip(),
        description=str(raw.get("desc") or raw.get("description") or raw.get("taxonomy_description") or "").strip(),
        primary=str(raw.get("primary") or raw.get("primary_switch") or "").strip().upper() in {"Y", "TRUE", "1"},
        license_number=str(raw.get("license") or raw.get("license_number") or "").strip(),
        license_state=str(raw.get("state") or raw.get("license_state") or "").strip(),
        taxonomy_group=str(raw.get("group") or raw.get("taxonomy_group") or "").strip(),
    )


def provider_entity_id(npi: str) -> str:
    return f"provider:nppes:{npi}"


def address_entity_id(npi: str, purpose: str, index: int) -> str:
    return f"address:nppes:{npi}:{purpose.lower()}:{index}"


def taxonomy_entity_id(code: str) -> str:
    return f"taxonomy:nppes:{code}"


def authorized_official_entity_id(npi: str) -> str:
    return f"authorized_official:nppes:{npi}"


def other_org_name_entity_id(npi: str, index: int) -> str:
    return f"other_organization_name:nppes:{npi}:{index}"


def normalize_provider_name(entity_type_code: str, organization_name: str, first_name: str, last_name: str) -> tuple[str, str]:
    if entity_type_code == "2":
        normalized = normalize_business_name(organization_name)
        return organization_name.strip(), normalized.get("normalized_value", "")
    display = " ".join(part for part in [first_name.strip(), last_name.strip()] if part).strip()
    normalized = normalize_person_name(display)
    return display, normalized.get("normalized_value", "")


def provider_from_api_record(record: dict[str, Any], *, imported_at: str, import_batch_id: str, source_url: str, source_mode: str) -> NPPESProvider:
    basic = record.get("basic") if isinstance(record.get("basic"), dict) else {}
    entity_type = "1" if str(record.get("enumeration_type", "")).endswith("1") else "2"
    organization_name = str(basic.get("organization_name") or record.get("organization_name") or "").strip()
    first_name = str(basic.get("first_name") or "").strip()
    middle_name = str(basic.get("middle_name") or "").strip()
    last_name = str(basic.get("last_name") or "").strip()
    display_name, _ = normalize_provider_name(entity_type, organization_name, first_name, last_name)
    practice_addresses: list[NPPESAddress] = []
    mailing_addresses: list[NPPESAddress] = []
    for raw_address in record.get("addresses", []) if isinstance(record.get("addresses"), list) else []:
        address = normalize_address_record(raw_address if isinstance(raw_address, dict) else {}, purpose=str(raw_address.get("address_purpose") if isinstance(raw_address, dict) else ""))
        if address.address_purpose == "LOCATION":
            practice_addresses.append(address)
        else:
            mailing_addresses.append(address)
    taxonomies = [
        normalize_taxonomy_record(raw)
        for raw in (record.get("taxonomies", []) if isinstance(record.get("taxonomies"), list) else [])
        if normalize_taxonomy_record(raw).code
    ]
    authorized_first = str(basic.get("authorized_official_first_name") or "").strip()
    authorized_last = str(basic.get("authorized_official_last_name") or "").strip()
    authorized_name = " ".join(part for part in [authorized_first, authorized_last] if part).strip()
    status_code = str(basic.get("status") or "").strip().upper()
    provider = NPPESProvider(
        npi=str(record.get("number") or "").strip(),
        enumeration_type=str(record.get("enumeration_type") or "").strip(),
        entity_type_code=entity_type,
        provider_name=display_name,
        organization_name=organization_name,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
        credential=str(basic.get("credential") or "").strip(),
        gender=str(basic.get("gender") or "").strip(),
        enumeration_date=str(basic.get("enumeration_date") or "").strip(),
        last_update_date=str(basic.get("last_updated") or "").strip(),
        deactivation_date=str(basic.get("deactivation_date") or "").strip(),
        reactivation_date=str(basic.get("reactivation_date") or "").strip(),
        replacement_npi=str(basic.get("replacement_npi") or "").strip(),
        sole_proprietor_indicator=str(basic.get("sole_proprietor") or "").strip(),
        organization_subpart_indicator=str(basic.get("organizational_subpart") or "").strip(),
        parent_organization_name=str(basic.get("parent_organization_lbn") or "").strip(),
        authorized_official_name=authorized_name,
        authorized_official_title=str(basic.get("authorized_official_title_or_position") or "").strip(),
        authorized_official_first_name=authorized_first,
        authorized_official_last_name=authorized_last,
        other_organization_names=[
            str(item.get("organization_name") or "").strip()
            for item in (record.get("other_names", []) if isinstance(record.get("other_names"), list) else [])
            if isinstance(item, dict) and str(item.get("organization_name") or "").strip()
        ],
        practice_addresses=practice_addresses,
        mailing_addresses=mailing_addresses,
        taxonomies=taxonomies,
        source_record_id=str(record.get("number") or "").strip(),
        source_url=source_url,
        imported_at=imported_at,
        import_batch_id=import_batch_id,
        source_mode=source_mode,
        active_flag=status_code != "D",
        incomplete_record=not bool(practice_addresses or mailing_addresses) or not bool(taxonomies),
        raw_payload_hash=hash_payload(record),
        raw_payload=record,
    )
    return provider


def provider_from_bulk_row(row: dict[str, Any], *, imported_at: str, import_batch_id: str, source_file: str) -> NPPESProvider:
    entity_type = str(row.get("Entity Type Code") or "").strip()
    organization_name = str(row.get("Provider Organization Name (Legal Business Name)") or "").strip()
    first_name = str(row.get("Provider First Name") or "").strip()
    middle_name = str(row.get("Provider Middle Name") or "").strip()
    last_name = str(row.get("Provider Last Name (Legal Name)") or "").strip()
    display_name, _ = normalize_provider_name(entity_type, organization_name, first_name, last_name)
    mailing = normalize_address_record(
        {
            "address_1": row.get("Provider Business Mailing Address 1", ""),
            "address_2": row.get("Provider Business Mailing Address 2", ""),
            "city": row.get("Provider Business Mailing Address City Name", ""),
            "state": row.get("Provider Business Mailing Address State Name", ""),
            "postal_code": row.get("Provider Business Mailing Address Postal Code", ""),
        },
        purpose="MAILING",
    )
    practice = normalize_address_record(
        {
            "address_1": row.get("Provider Practice Location Address 1", ""),
            "address_2": row.get("Provider Practice Location Address 2", ""),
            "city": row.get("Provider Practice Location Address City Name", ""),
            "state": row.get("Provider Practice Location Address State Name", ""),
            "postal_code": row.get("Provider Practice Location Address Postal Code", ""),
        },
        purpose="LOCATION",
    )
    taxonomy = normalize_taxonomy_record(
        {
            "taxonomy_code": row.get("Healthcare Provider Taxonomy Code_1", ""),
            "taxonomy_description": row.get("Healthcare Provider Taxonomy Description_1", ""),
            "primary_switch": row.get("Healthcare Provider Primary Taxonomy Switch_1", ""),
            "license_number": row.get("Provider License Number_1", ""),
            "license_state": row.get("Provider License Number State Code_1", ""),
            "taxonomy_group": row.get("Healthcare Provider Taxonomy Group_1", ""),
        }
    )
    authorized_first = str(row.get("Authorized Official First Name") or "").strip()
    authorized_last = str(row.get("Authorized Official Last Name") or "").strip()
    authorized_name = " ".join(part for part in [authorized_first, authorized_last] if part).strip()
    provider = NPPESProvider(
        npi=str(row.get("NPI") or "").strip(),
        enumeration_type=f"NPI-{entity_type}" if entity_type in {"1", "2"} else "",
        entity_type_code=entity_type,
        provider_name=display_name,
        organization_name=organization_name,
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
        credential=str(row.get("Provider Credential Text") or "").strip(),
        gender=str(row.get("Provider Gender Code") or "").strip(),
        enumeration_date=str(row.get("Provider Enumeration Date") or "").strip(),
        last_update_date=str(row.get("Last Update Date") or "").strip(),
        deactivation_date=str(row.get("NPI Deactivation Date") or "").strip(),
        reactivation_date=str(row.get("NPI Reactivation Date") or "").strip(),
        replacement_npi=str(row.get("Replacement NPI") or "").strip(),
        sole_proprietor_indicator=str(row.get("Is Sole Proprietor") or "").strip(),
        organization_subpart_indicator=str(row.get("Is Organization Subpart") or "").strip(),
        parent_organization_name=str(row.get("Parent Organization LBN") or "").strip(),
        authorized_official_name=authorized_name,
        authorized_official_title=str(row.get("Authorized Official Title or Position") or "").strip(),
        authorized_official_first_name=authorized_first,
        authorized_official_last_name=authorized_last,
        other_organization_names=[str(row.get("Other Organization Name") or "").strip()] if str(row.get("Other Organization Name") or "").strip() else [],
        practice_addresses=[practice] if practice.normalized_full_address else [],
        mailing_addresses=[mailing] if mailing.normalized_full_address else [],
        taxonomies=[taxonomy] if taxonomy.code else [],
        source_record_id=str(row.get("NPI") or "").strip(),
        source_file=source_file,
        imported_at=imported_at,
        import_batch_id=import_batch_id,
        source_mode="bulk",
        active_flag=not bool(str(row.get("NPI Deactivation Date") or "").strip()),
        incomplete_record=not bool(practice.normalized_full_address or mailing.normalized_full_address) or not bool(taxonomy.code),
        raw_payload_hash=hash_payload(row),
        raw_payload=row,
    )
    return provider


def providers_to_rows(
    providers: list[NPPESProvider], *, source_type_hint: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    provider_rows: list[dict[str, Any]] = []
    entity_rows: list[dict[str, Any]] = []
    relationship_rows: list[dict[str, Any]] = []
    seen_entities: set[str] = set()
    seen_relationships: set[str] = set()
    for provider in providers:
        normalized_name = normalize_business_name(provider.organization_name).get("normalized_value", "") if provider.entity_type_code == "2" else normalize_person_name(provider.provider_name).get("normalized_value", "")
        provider_rows.append(
            {
                "npi": provider.npi,
                "enumeration_type": provider.enumeration_type,
                "entity_type_code": provider.entity_type_code,
                "provider_name": provider.provider_name,
                "organization_name": provider.organization_name,
                "provider_first_name": provider.first_name,
                "provider_middle_name": provider.middle_name,
                "provider_last_name": provider.last_name,
                "credential": provider.credential,
                "gender": provider.gender,
                "enumeration_date": provider.enumeration_date,
                "last_update_date": provider.last_update_date,
                "deactivation_date": provider.deactivation_date,
                "reactivation_date": provider.reactivation_date,
                "replacement_npi": provider.replacement_npi,
                "sole_proprietor_indicator": provider.sole_proprietor_indicator,
                "organization_subpart_indicator": provider.organization_subpart_indicator,
                "parent_organization_name": provider.parent_organization_name,
                "authorized_official_name": provider.authorized_official_name,
                "authorized_official_title": provider.authorized_official_title,
                "source_record_id": provider.source_record_id,
                "source_url": provider.source_url,
                "source_file": provider.source_file,
                "imported_at": provider.imported_at,
                "import_batch_id": provider.import_batch_id,
                "source_name": SOURCE_NAME,
                "source_mode": provider.source_mode,
                "active_flag": str(provider.active_flag).lower(),
                "incomplete_record": str(provider.incomplete_record).lower(),
            }
        )
        provider_entity_type = "organization_provider" if provider.entity_type_code == "2" else "individual_provider"
        provider_entity = apply_provenance(
            {
                "entity_id": provider_entity_id(provider.npi),
                "display_name": provider.organization_name or provider.provider_name or provider.npi,
                "entity_type": provider_entity_type,
                "source": SOURCE_NAME,
                "source_url": provider.source_url or provider.source_file,
                "normalized_name": normalized_name,
                "original_name": provider.organization_name or provider.provider_name,
                "npi": provider.npi,
                "enumeration_type": provider.enumeration_type,
                "entity_type_code": provider.entity_type_code,
                "credential": provider.credential,
                "gender": provider.gender,
                "deactivation_date": provider.deactivation_date,
                "active_flag": str(provider.active_flag).lower(),
                "incomplete_record": str(provider.incomplete_record).lower(),
                "source_mode": provider.source_mode,
            },
            SOURCE_NAME,
            source_type_hint=source_type_hint,
            source_record_id=provider.source_record_id,
            connector_name=SOURCE_NAME,
            imported_at=provider.imported_at,
            jurisdiction="FL",
        )
        if provider_entity["entity_id"] not in seen_entities:
            seen_entities.add(provider_entity["entity_id"])
            entity_rows.append(provider_entity)
        for collection_name, addresses in [("practice", provider.practice_addresses), ("mailing", provider.mailing_addresses)]:
            for index, address in enumerate(addresses, start=1):
                entity = apply_provenance(
                    {
                        "entity_id": address_entity_id(provider.npi, collection_name, index),
                        "display_name": address.normalized_full_address,
                        "entity_type": "address",
                        "source": SOURCE_NAME,
                        "source_url": provider.source_url or provider.source_file,
                        "address_1": address.address_1,
                        "address_2": address.address_2,
                        "city": address.city,
                        "state": address.state,
                        "zip": address.postal_code,
                        "country": address.country_code,
                        "address_role": address.address_type,
                        "address_purpose": address.address_purpose,
                        "normalized_full_address": address.normalized_full_address,
                        "building_key": address.building_key,
                        "unit_key": address.unit_key,
                        "postal_code": address.postal_code,
                        "zip5": address.zip5,
                        "po_box_flag": str(address.po_box_flag).lower(),
                        "address_match_scope": address.address_match_scope,
                        "address_completeness": address.completeness,
                        "telephone": address.telephone_number,
                        "fax": address.fax_number,
                        "npi": provider.npi,
                        "incomplete_record": str(provider.incomplete_record).lower(),
                    },
                    SOURCE_NAME,
                    source_type_hint=source_type_hint,
                    source_record_id=f"{provider.npi}:{collection_name}:{index}",
                    connector_name=SOURCE_NAME,
                    imported_at=provider.imported_at,
                    jurisdiction=address.state or "FL",
                )
                if entity["entity_id"] not in seen_entities:
                    seen_entities.add(entity["entity_id"])
                    entity_rows.append(entity)
                relationship_type = "PROVIDER_PRACTICES_AT" if collection_name == "practice" else "PROVIDER_MAILS_TO"
                relationship = apply_provenance(
                    {
                        "relationship_id": f"{SOURCE_NAME}:{provider.npi}:{relationship_type}:{provider_entity['entity_id']}:{entity['entity_id']}",
                        "source_entity_id": provider_entity["entity_id"],
                        "target_entity_id": entity["entity_id"],
                        "relationship_type": relationship_type,
                        "confidence": 1.0,
                        "relationship_method": f"nppes_{collection_name}_address",
                        "evidence_summary": f"NPPES {collection_name} address reported for NPI {provider.npi}.",
                        "npi": provider.npi,
                    },
                    SOURCE_NAME,
                    source_type_hint=source_type_hint,
                    source_record_id=f"{provider.npi}:{collection_name}:{index}",
                    connector_name=SOURCE_NAME,
                    imported_at=provider.imported_at,
                    jurisdiction=address.state or "FL",
                )
                if relationship["relationship_id"] not in seen_relationships:
                    seen_relationships.add(relationship["relationship_id"])
                    relationship_rows.append(relationship)
        for taxonomy in provider.taxonomies:
            taxonomy_entity = apply_provenance(
                {
                    "entity_id": taxonomy_entity_id(taxonomy.code),
                    "display_name": taxonomy.description or taxonomy.code,
                    "entity_type": "taxonomy",
                    "source": SOURCE_NAME,
                    "taxonomy_code": taxonomy.code,
                    "taxonomy_description": taxonomy.description,
                    "license_number": taxonomy.license_number,
                    "license_state": taxonomy.license_state,
                    "primary_taxonomy_indicator": str(taxonomy.primary).lower(),
                    "taxonomy_group": taxonomy.taxonomy_group,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:taxonomy:{taxonomy.code}",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction=taxonomy.license_state or "FL",
            )
            if taxonomy_entity["entity_id"] not in seen_entities:
                seen_entities.add(taxonomy_entity["entity_id"])
                entity_rows.append(taxonomy_entity)
            for relationship_type, target_id, evidence in [
                ("PROVIDER_HAS_TAXONOMY", taxonomy_entity["entity_id"], f"NPPES taxonomy {taxonomy.code} reported for NPI {provider.npi}."),
                ("PROVIDER_LICENSE_REPORTED_IN_STATE", f"state:{taxonomy.license_state}" if taxonomy.license_state else "", f"NPPES reported license state {taxonomy.license_state} for taxonomy {taxonomy.code}."),
            ]:
                if not target_id:
                    continue
                relationship = apply_provenance(
                    {
                        "relationship_id": f"{SOURCE_NAME}:{provider.npi}:{relationship_type}:{provider_entity['entity_id']}:{target_id}",
                        "source_entity_id": provider_entity["entity_id"],
                        "target_entity_id": target_id,
                        "relationship_type": relationship_type,
                        "confidence": 1.0,
                        "relationship_method": "nppes_taxonomy",
                        "evidence_summary": evidence,
                        "npi": provider.npi,
                    },
                    SOURCE_NAME,
                    source_type_hint=source_type_hint,
                    source_record_id=f"{provider.npi}:taxonomy:{taxonomy.code}",
                    connector_name=SOURCE_NAME,
                    imported_at=provider.imported_at,
                    jurisdiction=taxonomy.license_state or "FL",
                )
                if relationship["relationship_id"] not in seen_relationships:
                    seen_relationships.add(relationship["relationship_id"])
                    relationship_rows.append(relationship)
        if provider.authorized_official_name:
            official_entity = apply_provenance(
                {
                    "entity_id": authorized_official_entity_id(provider.npi),
                    "display_name": provider.authorized_official_name,
                    "entity_type": "authorized_official",
                    "source": SOURCE_NAME,
                    "normalized_name": normalize_person_name(provider.authorized_official_name).get("normalized_value", ""),
                    "original_name": provider.authorized_official_name,
                    "title": provider.authorized_official_title,
                    "npi": provider.npi,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:authorized_official",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction="FL",
            )
            if official_entity["entity_id"] not in seen_entities:
                seen_entities.add(official_entity["entity_id"])
                entity_rows.append(official_entity)
            relationship = apply_provenance(
                {
                    "relationship_id": f"{SOURCE_NAME}:{provider.npi}:PROVIDER_REPORTS_AUTHORIZED_OFFICIAL:{provider_entity['entity_id']}:{official_entity['entity_id']}",
                    "source_entity_id": provider_entity["entity_id"],
                    "target_entity_id": official_entity["entity_id"],
                    "relationship_type": "PROVIDER_REPORTS_AUTHORIZED_OFFICIAL",
                    "confidence": 1.0,
                    "relationship_method": "nppes_authorized_official",
                    "evidence_summary": f"NPPES reports authorized official {provider.authorized_official_name} for NPI {provider.npi}.",
                    "npi": provider.npi,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:authorized_official",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction="FL",
            )
            if relationship["relationship_id"] not in seen_relationships:
                seen_relationships.add(relationship["relationship_id"])
                relationship_rows.append(relationship)
        for index, other_name in enumerate(provider.other_organization_names, start=1):
            other_entity = apply_provenance(
                {
                    "entity_id": other_org_name_entity_id(provider.npi, index),
                    "display_name": other_name,
                    "entity_type": "other_organization_name",
                    "source": SOURCE_NAME,
                    "normalized_name": normalize_business_name(other_name).get("normalized_value", ""),
                    "original_name": other_name,
                    "npi": provider.npi,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:other_name:{index}",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction="FL",
            )
            if other_entity["entity_id"] not in seen_entities:
                seen_entities.add(other_entity["entity_id"])
                entity_rows.append(other_entity)
            relationship = apply_provenance(
                {
                    "relationship_id": f"{SOURCE_NAME}:{provider.npi}:ORGANIZATION_HAS_OTHER_NAME:{provider_entity['entity_id']}:{other_entity['entity_id']}",
                    "source_entity_id": provider_entity["entity_id"],
                    "target_entity_id": other_entity["entity_id"],
                    "relationship_type": "ORGANIZATION_HAS_OTHER_NAME",
                    "confidence": 1.0,
                    "relationship_method": "nppes_other_name",
                    "evidence_summary": f"NPPES reports additional organization name {other_name} for NPI {provider.npi}.",
                    "npi": provider.npi,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:other_name:{index}",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction="FL",
            )
            if relationship["relationship_id"] not in seen_relationships:
                seen_relationships.add(relationship["relationship_id"])
                relationship_rows.append(relationship)
        if provider.parent_organization_name:
            target_id = f"business:{provider.parent_organization_name}"
            relationship = apply_provenance(
                {
                    "relationship_id": f"{SOURCE_NAME}:{provider.npi}:PROVIDER_REPORTS_PARENT_ORGANIZATION:{provider_entity['entity_id']}:{target_id}",
                    "source_entity_id": provider_entity["entity_id"],
                    "target_entity_id": target_id,
                    "relationship_type": "PROVIDER_REPORTS_PARENT_ORGANIZATION",
                    "confidence": 1.0,
                    "relationship_method": "nppes_parent_organization",
                    "evidence_summary": f"NPPES reports parent organization {provider.parent_organization_name} for NPI {provider.npi}.",
                    "npi": provider.npi,
                },
                SOURCE_NAME,
                source_type_hint=source_type_hint,
                source_record_id=f"{provider.npi}:parent_organization",
                connector_name=SOURCE_NAME,
                imported_at=provider.imported_at,
                jurisdiction="FL",
            )
            if relationship["relationship_id"] not in seen_relationships:
                seen_relationships.add(relationship["relationship_id"])
                relationship_rows.append(relationship)
    tax_rows = [
        {
            "npi": provider.npi,
            "taxonomy_code": taxonomy.code,
            "taxonomy_description": taxonomy.description,
            "primary_indicator": str(taxonomy.primary).lower(),
            "license_number": taxonomy.license_number,
            "license_state": taxonomy.license_state,
            "taxonomy_group": taxonomy.taxonomy_group,
            "source_name": SOURCE_NAME,
            "imported_at": provider.imported_at,
            "import_batch_id": provider.import_batch_id,
        }
        for provider in providers
        for taxonomy in provider.taxonomies
    ]
    return provider_rows, entity_rows, relationship_rows, tax_rows
