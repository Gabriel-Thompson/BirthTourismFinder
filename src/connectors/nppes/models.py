from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class NPPESAddress:
    address_purpose: str
    address_1: str
    address_2: str = ""
    city: str = ""
    state: str = ""
    postal_code: str = ""
    country_code: str = "US"
    telephone_number: str = ""
    fax_number: str = ""
    normalized_full_address: str = ""
    building_key: str = ""
    unit_key: str = ""
    zip5: str = ""
    po_box_flag: bool = False
    completeness: str = ""
    address_match_scope: str = ""
    address_type: str = ""


@dataclass
class NPPESTaxonomy:
    code: str
    description: str = ""
    primary: bool = False
    license_number: str = ""
    license_state: str = ""
    taxonomy_group: str = ""


@dataclass
class NPPESProvider:
    npi: str
    enumeration_type: str
    entity_type_code: str
    provider_name: str
    organization_name: str = ""
    first_name: str = ""
    middle_name: str = ""
    last_name: str = ""
    credential: str = ""
    gender: str = ""
    enumeration_date: str = ""
    last_update_date: str = ""
    deactivation_date: str = ""
    reactivation_date: str = ""
    replacement_npi: str = ""
    sole_proprietor_indicator: str = ""
    organization_subpart_indicator: str = ""
    parent_organization_name: str = ""
    authorized_official_name: str = ""
    authorized_official_title: str = ""
    authorized_official_first_name: str = ""
    authorized_official_last_name: str = ""
    other_organization_names: list[str] = field(default_factory=list)
    practice_addresses: list[NPPESAddress] = field(default_factory=list)
    mailing_addresses: list[NPPESAddress] = field(default_factory=list)
    taxonomies: list[NPPESTaxonomy] = field(default_factory=list)
    source_record_id: str = ""
    source_url: str = ""
    source_file: str = ""
    imported_at: str = ""
    import_batch_id: str = ""
    source_name: str = "nppes_npi"
    source_mode: str = "api"
    active_flag: bool = True
    incomplete_record: bool = False
    raw_payload_hash: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)
