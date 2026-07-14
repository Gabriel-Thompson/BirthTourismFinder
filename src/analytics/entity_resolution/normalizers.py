from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Dict
from urllib.parse import urlparse

from src.analytics.entity_resolution.models import NormalizedEntity

LEGAL_SUFFIXES = {
    "LLC": "LLC",
    "L L C": "LLC",
    "INC": "INC",
    "INCORPORATED": "INC",
    "CORP": "CORP",
    "CORPORATION": "CORP",
    "LTD": "LTD",
    "LIMITED": "LTD",
    "LP": "LP",
    "L P": "LP",
    "CO": "CO",
    "COMPANY": "CO",
}
STREET_SUFFIXES = {
    "STREET": "ST",
    "ST": "ST",
    "ROAD": "RD",
    "RD": "RD",
    "AVENUE": "AVE",
    "AVE": "AVE",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "DRIVE": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "CIRCLE": "CIR",
    "CIR": "CIR",
    "HIGHWAY": "HWY",
    "HWY": "HWY",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
}
DIRECTIONALS = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
}
UNIT_PATTERN = re.compile(r"(?:\b(?:APT|APARTMENT|UNIT|STE|SUITE)\b|#)\s*([A-Z0-9\-]+)")
PHONE_EXTENSION_PATTERN = re.compile(r"(?:EXT|X|EXTENSION)\s*([0-9]+)$")
EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$")
PERSON_SUFFIXES = {"JR", "SR", "II", "III", "IV", "V"}
PERSON_CREDENTIALS = {"MD", "DO", "DDS", "DMD", "PA", "NP", "RN", "ESQ", "CPA", "JD"}


def similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _clean_text(value: str) -> str:
    text = (value or "").strip().upper()
    text = re.sub(r"[^\w\s#/@\-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_phone(value: str) -> Dict[str, str]:
    text = (value or "").strip()
    if not text:
        return {"normalized_value": "", "phone_prefix": "", "extension": ""}
    upper = text.upper()
    extension_match = PHONE_EXTENSION_PATTERN.search(upper)
    extension = extension_match.group(1) if extension_match else ""
    if extension_match:
        upper = upper[: extension_match.start()].strip()
    digits = "".join(ch for ch in upper if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return {"normalized_value": "", "phone_prefix": "", "extension": extension}
    normalized = digits if not extension else f"{digits}X{extension}"
    return {"normalized_value": normalized, "phone_prefix": digits[:6], "extension": extension}


def normalize_email(value: str) -> Dict[str, str]:
    normalized = "".join((value or "").split()).lower()
    if not normalized or not EMAIL_PATTERN.match(normalized.upper()):
        return {"normalized_value": "", "domain": ""}
    domain = normalized.split("@", 1)[1]
    return {"normalized_value": normalized, "domain": domain}


def normalize_website(value: str) -> Dict[str, str]:
    text = (value or "").strip().lower()
    if not text:
        return {"normalized_value": "", "domain": ""}
    if "://" not in text:
        text = f"https://{text}"
    parsed = urlparse(text)
    host = (parsed.netloc or parsed.path or "").lower().strip("/")
    path = parsed.path if parsed.netloc else ""
    if host.startswith("www."):
        host = host[4:]
    path = path.rstrip("/")
    normalized = host
    if path and path not in {"", "/"}:
        normalized = f"{host}{path}"
    return {"normalized_value": normalized, "domain": host}


def normalize_business_name(value: str) -> Dict[str, str]:
    text = _clean_text(value).replace("#", " ")
    if not text:
        return {"normalized_value": "", "name_prefix": ""}
    tokens = text.split()
    while tokens:
        suffix = LEGAL_SUFFIXES.get(" ".join(tokens[-3:])) or LEGAL_SUFFIXES.get(" ".join(tokens[-2:])) or LEGAL_SUFFIXES.get(tokens[-1])
        if not suffix:
            break
        if LEGAL_SUFFIXES.get(" ".join(tokens[-3:])):
            tokens = tokens[:-3]
        elif LEGAL_SUFFIXES.get(" ".join(tokens[-2:])):
            tokens = tokens[:-2]
        else:
            tokens = tokens[:-1]
    normalized = " ".join(tokens).strip()
    normalized_compact = re.sub(r"\s+", " ", normalized)
    alias_key = normalized_compact.replace(" AND ", " & ")
    return {"normalized_value": normalized_compact, "name_prefix": normalized_compact[:8], "alias_key": alias_key}


def normalize_person_name(value: str) -> Dict[str, str]:
    text = _clean_text(value)
    if not text:
        return {
            "normalized_value": "",
            "surname_prefix": "",
            "middle_name": "",
            "middle_initial": "",
            "suffix": "",
            "name_confidence": "none",
        }
    tokens = [token for token in text.split() if token]
    suffix = tokens[-1] if tokens and tokens[-1] in PERSON_SUFFIXES else ""
    if suffix:
        tokens = tokens[:-1]
    tokens = [token for token in tokens if token not in PERSON_CREDENTIALS]
    first_name = tokens[0] if tokens else ""
    last_name = tokens[-1] if len(tokens) >= 2 else first_name
    middle_name = tokens[1] if len(tokens) >= 3 else ""
    middle_initial = middle_name[:1] if middle_name else ""
    normalized = " ".join(part for part in [first_name, last_name] if part).strip()
    if not normalized:
        normalized = " ".join(tokens[:2])
    surname_prefix = last_name[:6] if last_name else ""
    confidence = "strong" if first_name and last_name and middle_name else "medium" if first_name and last_name else "weak"
    return {
        "normalized_value": normalized,
        "surname_prefix": surname_prefix,
        "middle_name": middle_name,
        "middle_initial": middle_initial,
        "suffix": suffix,
        "name_confidence": confidence,
    }


def normalize_address_value(value: str) -> Dict[str, str]:
    text = _clean_text(value.replace(".", " ").replace(",", " "))
    if not text:
        return {
            "normalized_value": "",
            "building_key": "",
            "unit_key": "",
            "zip_code": "",
            "address_number": "",
        }
    unit_match = UNIT_PATTERN.search(text)
    unit = unit_match.group(1) if unit_match else ""
    if unit_match:
        text = f"{text[:unit_match.start()]} {text[unit_match.end():]}".strip()
    tokens = [token for token in text.split() if token]
    zip_code = ""
    for token in reversed(tokens):
        digits = "".join(ch for ch in token if ch.isdigit())
        if len(digits) >= 5:
            zip_code = digits[:5]
            break
    address_number = tokens[0] if tokens and tokens[0].isdigit() else ""
    normalized_tokens: list[str] = []
    for token in tokens:
        token = DIRECTIONALS.get(token, token)
        token = STREET_SUFFIXES.get(token, token)
        normalized_tokens.append(token)
    building_key = " ".join(normalized_tokens).strip()
    normalized = building_key
    if unit:
        normalized = f"{building_key} UNIT {unit}"
    street_name = " ".join(normalized_tokens[1:]) if len(normalized_tokens) > 1 else ""
    return {
        "normalized_value": normalized,
        "building_key": building_key,
        "unit_key": unit,
        "zip_code": zip_code,
        "address_number": address_number,
        "street_name": street_name,
    }


def normalize_property_value(value: str, entity_id: str, source_name: str) -> Dict[str, str]:
    raw = (value or "").strip()
    candidate = entity_id.split(":", 1)[1] if ":" in entity_id else raw
    cleaned = re.sub(r"[^A-Z0-9]", "", candidate.upper())
    jurisdiction = (source_name or "").strip().lower()
    parcel_key = cleaned
    return {"normalized_value": parcel_key or _clean_text(raw), "parcel_key": parcel_key, "jurisdiction": jurisdiction}


def normalize_generic_value(value: str) -> Dict[str, str]:
    normalized = _clean_text(value)
    return {"normalized_value": normalized}


def normalize_entity_row(row: Dict[str, str]) -> NormalizedEntity:
    entity_id = str(row.get("entity_id", "")).strip()
    entity_type = str(row.get("entity_type", "")).strip().lower()
    display_name = str(row.get("display_name", "")).strip()
    source = str(row.get("source", "")).strip()
    source_name = str(row.get("source_name", "")).strip()
    source_type = str(row.get("source_type", "")).strip()
    source_record_id = str(row.get("source_record_id", "")).strip() or entity_id
    jurisdiction = str(row.get("jurisdiction", "")).strip()

    normalized: Dict[str, str]
    if entity_type == "phone":
        normalized = normalize_phone(display_name)
    elif entity_type == "email":
        normalized = normalize_email(display_name)
    elif entity_type == "website":
        normalized = normalize_website(display_name)
    elif entity_type == "address":
        normalized = normalize_address_value(display_name)
    elif entity_type == "property":
        normalized = normalize_property_value(display_name, entity_id, source_name)
    elif entity_type in {"business"}:
        normalized = normalize_business_name(display_name)
    elif entity_type == "owner":
        owner_upper = _clean_text(display_name)
        if any(suffix in owner_upper.split() for suffix in {"LLC", "INC", "CORP", "LTD", "LP", "CO"}):
            normalized = normalize_business_name(display_name)
        else:
            normalized = normalize_person_name(display_name)
    elif entity_type in {"person", "registered_agent", "officer"}:
        normalized = normalize_person_name(display_name)
    else:
        normalized = normalize_generic_value(display_name)

    normalized.update(
        {
            "connector_name": str(row.get("connector_name", "")).strip(),
            "import_batch_id": str(row.get("import_batch_id", "")).strip(),
            "imported_at": str(row.get("imported_at", "")).strip(),
            "is_synthetic": str(row.get("is_synthetic", "")).strip(),
        }
    )
    normalized_value = normalized.get("normalized_value", "")
    canonical_basis = normalized.get("parcel_key") or normalized_value or display_name.upper()
    return NormalizedEntity(
        entity_id=entity_id,
        entity_type=entity_type,
        display_name=display_name,
        source=source,
        source_name=source_name,
        source_type=source_type,
        source_record_id=source_record_id,
        normalized_value=normalized_value,
        canonical_basis=canonical_basis,
        match_key=normalized_value,
        building_key=normalized.get("building_key", ""),
        unit_key=normalized.get("unit_key", ""),
        zip_code=normalized.get("zip_code", ""),
        name_prefix=normalized.get("name_prefix", ""),
        surname_prefix=normalized.get("surname_prefix", ""),
        domain=normalized.get("domain", ""),
        phone_prefix=normalized.get("phone_prefix", ""),
        parcel_key=normalized.get("parcel_key", ""),
        jurisdiction=jurisdiction or normalized.get("jurisdiction", ""),
        extra=normalized,
    )
