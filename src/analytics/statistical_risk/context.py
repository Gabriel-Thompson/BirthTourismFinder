from __future__ import annotations

import re
from typing import Any


UNIT_PATTERN = re.compile(r"\b(?:APT|APARTMENT|UNIT|STE|SUITE|#)\s*[A-Z0-9-]+\b", re.IGNORECASE)


def split_building_and_unit(address_text: str) -> tuple[str, str]:
    text = str(address_text or "").strip()
    if not text:
        return "", ""
    matches = list(UNIT_PATTERN.finditer(text))
    if not matches:
        return text, ""
    unit_text = matches[-1].group(0).strip()
    building_text = UNIT_PATTERN.sub("", text).replace(" ,", ",").replace("  ", " ").strip(" ,")
    return building_text.strip(), unit_text


def classify_address_context(
    address_text: str,
    *,
    property_use: str | None = None,
    land_use: str | None = None,
    connected_entity_types: list[str] | None = None,
) -> dict[str, Any]:
    text = str(address_text or "").upper().strip()
    property_hint = f"{property_use or ''} {land_use or ''}".upper().strip()
    building_address, unit_address = split_building_and_unit(text)
    connected_types = {str(value).lower().strip() for value in (connected_entity_types or []) if str(value).strip()}

    if not text:
        return {
            "address_context": "UNKNOWN",
            "base_building_address": "",
            "unit_level_address": "",
            "classification_confidence": 0.0,
        }
    if any(token in text for token in ("PO BOX", "P O BOX", "PMB", "UPS STORE", "MAIL DROP")):
        return {
            "address_context": "VIRTUAL_OFFICE_OR_MAILBOX",
            "base_building_address": building_address or text,
            "unit_level_address": unit_address,
            "classification_confidence": 0.95,
        }
    if any(token in text for token in ("COURTHOUSE", "CITY HALL", "COUNTY", "STATE OF", "DEPARTMENT OF")):
        return {
            "address_context": "GOVERNMENT_FACILITY",
            "base_building_address": building_address or text,
            "unit_level_address": unit_address,
            "classification_confidence": 0.8,
        }
    if "REGISTERED AGENT" in property_hint:
        return {
            "address_context": "REGISTERED_AGENT_SERVICE_ADDRESS",
            "base_building_address": building_address or text,
            "unit_level_address": unit_address,
            "classification_confidence": 0.85,
        }
    if unit_address and any(token in property_hint for token in ("SINGLE FAMILY", "RESIDENTIAL")):
        return {
            "address_context": "EXACT_APARTMENT_OR_UNIT",
            "base_building_address": building_address or text,
            "unit_level_address": unit_address,
            "classification_confidence": 0.9,
        }
    if unit_address:
        return {
            "address_context": "EXACT_APARTMENT_OR_UNIT",
            "base_building_address": building_address or text,
            "unit_level_address": unit_address,
            "classification_confidence": 0.75,
        }
    if any(token in property_hint for token in ("MULTIFAMILY", "APARTMENT")) or "address" in connected_types and "property" in connected_types:
        if "APARTMENT" in text or "APTS" in text:
            return {
                "address_context": "APARTMENT_BUILDING",
                "base_building_address": building_address or text,
                "unit_level_address": "",
                "classification_confidence": 0.8,
            }
    if any(token in property_hint for token in ("COMMERCIAL", "OFFICE", "RETAIL")) or any(token in text for token in ("PLAZA", "CENTER", "BLVD", "FLOOR", "TOWER")):
        return {
            "address_context": "COMMERCIAL_PROPERTY",
            "base_building_address": building_address or text,
            "unit_level_address": "",
            "classification_confidence": 0.7,
        }
    if any(token in property_hint for token in ("SINGLE FAMILY", "RESIDENTIAL")):
        return {
            "address_context": "SINGLE_FAMILY_RESIDENTIAL",
            "base_building_address": building_address or text,
            "unit_level_address": "",
            "classification_confidence": 0.8,
        }
    return {
        "address_context": "UNKNOWN",
        "base_building_address": building_address or text,
        "unit_level_address": unit_address,
        "classification_confidence": 0.35,
    }
