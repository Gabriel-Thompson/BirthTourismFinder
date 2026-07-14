from __future__ import annotations

import re

STREET_SUFFIXES = {
    "STREET": "ST",
    "ROAD": "RD",
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "DRIVE": "DR",
    "LANE": "LN",
    "COURT": "CT",
    "PLACE": "PL",
    "TERRACE": "TER",
    "CIRCLE": "CIR",
    "PARKWAY": "PKWY",
    "HIGHWAY": "HWY",
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
UNIT_MARKERS = {
    "APARTMENT": "APT",
    "APT": "APT",
    "SUITE": "STE",
    "STE": "STE",
    "UNIT": "UNIT",
}


def normalize_address(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    text = text.upper()
    text = text.replace(".", " ")
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\bP\s*O\s+BOX\b", "PO BOX", text)
    text = re.sub(r"\bRURAL ROUTE\b", "RR", text)
    text = re.sub(r"\bCOUNTY ROAD\b", "CR", text)
    text = re.sub(r"\bSTATE ROAD\b", "SR", text)
    text = re.sub(r"\b([A-Z]+)\s+HIGHWAY\b", r"\1 HWY", text)
    text = re.sub(r"\b#\s*([A-Z0-9\-]+)\b", r" UNIT \1", text)
    text = re.sub(r"[^A-Z0-9,\-#/ ]", " ", text)
    tokens = []
    for token in re.split(r"(\s+|, )", text):
        stripped = token.strip()
        if token in {" ", ", "}:
            tokens.append(token)
            continue
        if not stripped:
            continue
        mapped = DIRECTIONALS.get(stripped, stripped)
        mapped = STREET_SUFFIXES.get(mapped, mapped)
        mapped = UNIT_MARKERS.get(mapped, mapped)
        tokens.append(mapped)
    text = "".join(tokens)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\b(\d{5})-\d{4}\b", r"\1", text)
    return text.strip(" ,")
