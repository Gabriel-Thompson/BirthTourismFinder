from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable


def resolve_fieldnames(rows: list[dict[str, Any]], preferred_fieldnames: Iterable[str] | None = None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    if preferred_fieldnames:
        for field in preferred_fieldnames:
            if field not in seen:
                seen.add(field)
                ordered.append(field)
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                ordered.append(key)
    return ordered


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: Iterable[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    resolved = resolve_fieldnames(rows, fieldnames)
    with path.open("w", encoding="utf-8", newline="") as handle:
        if not resolved:
            handle.write("placeholder\n")
            return
        writer = csv.DictWriter(handle, fieldnames=resolved, extrasaction="ignore")
        writer.writeheader()
        if rows:
            writer.writerows(rows)
