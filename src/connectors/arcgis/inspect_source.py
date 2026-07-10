from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.arcgis.arcgis_connector import API_CONFIG_PATH, ArcGISRESTConnector

DEFAULT_OUTPUT_DIR = Path("data/processed")


def build_inspection_report(source_name: str, limit: int, config_path: Path | str | None = None) -> Dict[str, Any]:
    connector = ArcGISRESTConnector(source_name=source_name, config_path=config_path or API_CONFIG_PATH, limit=limit)
    metadata = connector.fetch_metadata()
    sample_payload = connector.fetch()
    sample_rows = connector.parse(sample_payload)
    fields = metadata.get("fields", [])
    field_rows: List[Dict[str, Any]] = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        field_rows.append(
            {
                "name": field.get("name", ""),
                "alias": field.get("alias", ""),
                "type": field.get("type", ""),
            }
        )
    return {
        "source_name": source_name,
        "limit": limit,
        "base_url": connector.base_url,
        "endpoint": connector.endpoint,
        "field_map": connector.source_config.get("field_map", {}),
        "fields": field_rows,
        "sample_rows": sample_rows[:limit],
    }


def save_inspection_report(report: Dict[str, Any], output_dir: Path | str = DEFAULT_OUTPUT_DIR) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / f"arcgis_inspection_{report['source_name']}.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def print_inspection_report(report: Dict[str, Any]) -> None:
    print(f"ArcGIS Source Inspection: {report['source_name']}")
    print("Available fields:")
    for field in report["fields"]:
        print(f"- {field['name']} | alias={field['alias']} | type={field['type']}")
    print("")
    print("Sample rows:")
    for index, row in enumerate(report["sample_rows"], start=1):
        print(f"[{index}] {json.dumps(row, indent=2)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect an ArcGIS source and print field metadata with sample rows.")
    parser.add_argument("--source", required=True, help="ArcGIS source name from config/api_sources.json")
    parser.add_argument("--limit", type=int, default=5, help="Number of sample rows to inspect")
    args = parser.parse_args()

    report = build_inspection_report(source_name=args.source, limit=args.limit)
    output_path = save_inspection_report(report)
    print_inspection_report(report)
    print("")
    print(f"Inspection saved to {output_path}")


if __name__ == "__main__":
    main()
