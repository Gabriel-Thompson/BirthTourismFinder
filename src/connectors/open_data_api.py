from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.api_base import APIConnectorBase
from src.connectors.source_metadata import apply_provenance, infer_source_metadata
from src.connectors.source_manifest import validate_source
from src.normalize.address_normalizer import normalize_address

API_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "api_sources.json"
DEFAULT_ENTITIES_PATH = Path("data/processed/api_entities.csv")
DEFAULT_RELATIONSHIPS_PATH = Path("data/processed/api_relationships.csv")


def load_api_sources(config_path: Path | str = API_CONFIG_PATH) -> Dict[str, Dict[str, Any]]:
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("API source config must be a JSON object keyed by source name.")
    return data


class OpenDataAPIConnector(APIConnectorBase):
    """Connector for approved official API or open-data endpoints."""

    def __init__(self, source_name: str, config_path: Path | str = API_CONFIG_PATH) -> None:
        self.config_path = Path(config_path)
        self.source_name = source_name
        self.source_config = self._load_source_config(source_name)
        validate_source(source_name, require_live_access=True)
        self.base_url = str(self.source_config["base_url"])
        self.endpoint = str(self.source_config["endpoint"])
        self.query_params = dict(self.source_config.get("query_params", {}))
        self.timeout_seconds = float(self.source_config.get("timeout_seconds", 10))
        self.retry_attempts = int(self.source_config.get("retry_attempts", 2))
        self.retry_backoff_seconds = float(self.source_config.get("retry_backoff_seconds", 0.5))
        self.rate_limit_per_minute = int(self.source_config.get("rate_limit_per_minute", 60))
        self.response_format = str(self.source_config.get("response_format", "json")).lower()
        self.mock_response_path = self.source_config.get("mock_response_path")

    def _load_source_config(self, source_name: str) -> Dict[str, Any]:
        sources = load_api_sources(self.config_path)
        if source_name not in sources:
            raise ValueError(f"Unknown API source '{source_name}' in API source config.")
        return sources[source_name]

    def fetch(self) -> str:
        if self.mock_response_path:
            mock_path = Path(self.mock_response_path)
            if not mock_path.is_absolute():
                mock_path = self.config_path.parent.parent / mock_path
            with mock_path.open("r", encoding="utf-8") as handle:
                return handle.read()

        request_url = self._build_request_url()
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                with urllib.request.urlopen(request_url, timeout=self.timeout_seconds) as response:
                    payload = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    return payload.decode(encoding)
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                if self.rate_limit_per_minute > 0:
                    minimum_delay = 60.0 / self.rate_limit_per_minute
                else:
                    minimum_delay = 0.0
                time.sleep(max(self.retry_backoff_seconds, minimum_delay))
        raise RuntimeError(f"API fetch failed for source '{self.source_name}': {last_error}")

    def _build_request_url(self) -> str:
        base = self.base_url.rstrip("/")
        endpoint = self.endpoint.lstrip("/")
        url = f"{base}/{endpoint}" if endpoint else base
        if self.query_params:
            return f"{url}?{urllib.parse.urlencode(self.query_params, doseq=True)}"
        return url

    def parse(self, payload: str) -> List[Dict[str, Any]]:
        if self.response_format == "json":
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                records = parsed.get("results", parsed.get("data", []))
            else:
                records = parsed
            if not isinstance(records, list):
                raise ValueError(f"JSON response for source '{self.source_name}' must contain a list of records.")
            return [record for record in records if isinstance(record, dict)]
        if self.response_format == "csv":
            reader = csv.DictReader(io.StringIO(payload))
            return [dict(row) for row in reader]
        raise ValueError(f"Unsupported response_format '{self.response_format}' for source '{self.source_name}'.")

    def normalize(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        field_map = self.source_config.get("field_map", {})
        normalized_rows: List[Dict[str, Any]] = []
        for row in records:
            record_id = str(row.get(field_map.get("record_id", "id"), "")).strip()
            if not record_id:
                continue
            normalized_rows.append(
                {
                    "record_id": record_id,
                    "display_name": str(row.get(field_map.get("display_name", "name"), "")).strip(),
                    "address": normalize_address(str(row.get(field_map.get("address", "address"), "")).strip()),
                    "website": str(row.get(field_map.get("website", "website"), "")).strip(),
                    "category": str(row.get(field_map.get("category", "category"), "")).strip(),
                }
            )
        return normalized_rows

    def to_entities(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entities: List[Dict[str, Any]] = []
        seen: set[str] = set()
        entity_type = str(self.source_config.get("entity_type", "open_data_record"))
        source_metadata = infer_source_metadata(self.source_name)
        for row in records:
            record_entity_id = f"api:{self.source_name}:{row['record_id']}"
            record_entity = apply_provenance(
                {
                "entity_id": record_entity_id,
                "display_name": row["display_name"] or row["record_id"],
                "entity_type": entity_type,
                "source": self.source_name,
                },
                self.source_name,
                source_type_hint=source_metadata["source_type"],
                source_record_id=row["record_id"],
            )
            if record_entity_id not in seen:
                seen.add(record_entity_id)
                entities.append(record_entity)
            if row["address"]:
                address_entity_id = f"address:{row['address']}"
                if address_entity_id not in seen:
                    seen.add(address_entity_id)
                    entities.append(
                        apply_provenance(
                            {
                            "entity_id": address_entity_id,
                            "display_name": row["address"],
                            "entity_type": "address",
                            "source": self.source_name,
                            },
                            self.source_name,
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=row["record_id"],
                        )
                    )
        return entities

    def to_relationships(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        relationships: List[Dict[str, Any]] = []
        source_metadata = infer_source_metadata(self.source_name)
        for row in records:
            record_entity_id = f"api:{self.source_name}:{row['record_id']}"
            if row["address"]:
                relationships.append(
                    apply_provenance(
                        {
                        "source_entity_id": record_entity_id,
                        "target_entity_id": f"address:{row['address']}",
                        "relationship_type": "LOCATED_AT",
                        "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["record_id"],
                    )
                )
            if row["website"]:
                relationships.append(
                    apply_provenance(
                        {
                        "source_entity_id": record_entity_id,
                        "target_entity_id": f"website:{row['website']}",
                        "relationship_type": "HAS_WEBSITE",
                        "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["record_id"],
                    )
                )
        return relationships

    def run(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        payload = self.fetch()
        parsed = self.parse(payload)
        normalized = self.normalize(parsed)
        return self.to_entities(normalized), self.to_relationships(normalized)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and normalize approved open-data API records.")
    parser.add_argument("--source", required=True, help="API source name from config/api_sources.json")
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH), help="Path to write API entities CSV")
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH), help="Path to write API relationships CSV")
    args = parser.parse_args()

    connector = OpenDataAPIConnector(source_name=args.source)
    entities, relationships = connector.run()
    write_csv(Path(args.entities_path), entities)
    write_csv(Path(args.relationships_path), relationships)
    print(f"Wrote {len(entities)} api entities and {len(relationships)} api relationships for source {args.source}")


if __name__ == "__main__":
    main()
