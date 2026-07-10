from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.api_base import APIConnectorBase
from src.connectors.open_data_api import load_api_sources, write_csv
from src.connectors.source_metadata import apply_provenance, infer_source_metadata
from src.connectors.source_manifest import validate_source
from src.normalize.address_normalizer import normalize_address

API_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "api_sources.json"
DEFAULT_ENTITIES_PATH = Path("data/processed/arcgis_entities.csv")
DEFAULT_RELATIONSHIPS_PATH = Path("data/processed/arcgis_relationships.csv")


class ArcGISRESTConnector(APIConnectorBase):
    """Connector for approved ArcGIS REST parcel-style endpoints."""

    def __init__(self, source_name: str, config_path: Path | str = API_CONFIG_PATH, limit: int | None = None) -> None:
        self.config_path = Path(config_path)
        self.source_name = source_name
        self.source_config = self._load_source_config(source_name)
        validate_source(source_name, require_live_access=True)
        self.base_url = str(self.source_config["base_url"])
        self.endpoint = str(self.source_config["endpoint"])
        self.query_params = dict(self.source_config.get("query_params", {}))
        if limit is not None:
            self.query_params["resultRecordCount"] = limit
        elif "resultRecordCount" not in self.query_params and self.source_config.get("default_limit") is not None:
            self.query_params["resultRecordCount"] = int(self.source_config["default_limit"])
        self.timeout_seconds = float(self.source_config.get("timeout_seconds", 10))
        self.retry_attempts = int(self.source_config.get("retry_attempts", 2))
        self.retry_backoff_seconds = float(self.source_config.get("retry_backoff_seconds", 0.5))
        self.rate_limit_per_minute = int(self.source_config.get("rate_limit_per_minute", 60))
        self.mock_response_path = self.source_config.get("mock_response_path")
        self.mock_metadata_path = self.source_config.get("mock_metadata_path")
        self.fallback_to_mock_on_error = bool(self.source_config.get("fallback_to_mock_on_error", False))

    def _load_source_config(self, source_name: str) -> Dict[str, Any]:
        sources = load_api_sources(self.config_path)
        if source_name not in sources:
            raise ValueError(f"Unknown API source '{source_name}' in API source config.")
        return sources[source_name]

    def fetch(self) -> str:
        if self.mock_response_path:
            if not self.source_config.get("prefer_live_fetch", False):
                return self._read_mock_file(self.mock_response_path)

        request_url = self._build_request_url()
        try:
            return self._fetch_url(request_url)
        except RuntimeError:
            if self.mock_response_path and self.fallback_to_mock_on_error:
                print(f"ArcGIS connector: live fetch unavailable for {self.source_name}; using mock response.")
                return self._read_mock_file(self.mock_response_path)
            raise

    def fetch_metadata(self) -> Dict[str, Any]:
        if self.mock_metadata_path:
            if not self.source_config.get("prefer_live_fetch", False):
                return json.loads(self._read_mock_file(self.mock_metadata_path))

        metadata_url = self._build_metadata_url()
        try:
            return json.loads(self._fetch_url(metadata_url))
        except RuntimeError:
            if self.mock_metadata_path and self.fallback_to_mock_on_error:
                print(f"ArcGIS connector: live metadata unavailable for {self.source_name}; using mock metadata.")
                return json.loads(self._read_mock_file(self.mock_metadata_path))
            raise

    def _read_mock_file(self, mock_path_value: str) -> str:
        mock_path = Path(mock_path_value)
        if not mock_path.is_absolute():
            mock_path = self.config_path.parent.parent / mock_path
        with mock_path.open("r", encoding="utf-8") as handle:
            return handle.read()

    def _fetch_url(self, request_url: str) -> str:
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
                minimum_delay = 60.0 / self.rate_limit_per_minute if self.rate_limit_per_minute > 0 else 0.0
                time.sleep(max(self.retry_backoff_seconds, minimum_delay))
        raise RuntimeError(f"ArcGIS fetch failed for source '{self.source_name}': {last_error}")

    def _build_layer_url(self) -> str:
        base = self.base_url.rstrip("/")
        endpoint = self.endpoint.lstrip("/")
        url = f"{base}/{endpoint}" if endpoint else base
        if url.rstrip("/").lower().endswith("/query"):
            return url[: -len("/query")]
        return url

    def _build_metadata_url(self) -> str:
        return f"{self._build_layer_url()}?{urllib.parse.urlencode({'f': 'json'})}"

    def _build_request_url(self) -> str:
        url = self._build_layer_url()
        if not url.rstrip("/").lower().endswith("/query"):
            url = f"{url.rstrip('/')}/query"
        params = {"f": "json", **self.query_params}
        return f"{url}?{urllib.parse.urlencode(params, doseq=True)}"

    def _resolve_attribute_value(self, attributes: Dict[str, Any], field_spec: Any) -> str:
        if isinstance(field_spec, (list, tuple)):
            parts = [str(attributes.get(str(field_name), "")).strip() for field_name in field_spec]
            return ", ".join(part for part in parts if part)
        if field_spec in (None, ""):
            return ""
        return str(attributes.get(str(field_spec), "")).strip()

    def parse(self, payload: str) -> List[Dict[str, Any]]:
        parsed = json.loads(payload)
        features = parsed.get("features", [])
        if not isinstance(features, list):
            raise ValueError(f"ArcGIS response for source '{self.source_name}' must contain a features list.")
        rows: List[Dict[str, Any]] = []
        for feature in features:
            if not isinstance(feature, dict):
                continue
            attributes = feature.get("attributes", {})
            geometry = feature.get("geometry", {})
            if not isinstance(attributes, dict):
                continue
            if not isinstance(geometry, dict):
                geometry = {}
            rows.append({"attributes": attributes, "geometry": geometry})
        return rows

    def normalize(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        field_map = self.source_config.get("field_map", {})
        normalized_rows: List[Dict[str, Any]] = []
        for row in records:
            attributes = row.get("attributes", {})
            geometry = row.get("geometry", {})
            parcel_id = self._resolve_attribute_value(attributes, field_map.get("parcel_id", "parcel_id"))
            if not parcel_id:
                continue
            normalized_rows.append(
                {
                    "parcel_id": parcel_id,
                    "owner_name": self._resolve_attribute_value(attributes, field_map.get("owner_name", "owner_name")),
                    "situs_address": normalize_address(self._resolve_attribute_value(attributes, field_map.get("situs_address", "situs_address"))),
                    "mailing_address": normalize_address(self._resolve_attribute_value(attributes, field_map.get("mailing_address", "mailing_address"))),
                    "land_use": self._resolve_attribute_value(attributes, field_map.get("land_use", "land_use")),
                    "assessed_value": self._resolve_attribute_value(attributes, field_map.get("assessed_value", "assessed_value")),
                    "sale_date": self._resolve_attribute_value(attributes, field_map.get("sale_date", "sale_date")),
                    "sale_price": self._resolve_attribute_value(attributes, field_map.get("sale_price", "sale_price")),
                    "latitude": str(geometry.get(field_map.get("latitude", "y"), geometry.get("y", ""))).strip(),
                    "longitude": str(geometry.get(field_map.get("longitude", "x"), geometry.get("x", ""))).strip(),
                }
            )
        return normalized_rows

    def to_entities(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entities: List[Dict[str, Any]] = []
        seen: set[str] = set()
        source_metadata = infer_source_metadata(self.source_name)
        for row in records:
            property_entity = apply_provenance(
                {
                "entity_id": f"property:{row['parcel_id']}",
                "display_name": row["situs_address"] or row["parcel_id"],
                "entity_type": "property",
                "source": self.source_name,
                },
                self.source_name,
                source_type_hint=source_metadata["source_type"],
                source_record_id=row["parcel_id"],
            )
            if property_entity["entity_id"] not in seen:
                seen.add(property_entity["entity_id"])
                entities.append(property_entity)

            for address_field in ["situs_address", "mailing_address"]:
                address = row[address_field]
                if address:
                    address_entity = {
                        "entity_id": f"address:{address}",
                        "display_name": address,
                        "entity_type": "address",
                        "source": self.source_name,
                    }
                    address_entity = apply_provenance(
                        address_entity,
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["parcel_id"],
                    )
                    if address_entity["entity_id"] not in seen:
                        seen.add(address_entity["entity_id"])
                        entities.append(address_entity)

            if row["owner_name"]:
                owner_entity = {
                    "entity_id": f"owner:{row['owner_name']}",
                    "display_name": row["owner_name"],
                    "entity_type": "owner",
                    "source": self.source_name,
                }
                owner_entity = apply_provenance(
                    owner_entity,
                    self.source_name,
                    source_type_hint=source_metadata["source_type"],
                    source_record_id=row["parcel_id"],
                )
                if owner_entity["entity_id"] not in seen:
                    seen.add(owner_entity["entity_id"])
                    entities.append(owner_entity)
        return entities

    def to_relationships(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        relationships: List[Dict[str, Any]] = []
        source_metadata = infer_source_metadata(self.source_name)
        for row in records:
            property_entity_id = f"property:{row['parcel_id']}"
            if row["owner_name"]:
                relationships.append(
                    apply_provenance(
                        {
                        "source_entity_id": property_entity_id,
                        "target_entity_id": f"owner:{row['owner_name']}",
                        "relationship_type": "PROPERTY_OWNED_BY",
                        "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["parcel_id"],
                    )
                )
            if row["situs_address"]:
                relationships.append(
                    apply_provenance(
                        {
                        "source_entity_id": property_entity_id,
                        "target_entity_id": f"address:{row['situs_address']}",
                        "relationship_type": "PROPERTY_HAS_SITUS_ADDRESS",
                        "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["parcel_id"],
                    )
                )
            if row["mailing_address"]:
                relationships.append(
                    apply_provenance(
                        {
                        "source_entity_id": property_entity_id,
                        "target_entity_id": f"address:{row['mailing_address']}",
                        "relationship_type": "PROPERTY_HAS_MAILING_ADDRESS",
                        "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=row["parcel_id"],
                    )
                )
        return relationships

    def run(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        payload = self.fetch()
        parsed = self.parse(payload)
        normalized = self.normalize(parsed)
        return self.to_entities(normalized), self.to_relationships(normalized)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and normalize approved ArcGIS REST parcel records.")
    parser.add_argument("--source", required=True, help="ArcGIS source name from config/api_sources.json")
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH), help="Path to write ArcGIS entities CSV")
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH), help="Path to write ArcGIS relationships CSV")
    parser.add_argument("--limit", type=int, default=None, help="Optional ArcGIS resultRecordCount override")
    args = parser.parse_args()

    connector = ArcGISRESTConnector(source_name=args.source, limit=args.limit)
    entities, relationships = connector.run()
    write_csv(Path(args.entities_path), entities)
    write_csv(Path(args.relationships_path), relationships)
    print(f"Wrote {len(entities)} arcgis entities and {len(relationships)} arcgis relationships for source {args.source}")


if __name__ == "__main__":
    main()
