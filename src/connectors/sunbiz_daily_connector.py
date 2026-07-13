from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.cross_source import run_cross_source_correlation
from src.analytics.entity_builder import build_entity_graph
from src.analytics.entity_resolution.resolver import resolve_entities
from src.connectors.api_base import APIConnectorBase
from src.connectors.source_manifest import REPO_ROOT, validate_source
from src.connectors.source_metadata import apply_provenance, infer_source_metadata
from src.normalize.address_normalizer import normalize_address

DEFAULT_CONFIG_PATH = Path("config/sunbiz_daily.json")
DEFAULT_ENTITIES_PATH = Path("data/processed/sunbiz_entities.csv")
DEFAULT_RELATIONSHIPS_PATH = Path("data/processed/sunbiz_relationships.csv")
DEFAULT_STATUS_PATH = Path("data/processed/sunbiz_daily_status.json")
DEFAULT_DB_PATH = Path("local_osint.duckdb")

ENTITY_FIELDS = [
    "entity_id",
    "display_name",
    "entity_type",
    "source",
    "source_name",
    "source_type",
    "source_record_id",
    "connector_name",
    "import_batch_id",
    "imported_at",
    "jurisdiction",
    "is_synthetic",
    "status",
    "filing_date",
    "address_kind",
    "county",
    "city",
    "zip",
]

RELATIONSHIP_FIELDS = [
    "source_entity_id",
    "target_entity_id",
    "relationship_type",
    "confidence",
    "source_name",
    "source_type",
    "source_record_id",
    "connector_name",
    "import_batch_id",
    "imported_at",
    "jurisdiction",
    "is_synthetic",
]


class MissingAPIKeyError(RuntimeError):
    pass


def load_sunbiz_daily_config(config_path: Path | str | None = None) -> Dict[str, Any]:
    path_value = config_path or os.getenv("OPENFRAUD_SUNBIZ_DAILY_CONFIG_PATH") or DEFAULT_CONFIG_PATH
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Sunbiz Daily config must be a JSON object.")
    return data


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def write_status(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _get_nested_value(record: Dict[str, Any], field_spec: Any) -> Any:
    if isinstance(field_spec, list):
        return [_get_nested_value(record, item) for item in field_spec]
    current: Any = record
    for token in str(field_spec or "").split("."):
        if isinstance(current, dict):
            current = current.get(token)
        else:
            return None
    return current


def _stringify_address(value: Any) -> str:
    if isinstance(value, dict):
        parts = [str(item).strip() for item in value.values() if str(item).strip()]
        return normalize_address(", ".join(parts))
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
        return normalize_address(", ".join(parts))
    return normalize_address(str(value or "").strip())


def _stringify(value: Any) -> str:
    return str(value or "").strip()


def _name_to_entity_id(prefix: str, value: str) -> str:
    cleaned = " ".join(str(value or "").strip().split())
    return f"{prefix}:{cleaned}"


def _parse_people(value: Any) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if value is None:
        return rows
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                name = _stringify(item.get("name") or item.get("full_name"))
                address = _stringify_address(item.get("address", ""))
            else:
                name = _stringify(item)
                address = ""
            if name:
                rows.append({"name": name, "address": address})
        return rows
    if isinstance(value, dict):
        name = _stringify(value.get("name") or value.get("full_name"))
        address = _stringify_address(value.get("address", ""))
        return [{"name": name, "address": address}] if name else []
    text = _stringify(value)
    if not text:
        return rows
    for token in [part.strip() for part in text.replace("|", ";").split(";") if part.strip()]:
        rows.append({"name": token, "address": ""})
    return rows


class SunbizDailyConnector(APIConnectorBase):
    def __init__(
        self,
        *,
        county: str | None = None,
        city: str | None = None,
        zip_code: str | None = None,
        limit: int | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_types: List[str] | None = None,
        config_path: Path | str | None = None,
        requester: Any | None = None,
    ) -> None:
        load_dotenv(REPO_ROOT / ".env")
        self.config_path = Path(config_path or os.getenv("OPENFRAUD_SUNBIZ_DAILY_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
        if not self.config_path.is_absolute():
            self.config_path = REPO_ROOT / self.config_path
        self.source_config = load_sunbiz_daily_config(self.config_path)
        self.source_name = str(self.source_config.get("source_name", "sunbiz_daily_api"))
        validate_source(self.source_name, require_live_access=True)
        self.base_url = str(self.source_config.get("base_url", "")).rstrip("/")
        self.endpoint = str(self.source_config.get("endpoint", "")).strip()
        self.query_params = {}
        self.timeout_seconds = float(self.source_config.get("timeout", 20))
        self.retry_attempts = int(self.source_config.get("retry_attempts", 3))
        self.retry_backoff = float(self.source_config.get("retry_backoff", 1.0))
        self.max_requests_per_hour = int(self.source_config.get("max_requests_per_hour", 300))
        self.default_page_size = int(self.source_config.get("default_page_size", 100))
        self.limit = int(limit) if limit is not None else int(self.source_config.get("default_page_size", 100))
        self.county = county or str(self.source_config.get("county_filter", "Hillsborough"))
        self.city = city or ""
        self.zip_code = zip_code or ""
        self.from_date = from_date or ""
        self.to_date = to_date or ""
        self.entity_types = entity_types or list(self.source_config.get("entity_types", []))
        self.page_param = str(self.source_config.get("page_param", "page"))
        self.page_size_param = str(self.source_config.get("page_size_param", "page_size"))
        self.limit_param = str(self.source_config.get("limit_param", "limit"))
        self.county_param = str(self.source_config.get("county_param", "county"))
        self.city_param = str(self.source_config.get("city_param", "city"))
        self.zip_param = str(self.source_config.get("zip_param", "zip"))
        self.from_date_param = str(self.source_config.get("from_date_param", "from_date"))
        self.to_date_param = str(self.source_config.get("to_date_param", "to_date"))
        self.entity_type_param = str(self.source_config.get("entity_type_param", "entity_type"))
        self.response_root = str(self.source_config.get("response_root", "results"))
        self.next_page_path = str(self.source_config.get("next_page_path", "pagination.next_page"))
        self.field_map = dict(self.source_config.get("field_map", {}))
        self.mock_response_path = str(self.source_config.get("mock_response_path", "")).strip()
        self.prefer_mock_response = bool(self.source_config.get("prefer_mock_response", False))
        self.auth_header = str(self.source_config.get("auth_header", "X-API-Key"))
        self.auth_prefix = str(self.source_config.get("auth_prefix", ""))
        self.accept_header = str(self.source_config.get("accept_header", "application/json"))
        self.api_key_env_var = str(self.source_config.get("api_key_env_var", "SUNBIZ_DAILY_API_KEY"))
        self.api_key = os.getenv(self.api_key_env_var, "").strip()
        self.requester = requester or urllib.request.urlopen
        self.last_request_started_at = 0.0

    def fetch(self) -> str:
        if not bool(self.source_config.get("enabled", True)):
            raise RuntimeError("Sunbiz Daily connector is disabled in config/sunbiz_daily.json.")
        if self.prefer_mock_response:
            return self._read_mock_response()
        if not self.api_key:
            raise MissingAPIKeyError(
                f"Missing {self.api_key_env_var}. Add it to your local .env file before running the Sunbiz Daily connector."
            )

        all_records: List[Dict[str, Any]] = []
        next_page_value: int | str | None = 1
        fetched_pages = 0
        while next_page_value and len(all_records) < self.limit:
            payload = self._fetch_page(next_page_value)
            parsed = json.loads(payload)
            page_records = parsed.get(self.response_root, [])
            if not isinstance(page_records, list):
                raise ValueError(f"Sunbiz Daily response_root '{self.response_root}' must contain a list of records.")
            all_records.extend([row for row in page_records if isinstance(row, dict)])
            fetched_pages += 1
            print(
                f"Sunbiz Daily: fetched page {fetched_pages} "
                f"rows={len(page_records)} accumulated={min(len(all_records), self.limit)}"
            )
            next_page_value = self._next_page(parsed, next_page_value)
            if not page_records:
                break
        return json.dumps(all_records[: self.limit])

    def _read_mock_response(self) -> str:
        if not self.mock_response_path:
            raise RuntimeError("Sunbiz Daily connector was configured for mock mode without a mock_response_path.")
        mock_path = Path(self.mock_response_path)
        if not mock_path.is_absolute():
            mock_path = REPO_ROOT / mock_path
        with mock_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows = payload.get(self.response_root, payload if isinstance(payload, list) else [])
        if not isinstance(rows, list):
            raise RuntimeError("Sunbiz Daily mock response must contain a list under the configured response_root.")
        return json.dumps(rows[: self.limit])

    def _headers(self) -> Dict[str, str]:
        header_value = f"{self.auth_prefix}{self.api_key}" if self.auth_prefix else self.api_key
        return {
            self.auth_header: header_value,
            "Accept": self.accept_header,
        }

    def _build_url(self, page_value: int | str) -> str:
        params: Dict[str, Any] = {
            self.page_param: page_value,
            self.page_size_param: self.default_page_size,
            self.limit_param: self.limit,
        }
        if self.county:
            params[self.county_param] = self.county
        if self.city:
            params[self.city_param] = self.city
        if self.zip_code:
            params[self.zip_param] = self.zip_code
        if self.from_date:
            params[self.from_date_param] = self.from_date
        if self.to_date:
            params[self.to_date_param] = self.to_date
        if self.entity_types:
            params[self.entity_type_param] = self.entity_types
        url = f"{self.base_url}{self.endpoint}"
        return f"{url}?{urllib.parse.urlencode(params, doseq=True)}"

    def _apply_rate_limit(self) -> None:
        if self.max_requests_per_hour <= 0:
            return
        minimum_interval = 3600.0 / float(self.max_requests_per_hour)
        elapsed = time.time() - self.last_request_started_at
        if elapsed < minimum_interval:
            time.sleep(minimum_interval - elapsed)
        self.last_request_started_at = time.time()

    def _fetch_page(self, page_value: int | str) -> str:
        request_url = self._build_url(page_value)
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            self._apply_rate_limit()
            request = urllib.request.Request(request_url, headers=self._headers(), method="GET")
            try:
                with self.requester(request, timeout=self.timeout_seconds) as response:
                    payload = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    return payload.decode(encoding)
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                time.sleep(self.retry_backoff * attempt)
        raise RuntimeError(f"Sunbiz Daily fetch failed after {self.retry_attempts} attempts: {last_error}")

    def _next_page(self, parsed: Dict[str, Any], current_page: int | str) -> int | str | None:
        current: Any = parsed
        for token in self.next_page_path.split("."):
            if not token:
                continue
            if isinstance(current, dict):
                current = current.get(token)
            else:
                current = None
                break
        if current in ("", None, False):
            if len(parsed.get(self.response_root, [])) < self.default_page_size:
                return None
            if isinstance(current_page, int):
                return current_page + 1
            return None
        return current

    def parse(self, payload: str) -> List[Dict[str, Any]]:
        parsed = json.loads(payload)
        if not isinstance(parsed, list):
            raise ValueError("Sunbiz Daily fetch output must be a JSON list after pagination flattening.")
        return [row for row in parsed if isinstance(row, dict)]

    def normalize(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized_rows: List[Dict[str, Any]] = []
        for record in records:
            document_number = _stringify(_get_nested_value(record, self.field_map.get("document_number", "document_number")))
            business_name = _stringify(_get_nested_value(record, self.field_map.get("business_name", "business_name")))
            if not document_number or not business_name:
                continue
            principal_address = _stringify_address(_get_nested_value(record, self.field_map.get("principal_address", "principal_address")))
            mailing_address = _stringify_address(_get_nested_value(record, self.field_map.get("mailing_address", "mailing_address")))
            registered_agent_name = _stringify(_get_nested_value(record, self.field_map.get("registered_agent_name", "registered_agent.name")))
            registered_agent_address = _stringify_address(_get_nested_value(record, self.field_map.get("registered_agent_address", "registered_agent.address")))
            officers = _parse_people(_get_nested_value(record, self.field_map.get("officers", "officers")))
            normalized_rows.append(
                {
                    "document_number": document_number,
                    "business_name": business_name,
                    "entity_type": _stringify(_get_nested_value(record, self.field_map.get("entity_type", "entity_type"))),
                    "status": _stringify(_get_nested_value(record, self.field_map.get("status", "status"))),
                    "filing_date": _stringify(_get_nested_value(record, self.field_map.get("filing_date", "filing_date"))),
                    "principal_address": principal_address,
                    "mailing_address": mailing_address,
                    "registered_agent_name": registered_agent_name,
                    "registered_agent_address": registered_agent_address,
                    "officers": officers,
                    "county": _stringify(_get_nested_value(record, self.field_map.get("county", "county"))) or self.county,
                    "city": _stringify(_get_nested_value(record, self.field_map.get("city", "city"))) or self.city,
                    "zip": _stringify(_get_nested_value(record, self.field_map.get("zip", "zip"))) or self.zip_code,
                }
            )
        return normalized_rows

    def to_entities(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        entities: List[Dict[str, Any]] = []
        seen: set[str] = set()
        source_metadata = infer_source_metadata(self.source_name)
        imported_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for record in records:
            county = record["county"]
            city = record["city"]
            zip_code = record["zip"]
            business_entity = apply_provenance(
                {
                    "entity_id": f"business:sunbiz_daily:{record['document_number']}",
                    "display_name": record["business_name"],
                    "entity_type": "business",
                    "source": self.source_name,
                    "status": record["status"],
                    "filing_date": record["filing_date"],
                    "address_kind": "",
                    "county": county,
                    "city": city,
                    "zip": zip_code,
                },
                self.source_name,
                source_type_hint=source_metadata["source_type"],
                source_record_id=record["document_number"],
                connector_name=self.source_name,
                imported_at=imported_at,
                jurisdiction=county,
            )
            if business_entity["entity_id"] not in seen:
                seen.add(business_entity["entity_id"])
                entities.append(business_entity)

            for address_kind, address in [("principal", record["principal_address"]), ("mailing", record["mailing_address"])]:
                if not address:
                    continue
                address_entity = apply_provenance(
                    {
                        "entity_id": f"address:{address}",
                        "display_name": address,
                        "entity_type": "address",
                        "source": self.source_name,
                        "status": record["status"],
                        "filing_date": record["filing_date"],
                        "address_kind": address_kind,
                        "county": county,
                        "city": city,
                        "zip": zip_code,
                    },
                    self.source_name,
                    source_type_hint=source_metadata["source_type"],
                    source_record_id=record["document_number"],
                    connector_name=self.source_name,
                    imported_at=imported_at,
                    jurisdiction=county,
                )
                if address_entity["entity_id"] not in seen:
                    seen.add(address_entity["entity_id"])
                    entities.append(address_entity)

            if record["registered_agent_name"]:
                agent_entity = apply_provenance(
                    {
                        "entity_id": _name_to_entity_id("registered_agent", record["registered_agent_name"]),
                        "display_name": record["registered_agent_name"],
                        "entity_type": "registered_agent",
                        "source": self.source_name,
                        "status": record["status"],
                        "filing_date": record["filing_date"],
                        "address_kind": "",
                        "county": county,
                        "city": city,
                        "zip": zip_code,
                    },
                    self.source_name,
                    source_type_hint=source_metadata["source_type"],
                    source_record_id=f"{record['document_number']}:registered_agent",
                    connector_name=self.source_name,
                    imported_at=imported_at,
                    jurisdiction=county,
                )
                if agent_entity["entity_id"] not in seen:
                    seen.add(agent_entity["entity_id"])
                    entities.append(agent_entity)

            for officer_index, officer in enumerate(record["officers"], start=1):
                officer_name = officer["name"]
                if not officer_name:
                    continue
                officer_entity = apply_provenance(
                    {
                        "entity_id": _name_to_entity_id("officer", officer_name),
                        "display_name": officer_name,
                        "entity_type": "officer",
                        "source": self.source_name,
                        "status": record["status"],
                        "filing_date": record["filing_date"],
                        "address_kind": "",
                        "county": county,
                        "city": city,
                        "zip": zip_code,
                    },
                    self.source_name,
                    source_type_hint=source_metadata["source_type"],
                    source_record_id=f"{record['document_number']}:officer:{officer_index}",
                    connector_name=self.source_name,
                    imported_at=imported_at,
                    jurisdiction=county,
                )
                if officer_entity["entity_id"] not in seen:
                    seen.add(officer_entity["entity_id"])
                    entities.append(officer_entity)
        return entities

    def to_relationships(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        relationships: List[Dict[str, Any]] = []
        source_metadata = infer_source_metadata(self.source_name)
        imported_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for record in records:
            business_id = f"business:sunbiz_daily:{record['document_number']}"
            county = record["county"]
            principal_address = record["principal_address"]
            mailing_address = record["mailing_address"]
            if principal_address:
                relationships.append(
                    apply_provenance(
                        {
                            "source_entity_id": business_id,
                            "target_entity_id": f"address:{principal_address}",
                            "relationship_type": "BUSINESS_LOCATED_AT",
                            "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=record["document_number"],
                        connector_name=self.source_name,
                        imported_at=imported_at,
                        jurisdiction=county,
                    )
                )
                relationships.append(
                    apply_provenance(
                        {
                            "source_entity_id": business_id,
                            "target_entity_id": f"address:{principal_address}",
                            "relationship_type": "LOCATED_AT",
                            "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=record["document_number"],
                        connector_name=self.source_name,
                        imported_at=imported_at,
                        jurisdiction=county,
                    )
                )
            if mailing_address:
                relationships.append(
                    apply_provenance(
                        {
                            "source_entity_id": business_id,
                            "target_entity_id": f"address:{mailing_address}",
                            "relationship_type": "BUSINESS_MAILING_ADDRESS",
                            "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=record["document_number"],
                        connector_name=self.source_name,
                        imported_at=imported_at,
                        jurisdiction=county,
                    )
                )
            if record["registered_agent_name"]:
                agent_id = _name_to_entity_id("registered_agent", record["registered_agent_name"])
                relationships.append(
                    apply_provenance(
                        {
                            "source_entity_id": agent_id,
                            "target_entity_id": business_id,
                            "relationship_type": "REGISTERED_AGENT_FOR",
                            "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=f"{record['document_number']}:registered_agent",
                        connector_name=self.source_name,
                        imported_at=imported_at,
                        jurisdiction=county,
                    )
                )
                agent_address = record["registered_agent_address"] or principal_address or mailing_address
                if agent_address:
                    relationships.append(
                        apply_provenance(
                            {
                                "source_entity_id": agent_id,
                                "target_entity_id": f"address:{agent_address}",
                                "relationship_type": "REGISTERED_AGENT_AT_ADDRESS",
                                "confidence": 0.95,
                            },
                            self.source_name,
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=f"{record['document_number']}:registered_agent",
                            connector_name=self.source_name,
                            imported_at=imported_at,
                            jurisdiction=county,
                        )
                    )
            for officer_index, officer in enumerate(record["officers"], start=1):
                officer_name = officer["name"]
                if not officer_name:
                    continue
                officer_id = _name_to_entity_id("officer", officer_name)
                relationships.append(
                    apply_provenance(
                        {
                            "source_entity_id": officer_id,
                            "target_entity_id": business_id,
                            "relationship_type": "OFFICER_OF",
                            "confidence": 1.0,
                        },
                        self.source_name,
                        source_type_hint=source_metadata["source_type"],
                        source_record_id=f"{record['document_number']}:officer:{officer_index}",
                        connector_name=self.source_name,
                        imported_at=imported_at,
                        jurisdiction=county,
                    )
                )
                officer_address = officer["address"] or principal_address or mailing_address
                if officer_address:
                    relationships.append(
                        apply_provenance(
                            {
                                "source_entity_id": officer_id,
                                "target_entity_id": f"address:{officer_address}",
                                "relationship_type": "OFFICER_AT_ADDRESS",
                                "confidence": 0.95,
                            },
                            self.source_name,
                            source_type_hint=source_metadata["source_type"],
                            source_record_id=f"{record['document_number']}:officer:{officer_index}",
                            connector_name=self.source_name,
                            imported_at=imported_at,
                            jurisdiction=county,
                        )
                    )
        return relationships

    def run(self) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
        start_time = time.time()
        payload = self.fetch()
        parsed = self.parse(payload)
        normalized = self.normalize(parsed)
        entities = self.to_entities(normalized)
        relationships = self.to_relationships(normalized)
        status = {
            "source_name": self.source_name,
            "api_status": "SUCCESS",
            "last_import": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "county_coverage": self.county,
            "city_filter": self.city,
            "zip_filter": self.zip_code,
            "from_date": self.from_date,
            "to_date": self.to_date,
            "businesses_imported": sum(1 for row in entities if row["entity_type"] == "business"),
            "officers_imported": sum(1 for row in entities if row["entity_type"] == "officer"),
            "registered_agents_imported": sum(1 for row in entities if row["entity_type"] == "registered_agent"),
            "addresses_imported": sum(1 for row in entities if row["entity_type"] == "address"),
            "relationships_created": len(relationships),
            "cross_source_matches": 0,
            "runtime_seconds": round(time.time() - start_time, 2),
            "error": "",
        }
        return entities, relationships, status


def refresh_cross_source_artifacts(
    *,
    db_path: Path,
    processed_dir: Path,
    sunbiz_entities_path: Path,
    sunbiz_relationships_path: Path,
) -> int:
    if not db_path.exists():
        print(f"Sunbiz Daily: skipped cross-source refresh because DuckDB was not found at {db_path}.")
        return 0
    connector_entity_paths = [
        path
        for path in [
            sunbiz_entities_path,
            processed_dir / "county_property_entities.csv",
            processed_dir / "county_clerk_entities.csv",
            processed_dir / "api_entities.csv",
            processed_dir / "arcgis_entities.csv",
        ]
        if path.exists() and path.stat().st_size > 0
    ]
    connector_relationship_paths = [
        path
        for path in [
            sunbiz_relationships_path,
            processed_dir / "county_property_relationships.csv",
            processed_dir / "county_clerk_relationships.csv",
            processed_dir / "api_relationships.csv",
            processed_dir / "arcgis_relationships.csv",
        ]
        if path.exists() and path.stat().st_size > 0
    ]
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    canonical_entities_path = processed_dir / "canonical_entities.csv"
    aliases_path = processed_dir / "entity_aliases.csv"
    matches_path = processed_dir / "entity_resolution_matches.csv"
    canonical_relationships_path = processed_dir / "canonical_relationships.csv"
    build_entity_graph(
        db_path=db_path,
        entities_path=entities_path,
        relationships_path=relationships_path,
        additional_entity_paths=connector_entity_paths,
        additional_relationship_paths=connector_relationship_paths,
    )
    resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=canonical_entities_path,
        aliases_path=aliases_path,
        matches_path=matches_path,
        canonical_relationships_path=canonical_relationships_path,
        db_path=db_path,
    )
    summary = run_cross_source_correlation(
        canonical_entities_path=canonical_entities_path,
        aliases_path=aliases_path,
        entity_resolution_matches_path=matches_path,
        canonical_relationships_path=canonical_relationships_path,
        fraud_markers_path=processed_dir / "fraud_markers.csv",
        prioritized_leads_path=processed_dir / "prioritized_leads.csv",
        cross_source_matches_path=processed_dir / "cross_source_matches.csv",
        diagnostics_path=processed_dir / "cross_source_diagnostics.csv",
        diagnostic_summary_path=processed_dir / "cross_source_diagnostic_summary.json",
    )
    return int(summary.get("cross_source_match_count", 0))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch bounded Sunbiz Daily business filings into local entity and relationship exports.")
    parser.add_argument("--county", default=None, help="County filter. Defaults to the county_filter in config/sunbiz_daily.json.")
    parser.add_argument("--city", default=None, help="Optional city filter.")
    parser.add_argument("--zip", dest="zip_code", default=None, help="Optional ZIP filter.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of business filings to ingest.")
    parser.add_argument("--from-date", default=None, help="Optional inclusive filing-date lower bound.")
    parser.add_argument("--to-date", default=None, help="Optional inclusive filing-date upper bound.")
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH), help="Path to write Sunbiz entities CSV.")
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH), help="Path to write Sunbiz relationships CSV.")
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH), help="Path to write Sunbiz connector status JSON.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="DuckDB path used when refreshing merged cross-source artifacts.")
    parser.add_argument(
        "--skip-cross-source-refresh",
        action="store_true",
        help="Skip rebuilding merged entities/canonical rows and cross-source matches after import.",
    )
    args = parser.parse_args()

    entities_path = Path(args.entities_path)
    relationships_path = Path(args.relationships_path)
    status_path = Path(args.status_path)
    start_time = time.time()

    try:
        connector = SunbizDailyConnector(
            county=args.county,
            city=args.city,
            zip_code=args.zip_code,
            limit=args.limit,
            from_date=args.from_date,
            to_date=args.to_date,
        )
        entities, relationships, status = connector.run()
        write_csv(entities_path, entities, ENTITY_FIELDS)
        write_csv(relationships_path, relationships, RELATIONSHIP_FIELDS)
        if not args.skip_cross_source_refresh:
            status["cross_source_matches"] = refresh_cross_source_artifacts(
                db_path=Path(args.db_path),
                processed_dir=entities_path.parent,
                sunbiz_entities_path=entities_path,
                sunbiz_relationships_path=relationships_path,
            )
        status["runtime_seconds"] = round(time.time() - start_time, 2)
        write_status(status_path, status)
        print(
            f"Sunbiz Daily: wrote businesses={status['businesses_imported']} "
            f"officers={status['officers_imported']} registered_agents={status['registered_agents_imported']} "
            f"addresses={status['addresses_imported']} relationships={status['relationships_created']} "
            f"cross_source_matches={status['cross_source_matches']}"
        )
    except MissingAPIKeyError as exc:
        write_csv(entities_path, [], ENTITY_FIELDS)
        write_csv(relationships_path, [], RELATIONSHIP_FIELDS)
        write_status(
            status_path,
            {
                "source_name": "sunbiz_daily_api",
                "api_status": "MISSING_API_KEY",
                "last_import": "",
                "county_coverage": args.county or "",
                "businesses_imported": 0,
                "officers_imported": 0,
                "registered_agents_imported": 0,
                "addresses_imported": 0,
                "relationships_created": 0,
                "cross_source_matches": 0,
                "runtime_seconds": round(time.time() - start_time, 2),
                "error": str(exc),
            },
        )
        print(str(exc))
    except Exception as exc:
        write_csv(entities_path, [], ENTITY_FIELDS)
        write_csv(relationships_path, [], RELATIONSHIP_FIELDS)
        write_status(
            status_path,
            {
                "source_name": "sunbiz_daily_api",
                "api_status": "FAILED",
                "last_import": "",
                "county_coverage": args.county or "",
                "businesses_imported": 0,
                "officers_imported": 0,
                "registered_agents_imported": 0,
                "addresses_imported": 0,
                "relationships_created": 0,
                "cross_source_matches": 0,
                "runtime_seconds": round(time.time() - start_time, 2),
                "error": str(exc),
            },
        )
        print(f"Sunbiz Daily connector failed: {exc}")


if __name__ == "__main__":
    main()
