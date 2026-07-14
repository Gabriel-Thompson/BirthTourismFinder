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
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.api_base import APIConnectorBase
from src.connectors.source_manifest import REPO_ROOT, validate_source

from .normalizer import SOURCE_NAME, build_import_batch_id, load_nppes_config, provider_from_api_record, providers_to_rows, utc_now

DEFAULT_OUTPUT_DIR = Path("data/processed")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


class NPPESAPIConnector(APIConnectorBase):
    def __init__(
        self,
        *,
        npi: str | None = None,
        enumeration_type: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        organization_name: str | None = None,
        address_purpose: str | None = None,
        city: str | None = None,
        state: str | None = None,
        postal_code: str | None = None,
        taxonomy_description: str | None = None,
        limit: int | None = None,
        max_records: int | None = None,
        skip: int | None = None,
        mock: bool = False,
        dry_run: bool = False,
        output_dir: Path | str | None = None,
        verbose: bool = False,
        config_path: Path | str | None = None,
        requester: Any | None = None,
    ) -> None:
        self.config = load_nppes_config(config_path)
        self.source_name = SOURCE_NAME
        self.base_url = str(self.config["api"]["base_url"]).rstrip("/")
        self.endpoint = str(self.config["api"]["endpoint"]).strip()
        self.output_dir = Path(output_dir or self.config.get("processed_dir", DEFAULT_OUTPUT_DIR))
        if not self.output_dir.is_absolute():
            self.output_dir = REPO_ROOT / self.output_dir
        self.raw_snapshot_dir = Path(self.config.get("raw_snapshot_dir", "data/raw/nppes/api"))
        if not self.raw_snapshot_dir.is_absolute():
            self.raw_snapshot_dir = REPO_ROOT / self.raw_snapshot_dir
        self.mock_path = Path(self.config.get("mock_response_path", ""))
        if not self.mock_path.is_absolute():
            self.mock_path = REPO_ROOT / self.mock_path
        self.timeout_seconds = float(self.config["api"].get("timeout_seconds", 30))
        self.retry_attempts = int(self.config["api"].get("retry_attempts", 3))
        self.retry_backoff_seconds = float(self.config["api"].get("retry_backoff_seconds", 1.0))
        self.default_limit = int(self.config["api"].get("default_limit", 200))
        self.max_records = int(max_records or self.config["api"].get("max_records", 1200))
        self.limit = int(limit or self.default_limit)
        self.skip = int(skip or 0)
        self.requester = requester or urllib.request.urlopen
        self.verbose = verbose
        self.mock = mock
        self.dry_run = dry_run
        self.imported_at = utc_now()
        self.query_params = {
            "version": str(self.config["api"].get("version", "2.1")),
            "number": npi or "",
            "enumeration_type": enumeration_type or "",
            "first_name": first_name or "",
            "last_name": last_name or "",
            "organization_name": organization_name or "",
            "address_purpose": address_purpose or "",
            "city": city or self.config.get("default_city", ""),
            "state": state or self.config.get("default_state", "FL"),
            "postal_code": postal_code or self.config.get("default_postal_code", ""),
            "taxonomy_description": taxonomy_description or self.config.get("default_taxonomy", ""),
            "limit": self.limit,
            "skip": self.skip,
        }
        self.import_batch_id = build_import_batch_id("api", self.query_params, imported_at=self.imported_at)
        self.last_diagnostics: list[dict[str, Any]] = []
        validate_source(SOURCE_NAME, require_live_access=not mock)

    def _request_url(self, skip: int) -> str:
        query = dict(self.query_params)
        query["skip"] = skip
        query = {key: value for key, value in query.items() if value not in {"", None}}
        return f"{self.base_url}{self.endpoint}?{urllib.parse.urlencode(query, doseq=True)}"

    def _snapshot(self, *, url: str, status_code: int, payload: Any, skip: int) -> None:
        self.raw_snapshot_dir.mkdir(parents=True, exist_ok=True)
        path = self.raw_snapshot_dir / f"{self.import_batch_id}_{skip}.json"
        write_json(
            path,
            {
                "request_timestamp": self.imported_at,
                "sanitized_query_parameters": {key: value for key, value in self.query_params.items() if key != "api_key"},
                "http_status": status_code,
                "response_result_count": int(payload.get("result_count", 0)) if isinstance(payload, dict) else 0,
                "skip": skip,
                "source_url": url,
                "raw_json_payload": payload,
            },
        )

    def _append_diagnostic(self, event: str, message: str, *, status_code: int = 200, skip: int = 0, records_seen: int = 0) -> None:
        self.last_diagnostics.append(
            {
                "run_id": self.import_batch_id,
                "timestamp": self.imported_at,
                "event": event,
                "severity": "INFO" if status_code < 400 else "ERROR",
                "message": message,
                "http_status": status_code,
                "skip": skip,
                "records_seen": records_seen,
            }
        )

    def fetch(self) -> str:
        if self.dry_run:
            print("NPPES API dry-run")
            print(f"  filters={json.dumps({key: value for key, value in self.query_params.items() if value not in {'', None}}, sort_keys=True)}")
            print(f"  output_dir={self.output_dir}")
            return json.dumps({"result_count": 0, "results": []})
        if self.mock:
            return self.mock_path.read_text(encoding="utf-8")

        collected: list[dict[str, Any]] = []
        skip = self.skip
        while len(collected) < self.max_records:
            url = self._request_url(skip)
            last_error: Exception | None = None
            payload: dict[str, Any] = {}
            for attempt in range(1, self.retry_attempts + 1):
                try:
                    with self.requester(url, timeout=self.timeout_seconds) as response:
                        body = response.read().decode(response.headers.get_content_charset() or "utf-8")
                        payload = json.loads(body)
                        status_code = getattr(response, "status", 200)
                        self._snapshot(url=url, status_code=status_code, payload=payload, skip=skip)
                        break
                except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
                    last_error = exc
                    if attempt == self.retry_attempts:
                        raise RuntimeError(f"NPPES API request failed: {exc}")
                    time.sleep(self.retry_backoff_seconds * attempt)
            results = payload.get("results", []) if isinstance(payload, dict) else []
            if not isinstance(results, list):
                raise ValueError("NPPES API response did not contain a results list.")
            self._append_diagnostic("api_fetch", f"NPPES API page fetched skip={skip}", records_seen=len(results), skip=skip)
            collected.extend([row for row in results if isinstance(row, dict)])
            if self.verbose:
                print(f"NPPES API: fetched {len(results)} rows at skip={skip}")
            if not results or len(results) < self.limit:
                break
            skip += self.limit
        return json.dumps({"result_count": len(collected), "results": collected[: self.max_records]})

    def parse(self, payload: str) -> list[dict[str, Any]]:
        parsed = json.loads(payload)
        results = parsed.get("results", parsed if isinstance(parsed, list) else [])
        if not isinstance(results, list):
            raise ValueError("NPPES API payload must contain a list of results.")
        return [row for row in results if isinstance(row, dict)]

    def normalize(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        providers = [
            provider_from_api_record(
                record,
                imported_at=self.imported_at,
                import_batch_id=self.import_batch_id,
                source_url=self._request_url(self.skip),
                source_mode="api",
            )
            for record in records
            if str(record.get("number") or "").strip()
        ]
        self.providers = providers
        return [provider.__dict__ for provider in providers]

    def to_entities(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        _, entities, _, _ = providers_to_rows(self.providers, source_type_hint="api")
        return entities

    def to_relationships(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        _, _, relationships, _ = providers_to_rows(self.providers, source_type_hint="api")
        return relationships

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
        payload = self.fetch()
        parsed = self.parse(payload)
        self.normalize(parsed)
        provider_rows, entity_rows, relationship_rows, taxonomy_rows = providers_to_rows(self.providers, source_type_hint="api")
        summary = {
            "source_name": SOURCE_NAME,
            "mode": "mock" if self.mock else "api",
            "records_read": len(parsed),
            "providers_normalized": len(provider_rows),
            "individual_providers": sum(1 for row in provider_rows if str(row.get("entity_type_code", "")) == "1"),
            "organization_providers": sum(1 for row in provider_rows if str(row.get("entity_type_code", "")) == "2"),
            "practice_addresses": sum(len(provider.practice_addresses) for provider in self.providers),
            "mailing_addresses": sum(len(provider.mailing_addresses) for provider in self.providers),
            "taxonomy_records": len(taxonomy_rows),
            "deactivated_npis": sum(1 for provider in self.providers if not provider.active_flag),
            "incomplete_records": sum(1 for provider in self.providers if provider.incomplete_record),
            "last_attempted_import": self.imported_at,
            "last_successful_import": self.imported_at,
            "filters": {key: value for key, value in self.query_params.items() if value not in {"", None}},
            "runtime_seconds": 0.0,
            "errors": "",
            "truncation_status": len(parsed) >= self.max_records,
        }
        manifest = {
            "connector_version": "6.0",
            "configuration_hash": build_import_batch_id("api-config", self.query_params, imported_at=self.imported_at),
            "filters": summary["filters"],
            "start_time": self.imported_at,
            "completion_time": utc_now(),
            "record_count": len(parsed),
            "raw_snapshot_dir": str(self.raw_snapshot_dir),
            "processed_outputs": [
                "data/processed/nppes_providers.csv",
                "data/processed/nppes_entities.csv",
                "data/processed/nppes_relationships.csv",
                "data/processed/nppes_taxonomies.csv"
            ],
            "result_status": "SUCCESS",
            "truncation_status": summary["truncation_status"],
        }
        return provider_rows, entity_rows, relationship_rows, taxonomy_rows, summary, manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch bounded official CMS NPPES API results into local CSV outputs.")
    parser.add_argument("--npi", default=None)
    parser.add_argument("--enumeration-type", default=None)
    parser.add_argument("--first-name", default=None)
    parser.add_argument("--last-name", default=None)
    parser.add_argument("--organization-name", default=None)
    parser.add_argument("--address-purpose", default=None)
    parser.add_argument("--city", default=None)
    parser.add_argument("--state", default=None)
    parser.add_argument("--postal-code", default=None)
    parser.add_argument("--taxonomy-description", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--skip", type=int, default=None)
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    connector = NPPESAPIConnector(
        npi=args.npi,
        enumeration_type=args.enumeration_type,
        first_name=args.first_name,
        last_name=args.last_name,
        organization_name=args.organization_name,
        address_purpose=args.address_purpose,
        city=args.city,
        state=args.state,
        postal_code=args.postal_code,
        taxonomy_description=args.taxonomy_description,
        limit=args.limit,
        max_records=args.max_records,
        skip=args.skip,
        mock=args.mock,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
        verbose=args.verbose,
    )
    provider_rows, entity_rows, relationship_rows, taxonomy_rows, summary, manifest = connector.run()
    if not args.dry_run:
        output_dir = Path(args.output_dir)
        if not output_dir.is_absolute():
            output_dir = REPO_ROOT / output_dir
        write_csv(output_dir / "nppes_providers.csv", provider_rows)
        write_csv(output_dir / "nppes_entities.csv", entity_rows)
        write_csv(output_dir / "nppes_relationships.csv", relationship_rows)
        write_csv(output_dir / "nppes_taxonomies.csv", taxonomy_rows)
        write_csv(output_dir / "nppes_import_diagnostics.csv", connector.last_diagnostics)
        write_json(output_dir / "nppes_import_summary.json", summary)
        write_json(output_dir / "nppes_import_manifest.json", manifest)
    print(
        f"NPPES API: mode={'mock' if args.mock else 'api'} providers={summary['providers_normalized']} "
        f"individual={summary['individual_providers']} organizations={summary['organization_providers']}"
    )


if __name__ == "__main__":
    main()
