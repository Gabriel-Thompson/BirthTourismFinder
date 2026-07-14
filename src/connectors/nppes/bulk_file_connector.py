from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.connectors.source_manifest import REPO_ROOT, ensure_local_only_path, validate_source

from .normalizer import SOURCE_NAME, build_import_batch_id, load_nppes_config, provider_from_bulk_row, providers_to_rows, utc_now

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


REQUIRED_COLUMNS = {
    "NPI",
    "Entity Type Code",
    "Provider Business Mailing Address Postal Code",
    "Provider Practice Location Address Postal Code",
}


class NPPESBulkFileConnector:
    def __init__(
        self,
        *,
        input_path: Path | str,
        state: str | None = None,
        city: str | None = None,
        postal_code: str | None = None,
        postal_prefix: str | None = None,
        taxonomy_code: str | None = None,
        taxonomy_description: str | None = None,
        enumeration_type: str | None = None,
        active_only: bool = False,
        chunk_size: int | None = None,
        max_records: int | None = None,
        output_dir: Path | str | None = None,
        dry_run: bool = False,
        verbose: bool = False,
        config_path: Path | str | None = None,
    ) -> None:
        validate_source(SOURCE_NAME)
        self.config = load_nppes_config(config_path)
        self.input_path = ensure_local_only_path(SOURCE_NAME, input_path)
        self.output_dir = Path(output_dir or self.config.get("processed_dir", DEFAULT_OUTPUT_DIR))
        if not self.output_dir.is_absolute():
            self.output_dir = REPO_ROOT / self.output_dir
        self.chunk_size = int(chunk_size or self.config["bulk"].get("chunk_size", 50000))
        self.max_records = int(max_records or self.config["api"].get("max_records", 1200))
        self.verbose = verbose
        self.dry_run = dry_run
        self.filters = {
            "state": state or self.config.get("default_state", "FL"),
            "city": city or self.config.get("default_city", ""),
            "postal_code": postal_code or self.config.get("default_postal_code", ""),
            "postal_prefix": postal_prefix or "",
            "taxonomy_code": taxonomy_code or "",
            "taxonomy_description": taxonomy_description or self.config.get("default_taxonomy", ""),
            "enumeration_type": enumeration_type or "",
            "active_only": active_only,
        }
        self.imported_at = utc_now()
        self.import_batch_id = build_import_batch_id("bulk", self.filters, imported_at=self.imported_at)
        self.last_diagnostics: list[dict[str, Any]] = []

    def _append_diagnostic(self, event: str, message: str, *, rows_seen: int = 0) -> None:
        self.last_diagnostics.append(
            {
                "run_id": self.import_batch_id,
                "timestamp": self.imported_at,
                "event": event,
                "severity": "INFO",
                "message": message,
                "records_seen": rows_seen,
            }
        )

    def _matches_filters(self, row: dict[str, Any]) -> bool:
        state = str(self.filters["state"] or "").upper()
        city = str(self.filters["city"] or "").upper()
        postal_code = str(self.filters["postal_code"] or "")
        postal_prefix = str(self.filters["postal_prefix"] or "")
        taxonomy_code = str(self.filters["taxonomy_code"] or "").upper()
        taxonomy_description = str(self.filters["taxonomy_description"] or "").upper()
        enumeration_type = str(self.filters["enumeration_type"] or "")
        if state:
            state_values = {
                str(row.get("Provider Business Mailing Address State Name") or "").upper(),
                str(row.get("Provider Practice Location Address State Name") or "").upper(),
            }
            if state not in state_values:
                return False
        if city:
            city_values = {
                str(row.get("Provider Business Mailing Address City Name") or "").upper(),
                str(row.get("Provider Practice Location Address City Name") or "").upper(),
            }
            if city not in city_values:
                return False
        if postal_code:
            postal_values = {
                str(row.get("Provider Business Mailing Address Postal Code") or ""),
                str(row.get("Provider Practice Location Address Postal Code") or ""),
            }
            if not any(value.startswith(postal_code) for value in postal_values):
                return False
        if postal_prefix:
            postal_values = {
                str(row.get("Provider Business Mailing Address Postal Code") or ""),
                str(row.get("Provider Practice Location Address Postal Code") or ""),
            }
            if not any(value.startswith(postal_prefix) for value in postal_values):
                return False
        if taxonomy_code and str(row.get("Healthcare Provider Taxonomy Code_1") or "").upper() != taxonomy_code:
            return False
        if taxonomy_description and taxonomy_description not in str(row.get("Healthcare Provider Taxonomy Description_1") or "").upper():
            return False
        if enumeration_type:
            entity_type = str(row.get("Entity Type Code") or "")
            expected = enumeration_type.replace("NPI-", "")
            if expected != entity_type:
                return False
        if self.filters["active_only"] and str(row.get("NPI Deactivation Date") or "").strip():
            return False
        return True

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        if self.dry_run:
            print("NPPES bulk dry-run")
            print(f"  input={self.input_path}")
            print(f"  filters={json.dumps(self.filters, sort_keys=True)}")
            print(f"  output_dir={self.output_dir}")
            summary = {"source_name": SOURCE_NAME, "mode": "bulk", "records_read": 0, "providers_normalized": 0}
            manifest = {"connector_version": "6.0", "filters": self.filters, "result_status": "DRY_RUN"}
            return [], [], [], [], summary, manifest
        providers = []
        rows_read = 0
        for chunk in pd.read_csv(self.input_path, chunksize=self.chunk_size, dtype=str):
            chunk = chunk.fillna("")
            missing = REQUIRED_COLUMNS - set(chunk.columns)
            if missing:
                raise ValueError(f"NPPES bulk file missing required columns: {', '.join(sorted(missing))}")
            for row in chunk.to_dict("records"):
                rows_read += 1
                if not self._matches_filters(row):
                    continue
                if not str(row.get("NPI") or "").strip():
                    continue
                providers.append(
                    provider_from_bulk_row(
                        row,
                        imported_at=self.imported_at,
                        import_batch_id=self.import_batch_id,
                        source_file=str(self.input_path),
                    )
                )
                if len(providers) >= self.max_records:
                    break
            self._append_diagnostic("chunk_processed", f"NPPES bulk chunk processed rows={rows_read}", rows_seen=rows_read)
            if self.verbose:
                print(f"NPPES bulk: rows_read={rows_read} matched={len(providers)}")
            if len(providers) >= self.max_records:
                break
        deduped = {provider.npi: provider for provider in providers}
        provider_rows, entity_rows, relationship_rows, taxonomy_rows = providers_to_rows(list(deduped.values()), source_type_hint="manual")
        summary = {
            "source_name": SOURCE_NAME,
            "mode": "bulk",
            "records_read": rows_read,
            "providers_normalized": len(provider_rows),
            "individual_providers": sum(1 for row in provider_rows if str(row.get("entity_type_code", "")) == "1"),
            "organization_providers": sum(1 for row in provider_rows if str(row.get("entity_type_code", "")) == "2"),
            "practice_addresses": sum(1 for row in entity_rows if str(row.get("address_role", "")) == "practice"),
            "mailing_addresses": sum(1 for row in entity_rows if str(row.get("address_role", "")) == "mailing"),
            "taxonomy_records": len(taxonomy_rows),
            "deactivated_npis": sum(1 for row in provider_rows if str(row.get("deactivation_date", "")).strip()),
            "incomplete_records": sum(1 for row in provider_rows if str(row.get("incomplete_record", "")).lower() == "true"),
            "last_attempted_import": self.imported_at,
            "last_successful_import": self.imported_at,
            "filters": self.filters,
            "runtime_seconds": 0.0,
            "errors": "",
            "truncation_status": rows_read >= self.max_records,
            "duplicate_records_removed": max(len(providers) - len(deduped), 0),
        }
        manifest = {
            "connector_version": "6.0",
            "configuration_hash": self.import_batch_id,
            "filters": self.filters,
            "start_time": self.imported_at,
            "completion_time": utc_now(),
            "record_count": len(provider_rows),
            "input_file": str(self.input_path),
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
    parser = argparse.ArgumentParser(description="Process a locally downloaded official CMS NPPES CSV file.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--state", default=None)
    parser.add_argument("--city", default=None)
    parser.add_argument("--postal-code", default=None)
    parser.add_argument("--postal-prefix", default=None)
    parser.add_argument("--taxonomy-code", default=None)
    parser.add_argument("--taxonomy-description", default=None)
    parser.add_argument("--enumeration-type", default=None)
    parser.add_argument("--active-only", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=None)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    connector = NPPESBulkFileConnector(
        input_path=args.input,
        state=args.state,
        city=args.city,
        postal_code=args.postal_code,
        postal_prefix=args.postal_prefix,
        taxonomy_code=args.taxonomy_code,
        taxonomy_description=args.taxonomy_description,
        enumeration_type=args.enumeration_type,
        active_only=args.active_only,
        chunk_size=args.chunk_size,
        max_records=args.max_records,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
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
        f"NPPES bulk: providers={summary.get('providers_normalized', 0)} "
        f"individual={summary.get('individual_providers', 0)} organizations={summary.get('organization_providers', 0)}"
    )


if __name__ == "__main__":
    main()
