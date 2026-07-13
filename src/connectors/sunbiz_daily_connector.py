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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.cross_source import run_cross_source_correlation
from src.analytics.entity_builder import build_entity_graph
from src.analytics.entity_resolution.normalizers import normalize_address_value, normalize_person_name
from src.analytics.entity_resolution.resolver import resolve_entities
from src.connectors.api_base import APIConnectorBase
from src.connectors.source_manifest import REPO_ROOT, validate_source
from src.connectors.source_metadata import apply_provenance, infer_source_metadata
from src.normalize.address_normalizer import normalize_address

DEFAULT_CONFIG_PATH = Path("config/sunbiz_daily.json")
DEFAULT_OUTPUT_DIR = Path("data/processed")
DEFAULT_RAW_SNAPSHOT_DIR = Path("data/raw/sunbiz_daily")
DEFAULT_BUSINESSES_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_businesses.csv"
DEFAULT_ENTITIES_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_entities.csv"
DEFAULT_RELATIONSHIPS_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_relationships.csv"
DEFAULT_IMPORT_SUMMARY_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_import_summary.json"
DEFAULT_DIAGNOSTICS_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_import_diagnostics.csv"
DEFAULT_MATCHES_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_parcel_matches.csv"
DEFAULT_STATUS_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_daily_status.json"
LEGACY_ENTITIES_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_entities.csv"
LEGACY_RELATIONSHIPS_PATH = DEFAULT_OUTPUT_DIR / "sunbiz_relationships.csv"
DEFAULT_DB_PATH = Path("local_osint.duckdb")

BUSINESS_FIELDS = [
    "corporation_number",
    "corporation_name",
    "filing_type",
    "filing_type_display",
    "status",
    "file_date",
    "fei_number",
    "principal_address",
    "mailing_address",
    "registered_agent_name",
    "registered_agent_address",
    "officer_count",
    "privacy_redacted",
    "incomplete_record",
    "source_name",
    "source_type",
    "source_record_id",
    "source_url",
    "imported_at",
    "county",
    "city",
    "state",
    "zip",
]

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
    "source_url",
    "normalized_name",
    "original_name",
    "title",
    "officer_type",
    "position",
    "agent_type",
    "address_1",
    "address_2",
    "city",
    "state",
    "zip",
    "country",
    "address_role",
    "status",
    "filing_type",
    "filing_type_display",
    "file_date",
    "corporation_number",
    "privacy_redacted",
    "incomplete_record",
]

RELATIONSHIP_FIELDS = [
    "relationship_id",
    "source_entity_id",
    "target_entity_id",
    "relationship_type",
    "confidence",
    "relationship_method",
    "evidence_summary",
    "source_name",
    "source_type",
    "source_record_id",
    "connector_name",
    "import_batch_id",
    "imported_at",
    "jurisdiction",
    "is_synthetic",
    "corporation_number",
]

DIAGNOSTIC_FIELDS = [
    "run_id",
    "timestamp",
    "event",
    "severity",
    "message",
    "http_status",
    "page",
    "job_id",
    "records_seen",
    "rate_limit_limit",
    "rate_limit_remaining",
    "retry_after_seconds",
    "truncated",
]


class MissingAPIKeyError(RuntimeError):
    pass


class ConnectorDisabledError(RuntimeError):
    pass


class LiveAccessNotApprovedError(RuntimeError):
    pass


class RateLimitExceededError(RuntimeError):
    pass


class PollingExpiredError(RuntimeError):
    pass


@dataclass
class RateLimitState:
    limit: int | None = None
    remaining: int | None = None
    retry_after_seconds: float | None = None


@dataclass
class RequestResult:
    status_code: int
    payload: Any
    headers: dict[str, Any]
    url: str


def load_sunbiz_daily_config(config_path: Path | str | None = None) -> dict[str, Any]:
    path_value = config_path or os.getenv("OPENFRAUD_SUNBIZ_DAILY_CONFIG_PATH") or DEFAULT_CONFIG_PATH
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("Sunbiz Daily config must be a JSON object.")
    if "list_endpoint" not in data and "endpoint" in data:
        data["list_endpoint"] = data.get("endpoint")
    if "api_key_env" not in data and "api_key_env_var" in data:
        data["api_key_env"] = data.get("api_key_env_var")
    if "default_county" not in data and "county_filter" in data:
        data["default_county"] = data.get("county_filter")
    if "timeout_seconds" not in data and "timeout" in data:
        data["timeout_seconds"] = data.get("timeout")
    if "retry_backoff_seconds" not in data and "retry_backoff" in data:
        data["retry_backoff_seconds"] = data.get("retry_backoff")
    if "rate_limit_per_hour" not in data and "max_requests_per_hour" in data:
        data["rate_limit_per_hour"] = data.get("max_requests_per_hour")
    return data


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _stringify(value: Any) -> str:
    return str(value or "").strip()


def _normalize_person(value: str) -> str:
    return normalize_person_name(value).get("normalized_value", "")


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _make_request_url(base_url: str, path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        parsed = urllib.parse.urlparse(path_or_url)
        base = urllib.parse.urlparse(base_url)
        if parsed.netloc != base.netloc:
            raise ValueError(f"Refusing to follow poll URL outside configured host: {path_or_url}")
        return path_or_url
    return urllib.parse.urljoin(base_url.rstrip("/") + "/", path_or_url.lstrip("/"))


def _response_charset(response: Any) -> str:
    headers = getattr(response, "headers", None)
    if headers is not None and hasattr(headers, "get_content_charset"):
        return headers.get_content_charset() or "utf-8"
    return "utf-8"


def _response_status(response: Any) -> int:
    if hasattr(response, "status"):
        return int(getattr(response, "status"))
    if hasattr(response, "getcode"):
        return int(response.getcode())
    return 200


def _headers_to_dict(headers: Any) -> dict[str, Any]:
    if headers is None:
        return {}
    if isinstance(headers, dict):
        return dict(headers)
    if hasattr(headers, "items"):
        return {str(key): value for key, value in headers.items()}
    return {}


def _extract_records_and_pagination(payload: Any) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)], {}, "bare_list"
    if not isinstance(payload, dict):
        raise ValueError("Sunbiz Daily payload must be a JSON object or list.")
    for key in ("filings", "results", "data", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)], payload.get("pagination", {}), key
    nested_result = payload.get("result")
    if isinstance(nested_result, dict):
        for key in ("filings", "results", "data", "items"):
            value = nested_result.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)], nested_result.get("pagination", {}), f"result.{key}"
    raise ValueError("Sunbiz Daily response did not contain a supported records envelope.")


def _build_address_text(address_value: Any) -> str:
    if isinstance(address_value, dict):
        ordered_fields = [
            "address_1",
            "address_2",
            "city",
            "state",
            "zip",
            "postal_code",
            "country",
            "line1",
            "line2",
        ]
        parts = [_stringify(address_value.get(field)) for field in ordered_fields if _stringify(address_value.get(field))]
        if not parts:
            parts = [_stringify(value) for value in address_value.values() if _stringify(value)]
        return normalize_address(", ".join(parts))
    if isinstance(address_value, list):
        return normalize_address(", ".join(_stringify(item) for item in address_value if _stringify(item)))
    return normalize_address(_stringify(address_value))


def _parse_address(address_value: Any, *, role: str) -> dict[str, str]:
    text = _build_address_text(address_value)
    if not text:
        return {
            "address_1": "",
            "address_2": "",
            "city": "",
            "state": "",
            "zip": "",
            "country": "",
            "normalized_full_address": "",
            "address_role": role,
        }
    if isinstance(address_value, dict):
        return {
            "address_1": _stringify(address_value.get("address_1") or address_value.get("line1")),
            "address_2": _stringify(address_value.get("address_2") or address_value.get("line2")),
            "city": _stringify(address_value.get("city")),
            "state": _stringify(address_value.get("state")),
            "zip": _stringify(address_value.get("zip") or address_value.get("postal_code")),
            "country": _stringify(address_value.get("country")),
            "normalized_full_address": text,
            "address_role": role,
        }
    return {
        "address_1": text,
        "address_2": "",
        "city": "",
        "state": "",
        "zip": normalize_address_value(text).get("zip_code", ""),
        "country": "",
        "normalized_full_address": text,
        "address_role": role,
    }


def _parse_officers(value: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if value is None:
        return rows
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                name = _stringify(item.get("name") or item.get("full_name"))
                if not name:
                    continue
                rows.append(
                    {
                        "original_name": name,
                        "normalized_name": _normalize_person(name),
                        "title": _stringify(item.get("title")),
                        "officer_type": _stringify(item.get("officer_type") or item.get("type")),
                        "position": _stringify(item.get("position")),
                        "address": _build_address_text(item.get("address")),
                    }
                )
            else:
                name = _stringify(item)
                if name:
                    rows.append(
                        {
                            "original_name": name,
                            "normalized_name": _normalize_person(name),
                            "title": "",
                            "officer_type": "",
                            "position": "",
                            "address": "",
                        }
                    )
        return rows
    if isinstance(value, dict):
        return _parse_officers([value])
    text = _stringify(value)
    if not text:
        return rows
    for token in [part.strip() for part in text.replace("|", ";").split(";") if part.strip()]:
        rows.append(
            {
                "original_name": token,
                "normalized_name": _normalize_person(token),
                "title": "",
                "officer_type": "",
                "position": "",
                "address": "",
            }
        )
    return rows


class SunbizDailyClient:
    def __init__(
        self,
        *,
        config: dict[str, Any],
        requester: Any | None = None,
        diagnostics_callback: Any | None = None,
        raw_snapshot_callback: Any | None = None,
        verbose: bool = False,
    ) -> None:
        self.config = config
        self.requester = requester or urllib.request.urlopen
        self.diagnostics_callback = diagnostics_callback
        self.raw_snapshot_callback = raw_snapshot_callback
        self.verbose = verbose
        self.base_url = str(config.get("base_url", "")).rstrip("/")
        self.list_endpoint = str(config.get("list_endpoint", "")).strip()
        self.detail_endpoint_template = str(config.get("detail_endpoint_template", "")).strip()
        self.timeout_seconds = float(config.get("timeout_seconds", 30))
        self.retry_attempts = int(config.get("retry_attempts", 3))
        self.retry_backoff_seconds = float(config.get("retry_backoff_seconds", 1.0))
        self.poll_interval_seconds = float(config.get("poll_interval_seconds", 2))
        self.max_poll_attempts = int(config.get("max_poll_attempts", 60))
        self.rate_limit_per_hour = int(config.get("rate_limit_per_hour", 1000))
        self.last_request_started_at = 0.0
        self.rate_limit_state = RateLimitState(limit=self.rate_limit_per_hour)
        self.api_key_env = str(config.get("api_key_env", "SUNBIZ_DAILY_API_KEY"))
        self.api_key = os.getenv(self.api_key_env, "").strip()

    def is_key_present(self) -> bool:
        return bool(self.api_key)

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message)

    def _emit_diagnostic(self, **payload: Any) -> None:
        if self.diagnostics_callback is not None:
            self.diagnostics_callback(**payload)

    def _snapshot(self, **payload: Any) -> None:
        if self.raw_snapshot_callback is not None:
            self.raw_snapshot_callback(**payload)

    def _headers(self) -> dict[str, str]:
        return {
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        }

    def _apply_minimum_interval(self) -> None:
        if self.rate_limit_per_hour <= 0:
            return
        minimum_interval = 3600.0 / float(self.rate_limit_per_hour)
        elapsed = time.time() - self.last_request_started_at
        if elapsed < minimum_interval:
            time.sleep(minimum_interval - elapsed)
        self.last_request_started_at = time.time()

    def _request_json(self, url: str, *, event: str, page: int | None = None, job_id: str = "") -> RequestResult:
        last_error: Exception | None = None
        for attempt in range(1, self.retry_attempts + 1):
            self._apply_minimum_interval()
            request = urllib.request.Request(url, headers=self._headers(), method="GET")
            try:
                with self.requester(request, timeout=self.timeout_seconds) as response:
                    headers = _headers_to_dict(getattr(response, "headers", None))
                    status_code = _response_status(response)
                    charset = _response_charset(response)
                    body = response.read().decode(charset)
                    payload = json.loads(body)
                    self._record_rate_limit(headers)
                    self._emit_diagnostic(
                        event=event,
                        severity="INFO",
                        message=f"HTTP {status_code} {event}",
                        http_status=status_code,
                        page=page or "",
                        job_id=job_id,
                        rate_limit_limit=self.rate_limit_state.limit or "",
                        rate_limit_remaining=self.rate_limit_state.remaining or "",
                        retry_after_seconds=self.rate_limit_state.retry_after_seconds or "",
                        truncated=bool(isinstance(payload, dict) and payload.get("truncated")),
                    )
                    self._snapshot(
                        event=event,
                        status_code=status_code,
                        url=url,
                        page=page,
                        job_id=job_id,
                        payload=payload,
                        headers=headers,
                    )
                    return RequestResult(status_code=status_code, payload=payload, headers=headers, url=url)
            except urllib.error.HTTPError as exc:
                headers = _headers_to_dict(getattr(exc, "headers", None))
                self._record_rate_limit(headers)
                body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
                payload: Any
                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    payload = {"raw_body": body}
                self._emit_diagnostic(
                    event=event,
                    severity="ERROR",
                    message=f"HTTP {exc.code} {event}",
                    http_status=exc.code,
                    page=page or "",
                    job_id=job_id,
                    rate_limit_limit=self.rate_limit_state.limit or "",
                    rate_limit_remaining=self.rate_limit_state.remaining or "",
                    retry_after_seconds=self.rate_limit_state.retry_after_seconds or "",
                    truncated=bool(isinstance(payload, dict) and payload.get("truncated")),
                )
                self._snapshot(
                    event=event,
                    status_code=exc.code,
                    url=url,
                    page=page,
                    job_id=job_id,
                    payload=payload,
                    headers=headers,
                )
                if exc.code == 429:
                    retry_after = self.rate_limit_state.retry_after_seconds
                    if retry_after is not None and attempt < self.retry_attempts:
                        time.sleep(retry_after)
                        continue
                    raise RateLimitExceededError("Sunbiz Daily API rate limit reached. Already-downloaded data has been preserved.")
                if exc.code == 401:
                    raise MissingAPIKeyError(
                        f"Missing or invalid {self.api_key_env}. Set it in your local .env file before running live Sunbiz Daily imports."
                    )
                if exc.code == 410:
                    raise PollingExpiredError("Sunbiz Daily asynchronous job expired before results were retrieved.")
                if exc.code in {422, 500}:
                    if attempt < self.retry_attempts and exc.code == 500:
                        time.sleep(self.retry_backoff_seconds * attempt)
                        continue
                    raise RuntimeError(f"Sunbiz Daily API request failed with HTTP {exc.code}.")
                raise RuntimeError(f"Sunbiz Daily API request failed with HTTP {exc.code}.")
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(f"Sunbiz Daily request failed after {self.retry_attempts} attempts: {last_error}")

    def _record_rate_limit(self, headers: dict[str, Any]) -> None:
        limit = headers.get("X-RateLimit-Limit") or headers.get("x-ratelimit-limit")
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining")
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if limit not in (None, ""):
            self.rate_limit_state.limit = _safe_int(limit, self.rate_limit_state.limit or self.rate_limit_per_hour)
        if remaining not in (None, ""):
            self.rate_limit_state.remaining = _safe_int(remaining, self.rate_limit_state.remaining or 0)
        if retry_after not in (None, ""):
            try:
                self.rate_limit_state.retry_after_seconds = float(retry_after)
            except (TypeError, ValueError):
                self.rate_limit_state.retry_after_seconds = None

    def get_rate_limit_state(self) -> dict[str, Any]:
        return {
            "limit": self.rate_limit_state.limit,
            "remaining": self.rate_limit_state.remaining,
            "retry_after_seconds": self.rate_limit_state.retry_after_seconds,
        }

    def list_filings(self, query_params: dict[str, Any]) -> RequestResult:
        encoded = urllib.parse.urlencode(query_params, doseq=True)
        url = f"{self.base_url}{self.list_endpoint}?{encoded}"
        result = self._request_json(url, event="list_filings", page=_safe_int(query_params.get("page"), 1))
        if result.status_code == 202:
            job_payload = result.payload if isinstance(result.payload, dict) else {}
            return self.poll_job(job_payload)
        return result

    def get_filing(self, corporation_number: str) -> RequestResult:
        url = f"{self.base_url}{self.detail_endpoint_template.format(corporation_number=corporation_number)}"
        result = self._request_json(url, event="get_filing_detail")
        if result.status_code == 202:
            job_payload = result.payload if isinstance(result.payload, dict) else {}
            return self.poll_job(job_payload)
        return result

    def poll_job(self, payload: dict[str, Any]) -> RequestResult:
        job_id = _stringify(payload.get("job_id"))
        poll_url = _stringify(payload.get("poll_url"))
        if not job_id or not poll_url:
            raise RuntimeError("Sunbiz Daily asynchronous job payload was malformed.")
        poll_request_url = _make_request_url(self.base_url, poll_url)
        for attempt in range(1, self.max_poll_attempts + 1):
            result = self._request_json(poll_request_url, event="poll_job", job_id=job_id)
            job_payload = result.payload if isinstance(result.payload, dict) else {}
            status = _stringify(job_payload.get("status")).lower()
            self._log(f"Sunbiz Daily: poll job {job_id} attempt={attempt} status={status}")
            if status == "done":
                return result
            if status not in {"queued", "running"}:
                raise RuntimeError(f"Sunbiz Daily job {job_id} returned unsupported status '{status}'.")
            time.sleep(self.poll_interval_seconds)
        raise RuntimeError(f"Sunbiz Daily job {job_id} exceeded max poll attempts ({self.max_poll_attempts}).")

    def iter_filings(
        self,
        *,
        filters: dict[str, Any],
        max_pages: int,
        max_records: int,
        per_page: int,
        detail_lookups: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        page = 1
        seen: set[str] = set()
        collected: list[dict[str, Any]] = []
        truncated = False
        async_jobs = 0
        response_shapes: list[str] = []
        incomplete_reason = ""

        while page <= max_pages and page <= 100 and len(collected) < max_records:
            query_params = dict(filters)
            query_params["page"] = page
            query_params["per_page"] = min(max(per_page, 1), 100)
            try:
                result = self.list_filings(query_params)
            except RateLimitExceededError as exc:
                incomplete_reason = str(exc)
                break
            if result.status_code == 202:
                async_jobs += 1
            records, pagination, response_shape = _extract_records_and_pagination(result.payload)
            response_shapes.append(response_shape)
            if isinstance(result.payload, dict) and result.payload.get("status") == "done":
                async_jobs += 1
            if isinstance(result.payload, dict) and result.payload.get("truncated"):
                truncated = True
            if pagination.get("total_capped") is True:
                truncated = True

            page_added = 0
            for record in records:
                corporation_number = _stringify(
                    record.get("corporation_number") or record.get("document_number") or record.get("corporate_number")
                )
                if not corporation_number or corporation_number in seen:
                    continue
                if detail_lookups:
                    try:
                        detail_result = self.get_filing(corporation_number)
                        detail_records, _, _ = _extract_records_and_pagination(detail_result.payload)
                        if detail_records:
                            record = detail_records[0]
                    except Exception:
                        pass
                seen.add(corporation_number)
                collected.append(record)
                page_added += 1
                if len(collected) >= max_records:
                    truncated = True
                    break

            self._emit_diagnostic(
                event="page_processed",
                severity="INFO",
                message=f"Sunbiz Daily page {page} processed",
                http_status=result.status_code,
                page=page,
                job_id="",
                records_seen=page_added,
                rate_limit_limit=self.rate_limit_state.limit or "",
                rate_limit_remaining=self.rate_limit_state.remaining or "",
                retry_after_seconds=self.rate_limit_state.retry_after_seconds or "",
                truncated=truncated,
            )
            print(f"Sunbiz Daily: page={page} records={page_added} total={len(collected)}")

            total_pages = _safe_int(pagination.get("total_pages"), 0)
            if not records:
                break
            if total_pages and page >= total_pages:
                break
            if page >= 100:
                truncated = True
                break
            page += 1

        return collected, {
            "response_shapes": sorted(set(response_shapes)),
            "async_jobs": async_jobs,
            "truncated_results": truncated,
            "incomplete_reason": incomplete_reason,
            "records_fetched": len(collected),
        }


class SunbizDailyConnector(APIConnectorBase):
    def __init__(
        self,
        *,
        county: str | None = None,
        city: str | None = None,
        state: str | None = None,
        zip_code: str | None = None,
        limit: int | None = None,
        status: str | None = None,
        period: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        filing_type: str | None = None,
        corporation_name: str | None = None,
        corporation_number: str | None = None,
        officer_name: str | None = None,
        registered_agent_name: str | None = None,
        page_size: int | None = None,
        max_pages: int | None = None,
        max_records: int | None = None,
        detail_lookups: bool = False,
        use_mock: bool = False,
        dry_run: bool = False,
        output_dir: Path | str | None = None,
        verbose: bool = False,
        config_path: Path | str | None = None,
        requester: Any | None = None,
    ) -> None:
        load_dotenv(REPO_ROOT / ".env")
        self.config_path = Path(config_path or os.getenv("OPENFRAUD_SUNBIZ_DAILY_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
        if not self.config_path.is_absolute():
            self.config_path = REPO_ROOT / self.config_path
        self.source_config = load_sunbiz_daily_config(self.config_path)
        self.source_name = str(self.source_config.get("source_name", "sunbiz_daily")).strip()
        self.base_url = str(self.source_config.get("base_url", "")).rstrip("/")
        self.endpoint = str(self.source_config.get("list_endpoint", "")).strip()
        self.query_params = {}
        self.use_mock = use_mock or bool(self.source_config.get("prefer_mock_response", False))
        self.dry_run = dry_run
        self.detail_lookups = detail_lookups
        self.verbose = verbose
        self.output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR)
        if not self.output_dir.is_absolute():
            self.output_dir = REPO_ROOT / self.output_dir
        self.raw_snapshot_dir = Path(self.source_config.get("raw_snapshot_dir", DEFAULT_RAW_SNAPSHOT_DIR))
        if not self.raw_snapshot_dir.is_absolute():
            self.raw_snapshot_dir = REPO_ROOT / self.raw_snapshot_dir

        if self.use_mock:
            validate_source(self.source_name)
        else:
            if not bool(self.source_config.get("enabled", False)):
                raise ConnectorDisabledError(
                    "Sunbiz Daily live access is disabled in config/sunbiz_daily.json. Use --mock for local validation or enable the connector intentionally after review."
                )
            validate_source(self.source_name, require_live_access=True)

        self.imported_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.run_id = f"{self.source_name}:{self.imported_at.replace(':', '').replace('-', '')}"
        self.mock_response_path = str(self.source_config.get("mock_response_path", "")).strip()
        self.filters = {
            "sort": str(self.source_config.get("default_sort", "file_date")),
            "order": str(self.source_config.get("default_order", "desc")),
            "county": county or str(self.source_config.get("default_county", "Hillsborough")),
            "city": city or "",
            "state": state or str(self.source_config.get("default_state", "FL")),
            "zip": zip_code or "",
            "status": status or str(self.source_config.get("default_status", "active")),
            "period": period or "",
            "start_date": start_date or from_date or "",
            "end_date": end_date or to_date or "",
            "filing_type": filing_type or "",
            "corporation_name": corporation_name or "",
            "corporation_number": corporation_number or "",
            "officer_name": officer_name or "",
            "registered_agent_name": registered_agent_name or "",
        }
        self.default_page_size = min(max(_safe_int(page_size, _safe_int(self.source_config.get("default_page_size"), 100)), 1), 100)
        self.max_pages = min(max(_safe_int(max_pages, _safe_int(self.source_config.get("max_pages"), 10)), 1), 100)
        max_records_value = max_records if max_records is not None else limit
        self.max_records = max(_safe_int(max_records_value, _safe_int(self.source_config.get("max_records"), 1000)), 1)
        self.client = SunbizDailyClient(
            config=self.source_config,
            requester=requester,
            diagnostics_callback=self._append_diagnostic,
            raw_snapshot_callback=self._write_raw_snapshot,
            verbose=verbose,
        )
        self.last_business_rows: list[dict[str, Any]] = []
        self.last_diagnostics: list[dict[str, Any]] = []
        self.last_summary: dict[str, Any] = {}
        self.last_records: list[dict[str, Any]] = []

    def _append_diagnostic(self, **payload: Any) -> None:
        row = {field: "" for field in DIAGNOSTIC_FIELDS}
        row.update(payload)
        row["run_id"] = self.run_id
        row["timestamp"] = self.imported_at
        self.last_diagnostics.append(row)

    def _write_raw_snapshot(self, *, event: str, status_code: int, url: str, page: int | None, job_id: str, payload: Any, headers: dict[str, Any]) -> None:
        self.raw_snapshot_dir.mkdir(parents=True, exist_ok=True)
        suffix = f"{event}"
        if job_id:
            suffix += f"_job_{job_id}"
        elif page is not None:
            suffix += f"_page_{page:03d}"
        filename = f"{self.imported_at.replace(':', '').replace('-', '')}_{suffix}.json"
        snapshot = {
            "request_timestamp": self.imported_at,
            "filters_used": self.filters,
            "page_number": page,
            "job_id": job_id,
            "response_status": status_code,
            "source_url": url,
            "rate_limit_metadata": {
                "X-RateLimit-Limit": headers.get("X-RateLimit-Limit") or headers.get("x-ratelimit-limit"),
                "X-RateLimit-Remaining": headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining"),
                "Retry-After": headers.get("Retry-After") or headers.get("retry-after"),
            },
            "raw_json_payload": payload,
        }
        with (self.raw_snapshot_dir / filename).open("w", encoding="utf-8") as handle:
            json.dump(snapshot, handle, indent=2, sort_keys=True)

    def fetch(self) -> str:
        if self.use_mock:
            return json.dumps(self._read_mock_records())
        if not self.client.is_key_present():
            raise MissingAPIKeyError(
                f"Missing {self.client.api_key_env}. Add `SUNBIZ_DAILY_API_KEY=YOUR_SUNBIZ_DAILY_API_KEY_HERE` to your local .env file before running live Sunbiz Daily imports."
            )
        records, fetch_summary = self.client.iter_filings(
            filters={key: value for key, value in self.filters.items() if value not in {"", None}},
            max_pages=self.max_pages,
            max_records=self.max_records,
            per_page=self.default_page_size,
            detail_lookups=self.detail_lookups,
        )
        self.last_summary.update(fetch_summary)
        return json.dumps(records)

    def _read_mock_records(self) -> list[dict[str, Any]]:
        if not self.mock_response_path:
            raise RuntimeError("Sunbiz Daily mock mode requires mock_response_path in config/sunbiz_daily.json.")
        path = Path(self.mock_response_path)
        if not path.is_absolute():
            path = REPO_ROOT / path
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        records, _, response_shape = _extract_records_and_pagination(payload)
        self.last_summary.update(
            {
                "response_shapes": [response_shape],
                "async_jobs": 0,
                "truncated_results": False,
                "incomplete_reason": "",
                "records_fetched": min(len(records), self.max_records),
            }
        )
        self._append_diagnostic(
            event="mock_fetch",
            severity="INFO",
            message="Loaded Sunbiz Daily mocked response.",
            http_status=200,
            page="",
            job_id="",
            records_seen=min(len(records), self.max_records),
            rate_limit_limit="",
            rate_limit_remaining="",
            retry_after_seconds="",
            truncated=False,
        )
        return records[: self.max_records]

    def parse(self, payload: str) -> list[dict[str, Any]]:
        parsed = json.loads(payload)
        if not isinstance(parsed, list):
            raise ValueError("Sunbiz Daily fetch output must be a JSON list after pagination flattening.")
        self.last_records = [row for row in parsed if isinstance(row, dict)]
        return self.last_records

    def normalize(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []
        for record in records:
            corporation_number = _stringify(record.get("corporation_number") or record.get("document_number") or record.get("corporate_number"))
            corporation_name = _stringify(record.get("corporation_name") or record.get("business_name") or record.get("name"))
            if not corporation_number or not corporation_name:
                continue
            raw_mailing_address = record.get("mailing_address")
            principal_address = _parse_address(record.get("principal_address"), role="principal_address")
            mailing_address = _parse_address(record.get("mailing_address"), role="mailing_address")
            registered_agent_payload = record.get("registered_agent") or {}
            registered_agent_name = _stringify(
                record.get("registered_agent_name")
                or (registered_agent_payload.get("name") if isinstance(registered_agent_payload, dict) else "")
            )
            registered_agent_address = _parse_address(
                record.get("registered_agent_address")
                or (registered_agent_payload.get("address") if isinstance(registered_agent_payload, dict) else ""),
                role="registered_agent_address",
            )
            officers = _parse_officers(record.get("officers"))
            privacy_redacted = bool(record.get("privacy_redacted") or record.get("redacted"))
            incomplete_record = bool(
                privacy_redacted
                or not officers
                or not registered_agent_name
                or not principal_address["normalized_full_address"]
                or raw_mailing_address is None
                or raw_mailing_address == ""
            )
            normalized_rows.append(
                {
                    "corporation_number": corporation_number,
                    "corporation_name": corporation_name,
                    "filing_type": _stringify(record.get("filing_type")),
                    "filing_type_display": _stringify(record.get("filing_type_display")),
                    "status": _stringify(record.get("status")),
                    "file_date": _stringify(record.get("file_date") or record.get("filing_date")),
                    "fei_number": _stringify(record.get("fei_number")),
                    "principal_address": principal_address,
                    "mailing_address": mailing_address,
                    "registered_agent_name": registered_agent_name,
                    "registered_agent_normalized_name": _normalize_person(registered_agent_name),
                    "registered_agent_address": registered_agent_address,
                    "registered_agent_type": _stringify(
                        record.get("registered_agent_type")
                        or (registered_agent_payload.get("agent_type") if isinstance(registered_agent_payload, dict) else "")
                    ),
                    "officers": officers,
                    "source_url": f"{self.base_url}{self.source_config.get('detail_endpoint_template', '').format(corporation_number=corporation_number)}",
                    "imported_at": self.imported_at,
                    "county": _stringify(record.get("county")) or _stringify(self.filters.get("county")),
                    "city": _stringify(record.get("city")) or _stringify(principal_address.get("city")) or _stringify(self.filters.get("city")),
                    "state": _stringify(record.get("state")) or _stringify(principal_address.get("state")) or _stringify(self.filters.get("state")),
                    "zip": _stringify(record.get("zip")) or _stringify(principal_address.get("zip")) or _stringify(self.filters.get("zip")),
                    "privacy_redacted": privacy_redacted,
                    "incomplete_record": incomplete_record,
                }
            )
        return normalized_rows

    def build_business_rows(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source_type = infer_source_metadata(self.source_name)["source_type"]
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in records:
            corporation_number = record["corporation_number"]
            if corporation_number in seen:
                continue
            seen.add(corporation_number)
            rows.append(
                {
                    "corporation_number": corporation_number,
                    "corporation_name": record["corporation_name"],
                    "filing_type": record["filing_type"],
                    "filing_type_display": record["filing_type_display"],
                    "status": record["status"],
                    "file_date": record["file_date"],
                    "fei_number": record["fei_number"],
                    "principal_address": record["principal_address"]["normalized_full_address"],
                    "mailing_address": record["mailing_address"]["normalized_full_address"],
                    "registered_agent_name": record["registered_agent_name"],
                    "registered_agent_address": record["registered_agent_address"]["normalized_full_address"],
                    "officer_count": len(record["officers"]),
                    "privacy_redacted": record["privacy_redacted"],
                    "incomplete_record": record["incomplete_record"],
                    "source_name": self.source_name,
                    "source_type": source_type,
                    "source_record_id": corporation_number,
                    "source_url": record["source_url"],
                    "imported_at": record["imported_at"],
                    "county": record["county"],
                    "city": record["city"],
                    "state": record["state"],
                    "zip": record["zip"],
                }
            )
        return rows

    def to_entities(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        entities: list[dict[str, Any]] = []
        seen: set[str] = set()
        source_type = infer_source_metadata(self.source_name)["source_type"]
        for record in records:
            county = record["county"]
            imported_at = record["imported_at"]
            business_entity = apply_provenance(
                {
                    "entity_id": f"business:sunbiz_daily:{record['corporation_number']}",
                    "display_name": record["corporation_name"],
                    "entity_type": "business",
                    "source": self.source_name,
                    "source_url": record["source_url"],
                    "normalized_name": "",
                    "original_name": record["corporation_name"],
                    "title": "",
                    "officer_type": "",
                    "position": "",
                    "agent_type": "",
                    "address_1": "",
                    "address_2": "",
                    "city": record["city"],
                    "state": record["state"],
                    "zip": record["zip"],
                    "country": "",
                    "address_role": "",
                    "status": record["status"],
                    "filing_type": record["filing_type"],
                    "filing_type_display": record["filing_type_display"],
                    "file_date": record["file_date"],
                    "corporation_number": record["corporation_number"],
                    "privacy_redacted": str(record["privacy_redacted"]).lower(),
                    "incomplete_record": str(record["incomplete_record"]).lower(),
                },
                self.source_name,
                source_type_hint=source_type,
                source_record_id=record["corporation_number"],
                connector_name=self.source_name,
                imported_at=imported_at,
                jurisdiction=county,
            )
            if business_entity["entity_id"] not in seen:
                seen.add(business_entity["entity_id"])
                entities.append(business_entity)

            for address_payload in [record["principal_address"], record["mailing_address"], record["registered_agent_address"]]:
                address = address_payload["normalized_full_address"]
                if not address:
                    continue
                address_entity = apply_provenance(
                    {
                        "entity_id": f"address:{address}",
                        "display_name": address,
                        "entity_type": "address",
                        "source": self.source_name,
                        "source_url": record["source_url"],
                        "normalized_name": "",
                        "original_name": address,
                        "title": "",
                        "officer_type": "",
                        "position": "",
                        "agent_type": "",
                        "address_1": address_payload["address_1"],
                        "address_2": address_payload["address_2"],
                        "city": address_payload["city"] or record["city"],
                        "state": address_payload["state"] or record["state"],
                        "zip": address_payload["zip"] or record["zip"],
                        "country": address_payload["country"],
                        "address_role": address_payload["address_role"],
                        "status": record["status"],
                        "filing_type": record["filing_type"],
                        "filing_type_display": record["filing_type_display"],
                        "file_date": record["file_date"],
                        "corporation_number": record["corporation_number"],
                        "privacy_redacted": str(record["privacy_redacted"]).lower(),
                        "incomplete_record": str(record["incomplete_record"]).lower(),
                    },
                    self.source_name,
                    source_type_hint=source_type,
                    source_record_id=f"{record['corporation_number']}:{address_payload['address_role']}",
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
                        "entity_id": f"registered_agent:{record['registered_agent_normalized_name'] or record['registered_agent_name']}",
                        "display_name": record["registered_agent_name"],
                        "entity_type": "registered_agent",
                        "source": self.source_name,
                        "source_url": record["source_url"],
                        "normalized_name": record["registered_agent_normalized_name"],
                        "original_name": record["registered_agent_name"],
                        "title": "",
                        "officer_type": "",
                        "position": "",
                        "agent_type": record["registered_agent_type"],
                        "address_1": "",
                        "address_2": "",
                        "city": record["city"],
                        "state": record["state"],
                        "zip": record["zip"],
                        "country": "",
                        "address_role": "",
                        "status": record["status"],
                        "filing_type": record["filing_type"],
                        "filing_type_display": record["filing_type_display"],
                        "file_date": record["file_date"],
                        "corporation_number": record["corporation_number"],
                        "privacy_redacted": str(record["privacy_redacted"]).lower(),
                        "incomplete_record": str(record["incomplete_record"]).lower(),
                    },
                    self.source_name,
                    source_type_hint=source_type,
                    source_record_id=f"{record['corporation_number']}:registered_agent",
                    connector_name=self.source_name,
                    imported_at=imported_at,
                    jurisdiction=county,
                )
                if agent_entity["entity_id"] not in seen:
                    seen.add(agent_entity["entity_id"])
                    entities.append(agent_entity)

            for index, officer in enumerate(record["officers"], start=1):
                officer_name = officer["original_name"]
                if not officer_name:
                    continue
                officer_entity = apply_provenance(
                    {
                        "entity_id": f"officer:{officer['normalized_name'] or officer_name}",
                        "display_name": officer_name,
                        "entity_type": "officer",
                        "source": self.source_name,
                        "source_url": record["source_url"],
                        "normalized_name": officer["normalized_name"],
                        "original_name": officer_name,
                        "title": officer["title"],
                        "officer_type": officer["officer_type"],
                        "position": officer["position"],
                        "agent_type": "",
                        "address_1": "",
                        "address_2": "",
                        "city": record["city"],
                        "state": record["state"],
                        "zip": record["zip"],
                        "country": "",
                        "address_role": "",
                        "status": record["status"],
                        "filing_type": record["filing_type"],
                        "filing_type_display": record["filing_type_display"],
                        "file_date": record["file_date"],
                        "corporation_number": record["corporation_number"],
                        "privacy_redacted": str(record["privacy_redacted"]).lower(),
                        "incomplete_record": str(record["incomplete_record"]).lower(),
                    },
                    self.source_name,
                    source_type_hint=source_type,
                    source_record_id=f"{record['corporation_number']}:officer:{index}",
                    connector_name=self.source_name,
                    imported_at=imported_at,
                    jurisdiction=county,
                )
                if officer_entity["entity_id"] not in seen:
                    seen.add(officer_entity["entity_id"])
                    entities.append(officer_entity)
        return entities

    def to_relationships(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        relationships: list[dict[str, Any]] = []
        seen: set[str] = set()
        source_type = infer_source_metadata(self.source_name)["source_type"]
        for record in records:
            corporation_number = record["corporation_number"]
            imported_at = record["imported_at"]
            jurisdiction = record["county"]
            business_id = f"business:sunbiz_daily:{corporation_number}"
            principal_address = record["principal_address"]["normalized_full_address"]
            mailing_address = record["mailing_address"]["normalized_full_address"]
            registered_agent_id = f"registered_agent:{record['registered_agent_normalized_name'] or record['registered_agent_name']}" if record["registered_agent_name"] else ""

            base_provenance = {
                "source_type_hint": source_type,
                "connector_name": self.source_name,
                "imported_at": imported_at,
                "jurisdiction": jurisdiction,
            }

            def add_relationship(row: dict[str, Any], source_record_id: str) -> None:
                relationship = apply_provenance(
                    row,
                    self.source_name,
                    source_record_id=source_record_id,
                    **base_provenance,
                )
                relationship_id = "|".join(
                    [
                        self.source_name,
                        corporation_number,
                        relationship["relationship_type"],
                        relationship["source_entity_id"],
                        relationship["target_entity_id"],
                    ]
                )
                relationship["relationship_id"] = relationship_id
                relationship["corporation_number"] = corporation_number
                if relationship_id not in seen:
                    seen.add(relationship_id)
                    relationships.append(relationship)

            if principal_address:
                add_relationship(
                    {
                        "source_entity_id": business_id,
                        "target_entity_id": f"address:{principal_address}",
                        "relationship_type": "BUSINESS_PRINCIPAL_ADDRESS",
                        "confidence": 1.0,
                        "relationship_method": "sunbiz_principal_address",
                        "evidence_summary": "Business principal address provided directly by Sunbiz Daily filing data.",
                    },
                    corporation_number,
                )
                add_relationship(
                    {
                        "source_entity_id": business_id,
                        "target_entity_id": f"address:{principal_address}",
                        "relationship_type": "BUSINESS_LOCATED_AT",
                        "confidence": 1.0,
                        "relationship_method": "sunbiz_principal_address_compatibility",
                        "evidence_summary": "Compatibility alias for principal business address used by downstream analytics.",
                    },
                    corporation_number,
                )
            if mailing_address:
                add_relationship(
                    {
                        "source_entity_id": business_id,
                        "target_entity_id": f"address:{mailing_address}",
                        "relationship_type": "BUSINESS_MAILING_ADDRESS",
                        "confidence": 1.0,
                        "relationship_method": "sunbiz_mailing_address",
                        "evidence_summary": "Business mailing address provided directly by Sunbiz Daily filing data.",
                    },
                    corporation_number,
                )
            if registered_agent_id:
                add_relationship(
                    {
                        "source_entity_id": registered_agent_id,
                        "target_entity_id": business_id,
                        "relationship_type": "REGISTERED_AGENT_FOR",
                        "confidence": 1.0,
                        "relationship_method": "sunbiz_registered_agent",
                        "evidence_summary": "Registered agent linked to business directly by Sunbiz Daily filing data.",
                    },
                    f"{corporation_number}:registered_agent",
                )
                registered_agent_address = record["registered_agent_address"]["normalized_full_address"]
                if registered_agent_address:
                    add_relationship(
                        {
                            "source_entity_id": registered_agent_id,
                            "target_entity_id": f"address:{registered_agent_address}",
                            "relationship_type": "REGISTERED_AGENT_ASSOCIATED_WITH_ADDRESS",
                            "confidence": 1.0,
                            "relationship_method": "sunbiz_registered_agent_address",
                            "evidence_summary": "Registered agent address explicitly provided in Sunbiz Daily filing data.",
                        },
                        f"{corporation_number}:registered_agent",
                    )
                    add_relationship(
                        {
                            "source_entity_id": registered_agent_id,
                            "target_entity_id": f"address:{registered_agent_address}",
                            "relationship_type": "REGISTERED_AGENT_AT_ADDRESS",
                            "confidence": 1.0,
                            "relationship_method": "sunbiz_registered_agent_address_compatibility",
                            "evidence_summary": "Compatibility alias for registered agent address used by downstream analytics.",
                        },
                        f"{corporation_number}:registered_agent",
                    )
            for index, officer in enumerate(record["officers"], start=1):
                officer_name = officer["original_name"]
                if not officer_name:
                    continue
                officer_id = f"officer:{officer['normalized_name'] or officer_name}"
                add_relationship(
                    {
                        "source_entity_id": officer_id,
                        "target_entity_id": business_id,
                        "relationship_type": "OFFICER_OF",
                        "confidence": 1.0,
                        "relationship_method": "sunbiz_officer",
                        "evidence_summary": "Officer linked to business directly by Sunbiz Daily filing data.",
                    },
                    f"{corporation_number}:officer:{index}",
                )
                officer_address = normalize_address(officer["address"])
                if officer_address:
                    add_relationship(
                        {
                            "source_entity_id": officer_id,
                            "target_entity_id": f"address:{officer_address}",
                            "relationship_type": "OFFICER_ASSOCIATED_WITH_ADDRESS",
                            "confidence": 1.0,
                            "relationship_method": "sunbiz_officer_address",
                            "evidence_summary": "Officer address explicitly provided in Sunbiz Daily filing data.",
                        },
                        f"{corporation_number}:officer:{index}",
                    )
                    add_relationship(
                        {
                            "source_entity_id": officer_id,
                            "target_entity_id": f"address:{officer_address}",
                            "relationship_type": "OFFICER_AT_ADDRESS",
                            "confidence": 1.0,
                            "relationship_method": "sunbiz_officer_address_compatibility",
                            "evidence_summary": "Compatibility alias for officer address used by downstream analytics.",
                        },
                        f"{corporation_number}:officer:{index}",
                    )
        return relationships

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        start_time = time.time()
        status_code = "SUCCESS"
        error_message = ""
        try:
            payload = self.fetch()
            parsed = self.parse(payload)
            normalized = self.normalize(parsed)
            entities = self.to_entities(normalized)
            relationships = self.to_relationships(normalized)
            businesses = self.build_business_rows(normalized)
        except MissingAPIKeyError:
            raise
        except (ConnectorDisabledError, LiveAccessNotApprovedError):
            raise
        except Exception as exc:
            status_code = "FAILED"
            error_message = str(exc)
            parsed = []
            normalized = []
            entities = []
            relationships = []
            businesses = []

        self.last_records = parsed
        self.last_business_rows = businesses
        rate_limit_state = self.client.get_rate_limit_state()
        incomplete_reason = _stringify(self.last_summary.get("incomplete_reason"))
        if incomplete_reason and status_code == "SUCCESS":
            status_code = "INCOMPLETE"
            error_message = incomplete_reason

        summary = {
            "source_name": self.source_name,
            "source_type": str(self.source_config.get("source_type", "official_api")),
            "api_status": status_code,
            "live_mode": not self.use_mock,
            "key_present": self.client.is_key_present(),
            "config_enabled": bool(self.source_config.get("enabled", False)),
            "last_attempted_import": self.imported_at,
            "last_successful_import": self.imported_at if status_code == "SUCCESS" else "",
            "import_filters": {key: value for key, value in self.filters.items() if value},
            "records_fetched": int(self.last_summary.get("records_fetched", len(parsed))),
            "raw_records_received": len(parsed),
            "normalized_businesses": len(businesses),
            "businesses_imported": len(businesses),
            "officers_imported": sum(1 for row in entities if row["entity_type"] == "officer"),
            "registered_agents_imported": sum(1 for row in entities if row["entity_type"] == "registered_agent"),
            "addresses_imported": sum(1 for row in entities if row["entity_type"] == "address"),
            "relationships_created": len(relationships),
            "redacted_or_incomplete_records": sum(1 for row in normalized if row["privacy_redacted"] or row["incomplete_record"]),
            "privacy_redacted_records": sum(1 for row in normalized if row["privacy_redacted"]),
            "asynchronous_jobs": int(self.last_summary.get("async_jobs", 0)),
            "truncated_results": bool(self.last_summary.get("truncated_results", False)),
            "response_shapes_supported": self.last_summary.get("response_shapes", []),
            "rate_limit_limit": rate_limit_state.get("limit"),
            "rate_limit_remaining": rate_limit_state.get("remaining"),
            "errors": error_message,
            "runtime_seconds": round(time.time() - start_time, 2),
            "county_coverage": _stringify(self.filters.get("county")),
            "city_filter": _stringify(self.filters.get("city")),
            "state_filter": _stringify(self.filters.get("state")),
            "zip_filter": _stringify(self.filters.get("zip")),
            "source_contract": {
                "base_url": self.base_url,
                "list_endpoint": self.source_config.get("list_endpoint"),
                "detail_endpoint_template": self.source_config.get("detail_endpoint_template"),
            },
            "cross_source_matches": 0,
        }
        self.last_summary = summary
        return entities, relationships, summary


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


def build_sunbiz_parcel_matches(
    *,
    cross_source_matches_path: Path,
    entities_path: Path,
    output_path: Path,
    source_name: str,
    imported_at: str,
) -> list[dict[str, Any]]:
    if not cross_source_matches_path.exists() or cross_source_matches_path.stat().st_size == 0:
        write_csv(output_path, [], [
            "match_id",
            "sunbiz_corporation_number",
            "sunbiz_business_name",
            "sunbiz_entity_id",
            "parcel_entity_id",
            "parcel_id",
            "match_type",
            "match_value",
            "normalized_match_value",
            "match_confidence",
            "decision",
            "sunbiz_source_record_id",
            "parcel_source_record_id",
            "sunbiz_source_name",
            "parcel_source_name",
            "explanation",
            "recommended_review",
            "imported_at",
        ])
        return []
    matches_df = json.loads("[]")
    try:
        import pandas as pd

        matches_df = pd.read_csv(cross_source_matches_path).fillna("")
        entities_df = pd.read_csv(entities_path).fillna("") if entities_path.exists() and entities_path.stat().st_size > 0 else pd.DataFrame()
    except Exception:
        write_csv(output_path, [], [
            "match_id",
            "sunbiz_corporation_number",
            "sunbiz_business_name",
            "sunbiz_entity_id",
            "parcel_entity_id",
            "parcel_id",
            "match_type",
            "match_value",
            "normalized_match_value",
            "match_confidence",
            "decision",
            "sunbiz_source_record_id",
            "parcel_source_record_id",
            "sunbiz_source_name",
            "parcel_source_name",
            "explanation",
            "recommended_review",
            "imported_at",
        ])
        return []

    entity_lookup = {
        str(row.get("entity_id", "")): row
        for _, row in entities_df.iterrows()
        if str(row.get("entity_id", "")).strip()
    }
    rows: list[dict[str, Any]] = []
    accepted_methods = {
        "property_situs_matches_business_address",
        "property_mailing_matches_business_address",
        "parcel_owner_matches_person_with_secondary",
        "parcel_owner_matches_business_name",
    }
    for _, row in matches_df.iterrows():
        left_source_name = str(row.get("left_source_name", ""))
        right_source_name = str(row.get("right_source_name", ""))
        match_method = str(row.get("match_method", ""))
        if source_name not in {left_source_name, right_source_name}:
            continue
        if match_method not in accepted_methods:
            continue
        sunbiz_is_left = left_source_name == source_name
        sunbiz_entity_id = str(row.get("left_entity_id" if sunbiz_is_left else "right_entity_id", ""))
        parcel_entity_id = str(row.get("right_entity_id" if sunbiz_is_left else "left_entity_id", ""))
        sunbiz_source_record_id = str(row.get("left_source_record_id" if sunbiz_is_left else "right_source_record_id", ""))
        parcel_source_record_id = str(row.get("right_source_record_id" if sunbiz_is_left else "left_source_record_id", ""))
        entity_row = entity_lookup.get(sunbiz_entity_id, {})
        parcel_row = entity_lookup.get(parcel_entity_id, {})
        rows.append(
            {
                "match_id": str(row.get("cross_source_match_id", "")),
                "sunbiz_corporation_number": sunbiz_source_record_id.split(":")[0],
                "sunbiz_business_name": str(entity_row.get("display_name", "")),
                "sunbiz_entity_id": sunbiz_entity_id,
                "parcel_entity_id": parcel_entity_id,
                "parcel_id": str(parcel_row.get("display_name", parcel_source_record_id)),
                "match_type": match_method,
                "match_value": str(row.get("canonical_entity_id", "")),
                "normalized_match_value": str(row.get("canonical_entity_id", "")),
                "match_confidence": row.get("confidence", ""),
                "decision": str(row.get("decision", "")),
                "sunbiz_source_record_id": sunbiz_source_record_id,
                "parcel_source_record_id": parcel_source_record_id,
                "sunbiz_source_name": source_name,
                "parcel_source_name": right_source_name if sunbiz_is_left else left_source_name,
                "explanation": str(row.get("evidence", "")),
                "recommended_review": "Review both parcel and corporate records side by side before treating this as a meaningful investigative lead.",
                "imported_at": imported_at,
            }
        )
    write_csv(output_path, rows, [
        "match_id",
        "sunbiz_corporation_number",
        "sunbiz_business_name",
        "sunbiz_entity_id",
        "parcel_entity_id",
        "parcel_id",
        "match_type",
        "match_value",
        "normalized_match_value",
        "match_confidence",
        "decision",
        "sunbiz_source_record_id",
        "parcel_source_record_id",
        "sunbiz_source_name",
        "parcel_source_name",
        "explanation",
        "recommended_review",
        "imported_at",
    ])
    return rows


def enrich_cross_source_matches_with_sunbiz_fields(
    *,
    cross_source_matches_path: Path,
    sunbiz_parcel_matches_path: Path,
) -> None:
    if not cross_source_matches_path.exists() or cross_source_matches_path.stat().st_size == 0:
        return
    if not sunbiz_parcel_matches_path.exists() or sunbiz_parcel_matches_path.stat().st_size == 0:
        return
    try:
        import pandas as pd

        cross_source_df = pd.read_csv(cross_source_matches_path)
        parcel_matches_df = pd.read_csv(sunbiz_parcel_matches_path)
    except Exception:
        return
    if cross_source_df.empty or parcel_matches_df.empty:
        return
    parcel_matches_df = parcel_matches_df.rename(columns={"match_id": "cross_source_match_id"})
    enriched = cross_source_df.merge(
        parcel_matches_df[["cross_source_match_id", "sunbiz_corporation_number", "sunbiz_business_name", "parcel_id"]],
        on="cross_source_match_id",
        how="left",
    )
    enriched.to_csv(cross_source_matches_path, index=False)


def _compatibility_paths(primary_entities_path: Path, primary_relationships_path: Path) -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    if primary_entities_path.resolve() != LEGACY_ENTITIES_PATH.resolve():
        pairs.append((primary_entities_path, LEGACY_ENTITIES_PATH))
    if primary_entities_path.resolve() != DEFAULT_ENTITIES_PATH.resolve():
        pairs.append((primary_entities_path, DEFAULT_ENTITIES_PATH))
    if primary_relationships_path.resolve() != LEGACY_RELATIONSHIPS_PATH.resolve():
        pairs.append((primary_relationships_path, LEGACY_RELATIONSHIPS_PATH))
    if primary_relationships_path.resolve() != DEFAULT_RELATIONSHIPS_PATH.resolve():
        pairs.append((primary_relationships_path, DEFAULT_RELATIONSHIPS_PATH))
    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch bounded Sunbiz Daily corporate filings into local entity and relationship exports.")
    parser.add_argument("--county", default=None)
    parser.add_argument("--city", default=None)
    parser.add_argument("--state", default=None)
    parser.add_argument("--zip", dest="zip_code", default=None)
    parser.add_argument("--status", default=None)
    parser.add_argument("--period", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--filing-type", default=None)
    parser.add_argument("--corporation-name", default=None)
    parser.add_argument("--corporation-number", default=None)
    parser.add_argument("--officer-name", default=None)
    parser.add_argument("--registered-agent-name", default=None)
    parser.add_argument("--page-size", type=int, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--detail-lookups", action="store_true")
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--businesses-path", default=str(DEFAULT_BUSINESSES_PATH))
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH))
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH))
    parser.add_argument("--import-summary-path", default=str(DEFAULT_IMPORT_SUMMARY_PATH))
    parser.add_argument("--diagnostics-path", default=str(DEFAULT_DIAGNOSTICS_PATH))
    parser.add_argument("--matches-path", default=str(DEFAULT_MATCHES_PATH))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--skip-cross-source-refresh", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir
    entities_path = Path(args.entities_path)
    if not entities_path.is_absolute():
        entities_path = REPO_ROOT / entities_path
    relationships_path = Path(args.relationships_path)
    if not relationships_path.is_absolute():
        relationships_path = REPO_ROOT / relationships_path
    businesses_path = Path(args.businesses_path)
    if not businesses_path.is_absolute():
        businesses_path = REPO_ROOT / businesses_path
    import_summary_path = Path(args.import_summary_path)
    if not import_summary_path.is_absolute():
        import_summary_path = REPO_ROOT / import_summary_path
    diagnostics_path = Path(args.diagnostics_path)
    if not diagnostics_path.is_absolute():
        diagnostics_path = REPO_ROOT / diagnostics_path
    matches_path = Path(args.matches_path)
    if not matches_path.is_absolute():
        matches_path = REPO_ROOT / matches_path
    status_path = Path(args.status_path)
    if not status_path.is_absolute():
        status_path = REPO_ROOT / status_path
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = REPO_ROOT / db_path

    start_time = time.time()
    try:
        connector = SunbizDailyConnector(
            county=args.county,
            city=args.city,
            state=args.state,
            zip_code=args.zip_code,
            status=args.status,
            period=args.period,
            start_date=args.start_date,
            end_date=args.end_date,
            filing_type=args.filing_type,
            corporation_name=args.corporation_name,
            corporation_number=args.corporation_number,
            officer_name=args.officer_name,
            registered_agent_name=args.registered_agent_name,
            page_size=args.page_size,
            limit=args.limit,
            max_pages=args.max_pages,
            max_records=args.max_records,
            detail_lookups=args.detail_lookups,
            use_mock=args.mock,
            dry_run=args.dry_run,
            output_dir=output_dir,
            verbose=args.verbose,
        )
        entities, relationships, summary = connector.run()
        if not args.dry_run:
            write_csv(businesses_path, connector.last_business_rows, BUSINESS_FIELDS)
            write_csv(entities_path, entities, ENTITY_FIELDS)
            write_csv(relationships_path, relationships, RELATIONSHIP_FIELDS)
            if entities_path.resolve() != LEGACY_ENTITIES_PATH.resolve():
                write_csv(LEGACY_ENTITIES_PATH, entities, ENTITY_FIELDS)
            if entities_path.resolve() != DEFAULT_ENTITIES_PATH.resolve():
                write_csv(DEFAULT_ENTITIES_PATH, entities, ENTITY_FIELDS)
            if relationships_path.resolve() != LEGACY_RELATIONSHIPS_PATH.resolve():
                write_csv(LEGACY_RELATIONSHIPS_PATH, relationships, RELATIONSHIP_FIELDS)
            if relationships_path.resolve() != DEFAULT_RELATIONSHIPS_PATH.resolve():
                write_csv(DEFAULT_RELATIONSHIPS_PATH, relationships, RELATIONSHIP_FIELDS)
            write_csv(diagnostics_path, connector.last_diagnostics, DIAGNOSTIC_FIELDS)
            if diagnostics_path.resolve() != DEFAULT_DIAGNOSTICS_PATH.resolve():
                write_csv(DEFAULT_DIAGNOSTICS_PATH, connector.last_diagnostics, DIAGNOSTIC_FIELDS)
            if not args.skip_cross_source_refresh:
                summary["cross_source_matches"] = refresh_cross_source_artifacts(
                    db_path=db_path,
                    processed_dir=entities_path.parent,
                    sunbiz_entities_path=entities_path,
                    sunbiz_relationships_path=relationships_path,
                )
            build_sunbiz_parcel_matches(
                cross_source_matches_path=entities_path.parent / "cross_source_matches.csv",
                entities_path=entities_path.parent / "entities.csv",
                output_path=matches_path,
                source_name=connector.source_name,
                imported_at=connector.imported_at,
            )
            enrich_cross_source_matches_with_sunbiz_fields(
                cross_source_matches_path=entities_path.parent / "cross_source_matches.csv",
                sunbiz_parcel_matches_path=matches_path,
            )
            write_json(import_summary_path, summary)
            if import_summary_path.resolve() != DEFAULT_IMPORT_SUMMARY_PATH.resolve():
                write_json(DEFAULT_IMPORT_SUMMARY_PATH, summary)
            write_json(status_path, summary)
            if status_path.resolve() != DEFAULT_STATUS_PATH.resolve():
                write_json(DEFAULT_STATUS_PATH, summary)
        print(
            f"Sunbiz Daily: api_status={summary['api_status']} businesses={summary['businesses_imported']} "
            f"officers={summary['officers_imported']} registered_agents={summary['registered_agents_imported']} "
            f"addresses={summary['addresses_imported']} relationships={summary['relationships_created']} "
            f"cross_source_matches={summary.get('cross_source_matches', 0)}"
        )
    except MissingAPIKeyError as exc:
        empty_summary = {
            "source_name": "sunbiz_daily",
            "api_status": "MISSING_API_KEY",
            "last_attempted_import": "",
            "last_successful_import": "",
            "businesses_imported": 0,
            "officers_imported": 0,
            "registered_agents_imported": 0,
            "addresses_imported": 0,
            "relationships_created": 0,
            "cross_source_matches": 0,
            "runtime_seconds": round(time.time() - start_time, 2),
            "errors": str(exc),
            "key_present": False,
        }
        if not args.dry_run:
            write_csv(businesses_path, [], BUSINESS_FIELDS)
            write_csv(entities_path, [], ENTITY_FIELDS)
            write_csv(relationships_path, [], RELATIONSHIP_FIELDS)
            write_csv(diagnostics_path, [], DIAGNOSTIC_FIELDS)
            write_json(import_summary_path, empty_summary)
            write_json(status_path, empty_summary)
        print(str(exc))
    except (ConnectorDisabledError, LiveAccessNotApprovedError) as exc:
        empty_summary = {
            "source_name": "sunbiz_daily",
            "api_status": "DISABLED",
            "last_attempted_import": "",
            "last_successful_import": "",
            "businesses_imported": 0,
            "officers_imported": 0,
            "registered_agents_imported": 0,
            "addresses_imported": 0,
            "relationships_created": 0,
            "cross_source_matches": 0,
            "runtime_seconds": round(time.time() - start_time, 2),
            "errors": str(exc),
        }
        if not args.dry_run:
            write_csv(businesses_path, [], BUSINESS_FIELDS)
            write_csv(entities_path, [], ENTITY_FIELDS)
            write_csv(relationships_path, [], RELATIONSHIP_FIELDS)
            write_csv(diagnostics_path, [], DIAGNOSTIC_FIELDS)
            write_json(import_summary_path, empty_summary)
            write_json(status_path, empty_summary)
        print(str(exc))
    except Exception as exc:
        failed_summary = {
            "source_name": "sunbiz_daily",
            "api_status": "FAILED",
            "last_attempted_import": "",
            "last_successful_import": "",
            "businesses_imported": 0,
            "officers_imported": 0,
            "registered_agents_imported": 0,
            "addresses_imported": 0,
            "relationships_created": 0,
            "cross_source_matches": 0,
            "runtime_seconds": round(time.time() - start_time, 2),
            "errors": str(exc),
        }
        if not args.dry_run:
            write_csv(businesses_path, [], BUSINESS_FIELDS)
            write_csv(entities_path, [], ENTITY_FIELDS)
            write_csv(relationships_path, [], RELATIONSHIP_FIELDS)
            write_csv(diagnostics_path, [], DIAGNOSTIC_FIELDS)
            write_json(import_summary_path, failed_summary)
            write_json(status_path, failed_summary)
        print(f"Sunbiz Daily connector failed: {exc}")


if __name__ == "__main__":
    main()
