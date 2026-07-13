from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.entity_builder import build_entity_graph
from src.analytics.entity_intelligence import main as entity_intelligence_main
from src.analytics.cross_source import run_cross_source_correlation
from src.analytics.network_detection.engine import build_network_intelligence
from src.analytics.entity_resolution.resolver import resolve_entities
from src.analytics.fraud_markers.engine import FraudMarkerEngine
from src.analytics.statistical_risk.engine import run_statistical_risk
from src.connectors.arcgis.arcgis_connector import DEFAULT_ENTITIES_PATH as DEFAULT_ARCGIS_ENTITIES_PATH
from src.connectors.arcgis.arcgis_connector import DEFAULT_RELATIONSHIPS_PATH as DEFAULT_ARCGIS_RELATIONSHIPS_PATH
from src.connectors.county_clerk.local_file_connector import DEFAULT_COUNTY_CLERK_INPUT_PATH
from src.connectors.county_property.local_file_connector import DEFAULT_COUNTY_PROPERTY_INPUT_PATH
from src.connectors.open_data_api import DEFAULT_ENTITIES_PATH as DEFAULT_API_ENTITIES_PATH
from src.connectors.open_data_api import DEFAULT_RELATIONSHIPS_PATH as DEFAULT_API_RELATIONSHIPS_PATH
from src.connectors.source_manifest import ensure_local_only_path, validate_source
from src.connectors.sunbiz_daily_connector import DEFAULT_STATUS_PATH as DEFAULT_SUNBIZ_DAILY_STATUS_PATH
from src.connectors.sunbiz.local_file_connector import DEFAULT_SUNBIZ_INPUT_PATH
from src.health_check import check_project_health
from src.ingest.generate_synthetic_data import generate_synthetic_dataset
from src.ingest.load_to_duckdb import TABLE_FILES, load_synthetic_data
from src.investigation.investigation_engine import run_investigation_engine
from src.investigation.workspace import build_investigation_workspace

DEFAULT_SOURCE_DIR = Path("data/raw/synthetic")
DEFAULT_OUTPUT_DB = Path("local_osint.duckdb")
DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "entities.csv"
DEFAULT_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "relationships.csv"
DEFAULT_ANOMALY_PATH = DEFAULT_PROCESSED_DIR / "anomaly_report.csv"
DEFAULT_ENTITY_RISK_PATH = DEFAULT_PROCESSED_DIR / "entity_risk.csv"
DEFAULT_FRAUD_MARKERS_PATH = DEFAULT_PROCESSED_DIR / "fraud_markers.csv"
DEFAULT_FRAUD_MARKER_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "fraud_marker_summary.csv"
DEFAULT_CANONICAL_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_ENTITY_ALIASES_PATH = DEFAULT_PROCESSED_DIR / "entity_aliases.csv"
DEFAULT_ENTITY_RESOLUTION_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "entity_resolution_matches.csv"
DEFAULT_CANONICAL_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_INVESTIGATION_LEADS_PATH = DEFAULT_PROCESSED_DIR / "investigation_leads.csv"
DEFAULT_ENTITY_TIMELINES_PATH = DEFAULT_PROCESSED_DIR / "entity_timelines.csv"
DEFAULT_EVIDENCE_PACKETS_PATH = DEFAULT_PROCESSED_DIR / "evidence_packets.csv"
DEFAULT_NETWORK_CLUSTERS_PATH = DEFAULT_PROCESSED_DIR / "network_clusters.csv"
DEFAULT_NETWORK_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "network_summary.csv"
DEFAULT_NETWORK_MEMBERS_PATH = DEFAULT_PROCESSED_DIR / "network_members.csv"
DEFAULT_NETWORK_EDGES_PATH = DEFAULT_PROCESSED_DIR / "network_edges.csv"
DEFAULT_CROSS_SOURCE_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "cross_source_matches.csv"
DEFAULT_CROSS_SOURCE_DIAGNOSTICS_PATH = DEFAULT_PROCESSED_DIR / "cross_source_diagnostics.csv"
DEFAULT_CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "cross_source_diagnostic_summary.json"
DEFAULT_STATISTICAL_BASELINES_PATH = DEFAULT_PROCESSED_DIR / "statistical_baselines.csv"
DEFAULT_STATISTICAL_RARITY_PATH = DEFAULT_PROCESSED_DIR / "statistical_rarity.csv"
DEFAULT_CONTEXTUAL_RISK_ADJUSTMENTS_PATH = DEFAULT_PROCESSED_DIR / "contextual_risk_adjustments.csv"
DEFAULT_STATISTICAL_MARKER_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "statistical_marker_summary.json"
DEFAULT_STATISTICAL_CALIBRATION_REPORT_PATH = DEFAULT_PROCESSED_DIR / "statistical_calibration_report.csv"
DEFAULT_PRIORITIZED_LEADS_PATH = DEFAULT_PROCESSED_DIR / "prioritized_leads.csv"
DEFAULT_INVESTIGATION_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "investigation_summary.csv"
DEFAULT_LEAD_EVIDENCE_INDEX_PATH = DEFAULT_PROCESSED_DIR / "lead_evidence_index.csv"
DEFAULT_REVIEW_RECOMMENDATIONS_PATH = DEFAULT_PROCESSED_DIR / "review_recommendations.csv"
DEFAULT_SUNBIZ_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "sunbiz_entities.csv"
DEFAULT_SUNBIZ_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "sunbiz_relationships.csv"
DEFAULT_SUNBIZ_DAILY_STATUS_JSON_PATH = DEFAULT_PROCESSED_DIR / "sunbiz_daily_status.json"
DEFAULT_SUNBIZ_DAILY_SOURCE_NAME = "sunbiz_daily_api"
DEFAULT_COUNTY_CLERK_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "county_clerk_entities.csv"
DEFAULT_COUNTY_CLERK_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "county_clerk_relationships.csv"
DEFAULT_COUNTY_PROPERTY_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "county_property_entities.csv"
DEFAULT_COUNTY_PROPERTY_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "county_property_relationships.csv"
DEFAULT_API_SOURCE_NAME = "sample_api"
DEFAULT_ARCGIS_SOURCE_NAME = "florida_county_arcgis_parcels"
RESET_FILE_PATTERNS = ("*.csv", "*.parquet", "*.json")


def synthetic_is_missing_or_empty(source_dir: Path) -> bool:
    if not source_dir.exists() or not source_dir.is_dir():
        return True
    for filename in TABLE_FILES.values():
        csv_path = source_dir / filename
        if not csv_path.exists() or not csv_path.is_file() or csv_path.stat().st_size == 0:
            return True
    return False


def _is_within(base_dir: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(base_dir)
        return True
    except ValueError:
        return False


def reset_generated_artifacts(
    output_db: Path,
    processed_dir: Path,
    exports_dir: Path = Path("exports"),
    workspace_root: Optional[Path] = None,
) -> list[Path]:
    root = (workspace_root or Path.cwd()).resolve()
    db_path = output_db.resolve()
    processed_path = processed_dir.resolve()
    exports_path = exports_dir.resolve()
    deleted: list[Path] = []

    if _is_within(root, db_path) and db_path.exists() and db_path.is_file():
        db_path.unlink()
        deleted.append(db_path)

    for directory in [processed_path, exports_path]:
        if not _is_within(root, directory) or not directory.exists() or not directory.is_dir():
            continue
        patterns = RESET_FILE_PATTERNS if directory == processed_path else ("*.csv",)
        for pattern in patterns:
            for path in sorted(directory.glob(pattern)):
                resolved = path.resolve()
                if resolved.is_file() and _is_within(root, resolved):
                    resolved.unlink()
                    deleted.append(resolved)

    return deleted


def clear_lead_packages(package_root: Path, workspace_root: Optional[Path] = None) -> list[Path]:
    root = (workspace_root or Path.cwd()).resolve()
    package_path = package_root.resolve()
    deleted: list[Path] = []
    if not _is_within(root, package_path) or not package_path.exists() or not package_path.is_dir():
        return deleted
    for child in sorted(package_path.iterdir()):
        resolved = child.resolve()
        if not _is_within(root, resolved):
            continue
        if resolved.is_dir():
            shutil.rmtree(resolved)
            deleted.append(resolved)
        elif resolved.is_file():
            resolved.unlink()
            deleted.append(resolved)
    return deleted


def run_pipeline(
    records: int = 250,
    source_dir: Optional[Path | str] = None,
    output_db: Optional[Path | str] = None,
    processed_dir: Optional[Path | str] = None,
    entities_path: Optional[Path | str] = None,
    relationships_path: Optional[Path | str] = None,
    anomaly_path: Optional[Path | str] = None,
    entity_risk_path: Optional[Path | str] = None,
    include_connectors: bool = False,
    include_sunbiz: bool = False,
    run_health_check: bool = False,
    reset: bool = False,
    clear_lead_packages_flag: bool = False,
) -> None:
    pipeline_start = time.time()
    total_steps = 11 + (1 if include_connectors or include_sunbiz else 0) + (1 if run_health_check else 0) + (1 if reset else 0) + (1 if clear_lead_packages_flag else 0)
    source_path = Path(source_dir or DEFAULT_SOURCE_DIR)
    db_path = Path(output_db or DEFAULT_OUTPUT_DB)
    processed_path = Path(processed_dir or DEFAULT_PROCESSED_DIR)
    workspace_root = db_path.resolve().parent
    lead_package_root = workspace_root / "exports" / "leads"
    entities_path = Path(entities_path or DEFAULT_ENTITIES_PATH)
    relationships_path = Path(relationships_path or DEFAULT_RELATIONSHIPS_PATH)
    anomaly_path = Path(anomaly_path or DEFAULT_ANOMALY_PATH)
    entity_risk_path = Path(entity_risk_path or DEFAULT_ENTITY_RISK_PATH)
    canonical_entities_path = processed_path / DEFAULT_CANONICAL_ENTITIES_PATH.name
    aliases_path = processed_path / DEFAULT_ENTITY_ALIASES_PATH.name
    resolution_matches_path = processed_path / DEFAULT_ENTITY_RESOLUTION_MATCHES_PATH.name
    canonical_relationships_path = processed_path / DEFAULT_CANONICAL_RELATIONSHIPS_PATH.name
    fraud_markers_path = processed_path / DEFAULT_FRAUD_MARKERS_PATH.name
    fraud_marker_summary_path = processed_path / DEFAULT_FRAUD_MARKER_SUMMARY_PATH.name

    validate_source("synthetic")
    ensure_local_only_path("synthetic", source_path)
    source_path.mkdir(parents=True, exist_ok=True)
    processed_path.mkdir(parents=True, exist_ok=True)
    print("Pipeline: started")
    print(f"Pipeline: synthetic input dir {source_path}")
    print(f"Pipeline: output DuckDB {db_path}")
    print(f"Pipeline: processed dir {processed_path}")
    print(f"Pipeline: include_connectors={include_connectors}")
    print(f"Pipeline: include_sunbiz={include_sunbiz}")
    print(f"Pipeline: run_health_check={run_health_check}")
    print(f"Pipeline: reset={reset}")
    print(f"Pipeline: clear_lead_packages={clear_lead_packages_flag}")

    current_step = 1
    if reset:
        print(f"Step {current_step}/{total_steps}: Reset generated artifacts")
        deleted_paths = reset_generated_artifacts(
            output_db=db_path,
            processed_dir=processed_path,
            workspace_root=workspace_root,
        )
        if deleted_paths:
            for deleted_path in deleted_paths:
                try:
                    display_path = deleted_path.relative_to(Path.cwd().resolve())
                except ValueError:
                    display_path = deleted_path
                print(f"  Deleted {display_path}")
        else:
            print("  No generated artifacts found to delete.")
        current_step += 1
    if clear_lead_packages_flag:
        print(f"Step {current_step}/{total_steps}: Clear generated lead packages")
        deleted_packages = clear_lead_packages(lead_package_root, workspace_root=workspace_root)
        if deleted_packages:
            for deleted_path in deleted_packages:
                try:
                    display_path = deleted_path.relative_to(Path.cwd().resolve())
                except ValueError:
                    display_path = deleted_path
                print(f"  Deleted {display_path}")
        else:
            print("  No generated lead packages found to delete.")
        current_step += 1

    print(f"Step {current_step}/{total_steps}: Ensure synthetic data")
    if synthetic_is_missing_or_empty(source_path):
        print(f"  Synthetic data missing or incomplete at {source_path}. Generating synthetic CSVs...")
        generate_synthetic_dataset(records=records, output_dir=source_path)
        print(f"  Generated synthetic CSV files in {source_path}")
    else:
        print(f"  Existing synthetic data found in {source_path}. Skipping generation.")
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Load CSVs into DuckDB")
    manifest = load_synthetic_data(source_dir=source_path, output_db=db_path, processed_dir=processed_path)
    print(f"  Loaded {len(manifest)} CSV files into DuckDB at {db_path}")
    current_step += 1

    connector_entity_paths: list[Path] = []
    connector_relationship_paths: list[Path] = []
    if include_connectors or include_sunbiz:
        print(f"Step {current_step}/{total_steps}: Ingest connector data")
        connector_input_root = source_path.parents[0]
        sunbiz_entities_path = processed_path / "sunbiz_entities.csv"
        sunbiz_relationships_path = processed_path / "sunbiz_relationships.csv"

        if include_sunbiz:
            try:
                validate_source(DEFAULT_SUNBIZ_DAILY_SOURCE_NAME, require_live_access=True)
                connector_script = Path(__file__).resolve().parents[0] / "connectors" / "sunbiz_daily_connector.py"
                print(f"  Running Sunbiz Daily API import for source {DEFAULT_SUNBIZ_DAILY_SOURCE_NAME}")
                subprocess.run(
                    [
                        sys.executable,
                        str(connector_script),
                        "--county",
                        "Hillsborough",
                        "--limit",
                        "100",
                        "--entities-path",
                        str(sunbiz_entities_path),
                        "--relationships-path",
                        str(sunbiz_relationships_path),
                        "--status-path",
                        str(processed_path / DEFAULT_SUNBIZ_DAILY_STATUS_PATH.name),
                        "--db-path",
                        str(db_path),
                        "--skip-cross-source-refresh",
                    ],
                    check=True,
                )
                print(f"  Wrote Sunbiz Daily entities to {sunbiz_entities_path}")
                print(f"  Wrote Sunbiz Daily relationships to {sunbiz_relationships_path}")
                connector_entity_paths.append(sunbiz_entities_path)
                connector_relationship_paths.append(sunbiz_relationships_path)
            except subprocess.CalledProcessError as exc:
                print(f"  Skipping Sunbiz Daily connector {DEFAULT_SUNBIZ_DAILY_SOURCE_NAME}: connector execution failed ({exc.returncode}).")
            except ValueError as exc:
                print(f"  Skipping Sunbiz Daily connector {DEFAULT_SUNBIZ_DAILY_SOURCE_NAME}: {exc}")
        elif include_connectors:
            sunbiz_input_path = connector_input_root / DEFAULT_SUNBIZ_INPUT_PATH.relative_to("data/raw")
            if sunbiz_input_path.exists():
                validate_source("sunbiz_local_file")
                ensure_local_only_path("sunbiz_local_file", sunbiz_input_path)
                print(f"  Found Sunbiz connector input at {sunbiz_input_path}")
                processed_path.mkdir(parents=True, exist_ok=True)
                connector_script = Path(__file__).resolve().parents[0] / "connectors" / "sunbiz" / "local_file_connector.py"
                subprocess.run(
                    [
                        sys.executable,
                        str(connector_script),
                        "--input",
                        str(sunbiz_input_path),
                        "--entities-path",
                        str(sunbiz_entities_path),
                        "--relationships-path",
                        str(sunbiz_relationships_path),
                    ],
                    check=True,
                )
                print(f"  Wrote sunbiz entities to {sunbiz_entities_path}")
                print(f"  Wrote sunbiz relationships to {sunbiz_relationships_path}")
                connector_entity_paths.append(sunbiz_entities_path)
                connector_relationship_paths.append(sunbiz_relationships_path)
            else:
                print(
                    "  No default Sunbiz connector input found at "
                    f"{sunbiz_input_path}. Manually place a file at "
                    "data/raw/sunbiz/sunbiz_entities.csv to include Sunbiz data in the "
                    "pipeline. Sample input is supported only when you run the connector "
                    "explicitly with --input."
                )

        county_property_input_path = connector_input_root / DEFAULT_COUNTY_PROPERTY_INPUT_PATH.relative_to("data/raw")
        if include_connectors and county_property_input_path.exists():
            validate_source("county_property_local_file")
            ensure_local_only_path("county_property_local_file", county_property_input_path)
            print(f"  Found county property connector input at {county_property_input_path}")
            county_property_entities_path = processed_path / "county_property_entities.csv"
            county_property_relationships_path = processed_path / "county_property_relationships.csv"
            connector_script = Path(__file__).resolve().parents[0] / "connectors" / "county_property" / "local_file_connector.py"
            subprocess.run(
                [
                    sys.executable,
                    str(connector_script),
                    "--input",
                    str(county_property_input_path),
                    "--entities-path",
                    str(county_property_entities_path),
                    "--relationships-path",
                    str(county_property_relationships_path),
                ],
                check=True,
            )
            print(f"  Wrote county property entities to {county_property_entities_path}")
            print(f"  Wrote county property relationships to {county_property_relationships_path}")
            connector_entity_paths.append(county_property_entities_path)
            connector_relationship_paths.append(county_property_relationships_path)
        elif include_connectors:
            print(
                "  No default county property connector input found at "
                f"{county_property_input_path}. Manually place a file at "
                "data/raw/county_property/property_records.csv to include county "
                "property data in the pipeline. Sample input is supported only when "
                "you run the connector explicitly with --input."
            )

        county_clerk_input_path = connector_input_root / DEFAULT_COUNTY_CLERK_INPUT_PATH.relative_to("data/raw")
        if include_connectors and county_clerk_input_path.exists():
            validate_source("county_clerk_local_file")
            ensure_local_only_path("county_clerk_local_file", county_clerk_input_path)
            print(f"  Found county clerk connector input at {county_clerk_input_path}")
            county_clerk_entities_path = processed_path / "county_clerk_entities.csv"
            county_clerk_relationships_path = processed_path / "county_clerk_relationships.csv"
            connector_script = Path(__file__).resolve().parents[0] / "connectors" / "county_clerk" / "local_file_connector.py"
            subprocess.run(
                [
                    sys.executable,
                    str(connector_script),
                    "--input",
                    str(county_clerk_input_path),
                    "--entities-path",
                    str(county_clerk_entities_path),
                    "--relationships-path",
                    str(county_clerk_relationships_path),
                ],
                check=True,
            )
            print(f"  Wrote county clerk entities to {county_clerk_entities_path}")
            print(f"  Wrote county clerk relationships to {county_clerk_relationships_path}")
            connector_entity_paths.append(county_clerk_entities_path)
            connector_relationship_paths.append(county_clerk_relationships_path)
        elif include_connectors:
            print(
                "  No default county clerk connector input found at "
                f"{county_clerk_input_path}. Manually place a file at "
                "data/raw/county_clerk/clerk_records.csv to include county clerk "
                "data in the pipeline. Sample input is supported only when you run "
                "the connector explicitly with --input."
            )

        if include_connectors:
            try:
                validate_source(DEFAULT_API_SOURCE_NAME, require_live_access=True)
                api_entities_path = processed_path / DEFAULT_API_ENTITIES_PATH.name
                api_relationships_path = processed_path / DEFAULT_API_RELATIONSHIPS_PATH.name
                connector_script = Path(__file__).resolve().parents[0] / "connectors" / "open_data_api.py"
                try:
                    subprocess.run(
                        [
                            sys.executable,
                            str(connector_script),
                            "--source",
                            DEFAULT_API_SOURCE_NAME,
                            "--entities-path",
                            str(api_entities_path),
                            "--relationships-path",
                            str(api_relationships_path),
                        ],
                        check=True,
                    )
                    print(f"  Wrote api entities to {api_entities_path}")
                    print(f"  Wrote api relationships to {api_relationships_path}")
                    connector_entity_paths.append(api_entities_path)
                    connector_relationship_paths.append(api_relationships_path)
                except subprocess.CalledProcessError as exc:
                    print(f"  Skipping API connector {DEFAULT_API_SOURCE_NAME}: connector execution failed ({exc.returncode}).")
            except ValueError as exc:
                print(f"  Skipping API connector {DEFAULT_API_SOURCE_NAME}: {exc}")

            try:
                validate_source(DEFAULT_ARCGIS_SOURCE_NAME, require_live_access=True)
                arcgis_entities_path = processed_path / DEFAULT_ARCGIS_ENTITIES_PATH.name
                arcgis_relationships_path = processed_path / DEFAULT_ARCGIS_RELATIONSHIPS_PATH.name
                connector_script = Path(__file__).resolve().parents[0] / "connectors" / "arcgis" / "arcgis_connector.py"
                try:
                    subprocess.run(
                        [
                            sys.executable,
                            str(connector_script),
                            "--source",
                            DEFAULT_ARCGIS_SOURCE_NAME,
                            "--entities-path",
                            str(arcgis_entities_path),
                            "--relationships-path",
                            str(arcgis_relationships_path),
                        ],
                        check=True,
                    )
                    print(f"  Wrote arcgis entities to {arcgis_entities_path}")
                    print(f"  Wrote arcgis relationships to {arcgis_relationships_path}")
                    connector_entity_paths.append(arcgis_entities_path)
                    connector_relationship_paths.append(arcgis_relationships_path)
                except subprocess.CalledProcessError as exc:
                    print(
                        f"  Skipping ArcGIS connector {DEFAULT_ARCGIS_SOURCE_NAME}: "
                        f"connector execution failed ({exc.returncode})."
                    )
            except ValueError as exc:
                print(f"  Skipping ArcGIS connector {DEFAULT_ARCGIS_SOURCE_NAME}: {exc}")
        current_step += 1

    print(f"Step {current_step}/{total_steps}: Build entities and relationships")

    build_entity_graph(
        db_path=db_path,
        entities_path=entities_path,
        relationships_path=relationships_path,
        additional_entity_paths=connector_entity_paths,
        additional_relationship_paths=connector_relationship_paths,
    )
    print(f"  Wrote entities to {entities_path}")
    print(f"  Wrote relationships to {relationships_path}")
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Resolve canonical entities")
    resolution_summary = resolve_entities(
        entities_path=entities_path,
        relationships_path=relationships_path,
        canonical_entities_path=canonical_entities_path,
        aliases_path=aliases_path,
        matches_path=resolution_matches_path,
        canonical_relationships_path=canonical_relationships_path,
        db_path=db_path,
    )
    print(f"  Wrote canonical entities to {canonical_entities_path}")
    print(f"  Wrote entity aliases to {aliases_path}")
    print(f"  Wrote resolution matches to {resolution_matches_path}")
    print(f"  Wrote canonical relationships to {canonical_relationships_path}")
    print(
        "  Compatibility note: anomaly rules still execute against existing local source tables; "
        "entity intelligence consumes canonical entities and canonical relationships with anomaly IDs "
        "rewritten to canonical IDs."
    )
    print(
        f"  Canonical entities={resolution_summary['canonical_entity_count']} "
        f"merged={resolution_summary['merged_entities']} "
        f"review={resolution_summary['review_candidates']}"
    )
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Build cross-source diagnostics and matches")
    cross_source_summary = run_cross_source_correlation(
        canonical_entities_path=canonical_entities_path,
        aliases_path=aliases_path,
        entity_resolution_matches_path=resolution_matches_path,
        canonical_relationships_path=canonical_relationships_path,
        fraud_markers_path=fraud_markers_path,
        prioritized_leads_path=processed_path / DEFAULT_PRIORITIZED_LEADS_PATH.name,
        cross_source_matches_path=processed_path / DEFAULT_CROSS_SOURCE_MATCHES_PATH.name,
        diagnostics_path=processed_path / DEFAULT_CROSS_SOURCE_DIAGNOSTICS_PATH.name,
        diagnostic_summary_path=processed_path / DEFAULT_CROSS_SOURCE_DIAGNOSTIC_SUMMARY_PATH.name,
    )
    print(
        f"  Cross-source matches={cross_source_summary['cross_source_match_count']} "
        f"auto={cross_source_summary['auto_match_count']} "
        f"review={cross_source_summary['review_match_count']} "
        f"rejected={cross_source_summary['rejected_match_count']}"
    )
    if include_sunbiz:
        status_path = processed_path / DEFAULT_SUNBIZ_DAILY_STATUS_JSON_PATH.name
        if status_path.exists() and status_path.stat().st_size > 0:
            try:
                with status_path.open("r", encoding="utf-8") as handle:
                    status_payload = json.load(handle)
                if isinstance(status_payload, dict):
                    status_payload["cross_source_matches"] = int(cross_source_summary.get("cross_source_match_count", 0))
                    with status_path.open("w", encoding="utf-8") as handle:
                        json.dump(status_payload, handle, indent=2)
            except Exception:
                pass
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Calculate statistical baselines and rarity")
    statistical_summary = run_statistical_risk(
        canonical_entities_path=canonical_entities_path,
        canonical_relationships_path=canonical_relationships_path,
        cross_source_matches_path=processed_path / DEFAULT_CROSS_SOURCE_MATCHES_PATH.name,
        baselines_path=processed_path / DEFAULT_STATISTICAL_BASELINES_PATH.name,
        rarity_path=processed_path / DEFAULT_STATISTICAL_RARITY_PATH.name,
        adjustments_path=processed_path / DEFAULT_CONTEXTUAL_RISK_ADJUSTMENTS_PATH.name,
        summary_path=processed_path / DEFAULT_STATISTICAL_MARKER_SUMMARY_PATH.name,
        calibration_report_path=processed_path / DEFAULT_STATISTICAL_CALIBRATION_REPORT_PATH.name,
    )
    print(
        f"  Statistical markers={statistical_summary['markers_evaluated']} "
        f"baseline_groups={statistical_summary['records_loaded']['baseline_groups_created']} "
        f"insufficient={statistical_summary['insufficient_baseline_count']}"
    )
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Run fraud marker engine")
    engine = FraudMarkerEngine(
        db_path=db_path,
        entities_path=canonical_entities_path,
        relationships_path=canonical_relationships_path,
        aliases_path=aliases_path,
        output_path=fraud_markers_path,
        summary_path=fraud_marker_summary_path,
        compatibility_output_path=anomaly_path,
        cross_source_matches_path=processed_path / DEFAULT_CROSS_SOURCE_MATCHES_PATH.name,
        statistical_rarity_path=processed_path / DEFAULT_STATISTICAL_RARITY_PATH.name,
        statistical_adjustments_path=processed_path / DEFAULT_CONTEXTUAL_RISK_ADJUSTMENTS_PATH.name,
    )
    findings = engine.run()
    summary = engine.summarize(findings)
    print(f"  Wrote fraud markers to {fraud_markers_path}")
    print(f"  Wrote fraud marker summary to {fraud_marker_summary_path}")
    print(f"  Wrote anomaly report to {anomaly_path}")
    print(f"  Found {summary['High']} High, {summary['Medium']} Medium, {summary['Low']} Low findings")
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Run entity intelligence")
    entity_intelligence_main(
        entities_path=canonical_entities_path,
        relationships_path=canonical_relationships_path,
        anomaly_path=fraud_markers_path,
        output_path=entity_risk_path,
        config_path=None,
    )
    print(f"  Wrote entity risk summary to {entity_risk_path}")
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Build investigation workspace")
    investigation_summary = build_investigation_workspace(
        entity_risk_path=entity_risk_path,
        fraud_markers_path=fraud_markers_path,
        canonical_entities_path=canonical_entities_path,
        canonical_relationships_path=canonical_relationships_path,
        aliases_path=aliases_path,
        leads_output_path=processed_path / DEFAULT_INVESTIGATION_LEADS_PATH.name,
        timelines_output_path=processed_path / DEFAULT_ENTITY_TIMELINES_PATH.name,
        evidence_output_path=processed_path / DEFAULT_EVIDENCE_PACKETS_PATH.name,
    )
    print(
        f"  Investigation leads={investigation_summary['lead_count']} "
        f"timeline_events={investigation_summary['timeline_event_count']} "
        f"evidence_packets={investigation_summary['evidence_packet_count']}"
    )
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Build network intelligence")
    network_summary = build_network_intelligence(
        entities_path=canonical_entities_path,
        relationships_path=canonical_relationships_path,
        fraud_markers_path=fraud_markers_path,
        timelines_path=processed_path / DEFAULT_ENTITY_TIMELINES_PATH.name,
        cluster_output_path=processed_path / DEFAULT_NETWORK_CLUSTERS_PATH.name,
        summary_output_path=processed_path / DEFAULT_NETWORK_SUMMARY_PATH.name,
        members_output_path=processed_path / DEFAULT_NETWORK_MEMBERS_PATH.name,
        edges_output_path=processed_path / DEFAULT_NETWORK_EDGES_PATH.name,
    )
    print(
        f"  Networks={network_summary['network_count']} "
        f"largest_network_size={network_summary['largest_network_size']} "
        f"communities={network_summary['community_count']} "
        f"bridge_entities={network_summary['bridge_entity_count']}"
    )
    current_step += 1

    print(f"Step {current_step}/{total_steps}: Prioritize and package final investigation leads")
    investigation_engine_summary = run_investigation_engine(
        investigation_leads_path=processed_path / DEFAULT_INVESTIGATION_LEADS_PATH.name,
        network_clusters_path=processed_path / DEFAULT_NETWORK_CLUSTERS_PATH.name,
        entity_risk_path=entity_risk_path,
        fraud_markers_path=fraud_markers_path,
        canonical_entities_path=canonical_entities_path,
        canonical_relationships_path=canonical_relationships_path,
        aliases_path=aliases_path,
        evidence_packets_path=processed_path / DEFAULT_EVIDENCE_PACKETS_PATH.name,
        entity_timelines_path=processed_path / DEFAULT_ENTITY_TIMELINES_PATH.name,
        network_members_path=processed_path / DEFAULT_NETWORK_MEMBERS_PATH.name,
        cross_source_matches_path=processed_path / DEFAULT_CROSS_SOURCE_MATCHES_PATH.name,
        prioritized_leads_path=processed_path / DEFAULT_PRIORITIZED_LEADS_PATH.name,
        investigation_summary_path=processed_path / DEFAULT_INVESTIGATION_SUMMARY_PATH.name,
        lead_evidence_index_path=processed_path / DEFAULT_LEAD_EVIDENCE_INDEX_PATH.name,
        review_recommendations_path=processed_path / DEFAULT_REVIEW_RECOMMENDATIONS_PATH.name,
        package_root=lead_package_root,
    )
    print(
        f"  Prioritized leads={investigation_engine_summary['total_prioritized_leads']} "
        f"packages={investigation_engine_summary['package_count']}"
    )
    current_step += 1

    if run_health_check:
        print(f"Step {current_step}/{total_steps}: Run health check")
        passed, messages = check_project_health()
        for message in messages:
            print(f"  {message}")
        if not passed:
            raise RuntimeError("Health check failed after pipeline execution.")
        print("  Health check passed.")
    print(f"Pipeline: completed successfully in {time.time() - pipeline_start:.2f}s")
    print("Pipeline completed successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local OpenFraud pipeline end to end.")
    parser.add_argument("--records", type=int, default=250, help="Number of synthetic records to generate if synthetic source data is missing")
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR), help="Directory containing synthetic CSV files")
    parser.add_argument("--output-db", default=str(DEFAULT_OUTPUT_DB), help="Path to the DuckDB database file")
    parser.add_argument("--processed-dir", default=str(DEFAULT_PROCESSED_DIR), help="Directory for processed Parquet and CSV exports")
    parser.add_argument("--entities-path", default=str(DEFAULT_ENTITIES_PATH), help="Path to the entities CSV export")
    parser.add_argument("--relationships-path", default=str(DEFAULT_RELATIONSHIPS_PATH), help="Path to the relationships CSV export")
    parser.add_argument("--anomaly-path", default=str(DEFAULT_ANOMALY_PATH), help="Path to the anomaly report CSV")
    parser.add_argument("--entity-risk-path", default=str(DEFAULT_ENTITY_RISK_PATH), help="Path to the entity risk CSV")
    parser.add_argument(
        "--include-connectors",
        action="store_true",
        help="Include local connector outputs such as Sunbiz in the merged entity/relationship build",
    )
    parser.add_argument(
        "--include-sunbiz",
        action="store_true",
        help="Run the authenticated Sunbiz Daily API connector and merge its outputs into the local pipeline.",
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run project health check after the pipeline completes",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete generated artifacts before rebuilding the local pipeline outputs",
    )
    parser.add_argument(
        "--clear-lead-packages",
        action="store_true",
        help="Delete generated contents under exports/leads before rebuilding outputs",
    )
    args = parser.parse_args()

    try:
        run_pipeline(
            records=args.records,
            source_dir=args.source_dir,
            output_db=args.output_db,
            processed_dir=args.processed_dir,
            entities_path=args.entities_path,
            relationships_path=args.relationships_path,
            anomaly_path=args.anomaly_path,
            entity_risk_path=args.entity_risk_path,
            include_connectors=args.include_connectors,
            include_sunbiz=args.include_sunbiz,
            run_health_check=args.health_check,
            reset=args.reset,
            clear_lead_packages_flag=args.clear_lead_packages,
        )
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
