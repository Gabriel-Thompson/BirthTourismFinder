# OpenFraud

OpenFraud is a local-first public-record fraud-marker discovery MVP. It uses DuckDB, CSV, Parquet, and Streamlit to build explainable investigative leads. All results are leads only, not proof of fraud.

## Quickstart

1. Create and activate a Python virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. For Sunbiz Daily imports, copy `.env.example` to `.env` and set:
   ```bash
   SUNBIZ_DAILY_API_KEY=YOUR_SUNBIZ_DAILY_API_KEY_HERE
   ```
4. Run the local pipeline:
   ```bash
   python src/run_pipeline.py
   ```
5. Run the statistical risk engine directly when you want to refresh baselines and rarity outputs only:
   ```bash
   python -m src.analytics.statistical_risk.engine
   ```
6. Run the full local pipeline with connectors and health check:
   ```bash
   python src/run_pipeline.py --include-connectors --health-check
   ```
7. To include the authenticated Sunbiz Daily API import, run:
   ```bash
   python src/run_pipeline.py --include-sunbiz --include-connectors --health-check
   ```
8. Reset generated artifacts and rebuild from scratch when needed:
   ```bash
   python src/run_pipeline.py --reset --clear-lead-packages --include-connectors --health-check
   ```
9. Launch the dashboard:
   ```bash
   streamlit run src/app/dashboard.py
   ```

## Demo Workflow

1. Run the pipeline:
   ```bash
   python src/run_pipeline.py --reset --clear-lead-packages --include-connectors --health-check
   ```
2. Inspect ArcGIS sample metadata before mapping a real source:
   ```bash
   python src/connectors/arcgis/inspect_source.py --source florida_county_arcgis_parcels --limit 5
   ```
3. Export lead files:
   ```bash
   python src/reports/export_leads.py
   ```
4. Launch the dashboard:
   ```bash
   streamlit run src/app/dashboard.py
   ```

## Full Command List

Pipeline:
```bash
python src/run_pipeline.py
python src/run_pipeline.py --reset
python src/run_pipeline.py --reset --include-connectors
python src/run_pipeline.py --reset --clear-lead-packages --include-connectors
python src/run_pipeline.py --reset --clear-lead-packages --include-connectors --health-check
python src/run_pipeline.py --include-connectors
python src/run_pipeline.py --reset --include-connectors --health-check
python src/run_pipeline.py --include-connectors --health-check
```

ArcGIS:
```bash
python src/connectors/arcgis/inspect_source.py --source florida_county_arcgis_parcels --limit 5
python src/connectors/arcgis/arcgis_connector.py --source florida_county_arcgis_parcels --limit 100
python src/connectors/arcgis/inspect_source.py --source sample_arcgis_parcels --limit 5
python src/connectors/arcgis/arcgis_connector.py --source sample_arcgis_parcels
python src/connectors/arcgis/arcgis_connector.py --source escambia_arcgis_parcels --limit 100
```

Local file connectors:
```bash
python src/connectors/sunbiz/local_file_connector.py
python src/connectors/county_property/local_file_connector.py
python src/connectors/county_clerk/local_file_connector.py
```

Sunbiz Daily:
```bash
python -m src.connectors.sunbiz_daily_connector --mock --county Hillsborough --max-records 100
python -m src.connectors.sunbiz_daily_connector --county Hillsborough --status active --max-records 100
python -m src.connectors.sunbiz_daily_connector --zip 336 --status active --max-records 500
python -m src.connectors.sunbiz_daily_connector --start-date 2026-01-01 --end-date 2026-07-13 --max-records 500
python src/run_pipeline.py --include-sunbiz --include-connectors --health-check
```

Reporting and validation:
```bash
python src/reports/export_leads.py
python src/health_check.py
python -m src.analytics.statistical_risk.engine
python src/analytics/entity_resolution/resolver.py
python src/analytics/fraud_markers/engine.py
python src/connectors/onboard_source.py --source florida_county_arcgis_parcels
python src/connectors/onboard_source.py --source sample_arcgis_parcels
```

Dashboard:
```bash
streamlit run src/app/dashboard.py
```

## ArcGIS Source Inspection

Use the inspection utility before mapping a real ArcGIS layer:

```bash
python src/connectors/arcgis/inspect_source.py --source florida_county_arcgis_parcels --limit 5
```

It will:
- load the source from `config/api_sources.json`
- enforce `live_access_allowed` through `config/sources.json`
- fetch mocked or live metadata depending on source approval
- print field names, aliases, and types
- print sample records
- save a JSON inspection snapshot to `data/processed/arcgis_inspection_<source>.json`

As of July 9, 2026, `florida_county_arcgis_parcels` points to Hillsborough County's official public parcel FeatureServer layer at `https://maps.hillsboroughcounty.org/arcgis/rest/services/InfoLayers/HC_ParcelsPublic/FeatureServer/0`, and the full pipeline now uses that source as its default live ArcGIS connector.

As of July 9, 2026, `escambia_arcgis_parcels` is documented but kept `live_access_allowed: false` because reviewed public pages did not clearly document a public ArcGIS REST parcel query endpoint with explicit automation allowance.

## Source Onboarding Checklist

Validate a source before connecting it:

```bash
python src/connectors/onboard_source.py --source florida_county_arcgis_parcels
```

The onboarding utility checks:
- source presence in `config/sources.json` or `config/api_sources.json`
- review-document presence
- explicit `live_access_allowed` and `terms_review_required`
- documented access method
- documented processed outputs
- API or ArcGIS field mapping when applicable
- a recommended safe test command

Final readiness status is one of:
- `READY_FOR_SAMPLE_TEST`
- `NEEDS_SOURCE_REVIEW`
- `LIVE_ACCESS_DISABLED`
- `CONFIG_INCOMPLETE`

## Dashboard

Run the dashboard locally:

```bash
streamlit run src/app/dashboard.py
```

The Analyst Workbench includes:
- `Overview`
- `Investigation Queue`
- `Fraud Markers`
- `Statistical Risk`
- `Network Intelligence`
- `Cross Source Intelligence`
- `Entity Explorer`
- `Reports`
- `Source Health`

The `Source Health` page includes a `Sunbiz Daily` section that shows:
- businesses imported
- officers imported
- registered agents
- addresses
- cross-source matches
- API status
- key present
- last successful import
- import filters
- county coverage
- redacted or incomplete records
- asynchronous jobs
- truncated results
- rate-limit remaining

Local workstation features:
- persisted analyst notes, reviewer, disposition, bookmark, follow-up, and priority override in `data/processed/analyst_lead_state.csv`
- local investigation history in `data/processed/analyst_history.csv`
- saved local filters in `data/processed/dashboard_saved_searches.json`
- local dashboard defaults in `config/dashboard.json`
- side-by-side entity and network comparison views
- CSV, JSON, Markdown, and HTML lead-summary exports under `exports/`

If processed files are missing, the dashboard shows recovery guidance instead of crashing.

## Cross-Source Intelligence

Phase 4 adds an explicit cross-source correlation layer built from canonical entities, aliases, canonical relationships, fraud markers, and prioritized leads.

New outputs:
- `data/processed/cross_source_matches.csv`
- `data/processed/cross_source_diagnostics.csv`
- `data/processed/cross_source_diagnostic_summary.json`

Each match row preserves:
- `source_name`
- `source_type`
- `source_record_id`
- `connector_name`
- `import_batch_id`
- `imported_at`
- `jurisdiction`
- `is_synthetic`

The dashboard `Cross-Source Intelligence` section shows:
- total matches
- source-pair breakdown
- real-data-only filtering
- confidence and decision filters
- side-by-side evidence for the selected match
- diagnostic summary for missing overlap or rejected candidates

Cross-source corroboration requires independent source names. Synthetic data can participate in diagnostics, but it does not count toward real-source support.

## Statistical Rarity

Phase 4 adds a local statistical-risk layer that computes observed-versus-expected baselines before fraud-marker scoring.

New outputs:
- `data/processed/statistical_baselines.csv`
- `data/processed/statistical_rarity.csv`
- `data/processed/contextual_risk_adjustments.csv`
- `data/processed/statistical_marker_summary.json`
- `data/processed/statistical_calibration_report.csv`

Purpose:
- distinguish common patterns from rare peer-group outliers
- preserve observed versus expected values
- adjust marker severity by context without suppressing the underlying marker
- keep risk separate from confidence

Review thresholds are configurable in `config/statistical_risk.json`. Safe defaults start with:
- probability below `15%`: `ROUTINE_REVIEW`
- probability below `5%`: `ELEVATED_REVIEW`
- probability below `1%`: `IMMEDIATE_REVIEW`
- probability below `0.1%`: `EXTREME_OUTLIER`

Observed versus expected values are always shown with:
- peer group
- comparison-group size
- model used
- assumptions
- contextual adjustment

Supported peer groups use available dimensions such as marker type, entity type, source scope, jurisdiction, and address/property context. The engine does not compare everything to one statewide average when a narrower peer group is available.

Temporal windows are configurable and currently support:
- `3` days
- `7` days
- `30` days
- `90` days
- `365` days

Small-sample safeguards:
- synthetic data is excluded from operational `REAL_ONLY` baselines
- inadequate peer groups are labeled `INSUFFICIENT_BASELINE`
- the engine avoids precise-looking probabilities when denominator data is weak

Statistical rarity influences risk. Source quality, exact matching, and evidence completeness continue to influence confidence. A rare pattern may warrant review, but statistical rarity does not prove fraud.

## Fraud Marker Engine

OpenFraud now uses a configurable Fraud Marker Engine (FME) as the primary analytics layer.

Core outputs:
- `data/processed/fraud_markers.csv`
- `data/processed/fraud_marker_summary.csv`
- `data/processed/entity_risk.csv`
- `data/processed/anomaly_report.csv` as a compatibility export

Marker vs risk vs confidence:
- A `marker` is one explainable public-record pattern such as a shared phone, shared address, or cross-source owner match.
- `risk` is the weighted total contribution from one or more markers plus relationship/context bonuses.
- `confidence` reflects how well the marker is supported by independent evidence, source coverage, and relationship support.
- `evidence` is always preserved in plain English and tied back to supporting entities, relationships, and sources.

The Fraud Marker Engine is registry-based and local-only. Markers are configurable in `config/fraud_markers.json`, including enable/disable state, weight, minimum confidence, and minimum support.

### Fraud Marker Commands

```bash
python src/analytics/fraud_markers/engine.py
python src/run_pipeline.py --reset --include-connectors --health-check
python src/reports/export_leads.py
```

Implemented marker areas include:
- address reuse and mailbox-style indicators
- shared phone, email, and website indicators
- similar-name business clustering
- cross-source owner and clerk/business matches
- dense-cluster and bridge-entity network markers
- compound business-cluster markers

Fraud markers do not determine guilt. They identify unusual, explainable public-record patterns for analyst review.

## Entity Resolution

OpenFraud now keeps two entity layers:
- `data/processed/entities.csv` and `data/processed/relationships.csv` remain the raw normalized layer.
- `data/processed/canonical_entities.csv`, `data/processed/entity_aliases.csv`, `data/processed/entity_resolution_matches.csv`, and `data/processed/canonical_relationships.csv` are the canonical resolution layer.

Canonical entities preserve:
- deterministic stable canonical IDs
- source provenance across synthetic, connector, API, ArcGIS, and manual data
- aliases back to every original entity record
- explicit match method and confidence

Matching tiers:
- Tier 1: exact deterministic matches such as phone, email, website, parcel ID, and exact unit-level address
- Tier 2: strong compound matches such as exact normalized name plus exact phone, email, or address
- Tier 3: fuzzy candidate generation only; fuzzy name similarity alone is never auto-merged

Confidence and review decisions:
- `AUTO_MERGE` at or above the configured auto-merge threshold
- `REVIEW` for explainable but uncertain matches
- `NO_MERGE` when evidence is insufficient or conflicts

Entity resolution exists to improve lead generation, not to prove fraud. A resolved match does not imply wrongdoing.

### Resolution Commands

```bash
python src/analytics/entity_resolution/resolver.py
python src/run_pipeline.py --reset --include-connectors --health-check
```

The pipeline runs entity resolution after raw entity building, then runs the Fraud Marker Engine against canonical entities and canonical relationships. `anomaly_report.csv` is now generated as a compatibility view from fraud-marker results.

### Canonical Model

`canonical_entities.csv` includes:
- `canonical_entity_id`
- `entity_type`
- `display_name`
- `normalized_value`
- `source_count`
- `record_count`
- `alias_count`
- `source_names`
- `resolution_confidence`
- `resolution_method`

`entity_aliases.csv` preserves:
- the original entity ID
- the original alias value
- the normalized alias
- source name and source type
- the canonical assignment method and confidence

`canonical_relationships.csv` rewrites raw relationship endpoints to canonical IDs while preserving the original contributing relationship IDs and merged source provenance.

## Export Leads

Export high-risk lead files after the pipeline runs:

```bash
python src/reports/export_leads.py
```

Outputs:
- `exports/high_risk_entities.csv`
- `exports/lead_summary.csv`

When the investigation workspace outputs exist, lead export now prefers:
- `data/processed/investigation_leads.csv`
- `data/processed/entity_timelines.csv`
- `data/processed/evidence_packets.csv`

Each exported lead can include:
- lead summary
- entity profile
- timeline event count
- fraud markers
- evidence
- relationships
- supporting sources
- recommended review

## Investigation Engine v1.0

The final Phase 3 lead queue is built from:
- canonical entities
- canonical relationships
- fraud markers
- entity risk
- investigation workspace leads
- network clusters
- timelines
- evidence packets
- source provenance

New outputs:
- `data/processed/prioritized_leads.csv`
- `data/processed/investigation_summary.csv`
- `data/processed/lead_evidence_index.csv`
- `data/processed/review_recommendations.csv`
- local lead packages under `exports/leads/`

Run it directly:

```bash
python src/investigation/investigation_engine.py
```

### Risk Versus Confidence

- `risk` answers: how concerning is the observed pattern
- `confidence` answers: how strongly the available public evidence supports that the pattern exists
- high priority does not prove fraud
- low confidence leads should be validated before referral or escalation

### Priority Levels

- `CRITICAL`
- `HIGH`
- `MEDIUM`
- `LOW`
- `INFORMATIONAL`

Priority weights and thresholds are local config in `config/investigation_engine.json`.

### Evidence Completeness

Every prioritized lead includes:
- `evidence_completeness_score`
- `missing_evidence_fields`
- `evidence_count`

This makes weakly supported leads visible even when the risk pattern is strong.

### Lead Types

OpenFraud currently supports:
- `ENTITY`
- `NETWORK`
- `CROSS_SOURCE_CLUSTER`
- `ADDRESS_CLUSTER`
- `BUSINESS_CLUSTER`
- `PROPERTY_CLUSTER`
- `COMMUNICATION_CLUSTER`
- `TEMPORAL_CLUSTER`

### Investigation Queue

The dashboard top-level queue supports:
- real-data-only, synthetic-only, and all-data views
- source, priority, confidence, lead-type, entity-type, status, and evidence filters
- lead detail with fraud markers, aliases, network context, timeline, evidence, provenance, related leads, and review recommendations

### Review Recommendations

Recommendations are deterministic review steps, not accusations. They are intended to help an analyst decide what to verify next for:
- shared addresses
- property and business overlap
- shared communication identifiers
- temporal clustering
- network and bridge-entity review

### Local Lead Packages

Each `HIGH` or `CRITICAL` prioritized lead can be exported to a local package under `exports/leads/` containing:
- `lead_summary.csv`
- `entities.csv`
- `relationships.csv`
- `fraud_markers.csv`
- `evidence.csv`
- `timeline.csv`
- `sources.csv`
- `recommendations.txt`

Lead IDs remain stable in CSV outputs. Package folder names are filesystem-safe local copies of those IDs.

### Analyst Status Preservation

Analyst-maintained state is stored separately in:
- `data/processed/analyst_lead_state.csv`

Supported local statuses:
- `NEW`
- `TRIAGED`
- `IN_REVIEW`
- `NEEDS_MORE_DATA`
- `REFERRED`
- `CLOSED_NO_ACTION`
- `CLOSED_DUPLICATE`
- `CLOSED_OTHER`

Pipeline rebuilds regenerate analytics outputs but preserve analyst notes and review status from the sidecar state file.

### Real Versus Synthetic Controls

Prioritized leads retain:
- `source_names`
- `source_types`
- `contains_real_data`
- `contains_synthetic_data`

Synthetic-only leads are labeled `DEMO`. The dashboard can isolate:
- all data
- real/API/connector/ArcGIS data only
- synthetic/demo data only

Public records may be incomplete, stale, or inconsistent across sources. Every result should be treated as a lead only, not proof of fraud.

## Investigation Workspace

Phase 3 adds a local investigation workspace built from the existing canonical and fraud-marker outputs. The pipeline now writes:
- `data/processed/investigation_leads.csv`
- `data/processed/entity_timelines.csv`
- `data/processed/evidence_packets.csv`

Lead fields include:
- `Lead ID`
- `Primary Entity`
- `Lead Title`
- `Lead Summary`
- `Risk Score`
- `Confidence`
- `Priority`
- `Status`
- `Date Generated`
- `Fraud Marker Count`
- `Supporting Source Count`
- `Relationship Count`

Optional local-only review fields are preserved in the leads CSV:
- `Lead Notes`
- `Reviewer`
- `Review Date`
- `Disposition`
- `Review Status`
- `Follow-up Needed`

The dashboard investigation workspace supports:
- lead queue
- lead search
- priority, status, risk, confidence, fraud-marker, and source filters
- source provenance display
- entity profile, aliases, timeline, evidence, and relationship review

Timeline rows use available relationship and marker evidence to preserve provenance for:
- business formations and public-record links when present
- property transfers and ownership links when present
- court-style relationship events when present
- source timestamps when available

Every investigation view remains local-only and explainable. No score or lead should be treated as proof of fraud.

## Network Intelligence

Phase 3 Prompt 4 adds the Organized Activity Detection Engine (OADE). It reuses:
- `data/processed/canonical_entities.csv`
- `data/processed/canonical_relationships.csv`
- `data/processed/fraud_markers.csv`
- `data/processed/entity_timelines.csv`

New outputs:
- `data/processed/network_clusters.csv`
- `data/processed/network_summary.csv`
- `data/processed/network_members.csv`
- `data/processed/network_edges.csv`

The network engine:
- discovers connected components as candidate networks
- computes explainable network statistics and risk
- identifies bridge entities
- assigns lightweight communities without a graph database
- preserves source provenance and confidence fields

The dashboard Network Intelligence section shows:
- highest risk networks
- largest networks
- fastest growing networks
- most connected addresses
- most connected owners
- most connected registered agents
- bridge entities
- community summary

Selecting a network shows:
- summary and cluster statistics
- members
- relationships
- fraud markers
- timeline
- evidence
- source provenance
- bridge entities

Run the full local workflow with network intelligence:

```bash
python src/run_pipeline.py --reset --clear-lead-packages --include-connectors --health-check
```

## Health Check

Validate the local MVP state:

```bash
python src/health_check.py
```

The health check verifies:
- `local_osint.duckdb`
- processed CSV outputs
- fraud marker outputs
- investigation engine outputs
- optional exports when present
- required config files:
  - `config/entity_scoring.json`
  - `config/rules.json`
  - `config/sources.json`
  - `config/api_sources.json`
  - `config/entity_resolution.json`
  - `config/fraud_markers.json`
  - `config/network_detection.json`
  - `config/investigation_engine.json`

## Reset and Rebuild

Use `--reset` to remove generated artifacts and rebuild cleanly without touching raw data, config, or docs:

```bash
python src/run_pipeline.py --reset
python src/run_pipeline.py --reset --include-connectors
python src/run_pipeline.py --reset --include-connectors --health-check
python src/run_pipeline.py --reset --clear-lead-packages --include-connectors
python src/run_pipeline.py --reset --clear-lead-packages --include-connectors --health-check
```

The reset step deletes only:
- `local_osint.duckdb`
- `data/processed/*.csv`
- `data/processed/*.parquet`
- `data/processed/*.json`
- `exports/*.csv`

Use `--clear-lead-packages` when you also want to remove generated package directories under `exports/leads/` before rebuilding. This does not delete raw data, config, docs, or analyst sidecar state outside those generated package folders.

## Local-Only Design

OpenFraud stays local-first:
- no Docker
- no cloud resources
- no database servers
- no browser automation
- no website scraping
- no API keys required for sample/demo sources

## Public Data and Source Review

Before enabling any live connector:
- prefer official APIs and open-data endpoints over scraping
- document the source review in `docs/source_reviews/`
- keep `live_access_allowed` disabled unless automated access is clearly permitted
- preserve source lineage in config and processed outputs
- keep restricted or private records out of the repository

## Investigator Limitations

- fraud markers are explainable indicators, not legal or factual conclusions
- high risk does not mean fraud occurred
- confidence reflects evidence quality, not certainty of wrongdoing
- canonical matching is deterministic and explainable, but not perfect
- person and business name similarity alone does not prove identity
- different units in the same building are kept separate unless exact unit evidence matches
- source disagreements are preserved as aliases or review candidates
- all results are investigative leads only, not proof of fraud

## Manual Local Imports

Manual connector entrypoints:
- Sunbiz:
  - `data/raw/sunbiz/sunbiz_entities.csv`
- County property:
  - `data/raw/county_property/property_records.csv`
- County clerk:
  - `data/raw/county_clerk/clerk_records.csv`

Sample/manual commands:
```bash
python src/connectors/sunbiz/local_file_connector.py --input data/raw/sunbiz/sample_sunbiz.csv
python src/connectors/county_property/local_file_connector.py --input data/raw/county_property/sample_property_records.csv
python src/connectors/county_clerk/local_file_connector.py --input data/raw/county_clerk/sample_clerk_records.csv
```

## Sunbiz Daily Integration

Use the authenticated Sunbiz Daily API as the primary corporate-record source when live API access is available.

Setup:
- obtain a Sunbiz Daily API key through your Sunbiz Daily account or account-admin workflow
- place `SUNBIZ_DAILY_API_KEY` in a local `.env`
- keep `.env` out of version control
- review `config/sunbiz_daily.json` before changing county, city, ZIP, date-range, or entity-type filters

Behavior:
- the connector is local-first and writes only local CSV/JSON artifacts
- it preserves source provenance on every entity and relationship
- it preserves bounded raw response snapshots in `data/raw/sunbiz_daily/`
- it does not fetch document images
- it does not scrape any websites
- it respects bounded pagination, retries, timeouts, async job polling, and rate limiting

Limitations:
- the connector only ingests filing metadata and linked parties/addresses
- county filtering is enrichment-based and incomplete, so empty county results do not prove the absence of businesses
- privacy-redacted or incomplete API fields are preserved as limitations, not treated as fraud indicators
- all results remain investigative leads only, not proof of fraud
- API access depends on the configured key and official account access

Outputs:
- `data/processed/sunbiz_daily_businesses.csv`
- `data/processed/sunbiz_daily_entities.csv`
- `data/processed/sunbiz_daily_relationships.csv`
- `data/processed/sunbiz_daily_import_summary.json`
- `data/processed/sunbiz_daily_import_diagnostics.csv`
- `data/processed/sunbiz_parcel_matches.csv`

## Phase 3 Note

OpenFraud currently combines local ingestion, entity resolution, fraud-marker evaluation, entity-risk scoring, exports, and dashboard review in one local workflow. All results are leads only, not proof of fraud.
