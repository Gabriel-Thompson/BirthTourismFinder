# County Property

- Source name: County Property Records
- Source URL: Escambia County Property Appraiser home page `https://www.escpa.org/` and public map `https://www.escpa.org/ecpamap/`
- Access method: Local file import only for production use; ArcGIS REST-style connector config retained in mock-only mode pending explicit automation approval
- Terms reviewed? yes
- Automated access allowed? unknown
- Rate limits: Unknown for live access; not applicable for local file-only workflow
- Data fields expected: parcel_id, owner_name, situs_address, mailing_address, property_use, land_use, assessed_value, sale_date, sale_price
- Privacy concerns: Ownership and mailing records may include personal addresses and related identifying details
- Recommended use: Prefer official county bulk export or open-data download workflows and keep all imported files local to the repository workspace
- Notes: As of 2026-07-09, the reviewed Escambia public pages exposed an official property appraiser site and map, but this repository did not confirm a clearly documented public ArcGIS REST parcel query endpoint or an explicit statement allowing automated API access from those pages. For that reason, `escambia_arcgis_parcels` is documented in config but kept `live_access_allowed: false`.

## First Live ArcGIS Source

- Source name: Hillsborough County Public Parcels
- Source URL: `https://maps.hillsboroughcounty.org/arcgis/rest/services/InfoLayers/HC_ParcelsPublic/FeatureServer/0`
- Agency: Hillsborough County, Florida
- Access method: Official county ArcGIS REST FeatureServer query endpoint
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: No published rate limit was found in the reviewed service metadata; the service reports `maxRecordCount: 1000`. OpenFraud defaults to `default_limit: 100`, `rate_limit_per_minute: 30`, `retry_attempts: 2`, and `timeout_seconds: 15`.
- Data fields expected: `STRAP`, `OWNER`, `ADDR_1`, `ADDR_2`, `CITY`, `STATE`, `ZIP`, `SITE_ADDR`, `SITE_CITY`, `SITE_ZIP`, `DOR_CODE`, `ASD_VAL`, `S_DATE`, `S_AMT`
- Privacy concerns: Owner and mailing-address fields can include personal names and residential mailing addresses. The county service description states this public layer masks confidential data, but the remaining public fields still require careful handling.
- Recommended use: Use the official public parcel layer for small bounded ArcGIS REST queries only. Keep requests narrow, prefer `resultRecordCount <= 100` for validation, and treat resulting entities and relationships as leads only.
- Decision: APPROVED WITH LIMITS
- Notes: As reviewed on 2026-07-09, `https://maps.hillsboroughcounty.org/arcgis/rest/services?f=pjson` exposed an official county ArcGIS REST directory, and `InfoLayers/HC_ParcelsPublic/FeatureServer/0` advertised `capabilities: "Query"` with service description `Parcel map service for general use. Masking confidential data`. This is the first live ArcGIS parcel source onboarded as `florida_county_arcgis_parcels`.

## ArcGIS Review Notes

- Source URL: `https://www.escpa.org/ecpamap/`
- Access method: Public county GIS map observed through the official property appraiser website
- Terms reviewed? yes
- Automated access allowed? unknown
- Fields used: `parcel_id`, `owner_name`, `situs_address`, `mailing_address`, `land_use`, `assessed_value`, `sale_date`, `sale_price`, `latitude`, `longitude`
- Notes: Until Escambia exposes a clearly documented ArcGIS REST parcel endpoint and explicitly permits automated access, OpenFraud uses only the mocked/sample ArcGIS response for connector tests.

## Manual Download Workflow

1. Manually review the current county property appraiser terms and access conditions before downloading any file.
2. Use a browser to manually obtain a public parcel or property export if the county terms permit that workflow.
3. Save the file locally as `data/raw/county_property/property_records.csv`.
4. The connector accepts flexible header names for these common fields:
   `parcel_id`, `owner_name`, `situs_address`, `mailing_address`, `property_use`, `land_use`, `assessed_value`, `sale_date`, `sale_price`.
5. Run the connector locally:
   `python src/connectors/county_property/local_file_connector.py`
6. To test the connector with the local sample file instead, run:
   `python src/connectors/county_property/local_file_connector.py --input data/raw/county_property/sample_property_records.csv`
7. To include the manually downloaded county property file in the full pipeline, run:
   `python src/run_pipeline.py --include-connectors`

This workflow does not scrape county sites, does not call live websites from the codebase, and does not add API integration.
