# Sunbiz

- Source name: Sunbiz
- Source URL: https://search.sunbiz.org/
- Access method: Local file import only
- Terms reviewed? yes
- Automated access allowed? unknown
- Rate limits: Unknown for live access; not applicable for local file-only workflow
- Data fields expected: record_id, business_name, address, phone, email, owner_name
- Privacy concerns: Business registration records may still contain personal names, mailing addresses, phone numbers, or emails that require careful handling
- Recommended use: Use only pre-downloaded local files after confirming terms and handling requirements; do not add scraping to this repo without a fresh review
- Notes: Current project use is restricted to local CSV ingestion. No live website access is implemented or allowed by default.

## Manual Download Workflow

1. Review the current Sunbiz terms and access conditions manually before downloading any data.
2. Use your browser to manually obtain a public Sunbiz export if the site terms permit that workflow.
3. Save the file locally as `data/raw/sunbiz/sunbiz_entities.csv`.
4. Ensure the CSV includes the fields expected by this connector:
   `record_id`, `business_name`, `address`, `phone`, `email`, `owner_name`.
5. Run the connector locally:
   `python src/connectors/sunbiz/local_file_connector.py`
6. To test the connector with a local sample file instead, run:
   `python src/connectors/sunbiz/local_file_connector.py --input data/raw/sunbiz/sample_sunbiz.csv`
7. To include the manually downloaded Sunbiz file in the full pipeline, run:
   `python src/run_pipeline.py --include-connectors`

This workflow does not scrape Sunbiz, does not call live websites from the codebase, and does not add any API integration.

## Sunbiz Daily API

- Source name: Sunbiz Daily API
- Source URL: Configured in `config/sunbiz_daily.json`
- Access method: Official authenticated API
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: Governed by `config/sunbiz_daily.json` and the account/API agreement
- Data fields expected: business filings, principal address, mailing address, registered agent, officers, entity type, status, filing date, document number
- Privacy concerns: Public corporate records may still include personal names and mailing addresses that require careful handling and lead-only treatment
- Recommended use: Use bounded county/date-filtered imports, preserve provenance on every entity and relationship, and correlate against local parcel and other public-source records only
- Notes: Place `SUNBIZ_DAILY_API_KEY` in a local `.env` file. The connector stays local, does not download document images, and does not perform any web scraping.

### Local Setup

1. Copy `.env.example` to `.env`.
2. Set `SUNBIZ_DAILY_API_KEY` in `.env`.
3. Review `config/sunbiz_daily.json` and confirm the county/date/entity filters you want to use.
4. Run a bounded import such as:
   `python src/connectors/sunbiz_daily_connector.py --county Hillsborough --limit 100`
5. To run the same import through the full pipeline, use:
   `python src/run_pipeline.py --include-sunbiz --include-connectors --health-check`
