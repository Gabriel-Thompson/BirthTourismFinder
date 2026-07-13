# Sunbiz

- Source name: Sunbiz local file workflow
- Source URL: https://search.sunbiz.org/
- Access method: Local file import only
- Terms reviewed? yes
- Automated access allowed? unknown
- Rate limits: Unknown for live website access; not applicable for local-file-only workflow
- Data fields expected: `record_id`, `business_name`, `address`, `phone`, `email`, `owner_name`
- Privacy concerns: Public corporate records may still contain personal names, mailing addresses, phone numbers, or emails that require careful handling
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
- Source URL: `https://sunbizdaily.com`
- Access method: Official authenticated API using `GET /api/v2/filings/` and `GET /api/v2/filings/{corporation_number}/`
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: Documented contract for this phase uses `per_page <= 100`, `page <= 100`, and `1,000` requests per rolling hour. Respect `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `Retry-After`.
- Data fields expected: corporation number, corporation name, filing type, status, file date, FEI number, principal address, mailing address, registered agent, officers
- Privacy concerns: The API may redact or suppress officers, owners, registered agents, or mailing-address data. Missing redacted fields must not be treated as suspicious.
- Recommended use: Keep imports bounded by county, city, ZIP, date range, and page/record caps. Treat every match as an investigative lead requiring independent review.
- Decision: APPROVED WITH LIMITS
- Notes: This repository only uses the official API contract provided for this phase. It does not scrape HTML, does not download document images, and does not expose the API key in logs or outputs.

### Local Setup

1. Copy `.env.example` to `.env`.
2. Set `SUNBIZ_DAILY_API_KEY=YOUR_SUNBIZ_DAILY_API_KEY_HERE` in `.env`.
3. Review `config/sunbiz_daily.json` and confirm the bounded filters you want to use.
4. Run a mocked validation without a real key:
   `python -m src.connectors.sunbiz_daily_connector --mock --county Hillsborough --max-records 100`
5. If a real key is present locally and live access remains approved, run a bounded live import:
   `python -m src.connectors.sunbiz_daily_connector --county Hillsborough --status active --max-records 100`
6. To run the same import through the full pipeline, use:
   `python src/run_pipeline.py --include-sunbiz --include-connectors --health-check`

### API Contract Notes

- Supported search parameters in this phase:
  `page`, `per_page`, `sort`, `order`, `county`, `city`, `state`, `zip`, `status`, `period`, `start_date`, `end_date`, `filing_type`, `corporation_name`, `corporation_number`, `officer_name`, `registered_agent_name`
- Supported response envelopes:
  `filings`, `results`, `data`, `items`, and a bare list for defensive compatibility
- Supported asynchronous search states:
  `queued`, `running`, `done`
- Connector outputs:
  - `data/processed/sunbiz_daily_businesses.csv`
  - `data/processed/sunbiz_daily_entities.csv`
  - `data/processed/sunbiz_daily_relationships.csv`
  - `data/processed/sunbiz_daily_import_summary.json`
  - `data/processed/sunbiz_daily_import_diagnostics.csv`
  - `data/processed/sunbiz_parcel_matches.csv`

All outputs remain investigative leads only, not proof of fraud.
