# County Clerk

- Source name: County Clerk Records
- Source URL: Escambia County clerk/court/official-record public source to be confirmed manually before each download
- Access method: Local file import only
- Terms reviewed? yes
- Automated access allowed? unknown
- Rate limits: Unknown for live access; not applicable for local file-only workflow
- Data fields expected: case_number, filing_date, record_type, party_name, party_role, business_name, address, document_type, status
- Privacy concerns: Clerk records may include personal names, addresses, signatures, and filing details that should not be redistributed casually
- Recommended use: Prefer official county downloads or public export workflows and keep all imported files local to the repository workspace
- Notes: Current project use is restricted to local CSV ingestion. No live website access, scraping, or API use is implemented by this repository.

## Manual Download Workflow

1. Manually review the current county clerk, court, or official-record source terms and access conditions before downloading any file.
2. Use a browser to manually obtain a public export if the county terms permit that workflow.
3. Save the file locally as `data/raw/county_clerk/clerk_records.csv`.
4. The connector accepts flexible header names for these common fields:
   `case_number`, `filing_date`, `record_type`, `party_name`, `party_role`, `business_name`, `address`, `document_type`, `status`.
5. Run the connector locally:
   `python src/connectors/county_clerk/local_file_connector.py`
6. To test the connector with the local sample file instead, run:
   `python src/connectors/county_clerk/local_file_connector.py --input data/raw/county_clerk/sample_clerk_records.csv`
7. To include the manually downloaded county clerk file in the full pipeline, run:
   `python src/run_pipeline.py --include-connectors`

This workflow does not scrape county sites, does not call live websites from the codebase, and does not add API integration.
