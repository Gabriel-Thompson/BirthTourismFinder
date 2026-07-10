# Synthetic Dataset

- Source name: Synthetic Dataset
- Source URL: Local generator in `src/ingest/generate_synthetic_data.py`
- Access method: Local generator
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: Not applicable for local generation
- Data fields expected: business_entities, properties, web_leads, known_patterns synthetic tables
- Privacy concerns: None for generated test data, but it should not be mixed with real restricted records without clear separation
- Recommended use: Use for local development, testing, and demonstrations where no live or third-party source should be contacted
- Notes: This source is generated entirely within the repository and is the default safe dataset for the pipeline.
