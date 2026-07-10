# Sample ArcGIS Parcels

- Source name: Sample ArcGIS Parcels
- Source URL: https://sampleserver.example.invalid/arcgis/rest/services/Parcels/FeatureServer/0/query
- Access method: Official ArcGIS REST API pattern using a local mock response for development
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: Configured locally in `config/api_sources.json`; live sample URL is a placeholder only
- Data fields expected: parcel_id, owner_name, situs_address, mailing_address, land_use, assessed_value, sale_date, sale_price, latitude, longitude
- Privacy concerns: Parcel ownership and mailing-address data can contain personal information and should be limited to lawful public records
- Recommended use: Use this sample as a safe ArcGIS REST framework example before enabling a real county GIS endpoint
- Notes: The repository uses a local mocked ArcGIS response file for this source. No API key is required and tests do not use the internet.
