# Sample API

- Source name: Sample API
- Source URL: https://demo.example.invalid/open-data/entities
- Access method: Official API or open-data endpoint pattern using local mock response for development
- Terms reviewed? yes
- Automated access allowed? yes
- Rate limits: Configured locally in `config/api_sources.json`; live demo URL is a placeholder only
- Data fields expected: id, name, address, website, category
- Privacy concerns: Keep restricted or private records out of the repository and prefer public/open datasets only
- Recommended use: Use this sample source as a safe framework example before enabling any real official API source
- Notes: The repository uses a local mocked response file for this source. No API key is required and no live website is contacted during tests.
