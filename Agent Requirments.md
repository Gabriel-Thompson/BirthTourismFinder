# Agent Instructions: Florida Public-Records Fraud Anomaly Discovery Platform

## Mission
Build a lawful, public-records-only anomaly discovery platform that identifies potential facilitator networks and suspicious entity/address clusters related to organized document, benefits, healthcare, and vital-records fraud patterns. The system must generate investigative leads only. It must not accuse individuals, make determinations of fraud, or attempt to access restricted government databases.

## Primary Use Case
Initial focus: Florida-based public-records and OSINT discovery related to potential organized birth-tourism facilitation networks.

The system should identify public signals such as shared addresses, registered agents, LLCs, phone numbers, domains, advertised services, court mentions, property ownership, and repeated entity relationships.

## Core Requirements
- Local-first.
- No Docker.
- No cloud infrastructure.
- No database server.
- Use DuckDB as the local SQL engine.
- Use Streamlit for the UI.
- Prefer APIs and open-data over scraping.
- Results must be explainable.

## Stack
- Python
- DuckDB
- Pandas/Polars
- Parquet
- Streamlit

## Workflow
1. Import data into data/raw.
2. Normalize records.
3. Store in Parquet.
4. Load into local_osint.duckdb.
5. Execute SQL anomaly queries.
6. Display results in Streamlit.
7. Export CSV/PDF reports.

## Future
Remain local until scale requires PostgreSQL, Docker, or cloud resources.

## Non-Negotiable Legal and Ethical Guardrails
1. Use only lawful data sources: official APIs, open-data downloads, public records portals that allow access, manual CSV imports, or user-provided documents.
2. Do not scrape websites when terms of service prohibit automated access.
3. Do not bypass CAPTCHAs, authentication, rate limits, paywalls, IP blocks, or access controls.
4. Do not collect or infer protected health information, birth certificate details, Medicaid claims, immigration records, or restricted vital records.
5. Do not target private individuals based on nationality, ethnicity, race, religion, or protected class.
6. Do not label any person, address, or company as fraudulent. Use terms such as “anomaly,” “lead,” “cluster,” “risk indicator,” and “requires human review.”
7. Maintain source attribution for every data point.
8. Log ingestion source, timestamp, URL or dataset name, and applicable license or access notes.
9. Support deletion/removal of records from the local database if a source requires it or data is later determined inappropriate.
10. Design for analyst review, not automated enforcement.

## Preferred System Name
Working name: Public Records Anomaly Discovery System (PRADS)

Alternate names:
- Government Record Anomaly Discovery
- Vital Records Anomaly Discovery Support Tool
- Public Entity Resolution and Fraud Analytics Platform

## MVP Goal
Build a local or cloud-deployable prototype that ingests public Florida records and produces a ranked list of suspicious public-record clusters for analyst review.

The MVP should demonstrate:
- Data ingestion from at least 2 public sources.
- Address normalization.
- Entity resolution.
- Relationship graph construction.
- Explainable anomaly scoring.
- Analyst report output.

## Recommended MVP Data Sources
Prioritize official, reliable sources in this order:

1. Florida Division of Corporations / Sunbiz
   - Entities, officers, registered agents, mailing addresses, principal addresses.
   - Use lawful access only. If no official API exists, support manual CSV/import or carefully reviewed permitted HTML parsing.

2. County Property Appraiser / GIS Open Data
   - Parcel ownership, site address, mailing address, property type, owner name, corporate ownership.
   - Prefer county open-data portals, GIS shapefiles, ArcGIS REST endpoints, or bulk downloads.

3. County Clerk / Court Records
   - Public case metadata where available.
   - Do not bypass access controls. Avoid bulk scraping unless explicitly permitted.

4. Federal Court Records / PACER-compatible exports
   - Accept manual CSV/PDF imports or lawful third-party APIs.
   - Do not automate unauthorized PACER scraping.

5. Business Websites and Public Advertisements
   - Use only pages that permit crawling or manual analyst-provided URLs.
   - Extract company names, service descriptions, locations, phones, emails, and domains.

6. WHOIS/RDAP Domain Data
   - Use official RDAP APIs or compliant providers.
   - Be aware that registrant data may be redacted.

7. News and Public Reports
   - Use RSS feeds, search APIs, or manual import.
   - Store source URL and publication date.

## Sources to Avoid or Treat Carefully
Avoid scraping or automated collection from:
- Zillow
- Realtor.com
- Airbnb
- VRBO
- Google Maps web pages
- Social media platforms without approved API access

If these are needed, use official APIs, licensed data, user-provided exports, or manual analyst entry.


## Repository Structure
Use this structure unless there is a strong reason not to:

```text
prads/
  README.md
  AGENTS.md
  docker-compose.yml
  .env.example
  backend/
    app/
      main.py
      config.py
      db.py
      models/
      schemas/
      services/
        ingestion/
        normalization/
        entity_resolution/
        scoring/
        reporting/
      routers/
      utils/
    tests/
  frontend/
    streamlit_app.py
  data/
    raw/
    processed/
    samples/
  docs/
    data_sources.md
    legal_guardrails.md
    scoring_model.md
    analyst_workflow.md
```

## Core Data Model
Create normalized tables similar to:

### source_records
- id
- source_name
- source_type
- source_url
- source_license_or_terms
- retrieved_at
- raw_payload_hash
- raw_payload_location

### entities
- id
- entity_type: person, company, address, phone, email, domain, parcel, court_case, website
- display_name
- normalized_name
- confidence
- created_at
- updated_at

### addresses
- id
- raw_address
- normalized_address
- street
- unit
- city
- county
- state
- zip
- latitude
- longitude
- address_type: residential, business, parcel, mailing, unknown
- normalization_confidence

### relationships
- id
- source_entity_id
- target_entity_id
- relationship_type: owns, officer_of, registered_agent_for, located_at, mailing_address, same_phone, same_domain, mentions, linked_to_case
- source_record_id
- confidence
- first_seen
- last_seen

### anomaly_clusters
- id
- cluster_type
- score
- severity: routine_review, elevated_review, immediate_review
- explanation
- created_at
- reviewed_status

### anomaly_indicators
- id
- cluster_id
- indicator_name
- indicator_value
- weight
- explanation
- source_record_id

## Address Normalization Requirements
Implement deterministic normalization before fuzzy matching.

Normalize:
- casing
- street suffixes: Street/St/St.
- directional prefixes/suffixes
- apartment/unit markers: Apt, Unit, #, Suite, Ste
- ZIP+4 to ZIP5 while preserving full ZIP where available
- punctuation removal

Never collapse apartment units into building-level addresses unless explicitly creating a building-level aggregate. Store both:
- normalized_unit_address
- normalized_building_address

This is critical because a cluster at one apartment complex is weaker than a cluster at one exact unit.

## Entity Resolution Requirements
Start with explainable deterministic rules:

High-confidence matches:
- exact normalized email
- exact normalized phone
- exact company document number
- exact parcel ID
- exact domain
- exact normalized unit address

Medium-confidence matches:
- same normalized company name + same officer
- same registered agent + same principal address
- same phone + similar company name
- same website + same address

Low-confidence matches:
- fuzzy name similarity only
- same building address only
- partial phone/address overlap

Do not merge entities permanently on weak evidence. Store candidate relationships with confidence.

## Initial Anomaly Rules
Build explainable scoring rules. Each rule should output supporting evidence.

### Rule 1: Shared Registered Agent / Address Cluster
Flag when multiple entities share the same registered agent, principal address, or mailing address and also match sensitive keywords or suspicious service categories.

### Rule 2: Residential Property Used by Many Business Entities
Flag a residential parcel or single-family address associated with unusually many companies, domains, ads, or court mentions.

### Rule 3: Maternity / Immigration / Concierge Keyword Network
Flag entities that publicly advertise combinations of terms such as:
- birth package
- maternity tourism
- give birth in USA
- U.S. citizenship for baby
- postpartum house
- confinement center
- visa support
- hospital package
- maternity concierge

Important: keywords are leads only and must not be treated as proof of wrongdoing.

### Rule 4: Known Case Pattern Similarity
Compare public entities to patterns from prior public prosecutions:
- multiple houses connected to one facilitator
- housing + transportation + hospital support
- visa coaching language
- same owner/agent across multiple LLCs
- repeated use of residential properties

### Rule 5: Cross-Source Reinforcement
Increase severity when the same entity/address appears across multiple independent public sources, such as corporation records + property records + website + court records.

## Suggested Scoring Bands
Use adjustable thresholds:

- 0-39: informational
- 40-59: routine analyst review
- 60-79: elevated review
- 80-100: immediate review candidate

Severity should be driven by both rarity and source corroboration.

Example indicators:
- exact same residential unit tied to 5+ companies: +20
- same phone across 3+ entities: +15
- maternity/visa/birth package keywords: +20
- linked public court mention: +25
- same principal address and registered agent across multiple entities: +10
- property classified as single-family residential: +10
- source corroboration across 3+ source types: +15

## Analyst Output Requirements
Every flagged cluster must include:
- cluster score
- severity band
- plain-English explanation
- involved entities
- involved addresses
- source records and links
- indicators that contributed to the score
- confidence notes
- recommended next step: review, dismiss, enrich, or refer

Never output “fraud confirmed.”

Use language like:
- “This cluster may warrant review.”
- “This public-record pattern is statistically unusual.”
- “This entity relationship resembles publicly reported facilitation patterns.”


## Documentation Requirements
Maintain:
- README with setup and run instructions
- data_sources.md with source access method and terms notes
- scoring_model.md with rule explanations and weights
- legal_guardrails.md with prohibited data/access patterns
- analyst_workflow.md with review process

## Agent Behavior Rules
When implementing:
1. Prefer simple, explainable logic over AI/ML.
2. Use SQL and deterministic matching first.
3. Do not add scraping of prohibited or questionable sources.
4. Do not add birth-record, Medicaid, immigration, or hospital-data fields unless explicitly provided through lawful authorized channels.
5. Do not make claims that the system detects crime. It detects anomalies and relationship patterns.
6. Keep the MVP small and deployable.
7. Ask for approval before adding any connector that automates access to a website without an official API or open-data download.
8. Preserve source lineage for every record.
9. Make every score explainable.
10. Avoid protected-class targeting and avoid nationality-based scoring.

Do not implement external crawlers in the first build. Start with manual CSV/open-data imports and source-attributed records.
