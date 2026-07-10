from __future__ import annotations

import argparse
import csv
from pathlib import Path
from random import Random
from typing import Dict, List


DEFAULT_OUTPUT_DIR = Path("data/raw/synthetic")
KEYWORDS = [
    "maternity",
    "postpartum",
    "travel",
    "baby",
    "birth",
    "care",
    "support",
    "itinerary",
    "visa",
    "lodging",
]


def generate_synthetic_dataset(records: int = 250, output_dir: Path | str | None = None) -> Dict[str, Path]:
    rng = Random(42)
    output_path = Path(output_dir or DEFAULT_OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    business_rows: List[Dict[str, str]] = []
    property_rows: List[Dict[str, str]] = []
    web_rows: List[Dict[str, str]] = []
    pattern_rows: List[Dict[str, str]] = []

    def make_business(index: int, name: str, address: str, phone: str, website: str, source_label: str) -> Dict[str, str]:
        created_date = f"2024-{(index % 12) + 1:02d}-{(index % 28) + 1:02d}"
        return {
            "record_id": f"biz-{index:04d}",
            "business_name": name,
            "llc_name": f"{name} LLC",
            "address": address,
            "phone": phone,
            "email": f"contact{index}@{name.replace(' ', '').lower()}.example",
            "website": website,
            "source_label": source_label,
            "created_date": created_date,
        }

    def make_property(index: int, property_address: str, owner_name: str, llc_name: str, source_label: str) -> Dict[str, str]:
        recorded_date = f"2024-{(index % 12) + 1:02d}-{(index % 28) + 1:02d}"
        return {
            "record_id": f"prop-{index:04d}",
            "property_address": property_address,
            "owner_name": owner_name,
            "llc_name": llc_name,
            "source_label": source_label,
            "recorded_date": recorded_date,
        }

    def make_web(index: int, lead_name: str, phone: str, email: str, website: str, notes: str, source_label: str) -> Dict[str, str]:
        collected_date = f"2024-{(index % 12) + 1:02d}-{(index % 28) + 1:02d}"
        return {
            "record_id": f"web-{index:04d}",
            "lead_name": lead_name,
            "phone": phone,
            "email": email,
            "website": website,
            "notes": notes,
            "source_label": source_label,
            "collected_date": collected_date,
        }

    def make_pattern(index: int, entity_name: str, keyword: str, context: str, source_label: str) -> Dict[str, str]:
        observed_date = f"2024-{(index % 12) + 1:02d}-{(index % 28) + 1:02d}"
        return {
            "record_id": f"pattern-{index:04d}",
            "entity_name": entity_name,
            "keyword": keyword,
            "context": context,
            "source_label": source_label,
            "observed_date": observed_date,
            "suspicion_score": 0.2 + (index % 5) * 0.15,
        }

    # Cluster 1: shared address
    shared_address = "100 Shared Address Ln, Miami, FL 33101"
    shared_address_names = [
        "Harbor Family Support",
        "Harbor Family Services",
        "Harbor Family Travel",
        "Harbor Family Care",
    ]
    for i, business_name in enumerate(shared_address_names):
        business_rows.append(make_business(i, business_name, shared_address, "305-555-0100", "https://harborfamily.example", "public_listing"))

    # Cluster 2: shared phone
    shared_phone_address = "200 Shared Phone Blvd, Miami, FL 33102"
    shared_phone_numbers = ["305-555-0200", "305-555-0201", "305-555-0202", "305-555-0203"]
    shared_phone_names = [
        "Phone Travel Advisors",
        "Phone Maternity Care",
        "Phone Family Services",
        "Phone Wellness Group",
    ]
    for i, business_name in enumerate(shared_phone_names, start=len(business_rows)):
        phone = shared_phone_numbers[i - len(shared_address_names)]
        business_rows.append(make_business(i, business_name, shared_phone_address, phone, "https://phonecare.example", "directory"))

    # Cluster 3: shared website
    shared_website_address = "300 Shared Website Ave, Miami, FL 33103"
    shared_websites = [
        "https://web-travel.example",
        "https://web-maternity.example",
        "https://web-care.example",
        "https://web-assist.example",
    ]
    shared_website_names = [
        "Web Travel Solutions",
        "Web Maternity Services",
        "Web Care Partners",
        "Web Support Collective",
    ]
    for i, business_name in enumerate(shared_website_names, start=len(business_rows)):
        website = shared_websites[i - len(shared_address_names) - len(shared_phone_names)]
        business_rows.append(make_business(i, business_name, shared_website_address, "305-555-0300", website, "partner_feed"))

    # Cluster 4: property ownership
    owner_business_name = "Sunrise Maternity Holdings"
    owner_llc_name = f"{owner_business_name} LLC"
    owner_address = "400 Ownership Way, Miami, FL 33104"
    business_rows.append(make_business(len(business_rows), owner_business_name, owner_address, "305-555-0400", "https://sunrisematernity.example", "manual_import"))
    for j in range(5):
        property_rows.append(
            make_property(
                j,
                f"{500 + j} Property Blvd, Miami, FL 33105",
                owner_llc_name,
                owner_llc_name,
                "properties_feed",
            )
        )

    # Cluster 5: maternity/travel keyword patterns
    keyword_names = [
        "Morning Maternity Journey",
        "Ocean Travel Births",
        "Postpartum Planning Group",
        "Family Travel Wellness",
    ]
    keyword_clusters = [
        ("maternity", keyword_names[0]),
        ("travel", keyword_names[1]),
        ("postpartum", keyword_names[2]),
        ("maternity", keyword_names[3]),
    ]
    for k_index, (keyword, entity_name) in enumerate(keyword_clusters):
        for repeat in range(5):
            pattern_rows.append(make_pattern(len(pattern_rows), entity_name, keyword, "listing", "manual_import"))

    # Build a few business entities that also match pattern entity names for known_pattern_address
    for i, entity_name in enumerate(keyword_names, start=len(business_rows)):
        business_rows.append(make_business(i, entity_name, f"{600 + i} Pattern Ave, Miami, FL 33106", "305-555-0500", f"https://{entity_name.replace(' ', '').lower()}.example", "public_listing"))

    # Filler rows to reach target counts and provide low-risk noise
    while len(business_rows) < records:
        index = len(business_rows)
        business_name = f"{rng.choice(['North', 'South', 'Harbor', 'Sunrise', 'Blue', 'Ocean'])} {rng.choice(['Family', 'Travel', 'Care', 'Support'])} {index}"
        llc_name = f"{business_name} LLC"
        address = f"{700 + index % 50} {rng.choice(['Oak', 'Pine', 'Cedar', 'Biscayne', 'Maple'])} St, {rng.choice(['Miami', 'Fort Lauderdale', 'Orlando', 'Tampa'])}, FL {33100 + (index % 20)}"
        phone = f"305-555-{2000 + index % 8000:04d}"
        website = f"https://{rng.choice(['harborfamily', 'sunrisecare', 'familytravel', 'carepath', 'oceanwellness'])}{index % 11}.example"
        source_label = rng.choice(["public_listing", "directory", "manual_import", "partner_feed"])
        business_rows.append(make_business(index, business_name, address, phone, website, source_label))

    while len(property_rows) < records:
        index = len(property_rows)
        address = f"{800 + index % 50} {rng.choice(['Main', 'River', 'Bay', 'Park', 'Lake'])} Ave, {rng.choice(['Miami', 'Hollywood', 'West Palm Beach'])}, FL {33400 + (index % 25)}"
        owner_name = rng.choice([row["llc_name"] for row in business_rows])
        property_rows.append(make_property(index, address, owner_name, owner_name, "properties_feed"))

    while len(web_rows) < records:
        index = len(web_rows)
        lead_name = f"{rng.choice(['Alicia', 'Marcus', 'Rina', 'Tomas', 'Leah', 'Jasmine'])} {rng.choice(['Rivera', 'Soto', 'Nguyen', 'Jones', 'Garcia', 'Kim'])}"
        phone = f"305-555-{3000 + index % 7000:04d}"
        email = f"lead{index}@{rng.choice(['harborcare.example', 'sunrisetravel.example', 'birthsupport.example'])}"
        website = f"https://{rng.choice(['harbormoms', 'travelbirth', 'carepath', 'familyhere'])}{index % 10}.example"
        notes = f"{rng.choice(KEYWORDS)} {rng.choice(['service', 'support', 'travel', 'planning'])} {rng.choice(['package', 'itinerary', 'assistance', 'guide'])}"
        web_rows.append(make_web(index, lead_name, phone, email, website, notes, "directory"))

    while len(pattern_rows) < records:
        index = len(pattern_rows)
        entity_name = rng.choice([row["business_name"] for row in business_rows])
        keyword = rng.choice(KEYWORDS)
        context = rng.choice(["website_copy", "listing", "manual_note", "advertisement"])
        source_label = rng.choice(["manual_import", "partner_feed", "public_listing"])
        pattern_rows.append(make_pattern(index, entity_name, keyword, context, source_label))

    files = {
        "business_entities": write_csv(output_path / "business_entities.csv", business_rows),
        "properties": write_csv(output_path / "properties.csv", property_rows),
        "web_leads": write_csv(output_path / "web_leads.csv", web_rows),
        "known_patterns": write_csv(output_path / "known_patterns.csv", pattern_rows),
    }
    return files


def write_csv(path: Path, rows: List[Dict[str, str]]) -> Path:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic public-record style datasets for local testing.")
    parser.add_argument("--records", type=int, default=250, help="Number of synthetic records to generate per file")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory where synthetic files should be written")
    args = parser.parse_args()

    files = generate_synthetic_dataset(records=args.records, output_dir=args.output_dir)
    print("Generated synthetic data files:")
    for name, path in files.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
