from pathlib import Path

import pandas as pd

from src.ingest.generate_synthetic_data import generate_synthetic_dataset


def test_generate_synthetic_dataset_creates_expected_files(tmp_path: Path) -> None:
    output_dir = tmp_path / "synthetic"

    manifest = generate_synthetic_dataset(records=40, output_dir=output_dir)

    assert set(manifest) == {
        "business_entities",
        "properties",
        "web_leads",
        "known_patterns",
    }

    for name in manifest:
        assert manifest[name].exists(), f"Expected {name} output file to exist"
        assert manifest[name].stat().st_size > 0, f"Expected {name} output file to be non-empty"

    business_rows = manifest["business_entities"].read_text(encoding="utf-8").splitlines()
    assert len(business_rows) > 1

    properties_rows = manifest["properties"].read_text(encoding="utf-8").splitlines()
    assert len(properties_rows) > 1

    web_leads_rows = manifest["web_leads"].read_text(encoding="utf-8").splitlines()
    assert len(web_leads_rows) > 1

    pattern_rows = manifest["known_patterns"].read_text(encoding="utf-8").splitlines()
    assert len(pattern_rows) > 1

    combined_text = "\n".join(
        [business_rows[0], properties_rows[0], web_leads_rows[0], pattern_rows[0]]
    )
    assert "address" in combined_text.lower() or "phone" in combined_text.lower()


def test_synthetic_data_generates_explainable_risk_clusters(tmp_path: Path) -> None:
    source_dir = tmp_path / "data" / "raw" / "synthetic"
    processed_dir = tmp_path / "data" / "processed"
    output_db = tmp_path / "local_osint.duckdb"
    entities_path = processed_dir / "entities.csv"
    relationships_path = processed_dir / "relationships.csv"
    anomaly_path = processed_dir / "anomaly_report.csv"
    entity_risk_path = processed_dir / "entity_risk.csv"

    from src.run_pipeline import run_pipeline

    run_pipeline(
        records=80,
        source_dir=source_dir,
        output_db=output_db,
        processed_dir=processed_dir,
        entities_path=entities_path,
        relationships_path=relationships_path,
        anomaly_path=anomaly_path,
        entity_risk_path=entity_risk_path,
    )

    entity_risk = pd.read_csv(entity_risk_path)
    assert (entity_risk["risk_level"] == "High").sum() >= 5
    assert (entity_risk["risk_level"] == "Medium").sum() >= 10
    assert (entity_risk["risk_level"] == "Low").sum() >= 20
