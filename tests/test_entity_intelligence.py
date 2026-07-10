from pathlib import Path

import pandas as pd

from src.analytics.entity_intelligence import build_entity_risk


def test_build_entity_risk_simple_case(tmp_path: Path) -> None:
    entities = pd.DataFrame([
        {"entity_id": "biz-1", "display_name": "Acme", "entity_type": "business", "source": "business_entities", "source_name": "synthetic", "source_type": "synthetic"},
        {"entity_id": "addr-1", "display_name": "123 Main St", "entity_type": "address", "source": "properties", "source_name": "synthetic", "source_type": "synthetic"},
    ])

    relationships = pd.DataFrame([
        {"source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "confidence": 1.0}
    ])

    anomalies = pd.DataFrame([
        {
            "Risk Score": 40,
            "Rule Triggered": "Shared address",
            "Supporting Evidence": "Acme appears near 123 Main St",
            "Entity IDs": "biz-1,addr-1",
            "Addresses": "123 Main St",
            "Phone Numbers": "",
            "Source Table": "business_entities",
            "source_name": "synthetic",
            "source_type": "synthetic",
        }
    ])

    # use explicit scoring for reproducibility
    scoring = {
        "direct_entity_id_match": 30,
        "address_text_match": 8,
        "phone_text_match": 6,
        "supporting_evidence_match": 10,
        "relationship_count_multiplier": 2,
        "max_relationship_bonus": 20,
        "max_score": 100,
        "high_threshold": 70,
        "medium_threshold": 35,
        "low_threshold": 1,
    }

    out = build_entity_risk(entities, relationships, anomalies, scoring=scoring)
    assert not out.empty
    # ensure processing stats exposed
    assert "anomaly_rows_processed" in out.attrs
    assert out.attrs["anomaly_rows_processed"] >= 1
    biz = out[out["entity_id"] == "biz-1"].iloc[0]
    addr = out[out["entity_id"] == "addr-1"].iloc[0]

    # both should have some risk because they appear in the anomaly
    assert biz["risk_score"] > 0
    assert addr["risk_score"] > 0
    assert "Shared address" in biz["contributing_rules"] or "Shared address" in addr["contributing_rules"]
    assert biz["source_name"] == "synthetic"
    assert biz["source_type"] == "synthetic"


def test_cli_writes_output(tmp_path: Path) -> None:
    # write small CSVs and run main to write output to tmp_path
    ents = tmp_path / "entities.csv"
    rels = tmp_path / "relationships.csv"
    anom = tmp_path / "anomaly.csv"
    outp = tmp_path / "entity_risk.csv"

    entities = pd.DataFrame([
        {"entity_id": "biz-1", "display_name": "Acme", "entity_type": "business", "source": "business_entities", "source_name": "synthetic", "source_type": "synthetic"},
        {"entity_id": "addr-1", "display_name": "123 Main St", "entity_type": "address", "source": "properties", "source_name": "synthetic", "source_type": "synthetic"},
    ])
    relationships = pd.DataFrame([
        {"source_entity_id": "biz-1", "target_entity_id": "addr-1", "relationship_type": "LOCATED_AT", "confidence": 1.0}
    ])
    anomalies = pd.DataFrame([
        {
            "Risk Score": 40,
            "Rule Triggered": "Shared address",
            "Supporting Evidence": "Acme appears near 123 Main St",
            "Entity IDs": "biz-1,addr-1",
            "Addresses": "123 Main St",
            "Phone Numbers": "",
            "Source Table": "business_entities",
            "source_name": "synthetic",
            "source_type": "synthetic",
        }
    ])

    entities.to_csv(ents, index=False)
    relationships.to_csv(rels, index=False)
    anomalies.to_csv(anom, index=False)

    # call main with paths
    from src.analytics import entity_intelligence as ei

    ei.main(entities_path=ents, relationships_path=rels, anomaly_path=anom, output_path=outp, config_path=None)
    assert outp.exists()
    # output should include expected columns
    d = pd.read_csv(outp)
    assert "entity_id" in d.columns
    assert "source_name" in d.columns
    assert "source_type" in d.columns
    assert len(d) == 2
