from pathlib import Path
import json

from src.analytics.entity_intelligence import load_scoring_config


def test_load_default_when_missing(tmp_path: Path) -> None:
    # point to non-existent file
    cfg = load_scoring_config(tmp_path / "nope.json")
    assert cfg["direct_entity_id_match"] == 30
    assert cfg["max_score"] == 100


def test_load_override(tmp_path: Path) -> None:
    data = {"direct_entity_id_match": 50, "max_score": 80}
    p = tmp_path / "sc.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    cfg = load_scoring_config(p)
    assert cfg["direct_entity_id_match"] == 50
    assert cfg["max_score"] == 80
