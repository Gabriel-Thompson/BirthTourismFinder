from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

from src.analytics.fraud_markers.engine import (
    ANOMALY_COMPAT_PATH,
    CANONICAL_ENTITIES_PATH,
    CANONICAL_RELATIONSHIPS_PATH,
    CONFIG_PATH,
    FRAUD_MARKERS_PATH,
    FRAUD_MARKER_SUMMARY_PATH,
    FraudMarkerEngine,
)

DB_PATH = Path("local_osint.duckdb")
OUTPUT_PATH = ANOMALY_COMPAT_PATH


class AnomalyEngine:
    """Compatibility wrapper around the Fraud Marker Engine."""

    def __init__(self, db_path: Path | str = DB_PATH, output_path: Path | str = OUTPUT_PATH, config_path: Path | str = CONFIG_PATH) -> None:
        self.db_path = Path(db_path)
        self.output_path = Path(output_path)
        self.config_path = Path(config_path)
        self.engine = FraudMarkerEngine(
            db_path=self.db_path,
            entities_path=CANONICAL_ENTITIES_PATH,
            relationships_path=CANONICAL_RELATIONSHIPS_PATH,
            output_path=FRAUD_MARKERS_PATH,
            summary_path=FRAUD_MARKER_SUMMARY_PATH,
            compatibility_output_path=self.output_path,
            config_path=self.config_path,
        )

    def run(self) -> List[Dict[str, object]]:
        return self.engine.run()

    def summarize(self, findings: List[Dict[str, object]]) -> Dict[str, int]:
        return self.engine.summarize(findings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OpenFraud fraud marker engine and compatibility anomaly export.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to the DuckDB database")
    parser.add_argument("--output-path", default=str(OUTPUT_PATH), help="Where to write the compatibility anomaly report")
    parser.add_argument("--config-path", default=str(CONFIG_PATH), help="Path to the fraud marker config")
    args = parser.parse_args()

    engine = AnomalyEngine(db_path=args.db_path, output_path=args.output_path, config_path=args.config_path)
    findings = engine.run()
    summary = engine.summarize(findings)
    print("Anomaly Engine: PASS")
    print(f"Found {summary['High']} High Risk Findings")
    print(f"Found {summary['Medium']} Medium Risk Findings")
    print(f"Found {summary['Low']} Low Risk Findings")


if __name__ == "__main__":
    main()
