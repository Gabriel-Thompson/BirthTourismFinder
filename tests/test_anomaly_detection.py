from pathlib import Path

import duckdb

from src.analytics.anomaly_detection import build_anomaly_report, summarize_findings


def test_build_anomaly_report_writes_csv_and_summarizes(tmp_path: Path) -> None:
    db_path = tmp_path / "local_osint.duckdb"
    output_path = tmp_path / "anomaly_report.csv"

    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE business_entities (
                record_id VARCHAR,
                business_name VARCHAR,
                llc_name VARCHAR,
                address VARCHAR,
                phone VARCHAR,
                website VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO business_entities VALUES
            ('b1', 'Alpha Care', 'Alpha LLC', '123 Main St', '555-0100', 'https://alpha.example'),
            ('b2', 'Beta Care', 'Beta LLC', '123 Main St', '555-0200', 'https://beta.example'),
            ('b3', 'Gamma Care', 'Alpha LLC', '123 Main St', '555-0300', 'https://gamma.example')
            """
        )
        conn.execute(
            """
            CREATE TABLE known_patterns (
                record_id VARCHAR,
                entity_name VARCHAR,
                keyword VARCHAR,
                source_label VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO known_patterns VALUES
            ('k1', 'Alpha Care', 'maternity', 'manual_import'),
            ('k2', 'Beta Care', 'postpartum', 'manual_import'),
            ('k3', 'Gamma Care', 'travel', 'manual_import')
            """
        )

    findings = build_anomaly_report(db_path=db_path, output_path=output_path)

    assert output_path.exists()
    assert len(findings) >= 6

    summary = summarize_findings(findings)
    assert summary["High"] >= 1
    assert summary["Medium"] >= 1
    assert summary["Low"] >= 1
