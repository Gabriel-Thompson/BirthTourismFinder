from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.statistical_risk.baselines import build_statistical_observations, summarize_baselines
from src.analytics.statistical_risk.calibration import adjustment_value_for_context, determine_review_level
from src.analytics.statistical_risk.models import ContextualAdjustmentRow, StatisticalRarityRow
from src.analytics.statistical_risk.rarity import (
    conservative_compound_score,
    empirical_percentile,
    iqr_outlier_score,
    poisson_probability_at_least,
    rarity_score_from_probability,
    robust_z_score,
)

DEFAULT_PROCESSED_DIR = Path("data/processed")
DEFAULT_CANONICAL_ENTITIES_PATH = DEFAULT_PROCESSED_DIR / "canonical_entities.csv"
DEFAULT_CANONICAL_RELATIONSHIPS_PATH = DEFAULT_PROCESSED_DIR / "canonical_relationships.csv"
DEFAULT_CROSS_SOURCE_MATCHES_PATH = DEFAULT_PROCESSED_DIR / "cross_source_matches.csv"
DEFAULT_BASELINES_PATH = DEFAULT_PROCESSED_DIR / "statistical_baselines.csv"
DEFAULT_RARITY_PATH = DEFAULT_PROCESSED_DIR / "statistical_rarity.csv"
DEFAULT_ADJUSTMENTS_PATH = DEFAULT_PROCESSED_DIR / "contextual_risk_adjustments.csv"
DEFAULT_SUMMARY_PATH = DEFAULT_PROCESSED_DIR / "statistical_marker_summary.json"
DEFAULT_CALIBRATION_REPORT_PATH = DEFAULT_PROCESSED_DIR / "statistical_calibration_report.csv"
CONFIG_PATH = Path("config/statistical_risk.json")

DEFAULT_CONFIG: dict[str, Any] = {
    "baseline_mode": "REAL_ONLY",
    "review_probability_thresholds": {
        "routine_review": 0.15,
        "elevated_review": 0.05,
        "immediate_review": 0.01,
        "extreme_outlier": 0.001,
        "percentile_elevated": 0.95,
        "percentile_immediate": 0.99,
        "percentile_extreme": 0.999,
    },
    "minimum_comparison_group_size": 5,
    "enabled_statistical_models": ["empirical_percentile", "poisson", "robust_z", "iqr"],
    "temporal_windows": [3, 7, 30, 90, 365],
    "address_context_adjustments": {
        "SINGLE_FAMILY_RESIDENTIAL": 8,
        "MULTIFAMILY_RESIDENTIAL": 3,
        "APARTMENT_BUILDING": -3,
        "EXACT_APARTMENT_OR_UNIT": 6,
        "COMMERCIAL_PROPERTY": -6,
        "OFFICE_BUILDING": -5,
        "VIRTUAL_OFFICE_OR_MAILBOX": -4,
        "GOVERNMENT_FACILITY": -6,
        "REGISTERED_AGENT_SERVICE_ADDRESS": -5,
        "UNKNOWN": 0,
    },
    "property_context_adjustments": {
        "UNKNOWN": 0,
        "SINGLE_FAMILY_RESIDENTIAL": 4,
        "COMMERCIAL_PROPERTY": -3,
    },
    "communication_context_adjustments": {
        "UNKNOWN": 0,
        "COMMERCIAL_PROPERTY": -2,
        "EXACT_APARTMENT_OR_UNIT": 3,
        "VIRTUAL_OFFICE_OR_MAILBOX": -2,
    },
    "maximum_positive_adjustment": 8,
    "maximum_negative_adjustment": -8,
    "insufficient_baseline_behavior": "LABEL_ONLY",
    "real_data_only_default": True,
    "compound_marker_strategy": "CONSERVATIVE_MEAN_OF_RAREST",
    "percentile_thresholds": {
        "elevated": 0.95,
        "immediate": 0.99,
        "extreme": 0.999,
    },
    "z_score_thresholds": {
        "elevated": 2.0,
        "immediate": 3.0,
        "extreme": 4.5,
    },
    "robust_z_score_thresholds": {
        "elevated": 2.5,
        "immediate": 3.5,
        "extreme": 5.0,
    },
}


def load_statistical_risk_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return json.loads(json.dumps(DEFAULT_CONFIG))
    try:
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return json.loads(json.dumps(DEFAULT_CONFIG))
    merged = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in data.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value
    return merged


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def evaluate_statistical_rarity(
    observations_df: pd.DataFrame,
    baselines_df: pd.DataFrame,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if observations_df.empty or baselines_df.empty:
        rarity_columns = [field for field in StatisticalRarityRow.__dataclass_fields__]
        adjustment_columns = [field for field in ContextualAdjustmentRow.__dataclass_fields__]
        return pd.DataFrame(columns=rarity_columns), pd.DataFrame(columns=adjustment_columns), {
            "markers_evaluated": 0,
            "insufficient_baseline_count": 0,
            "routine_review_count": 0,
            "elevated_review_count": 0,
            "immediate_review_count": 0,
            "extreme_outlier_count": 0,
        }

    observations = observations_df.copy()
    observations["comparison_group"] = observations.apply(
        lambda row: "|".join(
            [
                str(row.get("marker_id", "")),
                str(row.get("entity_type", "")),
                str(row.get("source_scope", "")),
                str(row.get("address_context", "UNKNOWN") or "UNKNOWN"),
                str(row.get("jurisdiction", "") or "ALL"),
                str(row.get("comparison_group_hint", "") or "ALL"),
            ]
        ),
        axis=1,
    )
    grouped_values = {
        comparison_group: pd.to_numeric(group["observed_value"], errors="coerce").fillna(0).tolist()
        for comparison_group, group in observations.groupby("comparison_group")
    }
    minimum_group_size = int(config.get("minimum_comparison_group_size", 5))
    rarity_rows: list[dict[str, object]] = []
    adjustment_rows: list[dict[str, object]] = []

    for _, row in observations.iterrows():
        values = grouped_values.get(str(row["comparison_group"]), [])
        observed_value = float(pd.to_numeric(row.get("observed_value", 0), errors="coerce"))
        expected_value = float(pd.Series(values, dtype=float).mean()) if values else 0.0
        comparison_group_size = int(len(values))
        insufficient_baseline = comparison_group_size < minimum_group_size
        percentile = empirical_percentile(values, observed_value) if values else 0.0
        probability = None if insufficient_baseline else poisson_probability_at_least(observed_value, expected_value)
        robust = None if insufficient_baseline else robust_z_score(values, observed_value)
        iqr_score = None if insufficient_baseline else iqr_outlier_score(values, observed_value)
        rarity_score = 0.0 if insufficient_baseline else rarity_score_from_probability(probability, percentile, robust)
        review_level = determine_review_level(
            probability,
            percentile,
            rarity_score,
            config.get("review_probability_thresholds", {}),
            insufficient_baseline=insufficient_baseline,
        )
        model_used = "empirical_percentile"
        if probability is not None:
            model_used = "poisson_tail + empirical_percentile"
        if robust is not None:
            model_used += " + robust_z"
        if iqr_score is not None:
            model_used += " + iqr"
        assumptions = "Count-based peer group comparison using deterministic grouped baselines."
        if insufficient_baseline:
            assumptions = "Comparison group is smaller than the configured minimum baseline size."
        explanation = (
            f"Observed {observed_value:g} for {row['marker_name']} versus expected {expected_value:.2f} "
            f"within peer group {row['comparison_group']}."
        )
        if probability is not None:
            explanation += f" Estimated tail probability {probability:.4f}."
        if robust is not None:
            explanation += f" Robust z-score {robust:.2f}."

        rarity_rows.append(
            StatisticalRarityRow(
                marker_id=str(row["marker_id"]),
                marker_name=str(row["marker_name"]),
                entity_id=str(row["entity_id"]),
                entity_type=str(row["entity_type"]),
                source_name=str(row.get("source_name", "")),
                source_type=str(row.get("source_type", "")),
                jurisdiction=str(row.get("jurisdiction", "")),
                source_scope=str(row.get("source_scope", "")),
                address_context=str(row.get("address_context", "UNKNOWN")),
                base_building_address=str(row.get("base_building_address", "")),
                unit_level_address=str(row.get("unit_level_address", "")),
                classification_confidence=float(row.get("classification_confidence", 0) or 0),
                observed_value=observed_value,
                expected_value=round(expected_value, 4),
                comparison_group=str(row["comparison_group"]),
                comparison_group_size=comparison_group_size,
                percentile=percentile,
                probability_or_p_value="" if probability is None else round(float(probability), 6),
                rarity_score=rarity_score,
                rarity_level=review_level,
                model_used=model_used,
                assumptions=assumptions,
                explanation=explanation,
                observation_date=str(row.get("observation_date", "")),
            ).to_dict()
        )

        base_weight = int(config.get("marker_weights", {}).get(str(row["marker_id"]), 0))
        if base_weight <= 0:
            base_weight = 12
        if str(row["marker_id"]) == "shared_address_businesses":
            base_weight = 18
        elif str(row["marker_id"]) == "shared_phone":
            base_weight = 14
        elif str(row["marker_id"]) == "shared_email":
            base_weight = 12
        elif str(row["marker_id"]) == "shared_website":
            base_weight = 10
        elif str(row["marker_id"]) in {"arcgis_owner_in_business_records", "county_clerk_party_in_business_records"}:
            base_weight = 20
        elif str(row["marker_id"]) == "cross_source_multi_source_cluster":
            base_weight = 24
        adjustment, category, reason = adjustment_value_for_context(
            str(row["marker_id"]),
            str(row.get("address_context", "UNKNOWN")),
            config,
            source_count=len([token for token in str(row.get("source_name", "")).split("|") if token.strip()]),
            support=int(observed_value),
        )
        if review_level in {"ELEVATED_REVIEW", "IMMEDIATE_REVIEW", "EXTREME_OUTLIER"}:
            adjustment += 2
            reason += " Statistical rarity increased the contextual adjustment."
        adjusted_score = max(1, base_weight + adjustment)
        adjustment_rows.append(
            ContextualAdjustmentRow(
                marker_id=str(row["marker_id"]),
                marker_name=str(row["marker_name"]),
                entity_id=str(row["entity_id"]),
                entity_type=str(row["entity_type"]),
                address_context=str(row.get("address_context", "UNKNOWN")),
                adjustment_category=category,
                original_marker_score=base_weight,
                contextual_adjustment=adjustment,
                adjusted_marker_score=adjusted_score,
                reason_for_adjustment=reason,
                source_scope=str(row.get("source_scope", "")),
            ).to_dict()
        )

    rarity_df = pd.DataFrame(rarity_rows)
    adjustments_df = pd.DataFrame(adjustment_rows)
    summary = {
        "markers_evaluated": int(len(rarity_df)),
        "insufficient_baseline_count": int((rarity_df["rarity_level"] == "INSUFFICIENT_BASELINE").sum()) if not rarity_df.empty else 0,
        "routine_review_count": int((rarity_df["rarity_level"] == "ROUTINE_REVIEW").sum()) if not rarity_df.empty else 0,
        "elevated_review_count": int((rarity_df["rarity_level"] == "ELEVATED_REVIEW").sum()) if not rarity_df.empty else 0,
        "immediate_review_count": int((rarity_df["rarity_level"] == "IMMEDIATE_REVIEW").sum()) if not rarity_df.empty else 0,
        "extreme_outlier_count": int((rarity_df["rarity_level"] == "EXTREME_OUTLIER").sum()) if not rarity_df.empty else 0,
        "largest_positive_adjustments": adjustments_df.sort_values("contextual_adjustment", ascending=False).head(5).to_dict("records") if not adjustments_df.empty else [],
        "largest_negative_adjustments": adjustments_df.sort_values("contextual_adjustment", ascending=True).head(5).to_dict("records") if not adjustments_df.empty else [],
    }
    return rarity_df, adjustments_df, summary


def build_calibration_report(
    rarity_df: pd.DataFrame,
    adjustments_df: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    if rarity_df.empty or adjustments_df.empty:
        return pd.DataFrame(columns=["metric", "value"])
    elevated = int((adjustments_df["contextual_adjustment"] > 0).sum())
    reduced = int((adjustments_df["contextual_adjustment"] < 0).sum())
    unchanged = int((adjustments_df["contextual_adjustment"] == 0).sum())
    rows = [
        {"metric": "marker_counts_before_adjustment", "value": int(len(adjustments_df))},
        {"metric": "marker_counts_after_adjustment", "value": int(len(adjustments_df))},
        {"metric": "number_elevated", "value": elevated},
        {"metric": "number_reduced", "value": reduced},
        {"metric": "number_unchanged", "value": unchanged},
        {"metric": "false_positive_risk_indicators", "value": int((adjustments_df["contextual_adjustment"] < 0).sum())},
        {"metric": "small_sample_exclusions", "value": int((rarity_df["rarity_level"] == "INSUFFICIENT_BASELINE").sum())},
        {"metric": "thresholds_used", "value": json.dumps(config.get("review_probability_thresholds", {}), sort_keys=True)},
        {"metric": "source_coverage", "value": json.dumps(rarity_df["source_scope"].value_counts().to_dict(), sort_keys=True)},
    ]
    return pd.DataFrame(rows)


def run_statistical_risk(
    canonical_entities_path: Path | str = DEFAULT_CANONICAL_ENTITIES_PATH,
    canonical_relationships_path: Path | str = DEFAULT_CANONICAL_RELATIONSHIPS_PATH,
    cross_source_matches_path: Path | str = DEFAULT_CROSS_SOURCE_MATCHES_PATH,
    baselines_path: Path | str = DEFAULT_BASELINES_PATH,
    rarity_path: Path | str = DEFAULT_RARITY_PATH,
    adjustments_path: Path | str = DEFAULT_ADJUSTMENTS_PATH,
    summary_path: Path | str = DEFAULT_SUMMARY_PATH,
    calibration_report_path: Path | str = DEFAULT_CALIBRATION_REPORT_PATH,
    config_path: Path | str = CONFIG_PATH,
) -> dict[str, Any]:
    start_time = time.time()
    config = load_statistical_risk_config(config_path)
    entities_df = _load_frame(Path(canonical_entities_path))
    relationships_df = _load_frame(Path(canonical_relationships_path))
    cross_source_matches_df = _load_frame(Path(cross_source_matches_path))

    print("Statistical Risk: started")
    print(f"Statistical Risk: entities loaded {len(entities_df)}")
    print(f"Statistical Risk: relationships loaded {len(relationships_df)}")
    print(f"Statistical Risk: cross-source matches loaded {len(cross_source_matches_df)}")

    observations_df = build_statistical_observations(entities_df, relationships_df, cross_source_matches_df, config)
    baselines_df = summarize_baselines(observations_df)
    rarity_df, adjustments_df, summary = evaluate_statistical_rarity(observations_df, baselines_df, config)
    calibration_df = build_calibration_report(rarity_df, adjustments_df, config)

    for path in [Path(baselines_path), Path(rarity_path), Path(adjustments_path), Path(calibration_report_path)]:
        path.parent.mkdir(parents=True, exist_ok=True)
    baselines_df.to_csv(baselines_path, index=False)
    rarity_df.to_csv(rarity_path, index=False)
    adjustments_df.to_csv(adjustments_path, index=False)
    calibration_df.to_csv(calibration_report_path, index=False)

    summary_payload = {
        **summary,
        "records_loaded": {
            "entities": int(len(entities_df)),
            "relationships": int(len(relationships_df)),
            "cross_source_matches": int(len(cross_source_matches_df)),
            "observations": int(len(observations_df)),
            "baseline_groups_created": int(len(baselines_df)),
        },
        "baseline_mode": str(config.get("baseline_mode", "REAL_ONLY")),
        "runtime_seconds": round(time.time() - start_time, 2),
    }
    with Path(summary_path).open("w", encoding="utf-8") as handle:
        json.dump(summary_payload, handle, indent=2, sort_keys=True)

    print(f"Statistical Risk: baseline groups created {len(baselines_df)}")
    print(f"Statistical Risk: markers evaluated {summary_payload['markers_evaluated']}")
    print(f"Statistical Risk: insufficient-baseline cases {summary_payload['insufficient_baseline_count']}")
    print(
        "Statistical Risk: review levels "
        f"routine={summary_payload['routine_review_count']} "
        f"elevated={summary_payload['elevated_review_count']} "
        f"immediate={summary_payload['immediate_review_count']} "
        f"extreme={summary_payload['extreme_outlier_count']}"
    )
    print(f"Statistical Risk: completed in {summary_payload['runtime_seconds']:.2f}s")
    print("Statistical Risk: PASS")
    return summary_payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Calculate statistical baselines, rarity, and contextual risk adjustments.")
    parser.add_argument("--canonical-entities-path", default=str(DEFAULT_CANONICAL_ENTITIES_PATH))
    parser.add_argument("--canonical-relationships-path", default=str(DEFAULT_CANONICAL_RELATIONSHIPS_PATH))
    parser.add_argument("--cross-source-matches-path", default=str(DEFAULT_CROSS_SOURCE_MATCHES_PATH))
    parser.add_argument("--baselines-path", default=str(DEFAULT_BASELINES_PATH))
    parser.add_argument("--rarity-path", default=str(DEFAULT_RARITY_PATH))
    parser.add_argument("--adjustments-path", default=str(DEFAULT_ADJUSTMENTS_PATH))
    parser.add_argument("--summary-path", default=str(DEFAULT_SUMMARY_PATH))
    parser.add_argument("--calibration-report-path", default=str(DEFAULT_CALIBRATION_REPORT_PATH))
    parser.add_argument("--config-path", default=str(CONFIG_PATH))
    args = parser.parse_args()
    run_statistical_risk(
        canonical_entities_path=args.canonical_entities_path,
        canonical_relationships_path=args.canonical_relationships_path,
        cross_source_matches_path=args.cross_source_matches_path,
        baselines_path=args.baselines_path,
        rarity_path=args.rarity_path,
        adjustments_path=args.adjustments_path,
        summary_path=args.summary_path,
        calibration_report_path=args.calibration_report_path,
        config_path=args.config_path,
    )


if __name__ == "__main__":
    main()
