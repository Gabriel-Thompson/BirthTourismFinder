from __future__ import annotations

from collections import Counter
from math import exp, factorial, log10
from typing import Iterable

import pandas as pd


def empirical_percentile(values: Iterable[float], observed_value: float) -> float:
    series = pd.Series(list(values), dtype=float).dropna()
    if series.empty:
        return 0.0
    return round(float((series <= float(observed_value)).sum()) / float(len(series)), 4)


def poisson_probability_at_least(observed_value: float, expected_value: float) -> float | None:
    observed = int(max(round(float(observed_value)), 0))
    expected = float(expected_value)
    if expected <= 0:
        return 1.0 if observed <= 0 else None
    if observed <= 0:
        return 1.0
    cumulative = 0.0
    for value in range(observed):
        cumulative += exp(-expected) * (expected ** value) / factorial(value)
    return max(0.0, min(1.0, 1.0 - cumulative))


def robust_z_score(values: Iterable[float], observed_value: float) -> float | None:
    series = pd.Series(list(values), dtype=float).dropna()
    if series.empty:
        return None
    median = float(series.median())
    mad = float((series - median).abs().median())
    if mad == 0:
        return None
    return round(0.6745 * (float(observed_value) - median) / mad, 4)


def iqr_outlier_score(values: Iterable[float], observed_value: float) -> float | None:
    series = pd.Series(list(values), dtype=float).dropna()
    if len(series) < 4:
        return None
    q1 = float(series.quantile(0.25))
    q3 = float(series.quantile(0.75))
    iqr = q3 - q1
    if iqr <= 0:
        return None
    return round((float(observed_value) - q3) / iqr, 4)


def rolling_window_peak(date_values: Iterable[object], window_days: int) -> int:
    timestamps = sorted(pd.to_datetime(list(date_values), errors="coerce").dropna().tolist())
    if not timestamps:
        return 0
    best = 0
    left = 0
    for right, stamp in enumerate(timestamps):
        while left <= right and (stamp - timestamps[left]).days > window_days:
            left += 1
        best = max(best, right - left + 1)
    return best


def conservative_compound_score(probabilities: Iterable[float | None]) -> float | None:
    cleaned = [float(value) for value in probabilities if value is not None]
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    # Conservative: use the mean of the two rarest probabilities rather than multiplying
    # potentially dependent events.
    rarest = sorted(cleaned)[:2]
    return round(sum(rarest) / len(rarest), 6)


def rarity_score_from_probability(probability: float | None, percentile: float, robust_z: float | None) -> float:
    probability_score = 0.0
    if probability is not None and probability > 0:
        probability_score = min(100.0, max(0.0, -log10(max(probability, 1e-9)) * 25.0))
    percentile_score = max(0.0, (float(percentile) - 0.5) * 200.0)
    z_score = 0.0 if robust_z is None else max(0.0, min(100.0, (float(robust_z) / 6.0) * 100.0))
    return round(max(probability_score, percentile_score, z_score), 2)


def most_common(values: Iterable[str]) -> str:
    tokens = [str(value).strip() for value in values if str(value).strip()]
    if not tokens:
        return ""
    return Counter(tokens).most_common(1)[0][0]
