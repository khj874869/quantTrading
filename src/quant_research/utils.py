from __future__ import annotations

import csv
import math
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


DATE_FORMATS = ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y")


def parse_date(value: str) -> date:
    text = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def parse_optional_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return parse_date(text)


def float_or_none(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text in {".", "NA", "NaN", "None"}:
        return None
    return float(text)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader)


def write_csv_dicts(path: Path, rows: Iterable[dict[str, object]]) -> None:
    rows = list(rows)
    ensure_directory(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def month_end(value: date) -> date:
    if value.month == 12:
        return date(value.year, 12, 31)
    next_month = date(value.year, value.month + 1, 1)
    return next_month.fromordinal(next_month.toordinal() - 1)


def zscore(values: list[float]) -> list[float]:
    if not values:
        return []
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    std_value = math.sqrt(variance)
    if std_value == 0:
        return [0.0 for _ in values]
    return [(value - mean_value) / std_value for value in values]


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if q <= 0:
        return ordered[0]
    if q >= 1:
        return ordered[-1]
    position = (len(ordered) - 1) * q
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        return ordered[lower_index]
    weight = position - lower_index
    return ordered[lower_index] * (1.0 - weight) + ordered[upper_index] * weight


def winsorize(values: list[float], lower_q: float = 0.0, upper_q: float = 1.0) -> list[float]:
    if not values:
        return []
    lower_bound = quantile(values, lower_q)
    upper_bound = quantile(values, upper_q)
    if upper_bound < lower_bound:
        lower_bound, upper_bound = upper_bound, lower_bound
    return [min(max(value, lower_bound), upper_bound) for value in values]


def winsorize_by_rank(values: list[float], tail_count: int) -> list[float]:
    if not values:
        return []
    ordered = sorted(values)
    bounded_tail_count = min(max(tail_count, 0), max(len(ordered) // 2 - 1, 0))
    if bounded_tail_count <= 0:
        return list(values)
    lower_bound = ordered[bounded_tail_count]
    upper_bound = ordered[-bounded_tail_count - 1]
    return [min(max(value, lower_bound), upper_bound) for value in values]


def robust_zscore(values: list[float]) -> list[float]:
    if not values:
        return []
    median_value = median(values)
    deviations = [abs(value - median_value) for value in values]
    mad = median(deviations)
    scale = 1.4826 * mad
    if scale == 0:
        return zscore(values)
    return [(value - median_value) / scale for value in values]


def normalize_cross_section(values: list[float], method: str = "standard", winsor_quantile: float = 0.0) -> list[float]:
    if not values:
        return []
    quantile_width = min(max(winsor_quantile, 0.0), 0.49)
    if method == "robust" and quantile_width > 0.0:
        tail_count = max(int(math.ceil(len(values) * quantile_width)), 1)
        clipped = winsorize_by_rank(values, tail_count)
        return zscore(clipped)
    clipped = winsorize(values, quantile_width, 1.0 - quantile_width)
    if method == "robust":
        return robust_zscore(clipped)
    return zscore(clipped)


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous)
