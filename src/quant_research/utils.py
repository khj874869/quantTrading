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


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def pct_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous)
