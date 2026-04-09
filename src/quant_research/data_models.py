from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class PortfolioDay:
    date: date
    gross_return: float
    net_return: float
    benchmark_return: float
    active_return: float
    exposure: float
    holdings: int
    turnover: float
    transaction_cost: float
