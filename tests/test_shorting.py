from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.backtest import Backtester
from quant_research.pipeline import PreparedData
from quant_research.strategy import MultiSignalStrategy


class ShortingTest(unittest.TestCase):
    def test_short_selection_respects_shorting_gates(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": True,
                "bottom_quantile": 0.34,
                "short_min_avg_dollar_volume": 200_000.0,
                "short_exclude_sectors": ["HEALTH"],
            }
        )

        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "avg_dollar_volume": 500_000.0, "market_cap": 500_000_000.0, "liquidity_ratio": 0.02},
                {"permno": "10002", "risk_adjusted_score": -2.0, "macro_score": 1.0, "vix": 20.0, "sector": "HEALTH", "beta": 1.0, "avg_dollar_volume": 500_000.0, "market_cap": 500_000_000.0, "liquidity_ratio": 0.02},
                {"permno": "10003", "risk_adjusted_score": -1.5, "macro_score": 1.0, "vix": 20.0, "sector": "IND", "beta": 1.0, "avg_dollar_volume": 500_000.0, "market_cap": 500_000_000.0, "liquidity_ratio": 0.02},
            ],
        )

        self.assertIn("10001", portfolio.weights)
        self.assertIn("10003", portfolio.weights)
        self.assertNotIn("10002", portfolio.weights)
        self.assertLess(portfolio.weights["10003"], 0.0)

    def test_short_selection_requires_locate_and_caps_hard_to_borrow_names(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": True,
                "bottom_quantile": 0.5,
                "short_locate_required": True,
                "short_locate_min_score": 0.5,
                "short_max_borrow_cost_bps_annual": 300.0,
            }
        )

        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "short_locate_available": True, "short_borrow_cost_bps_annual": 100.0},
                {"permno": "10002", "risk_adjusted_score": -2.5, "macro_score": 1.0, "vix": 20.0, "sector": "IND", "beta": 1.0, "short_locate_available": False, "short_borrow_cost_bps_annual": 100.0},
                {"permno": "10003", "risk_adjusted_score": -2.0, "macro_score": 1.0, "vix": 20.0, "sector": "UTIL", "beta": 1.0, "short_locate_score": 0.9, "short_borrow_cost_bps_annual": 500.0},
                {"permno": "10004", "risk_adjusted_score": -1.5, "macro_score": 1.0, "vix": 20.0, "sector": "CONS", "beta": 1.0, "short_locate_score": 0.8, "short_borrow_cost_bps_annual": 200.0},
            ],
        )

        self.assertIn("10001", portfolio.weights)
        self.assertIn("10004", portfolio.weights)
        self.assertNotIn("10002", portfolio.weights)
        self.assertNotIn("10003", portfolio.weights)
        self.assertLess(portfolio.weights["10004"], 0.0)

    def test_short_borrow_cost_is_charged_daily_on_short_exposure(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": -2.0, "macro_score": 1.0, "vix": 20.0, "sector": "IND", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": True,
                "bottom_quantile": 0.5,
                "short_borrow_cost_bps_annual": 252.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=0.0,
                commission_cost_bps=0.0,
                slippage_cost_bps=0.0,
            ).run()
            with (Path(tmp) / "portfolio_daily_returns.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            summary_payload = json.loads((Path(tmp) / "summary.json").read_text(encoding="utf-8"))

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0]["short_borrow_cost"]), 0.0001, places=8)
        self.assertAlmostEqual(float(rows[0]["net_return"]), -0.0001, places=8)
        self.assertAlmostEqual(summary["total_short_borrow_cost"], 0.0001, places=8)
        self.assertAlmostEqual(summary["average_short_borrow_cost"], 0.0001, places=8)
        self.assertAlmostEqual(summary_payload["total_short_borrow_cost"], 0.0001, places=8)

    def test_position_specific_short_borrow_cost_field_overrides_global_fee(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": -2.0, "macro_score": 1.0, "vix": 20.0, "sector": "IND", "beta": 1.0, "short_borrow_cost_bps_annual": 504.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": True,
                "bottom_quantile": 0.5,
                "short_borrow_cost_bps_annual": 252.0,
                "short_borrow_cost_field": "short_borrow_cost_bps_annual",
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=0.0,
                commission_cost_bps=0.0,
                slippage_cost_bps=0.0,
            ).run()
            with (Path(tmp) / "portfolio_daily_returns.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0]["short_borrow_cost"]), 0.0002, places=8)
        self.assertAlmostEqual(summary["total_short_borrow_cost"], 0.0002, places=8)


if __name__ == "__main__":
    unittest.main()
