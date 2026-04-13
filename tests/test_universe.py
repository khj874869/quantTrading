from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.exports import export_universe_snapshot
from quant_research.pipeline import DataPipeline, PreparedData


class UniverseTest(unittest.TestCase):
    def test_universe_filters_respect_sector_liquidity_and_top_n(self) -> None:
        pipeline = DataPipeline(
            Config(
                path=Path("config/sample_config.json").resolve(),
                raw={
                    "strategy": {
                        "min_price": 5.0,
                        "min_market_cap": 100_000_000.0,
                        "min_avg_dollar_volume": 200_000.0,
                        "universe_include_sectors": ["TECH", "HEALTH"],
                        "universe_exclude_sectors": ["HEALTH"],
                        "universe_top_n_by_market_cap": 1,
                    },
                },
            )
        )

        filtered = pipeline._apply_universe_filters(
            [
                {"permno": "10001", "sector": "TECH", "price": 10.0, "market_cap": 300_000_000.0, "avg_dollar_volume": 500_000.0},
                {"permno": "10002", "sector": "TECH", "price": 10.0, "market_cap": 200_000_000.0, "avg_dollar_volume": 500_000.0},
                {"permno": "10003", "sector": "HEALTH", "price": 10.0, "market_cap": 400_000_000.0, "avg_dollar_volume": 500_000.0},
                {"permno": "10004", "sector": "TECH", "price": 10.0, "market_cap": 500_000_000.0, "avg_dollar_volume": 100_000.0},
            ]
        )

        self.assertEqual([row["permno"] for row in filtered], ["10001"])

    def test_equal_weight_universe_benchmark_uses_filtered_universe_returns(self) -> None:
        pipeline = DataPipeline(
            Config(
                path=Path("config/sample_config.json").resolve(),
                raw={
                    "strategy": {
                        "benchmark_mode": "equal_weight_universe",
                    },
                },
            )
        )
        features_by_rebalance = {
            date(2025, 1, 31): [
                {"permno": "10001"},
                {"permno": "10002"},
            ]
        }
        returns_by_date = {
            date(2025, 2, 3): {"10001": 0.02, "10002": 0.00, "10003": 0.50},
            date(2025, 2, 4): {"10001": -0.01, "10002": 0.03},
        }

        benchmark = pipeline._build_equal_weight_universe_benchmark(features_by_rebalance, returns_by_date)

        self.assertAlmostEqual(benchmark[date(2025, 2, 3)], 0.01, places=8)
        self.assertAlmostEqual(benchmark[date(2025, 2, 4)], 0.01, places=8)

    def test_universe_snapshot_includes_benchmark_weight_hint(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {
                        "signal_date": date(2025, 1, 20),
                        "permno": "10001",
                        "sector": "TECH",
                        "market_cap": 300_000_000.0,
                        "avg_dollar_volume": 500_000.0,
                        "liquidity_ratio": 0.02,
                        "composite_score": 1.2,
                        "risk_adjusted_score": 1.1,
                        "beta": 1.0,
                        "downside_beta": 1.1,
                        "idio_vol": 0.2,
                    },
                    {
                        "signal_date": date(2025, 1, 20),
                        "permno": "10002",
                        "sector": "HEALTH",
                        "market_cap": 200_000_000.0,
                        "avg_dollar_volume": 300_000.0,
                        "liquidity_ratio": 0.015,
                        "composite_score": 0.8,
                        "risk_adjusted_score": 0.7,
                        "beta": 0.9,
                        "downside_beta": 1.0,
                        "idio_vol": 0.25,
                    },
                ]
            },
            returns_by_date={},
            benchmark_by_date={},
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_path = export_universe_snapshot(prepared, Path(tmp), benchmark_mode="equal_weight_universe")
            with output_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["market_cap_rank"], "1")
        self.assertEqual(rows[0]["benchmark_weight_hint"], "0.50000000")


if __name__ == "__main__":
    unittest.main()
