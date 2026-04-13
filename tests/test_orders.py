from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.exports import export_order_blotter
from quant_research.pipeline import PreparedData
from quant_research.strategy import MultiSignalStrategy


class OrderBlotterTest(unittest.TestCase):
    def test_order_blotter_exports_latest_rebalance_deltas_with_notional_and_shares(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {
                        "signal_date": date(2025, 1, 20),
                        "permno": "10001",
                        "sector": "TECH",
                        "risk_adjusted_score": 2.0,
                        "composite_score": 2.0,
                        "price": 10.0,
                        "avg_dollar_volume": 500_000.0,
                        "market_cap": 300_000_000.0,
                        "macro_score": 1.0,
                        "vix": 20.0,
                        "beta": 1.0,
                    },
                    {
                        "signal_date": date(2025, 1, 20),
                        "permno": "10002",
                        "sector": "HEALTH",
                        "risk_adjusted_score": 1.0,
                        "composite_score": 1.0,
                        "price": 20.0,
                        "avg_dollar_volume": 400_000.0,
                        "market_cap": 250_000_000.0,
                        "macro_score": 1.0,
                        "vix": 20.0,
                        "beta": 1.0,
                    },
                ],
                date(2025, 2, 28): [
                    {
                        "signal_date": date(2025, 2, 20),
                        "permno": "10001",
                        "sector": "TECH",
                        "risk_adjusted_score": 0.5,
                        "composite_score": 0.5,
                        "price": 10.0,
                        "avg_dollar_volume": 500_000.0,
                        "market_cap": 300_000_000.0,
                        "macro_score": 1.0,
                        "vix": 20.0,
                        "beta": 1.0,
                    },
                    {
                        "signal_date": date(2025, 2, 20),
                        "permno": "10002",
                        "sector": "HEALTH",
                        "risk_adjusted_score": 3.0,
                        "composite_score": 3.0,
                        "price": 20.0,
                        "avg_dollar_volume": 400_000.0,
                        "market_cap": 250_000_000.0,
                        "macro_score": 1.0,
                        "vix": 20.0,
                        "beta": 1.0,
                    },
                ],
            },
            returns_by_date={},
            benchmark_by_date={},
        )
        strategy = MultiSignalStrategy({"holding_count": 1, "long_short": False})

        with tempfile.TemporaryDirectory() as tmp:
            outputs = export_order_blotter(prepared, strategy, Path(tmp), blotter_notional=1_000_000.0, order_type="moc")
            with (Path(tmp) / "order_blotter.csv").open("r", encoding="utf-8", newline="") as handle:
                all_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "order_blotter_latest.csv").open("r", encoding="utf-8", newline="") as handle:
                latest_rows = list(csv.DictReader(handle))
            summary_payload = json.loads((Path(tmp) / "order_blotter_summary.json").read_text(encoding="utf-8"))

        self.assertEqual(len(outputs), 3)
        self.assertEqual(len(all_rows), 3)
        self.assertEqual(len(latest_rows), 2)
        latest_by_permno = {row["permno"]: row for row in latest_rows}
        self.assertEqual(latest_by_permno["10001"]["side"], "SELL")
        self.assertEqual(latest_by_permno["10001"]["position_transition"], "CLOSE")
        self.assertEqual(latest_by_permno["10002"]["side"], "BUY")
        self.assertEqual(latest_by_permno["10002"]["position_transition"], "OPEN")
        self.assertEqual(latest_by_permno["10002"]["order_type"], "MOC")
        self.assertAlmostEqual(float(latest_by_permno["10002"]["order_weight"]), 1.0, places=8)
        self.assertAlmostEqual(float(latest_by_permno["10002"]["estimated_notional"]), 1_000_000.0, places=2)
        self.assertAlmostEqual(float(latest_by_permno["10002"]["estimated_shares"]), 50_000.0, places=4)
        self.assertEqual(summary_payload["latest_rebalance_date"], "2025-02-28")
        self.assertEqual(summary_payload["total_order_count"], 3)
        self.assertEqual(summary_payload["rebalances"][-1]["order_count"], 2)


if __name__ == "__main__":
    unittest.main()
