from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.execution import ExecutionReconciler
from quant_research.pipeline import PreparedData


class ExecutionReconciliationTest(unittest.TestCase):
    def test_reconciliation_compares_order_blotter_against_fills(self) -> None:
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

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            fills_path = tmp_path / "fills.csv"
            fills_path.write_text(
                "\n".join(
                    [
                        "rebalance_date,permno,side,filled_shares,fill_price,commission,exchange_fee,fill_timestamp",
                        "2025-02-28,10001,SELL,100000,9.80,8.0,2.0,2025-02-28T15:58:00",
                        "2025-02-28,10002,BUY,40000,20.50,5.0,1.0,2025-02-28T15:59:00",
                        "2025-02-28,99999,BUY,1000,10.00,0.0,0.0,2025-02-28T15:59:30",
                    ]
                ),
                encoding="utf-8",
            )
            output_dir = tmp_path / "output"
            config = Config(
                path=tmp_path / "config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "broker_fills": str(fills_path),
                    },
                    "strategy": {
                        "holding_count": 1,
                        "long_short": False,
                        "order_blotter_notional": 1_000_000.0,
                        "order_blotter_order_type": "MOC",
                    },
                },
            )

            outputs, summary = ExecutionReconciler(config, prepared, output_dir).run()
            with (output_dir / "execution_reconciliation.csv").open("r", encoding="utf-8", newline="") as handle:
                reconciliation_rows = list(csv.DictReader(handle))
            with (output_dir / "execution_unmatched_fills.csv").open("r", encoding="utf-8", newline="") as handle:
                unmatched_rows = list(csv.DictReader(handle))
            with (output_dir / "execution_costs_by_rebalance.csv").open("r", encoding="utf-8", newline="") as handle:
                rebalance_cost_rows = list(csv.DictReader(handle))
            with (output_dir / "execution_costs_by_side.csv").open("r", encoding="utf-8", newline="") as handle:
                side_cost_rows = list(csv.DictReader(handle))
            with (output_dir / "execution_costs_top_orders.csv").open("r", encoding="utf-8", newline="") as handle:
                top_cost_rows = list(csv.DictReader(handle))
            summary_payload = json.loads((output_dir / "execution_summary.json").read_text(encoding="utf-8"))

        self.assertEqual(len(outputs), 9)
        self.assertEqual(len(reconciliation_rows), 3)
        rows_by_permno_side = {(row["permno"], row["side"]): row for row in reconciliation_rows}
        self.assertEqual(rows_by_permno_side[("10001", "BUY")]["fill_status"], "unfilled")
        self.assertEqual(rows_by_permno_side[("10001", "SELL")]["fill_status"], "filled")
        self.assertEqual(rows_by_permno_side[("10002", "BUY")]["fill_status"], "partial")
        self.assertAlmostEqual(
            float(rows_by_permno_side[("10002", "BUY")]["implementation_shortfall_bps"]),
            250.0,
            places=8,
        )
        self.assertAlmostEqual(float(rows_by_permno_side[("10002", "BUY")]["implementation_shortfall_dollars"]), 20000.0, places=8)
        self.assertAlmostEqual(float(rows_by_permno_side[("10002", "BUY")]["explicit_fee_cost"]), 6.0, places=8)
        self.assertAlmostEqual(float(rows_by_permno_side[("10002", "BUY")]["total_execution_cost"]), 20006.0, places=8)
        self.assertEqual(len(unmatched_rows), 1)
        self.assertEqual(unmatched_rows[0]["permno"], "99999")
        self.assertEqual(len(rebalance_cost_rows), 2)
        rebalance_rows_by_date = {row["rebalance_date"]: row for row in rebalance_cost_rows}
        self.assertAlmostEqual(float(rebalance_rows_by_date["2025-02-28"]["total_execution_cost"]), 40016.0, places=8)
        side_rows_by_side = {row["side"]: row for row in side_cost_rows}
        self.assertAlmostEqual(float(side_rows_by_side["BUY"]["total_execution_cost"]), 20006.0, places=8)
        self.assertAlmostEqual(float(side_rows_by_side["SELL"]["total_execution_cost"]), 20010.0, places=8)
        self.assertEqual(top_cost_rows[0]["permno"], "10001")
        self.assertEqual(summary["unmatched_fill_count"], 1)
        self.assertEqual(summary_payload["matched_order_count"], 2)
        self.assertAlmostEqual(summary_payload["total_implementation_shortfall_dollars"], 40000.0, places=8)
        self.assertAlmostEqual(summary_payload["total_explicit_fee_cost"], 16.0, places=8)
        self.assertAlmostEqual(summary_payload["total_execution_cost"], 40016.0, places=8)
        self.assertEqual(summary_payload["worst_order_by_execution_cost"]["permno"], "10001")
        self.assertEqual(summary_payload["latest_rebalance_date"], "2025-02-28")


if __name__ == "__main__":
    unittest.main()
