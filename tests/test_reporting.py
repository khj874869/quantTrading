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
from quant_research.config import Config
from quant_research.pipeline import PreparedData
from quant_research.reporting import PerformanceReporter
from quant_research.strategy import MultiSignalStrategy


class ReportingTest(unittest.TestCase):
    def test_report_writes_monthly_sector_and_security_contributions(self) -> None:
        strategy_config = {
            "holding_count": 1,
            "long_short": False,
            "beta_neutral": False,
            "benchmark_hedge": False,
            "slippage_model": "liquidity_aware",
            "slippage_notional": 100000.0,
            "slippage_adv_floor": 100000.0,
            "slippage_impact_bps_per_adv": 100.0,
            "max_trade_participation_ratio": 0.5,
            "capacity_aum_levels": [100000.0, 300000.0],
            "commission_cost_bps": 2.0,
            "slippage_cost_bps": 8.0,
            "transaction_cost_bps": 10.0,
        }
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "avg_dollar_volume": 100000.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "HEALTH", "beta": 1.0, "avg_dollar_volume": 150000.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.10, "10002": -0.02},
                date(2025, 2, 4): {"10001": -0.05, "10002": 0.01},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 2, 4): 0.0,
            },
        )
        strategy = MultiSignalStrategy(strategy_config)

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "output"
            config = Config(
                path=Path(tmp) / "config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                    },
                    "strategy": strategy_config,
                },
            )

            Backtester(
                prepared,
                strategy,
                output_dir=output_dir,
                transaction_cost_bps=float(strategy_config["transaction_cost_bps"]),
                commission_cost_bps=float(strategy_config["commission_cost_bps"]),
                slippage_cost_bps=float(strategy_config["slippage_cost_bps"]),
            ).run()
            outputs, summary = PerformanceReporter(config, prepared, output_dir).run()

            self.assertEqual(len(outputs), 10)
            with (output_dir / "report_monthly_returns.csv").open("r", encoding="utf-8", newline="") as handle:
                monthly_rows = list(csv.DictReader(handle))
            with (output_dir / "report_sector_contributions.csv").open("r", encoding="utf-8", newline="") as handle:
                sector_rows = list(csv.DictReader(handle))
            with (output_dir / "report_security_contributions.csv").open("r", encoding="utf-8", newline="") as handle:
                security_rows = list(csv.DictReader(handle))
            with (output_dir / "report_factor_diagnostics.csv").open("r", encoding="utf-8", newline="") as handle:
                factor_rows = list(csv.DictReader(handle))
            with (output_dir / "report_factor_regime_diagnostics.csv").open("r", encoding="utf-8", newline="") as handle:
                factor_regime_rows = list(csv.DictReader(handle))
            with (output_dir / "report_capacity_curve.csv").open("r", encoding="utf-8", newline="") as handle:
                capacity_rows = list(csv.DictReader(handle))
            with (output_dir / "report_capacity_breaches.csv").open("r", encoding="utf-8", newline="") as handle:
                capacity_breach_rows = list(csv.DictReader(handle))
            with (output_dir / "report_stress_scenarios.csv").open("r", encoding="utf-8", newline="") as handle:
                stress_rows = list(csv.DictReader(handle))
            summary_payload = json.loads((output_dir / "report_summary.json").read_text(encoding="utf-8"))
            html_report = (output_dir / "report_dashboard.html").read_text(encoding="utf-8")

        self.assertEqual(monthly_rows[0]["month"], "2025-02")
        self.assertGreater(float(monthly_rows[0]["gross_total_return"]), 0.0)
        self.assertLess(float(monthly_rows[0]["net_total_return"]), float(monthly_rows[0]["gross_total_return"]))
        self.assertGreater(float(monthly_rows[0]["total_transaction_cost"]), 0.0)
        self.assertIn("total_short_borrow_cost", monthly_rows[0])
        self.assertEqual(sector_rows[0]["sector"], "TECH")
        self.assertGreater(float(sector_rows[0]["total_contribution"]), 0.0)
        self.assertEqual(security_rows[0]["permno"], "10001")
        self.assertGreater(float(security_rows[0]["total_contribution"]), 0.0)
        self.assertEqual(factor_rows[0]["factor"], "risk_adjusted_score")
        self.assertEqual(factor_regime_rows[0]["factor"], "risk_adjusted_score")
        self.assertEqual(len(capacity_rows), 2)
        self.assertEqual(capacity_rows[0]["capacity_status"], "within_limits")
        self.assertEqual(capacity_rows[1]["capacity_status"], "breached")
        self.assertGreater(float(capacity_rows[1]["estimated_slippage_cost"]), float(capacity_rows[0]["estimated_slippage_cost"]))
        self.assertEqual(capacity_breach_rows[0]["rebalance_date"], "2025-01-31")
        self.assertTrue(any(row["scenario_type"] == "vix_regime" and row["scenario_bucket"] == "low_vix" for row in stress_rows))
        self.assertTrue(any(row["scenario_type"] == "macro_regime" and row["scenario_bucket"] == "strong_macro" for row in stress_rows))
        self.assertEqual(summary["best_month"]["month"], "2025-02")
        self.assertEqual(summary_payload["top_securities"][0]["permno"], "10001")
        self.assertEqual(summary_payload["top_sectors"][0]["sector"], "TECH")
        self.assertEqual(summary_payload["top_factors_by_ic"][0]["factor"], "risk_adjusted_score")
        self.assertEqual(summary_payload["largest_aum_without_breach"], 100000.0)
        self.assertEqual(summary_payload["first_breached_aum"], 300000.0)
        self.assertTrue(any(row["scenario_type"] == "vix_regime" for row in summary_payload["stress_scenarios"]))
        self.assertIn("Quant Research Dashboard", html_report)
        self.assertIn("Capacity Curve", html_report)
        self.assertIn("Top Securities", html_report)


if __name__ == "__main__":
    unittest.main()
