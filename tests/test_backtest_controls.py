from __future__ import annotations

import csv
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.backtest import Backtester
from quant_research.pipeline import PreparedData
from quant_research.strategy import MultiSignalStrategy


class BacktestControlsTest(unittest.TestCase):
    def test_transaction_cost_components_are_reported_separately(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_daily_returns.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            summary_payload = json.loads((Path(tmp) / "summary.json").read_text(encoding="utf-8"))

        second_day = [row for row in rows if row["date"] == "2025-03-03"][0]
        self.assertAlmostEqual(float(second_day["commission_cost"]), 0.0002, places=8)
        self.assertAlmostEqual(float(second_day["slippage_cost"]), 0.0008, places=8)
        self.assertAlmostEqual(float(second_day["transaction_cost"]), 0.0010, places=8)
        self.assertAlmostEqual(summary["total_commission_cost"], 0.0003, places=8)
        self.assertAlmostEqual(summary["total_slippage_cost"], 0.0012, places=8)
        self.assertAlmostEqual(summary["total_transaction_cost"], 0.0015, places=8)
        self.assertAlmostEqual(summary_payload["total_transaction_cost"], 0.0015, places=8)

    def test_liquidity_aware_slippage_increases_cost_for_large_participation(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 200000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.0, "avg_dollar_volume": 200000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "avg_dollar_volume": 200000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy({"holding_count": 1, "long_short": False, "slippage_model": "liquidity_aware", "slippage_notional": 1000000.0, "slippage_adv_floor": 100000.0, "slippage_impact_bps_per_adv": 50.0})

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        self.assertGreater(float(second_rebalance_rows[0]["average_slippage_bps"]), 8.0)
        self.assertGreater(float(second_rebalance_rows[0]["max_participation_ratio"]), 0.0)
        self.assertGreater(summary["total_slippage_cost"], 0.0012)

    def test_participation_cap_partially_scales_rebalance_for_illiquid_trade(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "slippage_notional": 1_000_000.0,
                "max_trade_participation_ratio": 1.0,
                "execution_backlog_carry_forward_enabled": True,
                "execution_backlog_decay": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        weights = {row["permno"]: float(row["weight"]) for row in second_rebalance_rows}
        self.assertAlmostEqual(weights["10001"], 0.8, places=8)
        self.assertAlmostEqual(weights["10002"], 0.2, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["target_max_participation_ratio"]), 5.0, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["max_participation_ratio"]), 1.0, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["liquidity_scale"]), 0.2, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.2, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["turnover_scale"]), 0.2, places=8)
        self.assertAlmostEqual(summary["average_turnover"], 0.35, places=8)

    def test_execution_diagnostics_aggregate_partial_and_full_fills(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "slippage_notional": 1_000_000.0,
                "max_trade_participation_ratio": 1.0,
                "execution_backlog_carry_forward_enabled": True,
                "execution_backlog_decay": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "execution_diagnostics.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_diagnostics_by_bucket.csv").open("r", encoding="utf-8", newline="") as handle:
                bucket_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_diagnostics_by_bucket_timeseries.csv").open("r", encoding="utf-8", newline="") as handle:
                bucket_timeseries_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_aging.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_aging_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_aging_events.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_aging_event_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_dropoff_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_events.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_dropoff_event_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_timeseries.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_dropoff_timeseries_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_by_regime.csv").open("r", encoding="utf-8", newline="") as handle:
                backlog_dropoff_regime_rows = list(csv.DictReader(handle))

        diagnostics = {row["permno"]: row for row in rows}
        bucket_diagnostics = {(row["trade_side"], row["bucket"]): row for row in bucket_rows}
        bucket_timeseries = {
            (row["rebalance_date"], row["trade_side"], row["bucket"]): row
            for row in bucket_timeseries_rows
        }
        backlog_aging = {row["backlog_side"]: row for row in backlog_aging_rows}
        backlog_aging_events = {(row["permno"], row["backlog_side"]): row for row in backlog_aging_event_rows}
        backlog_dropoff = {row["backlog_side"]: row for row in backlog_dropoff_rows}
        backlog_dropoff_events = {(row["permno"], row["backlog_side"]): row for row in backlog_dropoff_event_rows}
        backlog_dropoff_timeseries = {
            (row["terminal_rebalance_date"], row["backlog_side"], row["terminal_status"]): row
            for row in backlog_dropoff_timeseries_rows
        }
        backlog_dropoff_regimes = {
            (
                row["backlog_side"],
                row["terminal_status"],
                row["score_dispersion_bucket"],
                row["turnover_bucket"],
                row["liquidity_bucket"],
            ): row
            for row in backlog_dropoff_regime_rows
        }
        self.assertIn("10001", diagnostics)
        self.assertEqual(int(diagnostics["10001"]["event_count"]), 2)
        self.assertEqual(int(diagnostics["10001"]["trade_event_count"]), 2)
        self.assertEqual(int(diagnostics["10001"]["partial_fill_count"]), 1)
        self.assertAlmostEqual(float(diagnostics["10001"]["full_fill_rate"]), 0.5, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_fill_ratio"]), 0.6, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_target_trade"]), 0.9, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_implemented_trade"]), 0.5, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_residual_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_backlog_age"]), 0.5, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["max_backlog_age"]), 1.0, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_target_participation_ratio"]), 2.54, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_implemented_participation_ratio"]), 0.54, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["max_implemented_participation_ratio"]), 1.0, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_target_weight"]), 1.0, places=8)
        self.assertAlmostEqual(float(diagnostics["10001"]["average_implemented_weight"]), 0.6, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["total_target_trade"]), 1.0, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["total_implemented_trade"]), 0.2, places=8)
        self.assertEqual(int(bucket_diagnostics[("buy", "new")]["partial_fill_count"]), 1)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["average_fill_ratio"]), 0.2, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["total_commission_cost"]), 0.00002, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["total_slippage_cost"]), 0.00008, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["total_transaction_cost"]), 0.00010, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "new")]["average_transaction_cost_bps"]), 10.0, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "existing")]["total_target_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "existing")]["total_implemented_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "existing")]["total_transaction_cost"]), 0.00020, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "existing")]["average_transaction_cost_bps"]), 10.0, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["total_target_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["total_implemented_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["average_backlog_age"]), 1.0, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["full_fill_rate"]), 1.0, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["total_transaction_cost"]), 0.00020, places=8)
        self.assertAlmostEqual(float(bucket_diagnostics[("buy", "backlog")]["average_transaction_cost_bps"]), 10.0, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-01-31", "buy", "new")]["total_target_trade"]), 1.0, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-01-31", "buy", "new")]["total_implemented_trade"]), 0.2, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-01-31", "buy", "new")]["total_transaction_cost"]), 0.00010, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-02-28", "buy", "backlog")]["total_target_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-02-28", "buy", "backlog")]["average_backlog_age"]), 1.0, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-02-28", "buy", "existing")]["total_target_trade"]), 0.4, places=8)
        self.assertAlmostEqual(float(bucket_timeseries[("2025-02-28", "buy", "existing")]["total_transaction_cost"]), 0.00020, places=8)
        self.assertEqual(int(backlog_aging["buy"]["episode_count"]), 1)
        self.assertEqual(int(backlog_aging["buy"]["resolved_episode_count"]), 1)
        self.assertEqual(int(backlog_aging["buy"]["open_episode_count"]), 0)
        self.assertAlmostEqual(float(backlog_aging["buy"]["resolution_rate"]), 1.0, places=8)
        self.assertAlmostEqual(float(backlog_aging["buy"]["average_resolution_rebalances"]), 1.0, places=8)
        self.assertAlmostEqual(float(backlog_aging["buy"]["resolved_within_1_rebalance_rate"]), 1.0, places=8)
        self.assertAlmostEqual(float(backlog_aging["buy"]["average_initial_backlog_trade"]), 0.8, places=8)
        self.assertEqual(backlog_aging_events[("10001", "buy")]["status"], "resolved")
        self.assertEqual(backlog_aging_events[("10001", "buy")]["start_rebalance_date"], "2025-01-31")
        self.assertEqual(backlog_aging_events[("10001", "buy")]["resolved_rebalance_date"], "2025-02-28")
        self.assertAlmostEqual(float(backlog_aging_events[("10001", "buy")]["initial_backlog_trade"]), 0.8, places=8)
        self.assertAlmostEqual(float(backlog_aging_events[("10001", "buy")]["resolution_rebalances"]), 1.0, places=8)
        self.assertAlmostEqual(float(backlog_aging_events[("10001", "buy")]["max_backlog_age"]), 1.0, places=8)
        self.assertEqual(int(backlog_dropoff["buy"]["episode_count"]), 1)
        self.assertEqual(int(backlog_dropoff["buy"]["executed_episode_count"]), 1)
        self.assertEqual(int(backlog_dropoff["buy"]["dropped_episode_count"]), 0)
        self.assertAlmostEqual(float(backlog_dropoff["buy"]["total_executed_backlog_trade"]), 0.8, places=8)
        self.assertAlmostEqual(float(backlog_dropoff["buy"]["total_dropped_backlog_trade"]), 0.0, places=8)
        self.assertEqual(backlog_dropoff_events[("10001", "buy")]["terminal_status"], "executed")
        self.assertAlmostEqual(float(backlog_dropoff_events[("10001", "buy")]["executed_backlog_trade"]), 0.8, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_events[("10001", "buy")]["dropped_backlog_trade"]), 0.0, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_events[("10001", "buy")]["executed_ratio"]), 1.0, places=8)
        self.assertEqual(int(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["episode_count"]), 1)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["total_executed_backlog_trade"]), 0.8, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["total_dropped_backlog_trade"]), 0.0, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["terminal_score_dispersion"]), 0.5, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["terminal_effective_turnover_budget"]), 0.0, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["terminal_implemented_turnover"]), 0.4, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["terminal_target_max_participation_ratio"]), 0.08, places=8)
        self.assertAlmostEqual(float(backlog_dropoff_timeseries[("2025-02-28", "buy", "executed")]["terminal_liquidity_scale"]), 1.0, places=8)
        executed_regime = backlog_dropoff_regimes[("buy", "executed", "medium", "turnover_uncapped", "liquidity_full")]
        self.assertEqual(int(executed_regime["episode_count"]), 1)
        self.assertAlmostEqual(float(executed_regime["total_executed_backlog_trade"]), 0.8, places=8)
        self.assertAlmostEqual(float(executed_regime["average_terminal_target_max_participation_ratio"]), 0.08, places=8)

    def test_execution_cash_weight_and_cash_drag_are_reported(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.10, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
            },
            risk_free_by_date={
                date(2025, 2, 3): 0.02,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "slippage_notional": 1_000_000.0,
                "max_trade_participation_ratio": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_daily_returns.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            summary_payload = json.loads((Path(tmp) / "summary.json").read_text(encoding="utf-8"))

        first_day = rows[0]
        self.assertAlmostEqual(float(first_day["cash_weight"]), 0.8, places=8)
        self.assertAlmostEqual(float(first_day["cash_carry"]), 0.016, places=8)
        self.assertAlmostEqual(float(first_day["cash_drag"]), 0.064, places=8)
        self.assertAlmostEqual(float(first_day["gross_return"]), 0.036, places=8)
        self.assertAlmostEqual(float(first_day["net_return"]), 0.0359, places=8)
        self.assertAlmostEqual(summary["average_cash_weight"], 0.8, places=8)
        self.assertAlmostEqual(summary["total_cash_carry"], 0.016, places=8)
        self.assertAlmostEqual(summary["total_cash_drag"], 0.064, places=8)
        self.assertAlmostEqual(summary_payload["average_cash_weight"], 0.8, places=8)
        self.assertAlmostEqual(summary_payload["total_cash_carry"], 0.016, places=8)
        self.assertAlmostEqual(summary_payload["total_cash_drag"], 0.064, places=8)

    def test_execution_backlog_dropoff_reports_signal_cancellation(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "avg_dollar_volume": 200_000.0, "capacity_metric": 10.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "capacity_metric": 10.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "capacity_metric": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "capacity_metric": 10.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "weighting_scheme": "score",
                "score_weight_floor": 1.0,
                "slippage_notional": 1_000_000.0,
                "max_trade_participation_ratio": 1.0,
                "execution_backlog_carry_forward_enabled": False,
                "execution_backlog_decay": 1.0,
                "liquidity_position_cap_ratio": 1.0,
                "liquidity_position_cap_field": "capacity_metric",
                "liquidity_position_cap_floor": 1.0,
                "liquidity_position_cap_notional": 5.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "execution_backlog_dropoff.csv").open("r", encoding="utf-8", newline="") as handle:
                dropoff_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_events.csv").open("r", encoding="utf-8", newline="") as handle:
                dropoff_event_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_timeseries.csv").open("r", encoding="utf-8", newline="") as handle:
                dropoff_timeseries_rows = list(csv.DictReader(handle))
            with (Path(tmp) / "execution_backlog_dropoff_by_regime.csv").open("r", encoding="utf-8", newline="") as handle:
                dropoff_regime_rows = list(csv.DictReader(handle))

        dropoff = {row["backlog_side"]: row for row in dropoff_rows}
        dropoff_events = {(row["permno"], row["backlog_side"]): row for row in dropoff_event_rows}
        dropoff_timeseries = {
            (row["terminal_rebalance_date"], row["backlog_side"], row["terminal_status"]): row
            for row in dropoff_timeseries_rows
        }
        dropoff_regimes = {
            (
                row["backlog_side"],
                row["terminal_status"],
                row["score_dispersion_bucket"],
                row["turnover_bucket"],
                row["liquidity_bucket"],
            ): row
            for row in dropoff_regime_rows
        }
        self.assertEqual(int(dropoff["buy"]["episode_count"]), 2)
        self.assertEqual(int(dropoff["buy"]["executed_episode_count"]), 1)
        self.assertEqual(int(dropoff["buy"]["dropped_episode_count"]), 1)
        self.assertAlmostEqual(float(dropoff["buy"]["total_dropped_backlog_trade"]), 0.55, places=8)
        self.assertEqual(dropoff_events[("10001", "buy")]["terminal_status"], "dropped")
        self.assertAlmostEqual(float(dropoff_events[("10001", "buy")]["executed_backlog_trade"]), 0.0, places=8)
        self.assertAlmostEqual(float(dropoff_events[("10001", "buy")]["dropped_backlog_trade"]), 0.55, places=8)
        self.assertAlmostEqual(float(dropoff_events[("10001", "buy")]["dropped_ratio"]), 1.0, places=8)
        self.assertEqual(int(dropoff_timeseries[("2025-02-28", "buy", "dropped")]["episode_count"]), 1)
        self.assertAlmostEqual(float(dropoff_timeseries[("2025-02-28", "buy", "dropped")]["total_dropped_backlog_trade"]), 0.55, places=8)
        self.assertAlmostEqual(float(dropoff_timeseries[("2025-02-28", "buy", "dropped")]["average_dropped_ratio"]), 1.0, places=8)
        self.assertAlmostEqual(float(dropoff_timeseries[("2025-02-28", "buy", "dropped")]["terminal_score_dispersion"]), 0.0, places=8)
        self.assertAlmostEqual(float(dropoff_timeseries[("2025-02-28", "buy", "dropped")]["terminal_liquidity_scale"]), 1.0, places=8)
        dropped_regime = dropoff_regimes[("buy", "dropped", "flat", "turnover_uncapped", "liquidity_full")]
        self.assertEqual(int(dropped_regime["episode_count"]), 1)
        self.assertAlmostEqual(float(dropped_regime["total_dropped_backlog_trade"]), 0.55, places=8)

    def test_execution_backlog_carry_forward_prioritizes_unfilled_names_next_rebalance(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 4.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "weighting_scheme": "score",
                "score_weight_floor": 1.0,
                "slippage_notional": 1_000_000.0,
                "max_trade_participation_ratio": 1.0,
                "execution_backlog_carry_forward_enabled": True,
                "execution_backlog_decay": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        first_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-01-31"]
        first_weights = {row["permno"]: float(row["weight"]) for row in first_rebalance_rows}
        self.assertAlmostEqual(first_weights["10001"], 0.2, places=8)
        self.assertAlmostEqual(first_weights["10002"], 0.05, places=8)

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        second_weights = {row["permno"]: float(row["weight"]) for row in second_rebalance_rows}
        self.assertAlmostEqual(second_weights["10001"], 0.73333333, places=8)
        self.assertAlmostEqual(second_weights["10002"], 0.26666667, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["backlog_turnover"]), 0.375, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["carried_forward_turnover"]), 0.06666667, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["target_turnover"]), 0.375, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.375, places=8)
        self.assertAlmostEqual(summary["average_turnover"], 0.25, places=8)

    def test_execution_buy_priority_prefers_existing_add_over_new_entry(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.5, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                    {"permno": "10003", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0, "10003": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "weighting_scheme": "score",
                "score_weight_floor": 1.0,
                "execution_backlog_carry_forward_enabled": True,
                "execution_backlog_decay": 1.0,
                "execution_priority_backlog_buy": 1.0,
                "execution_priority_existing_buy": 2.0,
                "execution_priority_new_buy": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        weights = {row["permno"]: float(row["weight"]) for row in second_rebalance_rows}
        self.assertAlmostEqual(weights["10001"], 0.83333333, places=8)
        self.assertAlmostEqual(weights["10003"], 0.16666667, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["buy_priority_existing_weight"]), 2.0, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["buy_priority_new_weight"]), 1.0, places=8)

    def test_execution_sell_priority_prefers_existing_exit_over_new_short_entry(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                    {"permno": "10003", "risk_adjusted_score": -3.0, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 0.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": -3.0, "macro_score": 0.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                    {"permno": "10003", "risk_adjusted_score": 0.0, "macro_score": 0.0, "vix": 20.0, "sector": "20", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0, "10003": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0, "10003": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": True,
                "bottom_quantile": 0.34,
                "execution_backlog_carry_forward_enabled": False,
                "execution_priority_backlog_sell": 1.0,
                "execution_priority_existing_sell": 2.0,
                "execution_priority_new_sell": 1.0,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(
                prepared,
                strategy,
                output_dir=Path(tmp),
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        weights = {row["permno"]: float(row["weight"]) for row in second_rebalance_rows}
        self.assertAlmostEqual(weights["10001"], 0.33333333, places=8)
        self.assertAlmostEqual(weights["10002"], -0.33333333, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["sell_priority_existing_weight"]), 2.0, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["sell_priority_new_weight"]), 1.0, places=8)

    def test_execution_backlog_age_decay_reduces_priority_of_stale_backlog(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 200_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 3, 31): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.5, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                    {"permno": "10003", "risk_adjusted_score": 1.0, "avg_dollar_volume": 10_000_000.0, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 4, 1): {"10001": 0.0, "10002": 0.0, "10003": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
                date(2025, 4, 1): 0.0,
            },
        )
        base_config = {
            "holding_count": 2,
            "long_short": False,
            "weighting_scheme": "score",
            "score_weight_floor": 1.0,
            "slippage_notional": 1_000_000.0,
            "max_trade_participation_ratio": 1.0,
            "execution_backlog_carry_forward_enabled": True,
            "execution_backlog_decay": 1.0,
            "execution_priority_backlog_buy": 2.0,
            "execution_priority_existing_buy": 1.0,
            "execution_priority_new_buy": 1.0,
        }

        with tempfile.TemporaryDirectory() as tmp:
            no_decay_dir = Path(tmp) / "no_decay"
            with_decay_dir = Path(tmp) / "with_decay"
            Backtester(
                prepared,
                MultiSignalStrategy({**base_config, "execution_backlog_age_half_life_rebalances": 0.0}),
                output_dir=no_decay_dir,
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            Backtester(
                prepared,
                MultiSignalStrategy({**base_config, "execution_backlog_age_half_life_rebalances": 1.0}),
                output_dir=with_decay_dir,
                transaction_cost_bps=10.0,
                commission_cost_bps=2.0,
                slippage_cost_bps=8.0,
            ).run()
            with (no_decay_dir / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                no_decay_rows = list(csv.DictReader(handle))
            with (with_decay_dir / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                with_decay_rows = list(csv.DictReader(handle))

        no_decay_third_rows = [row for row in no_decay_rows if row["rebalance_date"] == "2025-03-31"]
        with_decay_third_rows = [row for row in with_decay_rows if row["rebalance_date"] == "2025-03-31"]
        no_decay_weights = {row["permno"]: float(row["weight"]) for row in no_decay_third_rows}
        with_decay_weights = {row["permno"]: float(row["weight"]) for row in with_decay_third_rows}
        self.assertGreater(with_decay_weights["10003"], no_decay_weights["10003"])
        self.assertLess(with_decay_weights["10001"], no_decay_weights["10001"])
        self.assertAlmostEqual(float(with_decay_third_rows[0]["average_backlog_age"]), 2.0, places=8)
        self.assertAlmostEqual(float(with_decay_third_rows[0]["average_backlog_age_multiplier"]), 0.25, places=8)

    def test_max_turnover_per_rebalance_partially_transitions_weights(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.5, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "max_turnover_per_rebalance": 0.25,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            self.assertAlmostEqual(summary["average_turnover"], 0.375, places=8)

            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        weights = {row["permno"]: float(row["weight"]) for row in second_rebalance_rows}
        self.assertAlmostEqual(weights["10001"], 0.75, places=8)
        self.assertAlmostEqual(weights["10002"], 0.25, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["target_turnover"]), 1.0, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.25, places=8)
        self.assertAlmostEqual(float(second_rebalance_rows[0]["turnover_scale"]), 0.25, places=8)

    def test_incumbent_score_bonus_keeps_existing_holding_when_edge_is_small(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 1.50, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.05, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "incumbent_score_bonus": 0.1,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            self.assertAlmostEqual(summary["average_turnover"], 0.25, places=8)

            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        self.assertEqual([row["permno"] for row in second_rebalance_rows], ["10001"])
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.0, places=8)

    def test_entry_score_threshold_keeps_existing_holding_when_edge_is_small(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 1.50, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.05, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.1,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            self.assertAlmostEqual(summary["average_turnover"], 0.25, places=8)

            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        self.assertEqual([row["permno"] for row in second_rebalance_rows], ["10001"])
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.0, places=8)

    def test_turnover_penalty_keeps_existing_holding_when_edge_is_small(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 1.50, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 1.10, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.0,
                "entry_turnover_penalty_per_weight": 0.2,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            summary = Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            self.assertAlmostEqual(summary["average_turnover"], 0.25, places=8)

            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        self.assertEqual([row["permno"] for row in second_rebalance_rows], ["10001"])
        self.assertAlmostEqual(float(second_rebalance_rows[0]["implemented_turnover"]), 0.0, places=8)

    def test_adaptive_turnover_budget_expands_when_score_dispersion_is_high(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "max_turnover_per_rebalance": 0.25,
                "adaptive_turnover_budget_enabled": True,
                "adaptive_turnover_budget_floor": 0.15,
                "adaptive_turnover_budget_ceiling": 0.35,
                "adaptive_turnover_budget_score_scale": 0.4,
                "adaptive_turnover_budget_recent_turnover_scale": 1.0,
                "adaptive_turnover_budget_lookback_rebalances": 3,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        self.assertAlmostEqual(float(second_rebalance_rows[0]["effective_turnover_budget"]), 0.35, places=8)
        self.assertGreater(float(second_rebalance_rows[0]["implemented_turnover"]), 0.25)

    def test_adaptive_turnover_budget_contracts_after_high_recent_turnover(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
                date(2025, 3, 31): [
                    {"permno": "10001", "risk_adjusted_score": 3.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                    {"permno": "10002", "risk_adjusted_score": 0.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 3, 3): {"10001": 0.0, "10002": 0.0},
                date(2025, 4, 1): {"10001": 0.0, "10002": 0.0},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 3, 3): 0.0,
                date(2025, 4, 1): 0.0,
            },
        )
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "max_turnover_per_rebalance": 0.25,
                "adaptive_turnover_budget_enabled": True,
                "adaptive_turnover_budget_floor": 0.15,
                "adaptive_turnover_budget_ceiling": 0.35,
                "adaptive_turnover_budget_score_scale": 0.4,
                "adaptive_turnover_budget_recent_turnover_scale": 1.0,
                "adaptive_turnover_budget_lookback_rebalances": 3,
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            Backtester(prepared, strategy, output_dir=Path(tmp)).run()
            with (Path(tmp) / "portfolio_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        second_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-02-28"]
        third_rebalance_rows = [row for row in rows if row["rebalance_date"] == "2025-03-31"]
        self.assertAlmostEqual(float(second_rebalance_rows[0]["effective_turnover_budget"]), 0.35, places=8)
        self.assertAlmostEqual(float(third_rebalance_rows[0]["effective_turnover_budget"]), 0.29629630, places=6)
        self.assertLess(float(third_rebalance_rows[0]["implemented_turnover"]), float(second_rebalance_rows[0]["implemented_turnover"]))


if __name__ == "__main__":
    unittest.main()
