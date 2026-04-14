from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.backtest import Backtester
from quant_research.config import Config
from quant_research.exports import export_rebalance_signals
from quant_research.pipeline import DataPipeline
from quant_research.pipeline import PreparedData
from quant_research.strategy import MultiSignalStrategy


class StrategyTest(unittest.TestCase):
    def test_backtest_runs(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "composite_score": 1.2, "macro_score": 1.0, "vix": 20.0, "sector": "35", "signal_date": date(2025, 1, 20)},
                    {"permno": "10002", "composite_score": 0.8, "macro_score": 1.0, "vix": 20.0, "sector": "28", "signal_date": date(2025, 1, 20)},
                ]
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.01, "10002": 0.0},
                date(2025, 2, 4): {"10001": -0.01, "10002": 0.01},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.001,
                date(2025, 2, 4): -0.002,
            },
        )
        strategy = MultiSignalStrategy({"holding_count": 1, "long_short": False})
        summary = Backtester(prepared, strategy, output_dir=Path("test_output")).run()
        self.assertGreater(summary["days"], 0.0)
        self.assertIn("average_turnover", summary)
        self.assertTrue((Path("test_output") / "portfolio_rebalances.csv").exists())

    def test_sector_neutral_selection_picks_multiple_sectors(self) -> None:
        strategy = MultiSignalStrategy({"holding_count": 2, "sector_neutral": True, "long_short": False})
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "composite_score": 1.5, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10003", "composite_score": 1.8, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                {"permno": "10004", "composite_score": 0.5, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
        )
        self.assertEqual(set(portfolio.weights), {"10001", "10003"})

    def test_strategy_uses_risk_adjusted_score_when_configured(self) -> None:
        strategy = MultiSignalStrategy({"holding_count": 1, "long_short": False, "score_field": "risk_adjusted_score"})
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.0, "risk_adjusted_score": 0.7, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "composite_score": 1.5, "risk_adjusted_score": 1.2, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
        )
        self.assertEqual(set(portfolio.weights), {"10002"})

    def test_beta_neutral_adds_benchmark_hedge_for_long_only(self) -> None:
        strategy = MultiSignalStrategy({"holding_count": 2, "beta_neutral": True, "benchmark_hedge": True, "long_short": False})
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.2},
                {"permno": "10002", "composite_score": 1.8, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 0.8},
            ],
        )
        self.assertIn("__BENCH__", portfolio.weights)
        self.assertAlmostEqual(sum(portfolio.weights.values()), 0.0, places=8)

    def test_beta_neutral_scales_short_leg_for_long_short(self) -> None:
        strategy = MultiSignalStrategy({"holding_count": 2, "long_short": True, "bottom_quantile": 0.5, "beta_neutral": True})
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.5},
                {"permno": "10002", "composite_score": 1.8, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.3},
                {"permno": "10003", "composite_score": -1.0, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 0.5},
                {"permno": "10004", "composite_score": -1.2, "macro_score": 1.0, "vix": 20.0, "sector": "10", "beta": 0.4},
            ],
        )
        total_beta = (
            portfolio.weights["10001"] * 1.5
            + portfolio.weights["10002"] * 1.3
            + portfolio.weights["10003"] * 0.5
            + portfolio.weights["10004"] * 0.4
        )
        self.assertAlmostEqual(total_beta, 0.0, places=6)

    def test_constraint_neutralization_reduces_beta_size_and_sector_exposure(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 3,
                "long_short": True,
                "bottom_quantile": 0.5,
                "constraint_neutral": True,
                "constraint_neutral_factors": ["beta", "size", "sector"],
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.2, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.5, "market_cap": 1000.0},
                {"permno": "10002", "composite_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.2, "market_cap": 900.0},
                {"permno": "10003", "composite_score": 1.7, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0, "market_cap": 700.0},
                {"permno": "10004", "composite_score": -1.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 0.7, "market_cap": 300.0},
                {"permno": "10005", "composite_score": -1.2, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 0.5, "market_cap": 200.0},
                {"permno": "10006", "composite_score": -1.4, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 0.4, "market_cap": 150.0},
            ],
        )
        diagnostics = portfolio.diagnostics or {}
        self.assertAlmostEqual(diagnostics["beta_exposure"], 0.0, places=5)
        self.assertAlmostEqual(diagnostics["size_exposure"], 0.0, places=5)
        self.assertAlmostEqual(diagnostics["sector_35_exposure"], 0.0, places=5)

    def test_constraint_neutralization_handles_downside_beta_and_idio_vol(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 3,
                "long_short": True,
                "bottom_quantile": 0.5,
                "constraint_neutral": True,
                "constraint_neutral_factors": ["beta", "downside_beta", "idio_vol", "size", "sector"],
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "composite_score": 2.2, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.5, "downside_beta": 1.8, "idio_vol": 0.35, "market_cap": 1000.0},
                {"permno": "10002", "composite_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.2, "downside_beta": 1.4, "idio_vol": 0.28, "market_cap": 900.0},
                {"permno": "10003", "composite_score": 1.7, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0, "downside_beta": 1.1, "idio_vol": 0.22, "market_cap": 700.0},
                {"permno": "10004", "composite_score": -1.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 0.7, "downside_beta": 0.9, "idio_vol": 0.12, "market_cap": 300.0},
                {"permno": "10005", "composite_score": -1.2, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 0.5, "downside_beta": 0.6, "idio_vol": 0.10, "market_cap": 200.0},
                {"permno": "10006", "composite_score": -1.4, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 0.4, "downside_beta": 0.5, "idio_vol": 0.08, "market_cap": 150.0},
            ],
        )
        diagnostics = portfolio.diagnostics or {}
        self.assertLess(abs(diagnostics["beta_exposure"]), 2e-3)
        self.assertLess(abs(diagnostics["downside_beta_exposure"]), 2e-3)
        self.assertLess(abs(diagnostics["idio_vol_exposure"]), 5e-3)

    def test_signal_export_runs(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {
                        "signal_date": date(2025, 1, 20),
                        "permno": "10001",
                        "sector": "35",
                        "composite_score": 1.2,
                        "market_cap": 1000000000.0,
                        "book_to_market": 0.1,
                        "roa": 0.02,
                        "asset_growth": 0.03,
                        "cash_flow_ratio": 0.04,
                        "revision": 0.05,
                        "dispersion": -0.02,
                        "surprise": 0.07,
                        "patent_intensity": 0.01,
                        "citation_intensity": 0.02,
                        "net_upgrades": 2.0,
                        "beta": 1.1,
                        "downside_beta": 1.3,
                        "idio_vol": 0.25,
                        "macro_score": 1.0,
                        "vix": 18.0,
                    }
                ]
            },
            returns_by_date={},
            benchmark_by_date={},
        )
        path = export_rebalance_signals(prepared, Path("test_output"))
        self.assertTrue(path.exists())

    def test_ewma_beta_emphasizes_recent_observations(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.min_beta_observations = 1
        rebalance_date = date(2025, 1, 10)
        returns_by_date = {
            date(2025, 1, 2): {"10001": -0.04},
            date(2025, 1, 3): {"10001": -0.02},
            date(2025, 1, 6): {"10001": 0.005},
            date(2025, 1, 7): {"10001": 0.01},
        }
        benchmark_by_date = {
            date(2025, 1, 2): -0.02,
            date(2025, 1, 3): -0.01,
            date(2025, 1, 6): 0.01,
            date(2025, 1, 7): 0.02,
        }

        pipeline.beta_method = "ols"
        ols_beta = pipeline._estimate_beta("10001", rebalance_date, returns_by_date, benchmark_by_date)
        pipeline.beta_method = "ewma"
        pipeline.beta_ewma_halflife_days = 1
        ewma_beta = pipeline._estimate_beta("10001", rebalance_date, returns_by_date, benchmark_by_date)

        self.assertLess(ewma_beta, ols_beta)

    def test_beta_shrinkage_pulls_toward_target(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.min_beta_observations = 1
        pipeline.beta_method = "ols"
        rebalance_date = date(2025, 1, 10)
        returns_by_date = {
            date(2025, 1, 2): {"10001": -0.03},
            date(2025, 1, 3): {"10001": -0.015},
            date(2025, 1, 6): {"10001": 0.03},
            date(2025, 1, 7): {"10001": 0.045},
        }
        benchmark_by_date = {
            date(2025, 1, 2): -0.02,
            date(2025, 1, 3): -0.01,
            date(2025, 1, 6): 0.02,
            date(2025, 1, 7): 0.03,
        }

        pipeline.beta_shrinkage = 0.0
        raw_beta = pipeline._estimate_beta("10001", rebalance_date, returns_by_date, benchmark_by_date)

        pipeline.beta_shrinkage = 0.5
        pipeline.beta_shrinkage_target = 1.0
        shrunk_beta = pipeline._estimate_beta("10001", rebalance_date, returns_by_date, benchmark_by_date)

        self.assertAlmostEqual(shrunk_beta, 0.5 * raw_beta + 0.5, places=8)

    def test_risk_metrics_include_downside_beta_and_idio_vol(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.min_beta_observations = 4
        pipeline.beta_method = "ols"
        rebalance_date = date(2025, 1, 10)
        returns_by_date = {
            date(2025, 1, 2): {"10001": -0.05},
            date(2025, 1, 3): {"10001": -0.02},
            date(2025, 1, 6): {"10001": -0.03},
            date(2025, 1, 7): {"10001": 0.015},
            date(2025, 1, 8): {"10001": 0.02},
            date(2025, 1, 9): {"10001": 0.01},
        }
        benchmark_by_date = {
            date(2025, 1, 2): -0.02,
            date(2025, 1, 3): -0.01,
            date(2025, 1, 6): -0.015,
            date(2025, 1, 7): 0.02,
            date(2025, 1, 8): 0.02,
            date(2025, 1, 9): 0.01,
        }
        risk_metrics = pipeline._estimate_risk_metrics("10001", rebalance_date, returns_by_date, benchmark_by_date)

        self.assertGreater(risk_metrics["downside_beta"], risk_metrics["beta"])
        self.assertGreater(risk_metrics["idio_vol"], 0.0)

    def test_risk_penalty_reduces_risk_adjusted_score_for_riskier_names(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.risk_penalty_downside_beta_weight = 0.5
        pipeline.risk_penalty_idio_vol_weight = 0.5
        rows = [
            {"permno": "10001", "composite_score": 1.0, "beta": 1.0, "downside_beta": 1.8, "idio_vol": 0.40},
            {"permno": "10002", "composite_score": 1.0, "beta": 1.0, "downside_beta": 0.8, "idio_vol": 0.10},
        ]

        pipeline._attach_risk_adjusted_scores({date(2025, 1, 31): rows})

        self.assertLess(rows[0]["risk_adjusted_score"], rows[1]["risk_adjusted_score"])
        self.assertGreater(rows[0]["risk_penalty"], rows[1]["risk_penalty"])

    def test_regime_penalty_multiplier_increases_in_stress_regime(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.regime_risk_scaling_enabled = True
        pipeline.regime_vix_threshold = 30.0
        pipeline.regime_vix_penalty_multiplier = 1.5
        pipeline.regime_macro_threshold = 0.5
        pipeline.regime_macro_penalty_multiplier = 1.25
        pipeline.regime_penalty_cap = 3.0

        calm_multiplier = pipeline._regime_penalty_multiplier({"vix": 20.0, "macro_score": 1.0})
        stress_multiplier = pipeline._regime_penalty_multiplier({"vix": 35.0, "macro_score": 0.0})

        self.assertEqual(calm_multiplier, 1.0)
        self.assertAlmostEqual(stress_multiplier, 1.875, places=8)

    def test_regime_penalty_amplifies_base_penalty(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.risk_penalty_downside_beta_weight = 0.5
        pipeline.risk_penalty_idio_vol_weight = 0.5
        pipeline.regime_risk_scaling_enabled = True
        pipeline.regime_vix_threshold = 30.0
        pipeline.regime_vix_penalty_multiplier = 2.0
        pipeline.regime_macro_threshold = 0.5
        pipeline.regime_macro_penalty_multiplier = 1.5
        rows = [
            {"permno": "10001", "composite_score": 1.0, "beta": 1.0, "downside_beta": 1.8, "idio_vol": 0.40, "vix": 35.0, "macro_score": 0.0},
            {"permno": "10002", "composite_score": 1.0, "beta": 1.0, "downside_beta": 0.8, "idio_vol": 0.10, "vix": 20.0, "macro_score": 1.0},
        ]

        pipeline._attach_risk_adjusted_scores({date(2025, 1, 31): rows})

        self.assertGreater(rows[0]["risk_regime_multiplier"], rows[1]["risk_regime_multiplier"])
        self.assertGreater(rows[0]["risk_penalty"], rows[0]["risk_penalty_base"])

    def test_pipeline_prefers_rdq_for_rebalance_timing(self) -> None:
        config = Config.load(Path("config/sample_config.json"))
        prepared = DataPipeline(config).load()
        self.assertIn(date(2025, 2, 28), prepared.features_by_rebalance)

if __name__ == "__main__":
    unittest.main()
