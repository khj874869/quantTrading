from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.strategy import MultiSignalStrategy


class StrategyWeightingTest(unittest.TestCase):
    def test_score_inverse_vol_weighting_prefers_stronger_lower_risk_names(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "weighting_scheme": "score_inverse_vol",
                "risk_weight_field": "idio_vol",
                "risk_weight_floor": 0.05,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 2.0, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.0, "idio_vol": 0.30, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
        )

        self.assertGreater(portfolio.weights["10001"], portfolio.weights["10002"])
        self.assertAlmostEqual(sum(portfolio.weights.values()), 1.0, places=8)

    def test_max_position_weight_caps_concentrated_score_weights(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 3,
                "long_short": False,
                "weighting_scheme": "score",
                "score_weight_floor": 0.01,
                "max_position_weight": 0.45,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 4.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                {"permno": "10003", "risk_adjusted_score": 0.5, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
            ],
        )

        self.assertLessEqual(portfolio.weights["10001"], 0.45 + 1e-8)
        self.assertAlmostEqual(sum(portfolio.weights.values()), 1.0, places=8)

    def test_liquidity_position_cap_reduces_target_weight_for_illiquid_name(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "weighting_scheme": "score",
                "score_weight_floor": 0.01,
                "liquidity_position_cap_ratio": 0.5,
                "liquidity_position_cap_field": "avg_dollar_volume",
                "liquidity_position_cap_floor": 100000.0,
                "liquidity_position_cap_notional": 1000000.0,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 4.0, "avg_dollar_volume": 200000.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.0, "avg_dollar_volume": 2000000.0, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
        )

        self.assertAlmostEqual(portfolio.weights["10001"], 0.1, places=8)
        self.assertAlmostEqual(portfolio.weights["10002"], 0.9, places=8)
        self.assertAlmostEqual(sum(portfolio.weights.values()), 1.0, places=8)

    def test_score_weighting_scales_short_leg_by_weaker_scores(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": True,
                "bottom_quantile": 0.5,
                "weighting_scheme": "score",
                "score_weight_floor": 0.01,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.5, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                {"permno": "10003", "risk_adjusted_score": -0.5, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
                {"permno": "10004", "risk_adjusted_score": -2.0, "macro_score": 1.0, "vix": 20.0, "sector": "10", "beta": 1.0},
            ],
        )

        self.assertLess(portfolio.weights["10004"], portfolio.weights["10003"])
        self.assertAlmostEqual(sum(portfolio.weights.values()), 0.0, places=8)

    def test_incumbent_score_bonus_reduces_unnecessary_replacements(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "incumbent_score_bonus": 0.1,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.05, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10001"})

    def test_entry_score_threshold_blocks_small_replacement(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.1,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.05, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10001"})

    def test_entry_score_threshold_allows_clear_replacement(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.1,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.20, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10002"})

    def test_dynamic_entry_threshold_scales_with_cross_section_dispersion(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.5,
            }
        )
        blocked_portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.06, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                {"permno": "10003", "risk_adjusted_score": 0.00, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )
        allowed_portfolio = strategy.build_weights(
            date(2025, 3, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.30, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
                {"permno": "10003", "risk_adjusted_score": 0.00, "macro_score": 1.0, "vix": 20.0, "sector": "20", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(blocked_portfolio.weights), {"10001"})
        self.assertEqual(set(allowed_portfolio.weights), {"10002"})

    def test_turnover_penalty_blocks_small_replacement_without_hard_threshold(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.0,
                "entry_turnover_penalty_per_weight": 0.2,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.10, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10001"})

    def test_turnover_penalty_allows_large_replacement_without_hard_threshold(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.0,
                "entry_turnover_penalty_per_weight": 0.2,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.30, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10002"})

    def test_liquidity_penalty_blocks_illiquid_replacement(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.0,
                "entry_turnover_penalty_per_weight": 0.0,
                "entry_liquidity_penalty_scale": 0.002,
                "entry_liquidity_field": "liquidity_ratio",
                "entry_liquidity_floor": 0.01,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "liquidity_ratio": 0.20, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.10, "liquidity_ratio": 0.01, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10001"})

    def test_liquidity_penalty_allows_liquid_replacement(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 1,
                "long_short": False,
                "entry_score_threshold": 0.0,
                "entry_score_threshold_dynamic_scale": 0.0,
                "entry_turnover_penalty_per_weight": 0.0,
                "entry_liquidity_penalty_scale": 0.002,
                "entry_liquidity_field": "liquidity_ratio",
                "entry_liquidity_floor": 0.01,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 2, 28),
            [
                {"permno": "10001", "risk_adjusted_score": 1.00, "liquidity_ratio": 0.20, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 1.10, "liquidity_ratio": 0.20, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
            previous_weights={"10001": 1.0},
        )

        self.assertEqual(set(portfolio.weights), {"10002"})

    def test_optimizer_construction_overweights_stronger_alpha_name(self) -> None:
        strategy = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "portfolio_construction": "optimizer",
                "optimizer_risk_aversion": 0.1,
                "optimizer_turnover_penalty": 0.0,
                "optimizer_iterations": 300,
                "optimizer_step_size": 0.05,
                "risk_weight_field": "idio_vol",
                "risk_weight_floor": 0.05,
                "max_position_weight": 0.8,
            }
        )
        portfolio = strategy.build_weights(
            date(2025, 1, 31),
            [
                {"permno": "10001", "risk_adjusted_score": 2.0, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
                {"permno": "10002", "risk_adjusted_score": 0.5, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
            ],
        )

        self.assertGreater(portfolio.weights.get("10001", 0.0), portfolio.weights.get("10002", 0.0))
        self.assertAlmostEqual(sum(portfolio.weights.values()), 1.0, places=6)

    def test_optimizer_turnover_penalty_keeps_more_weight_in_incumbent(self) -> None:
        base_rows = [
            {"permno": "10001", "risk_adjusted_score": 1.0, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "35", "beta": 1.0},
            {"permno": "10002", "risk_adjusted_score": 1.2, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "28", "beta": 1.0},
        ]
        low_penalty = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "portfolio_construction": "optimizer",
                "optimizer_risk_aversion": 0.1,
                "optimizer_turnover_penalty": 0.0,
                "optimizer_iterations": 300,
                "optimizer_step_size": 0.05,
                "max_position_weight": 0.8,
            }
        )
        high_penalty = MultiSignalStrategy(
            {
                "holding_count": 2,
                "long_short": False,
                "portfolio_construction": "optimizer",
                "optimizer_risk_aversion": 0.1,
                "optimizer_turnover_penalty": 5.0,
                "optimizer_iterations": 300,
                "optimizer_step_size": 0.05,
                "max_position_weight": 0.8,
            }
        )

        low_penalty_portfolio = low_penalty.build_weights(
            date(2025, 2, 28),
            base_rows,
            previous_weights={"10001": 1.0},
        )
        high_penalty_portfolio = high_penalty.build_weights(
            date(2025, 2, 28),
            base_rows,
            previous_weights={"10001": 1.0},
        )

        self.assertGreater(high_penalty_portfolio.weights.get("10001", 0.0), low_penalty_portfolio.weights.get("10001", 0.0))
        self.assertAlmostEqual(sum(high_penalty_portfolio.weights.values()), 1.0, places=6)

    def test_optimizer_covariance_penalty_prefers_less_correlated_name(self) -> None:
        rows = [
            {"permno": "10001", "risk_adjusted_score": 1.2, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "downside_beta": 1.0, "size": 1.0},
            {"permno": "10002", "risk_adjusted_score": 1.1, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "downside_beta": 1.0, "size": 1.0},
            {"permno": "10003", "risk_adjusted_score": 1.0, "idio_vol": 0.10, "macro_score": 1.0, "vix": 20.0, "sector": "UTIL", "beta": 0.2, "downside_beta": 0.2, "size": -1.0},
        ]
        low_covariance_penalty = MultiSignalStrategy(
            {
                "holding_count": 3,
                "long_short": False,
                "portfolio_construction": "optimizer",
                "optimizer_risk_aversion": 0.1,
                "optimizer_covariance_penalty": 0.0,
                "optimizer_turnover_penalty": 0.0,
                "optimizer_iterations": 400,
                "optimizer_step_size": 0.05,
                "max_position_weight": 0.8,
            }
        )
        high_covariance_penalty = MultiSignalStrategy(
            {
                "holding_count": 3,
                "long_short": False,
                "portfolio_construction": "optimizer",
                "optimizer_risk_aversion": 0.1,
                "optimizer_covariance_penalty": 2.0,
                "optimizer_covariance_fields": ["beta", "downside_beta", "size", "sector"],
                "optimizer_turnover_penalty": 0.0,
                "optimizer_iterations": 400,
                "optimizer_step_size": 0.05,
                "max_position_weight": 0.8,
            }
        )

        low_penalty_portfolio = low_covariance_penalty.build_weights(date(2025, 1, 31), rows)
        high_penalty_portfolio = high_covariance_penalty.build_weights(date(2025, 1, 31), rows)

        self.assertGreater(high_penalty_portfolio.weights.get("10003", 0.0), low_penalty_portfolio.weights.get("10003", 0.0))
        self.assertLess(high_penalty_portfolio.weights.get("10001", 0.0), low_penalty_portfolio.weights.get("10001", 0.0))
        self.assertLess(high_penalty_portfolio.weights.get("10002", 0.0), low_penalty_portfolio.weights.get("10002", 0.0))
        self.assertAlmostEqual(sum(high_penalty_portfolio.weights.values()), 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
