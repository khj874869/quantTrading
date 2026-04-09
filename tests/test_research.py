from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

from quant_research.config import Config
from quant_research.research import (
    _expand_strategy_grid,
    _resolve_walk_forward_windows,
    apply_recommended_config,
    load_recommended_strategy_overrides,
    run_parameter_sweep,
    run_walk_forward_optimization,
    write_applied_recommended_config,
)


class ResearchTest(unittest.TestCase):
    def test_expand_strategy_grid(self) -> None:
        combinations = _expand_strategy_grid(
            {
                "beta_method": ["ols", "ewma"],
                "risk_penalty_downside_beta_weight": [0.0, 0.2],
            }
        )
        self.assertEqual(len(combinations), 4)
        self.assertIn(
            {
                "beta_method": "ewma",
                "risk_penalty_downside_beta_weight": 0.2,
            },
            combinations,
        )

    def test_run_parameter_sweep_writes_summary(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "output"
            cache_dir = output_dir / "cache"
            config = Config(
                path=repo_root / "config" / "sample_config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "compustat_quarterly": str(repo_root / "data" / "sample_compustat_quarterly.csv"),
                        "crsp_daily": str(repo_root / "data" / "sample_crsp_daily.csv"),
                        "ccm_link": str(repo_root / "data" / "sample_ccm_link.csv"),
                        "ibes_link": str(repo_root / "data" / "sample_ibes_link.csv"),
                        "ibes_summary": str(repo_root / "data" / "sample_ibes_summary.csv"),
                        "ibes_surprise": str(repo_root / "data" / "sample_ibes_surprise.csv"),
                        "kpss_patent": str(repo_root / "data" / "sample_kpss_patent.csv"),
                        "ff_factors": str(repo_root / "data" / "sample_ff_factors.csv"),
                        "fred_dgs10": str(repo_root / "data" / "sample_fred_dgs10.csv"),
                        "cboe_vix": str(repo_root / "data" / "sample_cboe_vix.csv"),
                        "fmp_grades": str(repo_root / "data" / "sample_fmp_grades.csv"),
                    },
                    "cache": {
                        "enabled": True,
                        "cache_dir": str(cache_dir),
                    },
                    "sweep": {
                        "strategy_grid": {
                            "beta_method": ["ols", "ewma"],
                            "risk_penalty_downside_beta_weight": [0.0, 0.15],
                        }
                    },
                    "strategy": {
                        "rebalance_frequency": "monthly",
                        "holding_count": 1,
                        "score_field": "risk_adjusted_score",
                        "long_short": False,
                        "use_rdq": True,
                        "sector_neutral": True,
                        "beta_neutral": True,
                        "benchmark_hedge": True,
                        "constraint_neutral": False,
                        "constraint_neutral_factors": ["beta", "downside_beta", "idio_vol", "size", "sector"],
                        "constraint_ridge": 1e-08,
                        "beta_method": "ols",
                        "beta_lookback_days": 126,
                        "beta_ewma_halflife_days": 63,
                        "min_beta_observations": 60,
                        "beta_shrinkage": 0.0,
                        "beta_shrinkage_target": 1.0,
                        "risk_penalty_downside_beta_weight": 0.15,
                        "risk_penalty_idio_vol_weight": 0.10,
                        "regime_risk_scaling_enabled": True,
                        "regime_vix_threshold": 30.0,
                        "regime_vix_penalty_multiplier": 1.5,
                        "regime_macro_threshold": 0.5,
                        "regime_macro_penalty_multiplier": 1.25,
                        "regime_penalty_cap": 2.5,
                        "top_quantile": 0.5,
                        "bottom_quantile": 0.5,
                        "report_lag_days": 45,
                        "transaction_cost_bps": 10.0,
                        "vix_de_risk_level": 30.0,
                        "vix_flatten_level": 40.0,
                        "min_price": 5.0,
                        "min_market_cap": 100000000.0,
                        "start_date": "2024-01-01",
                        "end_date": "2025-12-31",
                    },
                },
            )

            summary_path = run_parameter_sweep(config)

            self.assertTrue(summary_path.exists())
            with summary_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 4)
            self.assertIn("information_ratio", rows[0])
            self.assertIn("beta_method", rows[0])

    def test_resolve_walk_forward_windows_from_months(self) -> None:
        config = Config(
            path=Path("config/sample_config.json").resolve(),
            raw={
                "walk_forward": {
                    "train_months": 12,
                    "test_months": 3,
                    "step_months": 3,
                },
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-06-30",
                },
            },
        )
        windows = _resolve_walk_forward_windows(config)
        self.assertEqual(len(windows), 2)
        self.assertEqual(
            windows[0],
            {
                "train_start": date(2024, 1, 1),
                "train_end": date(2024, 12, 31),
                "test_start": date(2025, 1, 1),
                "test_end": date(2025, 3, 31),
            },
        )
        self.assertEqual(
            windows[1],
            {
                "train_start": date(2024, 4, 1),
                "train_end": date(2025, 3, 31),
                "test_start": date(2025, 4, 1),
                "test_end": date(2025, 6, 30),
            },
        )

    def test_run_walk_forward_writes_summary(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "output"
            cache_dir = output_dir / "cache"
            config = Config(
                path=repo_root / "config" / "sample_config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "compustat_quarterly": str(repo_root / "data" / "sample_compustat_quarterly.csv"),
                        "crsp_daily": str(repo_root / "data" / "sample_crsp_daily.csv"),
                        "ccm_link": str(repo_root / "data" / "sample_ccm_link.csv"),
                        "ibes_link": str(repo_root / "data" / "sample_ibes_link.csv"),
                        "ibes_summary": str(repo_root / "data" / "sample_ibes_summary.csv"),
                        "ibes_surprise": str(repo_root / "data" / "sample_ibes_surprise.csv"),
                        "kpss_patent": str(repo_root / "data" / "sample_kpss_patent.csv"),
                        "ff_factors": str(repo_root / "data" / "sample_ff_factors.csv"),
                        "fred_dgs10": str(repo_root / "data" / "sample_fred_dgs10.csv"),
                        "cboe_vix": str(repo_root / "data" / "sample_cboe_vix.csv"),
                        "fmp_grades": str(repo_root / "data" / "sample_fmp_grades.csv"),
                    },
                    "cache": {
                        "enabled": True,
                        "cache_dir": str(cache_dir),
                    },
                    "sweep": {
                        "strategy_grid": {
                            "beta_method": ["ols", "ewma"],
                            "risk_penalty_downside_beta_weight": [0.0, 0.15],
                        }
                    },
                    "walk_forward": {
                        "selection_metric": "information_ratio",
                        "leaderboard_top_n": 2,
                        "leaderboard_sort_by": "consistency_score",
                        "leaderboard_min_selection_rate": 0.0,
                        "leaderboard_min_positive_window_rate": 0.0,
                        "windows": [
                            {
                                "train_start": "2024-01-01",
                                "train_end": "2025-03-31",
                                "test_start": "2025-04-01",
                                "test_end": "2025-04-30",
                            }
                        ],
                    },
                    "strategy": {
                        "rebalance_frequency": "monthly",
                        "holding_count": 1,
                        "score_field": "risk_adjusted_score",
                        "long_short": False,
                        "use_rdq": True,
                        "sector_neutral": True,
                        "beta_neutral": True,
                        "benchmark_hedge": True,
                        "constraint_neutral": False,
                        "constraint_neutral_factors": ["beta", "downside_beta", "idio_vol", "size", "sector"],
                        "constraint_ridge": 1e-08,
                        "beta_method": "ols",
                        "beta_lookback_days": 126,
                        "beta_ewma_halflife_days": 63,
                        "min_beta_observations": 60,
                        "beta_shrinkage": 0.0,
                        "beta_shrinkage_target": 1.0,
                        "risk_penalty_downside_beta_weight": 0.15,
                        "risk_penalty_idio_vol_weight": 0.10,
                        "regime_risk_scaling_enabled": True,
                        "regime_vix_threshold": 30.0,
                        "regime_vix_penalty_multiplier": 1.5,
                        "regime_macro_threshold": 0.5,
                        "regime_macro_penalty_multiplier": 1.25,
                        "regime_penalty_cap": 2.5,
                        "top_quantile": 0.5,
                        "bottom_quantile": 0.5,
                        "report_lag_days": 45,
                        "transaction_cost_bps": 10.0,
                        "vix_de_risk_level": 30.0,
                        "vix_flatten_level": 40.0,
                        "min_price": 5.0,
                        "min_market_cap": 100000000.0,
                        "start_date": "2024-01-01",
                        "end_date": "2025-12-31",
                    },
                },
            )

            summary_path = run_walk_forward_optimization(config)

            self.assertTrue(summary_path.exists())
            with summary_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertIn("window_id", rows[0])
            self.assertIn("selected_run_id", rows[0])
            self.assertIn("test_information_ratio", rows[0])
            self.assertTrue((summary_path.parent / "window_001" / "test_summary.json").exists())
            self.assertTrue((summary_path.parent / "selected_params_by_window.csv").exists())
            self.assertTrue((summary_path.parent / "selected_config_stability.csv").exists())
            self.assertTrue((summary_path.parent / "leaderboard.csv").exists())
            self.assertTrue((summary_path.parent / "recommended_config.json").exists())
            self.assertTrue((summary_path.parent / "oos_summary.json").exists())
            self.assertTrue((summary_path.parent / "oos_portfolio_daily_returns.csv").exists())
            oos_summary = summary_path.parent / "oos_summary.json"
            self.assertIn('"windows": 1.0', oos_summary.read_text(encoding="utf-8"))
            with (summary_path.parent / "selected_config_stability.csv").open("r", encoding="utf-8", newline="") as handle:
                stability_rows = list(csv.DictReader(handle))
            self.assertEqual(len(stability_rows), 1)
            self.assertEqual(stability_rows[0]["selected_windows"], "1.00000000")
            self.assertIn("median_test_information_ratio", stability_rows[0])
            self.assertIn("worst_test_information_ratio", stability_rows[0])
            self.assertIn("positive_window_rate", stability_rows[0])
            self.assertIn("consistency_score", stability_rows[0])
            with (summary_path.parent / "leaderboard.csv").open("r", encoding="utf-8", newline="") as handle:
                leaderboard_rows = list(csv.DictReader(handle))
            self.assertEqual(len(leaderboard_rows), 1)
            self.assertEqual(leaderboard_rows[0]["rank"], "1.00000000")
            self.assertEqual(leaderboard_rows[0]["leaderboard_sort_by"], "consistency_score")
            recommended_config = (summary_path.parent / "recommended_config.json").read_text(encoding="utf-8")
            self.assertIn('"strategy_overrides"', recommended_config)

    def test_apply_recommended_config_loads_strategy_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            recommendation_dir = Path(tmp) / "output" / "walk_forward"
            recommendation_dir.mkdir(parents=True, exist_ok=True)
            config_path.write_text(
                """
{
  "paths": {
    "output_dir": "output"
  },
  "strategy": {
    "beta_method": "ewma",
    "holding_count": 10
  }
}
                """.strip(),
                encoding="utf-8",
            )
            (recommendation_dir / "recommended_config.json").write_text(
                """
{
  "strategy_overrides": {
    "beta_method": "ols",
    "risk_penalty_idio_vol_weight": 0.2
  }
}
                """.strip(),
                encoding="utf-8",
            )
            config = Config.load(config_path)

            overrides = load_recommended_strategy_overrides(config)
            applied_config = apply_recommended_config(config)
            written_path = write_applied_recommended_config(config, applied_config)
            written_payload = written_path.read_text(encoding="utf-8")

            self.assertEqual(overrides["beta_method"], "ols")
            self.assertEqual(applied_config.strategy["beta_method"], "ols")
            self.assertEqual(applied_config.strategy["risk_penalty_idio_vol_weight"], 0.2)
            self.assertEqual(applied_config.strategy["holding_count"], 10)
            self.assertTrue(written_path.exists())
            self.assertIn('"beta_method": "ols"', written_payload)
            self.assertIn('"source_path"', written_payload)


if __name__ == "__main__":
    unittest.main()
