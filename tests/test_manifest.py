from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.main import main


class ManifestTest(unittest.TestCase):
    def test_backtest_writes_run_manifest(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            config_path = temp_root / "config.json"
            output_dir = temp_root / "output"
            cache_dir = output_dir / "cache"
            config_path.write_text(
                json.dumps(
                    {
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
                        "api": {
                            "fred_api_key": "fred-secret",
                            "fmp_api_key": "fmp-secret",
                        },
                        "cache": {
                            "enabled": True,
                            "cache_dir": str(cache_dir),
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
                            "commission_cost_bps": 2.0,
                            "slippage_cost_bps": 8.0,
                            "vix_de_risk_level": 30.0,
                            "vix_flatten_level": 40.0,
                            "min_price": 5.0,
                            "min_market_cap": 100000000.0,
                            "start_date": "2024-01-01",
                            "end_date": "2025-12-31",
                        },
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with patch.object(sys, "argv", ["run_quant.py", "backtest", "--config", str(config_path)]):
                with redirect_stdout(stdout):
                    main()

            manifest_path = output_dir / "run_manifest.json"
            self.assertTrue(manifest_path.exists())
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["requested_command"], "backtest")
        self.assertEqual(payload["executed_command"], "backtest")
        self.assertEqual(payload["command_options"]["config"], str(config_path))
        self.assertTrue(payload["cache"]["used"])
        self.assertIn("cache_hit", payload["cache"])
        self.assertEqual(payload["config"]["redacted"]["api"]["fred_api_key"], "***REDACTED***")
        self.assertEqual(payload["config"]["redacted"]["api"]["fmp_api_key"], "***REDACTED***")
        self.assertIn(str(output_dir / "portfolio_daily_returns.csv"), payload["outputs"])
        self.assertIn(str(output_dir / "summary.json"), payload["outputs"])
        self.assertIn("combined_metadata_fingerprint", payload["inputs"])
        self.assertIn("sharpe", payload["summary"])
        self.assertIn("commit", payload["git"])


if __name__ == "__main__":
    unittest.main()
