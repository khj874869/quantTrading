from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.publishing import DemoPublisher


class DemoPublisherTest(unittest.TestCase):
    def test_publish_demo_copies_outputs_and_builds_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            output_dir = tmp_path / "output"
            output_dir.mkdir(parents=True, exist_ok=True)
            demo_dir = tmp_path / "docs" / "demo"

            (output_dir / "report_dashboard.html").write_text("<html><body>dashboard</body></html>", encoding="utf-8")
            (output_dir / "report_summary.json").write_text(
                json.dumps(
                    {
                        "backtest_summary": {
                            "net_total_return": 0.12,
                            "sharpe": 1.8,
                            "max_drawdown": -0.07,
                        },
                        "largest_aum_without_breach": 1_000_000.0,
                        "best_month": {"month": "2025-02", "net_total_return": 0.05},
                        "worst_month": {"month": "2025-03", "net_total_return": -0.02},
                        "top_factors_by_ic": [{"factor": "risk_adjusted_score", "average_ic": 0.11}],
                        "top_securities": [{"permno": "10001", "total_contribution": 0.03}],
                        "top_sectors": [{"sector": "TECH", "total_contribution": 0.04}],
                    }
                ),
                encoding="utf-8",
            )
            (output_dir / "execution_summary.json").write_text(
                json.dumps({"execution_cost_bps_vs_filled_notional": 14.2}),
                encoding="utf-8",
            )
            (output_dir / "run_manifest.json").write_text(
                json.dumps({"generated_at": "2026-04-13T00:00:00Z"}),
                encoding="utf-8",
            )
            for name in [
                "report_monthly_returns.csv",
                "report_capacity_curve.csv",
                "report_factor_diagnostics.csv",
                "report_stress_scenarios.csv",
                "execution_reconciliation.csv",
                "order_blotter_latest.csv",
                "universe_snapshot.csv",
            ]:
                (output_dir / name).write_text("header\n", encoding="utf-8")

            config = Config(
                path=tmp_path / "config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "demo_site_dir": str(demo_dir),
                    }
                },
            )

            outputs, summary = DemoPublisher(config).publish()

            self.assertTrue((demo_dir / "index.html").exists())
            self.assertTrue((demo_dir / "bundle_summary.json").exists())
            self.assertTrue((demo_dir / "report_dashboard.html").exists())
            self.assertTrue((demo_dir / ".nojekyll").exists())
            self.assertGreaterEqual(len(outputs), 11)
            index_html = (demo_dir / "index.html").read_text(encoding="utf-8")
            bundle_summary = json.loads((demo_dir / "bundle_summary.json").read_text(encoding="utf-8"))

        self.assertIn("GitHub Pages Demo Bundle", index_html)
        self.assertIn("report_dashboard.html", index_html)
        self.assertEqual(summary["headline_metrics"]["largest_aum_without_breach"], 1_000_000.0)
        self.assertIn(".nojekyll", bundle_summary["files"])
        self.assertEqual(bundle_summary["top_factor"]["factor"], "risk_adjusted_score")
        self.assertEqual(bundle_summary["top_security"]["permno"], "10001")


if __name__ == "__main__":
    unittest.main()
