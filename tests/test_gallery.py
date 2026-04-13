from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.gallery import StrategyGalleryBuilder
from quant_research.pipeline import PreparedData


class StrategyGalleryBuilderTest(unittest.TestCase):
    def test_gallery_builds_pages_for_multiple_presets(self) -> None:
        prepared = PreparedData(
            features_by_rebalance={
                date(2025, 1, 31): [
                    {"permno": "10001", "risk_adjusted_score": 2.0, "macro_score": 1.0, "vix": 20.0, "sector": "TECH", "beta": 1.0, "avg_dollar_volume": 100000.0},
                    {"permno": "10002", "risk_adjusted_score": 1.0, "macro_score": 1.0, "vix": 20.0, "sector": "HEALTH", "beta": 1.0, "avg_dollar_volume": 150000.0},
                ],
                date(2025, 2, 28): [
                    {"permno": "10001", "risk_adjusted_score": 0.5, "macro_score": 0.9, "vix": 24.0, "sector": "TECH", "beta": 1.0, "avg_dollar_volume": 100000.0},
                    {"permno": "10002", "risk_adjusted_score": 3.0, "macro_score": 0.9, "vix": 24.0, "sector": "HEALTH", "beta": 1.0, "avg_dollar_volume": 150000.0},
                ],
            },
            returns_by_date={
                date(2025, 2, 3): {"10001": 0.10, "10002": -0.02},
                date(2025, 2, 4): {"10001": -0.05, "10002": 0.01},
                date(2025, 3, 3): {"10001": 0.02, "10002": 0.04},
                date(2025, 3, 4): {"10001": -0.01, "10002": 0.03},
            },
            benchmark_by_date={
                date(2025, 2, 3): 0.0,
                date(2025, 2, 4): 0.0,
                date(2025, 3, 3): 0.0,
                date(2025, 3, 4): 0.0,
            },
        )

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            output_dir = tmp_path / "output"
            demo_dir = tmp_path / "docs" / "demo"
            config = Config(
                path=tmp_path / "config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "demo_site_dir": str(demo_dir),
                    },
                    "strategy": {
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
                        "gallery_presets": [
                            {
                                "slug": "balanced",
                                "title": "Balanced",
                                "description": "Base balanced preset.",
                                "overrides": {
                                    "portfolio_construction": "heuristic",
                                    "max_turnover_per_rebalance": 0.25,
                                },
                            },
                            {
                                "slug": "fast",
                                "title": "Fast",
                                "description": "Higher turnover preset.",
                                "overrides": {
                                    "portfolio_construction": "optimizer",
                                    "optimizer_turnover_penalty": 0.1,
                                    "max_turnover_per_rebalance": 0.4,
                                },
                            },
                        ],
                    },
                },
            )

            outputs, summary = StrategyGalleryBuilder(config, prepared).publish()
            gallery_html = (demo_dir / "gallery.html").read_text(encoding="utf-8")
            gallery_summary = json.loads((demo_dir / "gallery_summary.json").read_text(encoding="utf-8"))
            root_index_html = (demo_dir / "index.html").read_text(encoding="utf-8")
            self.assertTrue((demo_dir / "index.html").exists())
            self.assertTrue((demo_dir / "gallery" / "balanced" / "index.html").exists())
            self.assertTrue((demo_dir / "gallery" / "fast" / "index.html").exists())
            self.assertTrue((demo_dir / "gallery" / "balanced" / "preset_overrides.json").exists())
            self.assertTrue((demo_dir / "gallery" / "balanced" / "share_card.svg").exists())
            self.assertTrue((demo_dir / "gallery" / "balanced" / "share_card.html").exists())
            self.assertTrue((demo_dir / "latest_winner.json").exists())
            self.assertTrue((demo_dir / "latest_winner_badge.svg").exists())
            self.assertTrue((demo_dir / "latest_winner.md").exists())
            self.assertTrue((demo_dir / "latest_winner_readme_snippet.md").exists())
            self.assertTrue((demo_dir / "latest_winner_release_note.md").exists())
            self.assertTrue((demo_dir / "latest_winner_social_post.txt").exists())
            self.assertGreaterEqual(len(outputs), 10)
            self.assertEqual(summary["preset_count"], 2)
            self.assertIn("available_tags", summary)
            self.assertEqual(len(gallery_summary["presets"]), 2)
            self.assertIn("Strategy Gallery", gallery_html)
            self.assertIn("search-input", gallery_html)
            self.assertIn("sort-select", gallery_html)
            self.assertIn("Preset diff JSON", gallery_html)
            self.assertIn("Share SVG", gallery_html)
            self.assertIn("share_card_path", json.dumps(gallery_summary))
            self.assertIn("Preset Spotlight", root_index_html)
            self.assertIn("Open full strategy gallery", root_index_html)
            self.assertIn("Open latest winner badge", root_index_html)
            self.assertIn("Open README snippet", root_index_html)
            self.assertIn("Open release note", root_index_html)
            self.assertIn("Open social post", root_index_html)


if __name__ == "__main__":
    unittest.main()
