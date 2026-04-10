from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.pipeline import DataPipeline


class PipelineLiquidityTest(unittest.TestCase):
    def test_average_dollar_volume_uses_volume_when_available(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        pipeline.liquidity_lookback_days = 10
        rows = [
            {"date": date(2025, 1, 20), "prc": 10.0, "vol": 100.0},
            {"date": date(2025, 1, 21), "prc": 12.0, "vol": 200.0},
            {"date": date(2025, 1, 31), "prc": 11.0, "vol": 150.0},
        ]

        avg_dollar_volume = pipeline._average_dollar_volume(rows, date(2025, 1, 31))

        self.assertAlmostEqual(avg_dollar_volume or 0.0, 2400.0, places=8)


if __name__ == "__main__":
    unittest.main()
